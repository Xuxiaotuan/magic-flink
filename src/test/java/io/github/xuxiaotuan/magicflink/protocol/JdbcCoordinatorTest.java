package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.connectors.*;
import io.github.xuxiaotuan.magicflink.control.AttemptContracts.*;
import io.github.xuxiaotuan.magicflink.control.CoordinatorContracts.*;
import io.github.xuxiaotuan.magicflink.control.StageIdentity;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import javax.sql.DataSource;
import java.net.URI;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.*;
import java.util.*;
import static io.github.xuxiaotuan.magicflink.protocol.JdbcTransactionTestSupport.*;
import static io.github.xuxiaotuan.magicflink.protocol.FileStageProtocolTest.*;
import static org.junit.jupiter.api.Assertions.*;

class JdbcCoordinatorTest {
    static final ResolvedRun RUN=resolved();
    static final FileStageProtocol.Plan PLAN=plan(RUN);
    static final AttemptRef REF=new AttemptRef(PLAN.key(),PLAN.attemptId());
    @TempDir Path directory;
    DataSource ds;
    LocalImmutableObjectStore objects;
    void init()throws Exception {ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");objects=new LocalImmutableObjectStore(directory.resolve("objects"),RUN.catalog().runtime().storageScope());}
    JdbcCoordinator coordinator(long seconds){return coordinator(ds,seconds);}
    JdbcCoordinator coordinator(DataSource source,long seconds){return new JdbcCoordinator(source,3,Clock.fixed(START.plusSeconds(seconds),ZoneOffset.UTC),Duration.ofSeconds(30),objects);}
    JdbcRunStore runs(long seconds){return new JdbcRunStore(ds,3,Clock.fixed(START.plusSeconds(seconds),ZoneOffset.UTC));}
    BudgetPolicy policy(int apps,int queries){return new BudgetPolicy(2,apps,2,Map.of("postgres_local",new ConnectionBudget(queries+1,1)));}
    Lease admitted()throws Exception {var c=coordinator(0);assertEquals(Decision.APPLIED,c.admit(RUN,List.of(PLAN)));c.configure(policy(1,2));return c.acquire("run1","service1").orElseThrow();}
    StopEvidence stop(DispatchPermit permit,DispatchObservation dispatch,ConnectionObservation connections){return new StopEvidence(REF,permit.plan().planDigest(),Optional.empty(),Optional.of(permit.dispatchId()),dispatch,ExecutionObservation.NOT_CREATED,connections,RetryReason.NONE,START.plusSeconds(5),URI.create(PLAN.stagePrefix()+"stop.json"),HASH);}

    @Test void firstAdmissionIsAtomicUnboundAndIdempotent()throws Exception {
        init();var c=coordinator(0);assertEquals(Decision.APPLIED,c.admit(RUN,List.of(PLAN)));assertEquals(Decision.ALREADY_APPLIED,c.admit(RUN,List.of(PLAN)));
        var snap=runs(0).recover("run1");assertEquals(1,snap.current().size());var attempt=snap.current().get(0);
        assertEquals(ExecutionState.PLANNED,attempt.state());assertTrue(attempt.identity().isEmpty());assertEquals(DomainSnapshots.plan(PLAN).sha256(),attempt.plan().orElseThrow().planDigest());
        assertEquals(0,count(ds,"mf_reservation"));assertEquals(List.of("run1"),c.pendingRuns(10));
    }
    @Test void admissionCommitFailureRollsBackAndLostAckReusesOriginalPlans()throws Exception {
        init();assertThrows(SQLException.class,()->coordinator(failCommit(ds,false),0).admit(RUN,List.of(PLAN)));assertEquals(0,count(ds,"mf_run"));assertEquals(0,count(ds,"mf_attempt"));
        assertThrows(SQLException.class,()->coordinator(failCommit(ds,true),0).admit(RUN,List.of(PLAN)));
        assertEquals(Decision.ALREADY_APPLIED,coordinator(0).admit(RUN,List.of(PLAN)));assertEquals(1,count(ds,"mf_attempt"));
    }
    @Test void leaseTakeoverRestoresObservationWithoutNewDispatchOrQuota()throws Exception {
        init();var lease=admitted();var result=coordinator(0).dispatch(lease,0,REF);assertEquals(Decision.APPLIED,result.decision());var permit=result.permit().orElseThrow();
        assertTrue(coordinator(10).acquire("run1","service2").isEmpty());var successor=coordinator(31).acquire("run1","service2").orElseThrow();assertTrue(successor.epoch()>lease.epoch());
        var recovered=coordinator(31).recover(successor);assertEquals(WorkKind.OBSERVE,recovered.work().get(0).kind());assertEquals(permit.dispatchId(),recovered.work().get(0).dispatchId().orElseThrow());
        var duplicate=coordinator(31).dispatch(successor,1,REF);assertEquals(Decision.ALREADY_APPLIED,duplicate.decision());assertTrue(duplicate.permit().isEmpty());assertEquals(1,count(ds,"mf_reservation"));
        assertEquals(1,coordinator(31).usage().applications());assertThrows(SQLException.class,()->coordinator(31).dispatch(lease,1,REF));
    }
    @Test void resourceShortageDoesNotPartiallyReserveOrStartDispatch()throws Exception {
        init();var c=coordinator(0);c.admit(RUN,List.of(PLAN));c.configure(policy(1,1));var lease=c.acquire("run1","service1").orElseThrow();
        assertEquals(Decision.NOT_READY,c.dispatch(lease,0,REF).decision());assertEquals(0,count(ds,"mf_reservation"));assertEquals(0,count(ds,"mf_run_slot"));assertEquals(ExecutionState.PLANNED,runs(0).recover("run1").current().get(0).state());
    }
    @Test void unknownStopAndExpiredLeaseKeepQuotaUntilExplicitConfirmedRelease()throws Exception {
        init();var lease=admitted();var permit=coordinator(0).dispatch(lease,0,REF).permit().orElseThrow();
        assertEquals(Decision.WAIT_STOP,coordinator(5).release(lease,1,REF));assertEquals(Decision.WAIT_STOP,runs(5).recordStop(1,stop(permit,DispatchObservation.UNKNOWN,ConnectionObservation.CLOSED)));
        assertEquals(Decision.WAIT_STOP,coordinator(5).release(lease,2,REF));assertEquals(Decision.APPLIED,runs(5).recordStop(2,stop(permit,DispatchObservation.DRAINED,ConnectionObservation.CLOSED)));
        var next=coordinator(31).acquire("run1","service2").orElseThrow();assertEquals(1,coordinator(31).usage().applications());
        assertEquals(Decision.APPLIED,coordinator(31).release(next,3,REF));assertEquals(0,coordinator(31).usage().applications());assertEquals(1,coordinator(31).usage().activeRuns());
        assertEquals(Decision.ALREADY_APPLIED,coordinator(31).release(next,4,REF));assertEquals(Decision.ARCHIVE_ONLY,coordinator(31).dispatch(next,4,REF).decision());
    }
    @Test void legacyDispatchCannotBypassManagedResourceAdmission()throws Exception {
        init();admitted();assertEquals(Decision.NOT_READY,runs(0).beginDispatch(0,REF,"bypass"));assertEquals(0,count(ds,"mf_reservation"));
    }
    @Test void unknownBudgetAndCorruptOrMissingPlanNeverProducePermit()throws Exception {
        init();var c=coordinator(0);c.admit(RUN,List.of(PLAN));var lease=c.acquire("run1","service1").orElseThrow();
        assertEquals(Decision.NOT_READY,c.dispatch(lease,0,REF).decision());
        c.configure(policy(1,2));var objectPath=directory.resolve("objects/test-bucket/magic/runs/run1/inputs/orders/attempt1/plan.json");java.nio.file.Files.delete(objectPath);
        assertThrows(java.io.IOException.class,()->c.recover(lease));assertThrows(java.io.IOException.class,()->c.dispatch(lease,0,REF));assertEquals(0,count(ds,"mf_reservation"));
    }
    @Test void lostDispatchCommitAckNeverReturnsASecondPermit()throws Exception {
        init();var lease=admitted();assertThrows(SQLException.class,()->coordinator(failCommitOnSql(ds,true,"INSERT INTO mf_reservation"),0).dispatch(lease,0,REF));
        assertEquals(1,count(ds,"mf_reservation"));var again=coordinator(0).dispatch(lease,1,REF);assertEquals(Decision.ALREADY_APPLIED,again.decision());assertTrue(again.permit().isEmpty());
    }
    static ResolvedRun named(ResolvedRun run,String id){return new ResolvedRun(id,run.jobId(),run.jobVersion(),run.subject(),run.job(),run.inputVersions(),run.outputVersion(),run.catalog(),run.acceptedAt(),run.runDeadline());}
    static FileStageProtocol.Plan collectPlan(ResolvedRun run,String alias){
        var original=PLAN.inputs().get(0);var dataset=run.catalog().inputs().stream().filter(d->d.ref().equals(run.inputVersions().get(alias))).findFirst().orElseThrow();
        var input=new FileStageProtocol.InputSnapshot(alias,dataset.ref(),original.schema(),original.batchType(),original.batch(),original.sourceVersion(),original.readinessDigest(),original.projection(),DomainSnapshots.filter(dataset,original.batch()).sha256(),original.timezone(),original.coverage(),original.nullPartitionRequired(),original.expectedRows(),original.tokenExpiresAt(),Optional.empty());
        return new FileStageProtocol.Plan(new StageIdentity.StageKey(run.runId(),StageIdentity.StageType.COLLECT,Optional.of(alias)),"attempt1",PLAN.releaseId(),DomainSnapshots.catalog(run.catalog()).sha256(),DomainSnapshots.run(run).sha256(),DomainSnapshots.inputs(List.of(input)).sha256(),PLAN.runtime(),PLAN.outputSchema(),List.of(input),PLAN.expectedWriters(),List.of(new FileStageProtocol.Split("s0",alias,Optional.of(original.coverage()))),PLAN.plannedAt(),PLAN.deadline(),run.runDeadline(),PLAN.retainUntil());
    }
    @Test void fullDefinitionIsAvailableForRecoveryWithoutAnExternalRegistry()throws Exception {
        init();var lease=admitted();var key=URI.create(RUN.catalog().runtime().storageScope()+"runs/run1/job.json");
        assertTrue(objects.get(key,ObjectDigests.definition(RUN.job().spec()).sha256()).isPresent());
        java.nio.file.Files.delete(directory.resolve("objects/test-bucket/magic/runs/run1/job.json"));
        assertThrows(java.io.IOException.class,()->coordinator(0).recover(lease));
    }
    @Test void missingReservationAfterDispatchIsCorruptionInsteadOfFreeCapacity()throws Exception {
        init();var lease=admitted();coordinator(0).dispatch(lease,0,REF);sql(ds,"DELETE FROM mf_reservation");
        assertThrows(SQLException.class,()->coordinator(0).recover(lease));
    }
    @Test void completeInitialInputSetIsMandatory()throws Exception {
        init();var two=resolved(true);assertThrows(IllegalArgumentException.class,()->coordinator(0).admit(two,List.of(collectPlan(two,"orders"))));assertEquals(0,count(ds,"mf_run"));
    }
    @Test void physicalBudgetIsSharedAcrossRunsAndConnectionsCannotBePartiallyReserved()throws Exception {
        init();var run2=named(RUN,"run2");var plan2=collectPlan(run2,"orders");var c=coordinator(0);
        c.admit(RUN,List.of(PLAN));c.admit(run2,List.of(plan2));c.configure(policy(2,2));var one=c.acquire("run1","service1").orElseThrow();var two=c.acquire("run2","service2").orElseThrow();
        var results=race(()->coordinator(0).dispatch(one,0,REF).decision(),()->coordinator(0).dispatch(two,0,new AttemptRef(plan2.key(),plan2.attemptId())).decision());
        assertEquals(1,results.stream().filter(d->d==Decision.APPLIED).count());assertEquals(1,results.stream().filter(d->d==Decision.NOT_READY).count());assertEquals(1,count(ds,"mf_reservation"));assertEquals(2,c.usage().connections().get("postgres_local"));assertEquals(1,c.usage().applications());
    }
    @Test void connectionAliasesAndInputAliasesShareOnePhysicalBudget()throws Exception {
        init();var two=resolved(true);var orders=collectPlan(two,"orders");var customers=collectPlan(two,"customers");var c=coordinator(0);c.admit(two,List.of(orders,customers));c.configure(policy(2,2));var lease=c.acquire("run1","service1").orElseThrow();
        assertEquals(Decision.APPLIED,c.dispatch(lease,0,new AttemptRef(orders.key(),orders.attemptId())).decision());
        assertEquals(Decision.NOT_READY,c.dispatch(lease,1,new AttemptRef(customers.key(),customers.attemptId())).decision());assertEquals(2,c.usage().connections().get("postgres_local"));
    }
    @Test void dispatchAndReservationRollbackTogether()throws Exception {
        init();var lease=admitted();sql(ds,"ALTER TABLE mf_attempt ADD CONSTRAINT reject_dispatch CHECK (execution_state <> 'SUBMITTING')");
        assertThrows(SQLException.class,()->coordinator(0).dispatch(lease,0,REF));assertEquals(0,count(ds,"mf_reservation"));assertEquals(0,count(ds,"mf_run_slot"));assertEquals(0,runs(0).read("run1").revision());
    }
    @Test void renewalAndTakeoverCannotAuthorizeOldLease()throws Exception {
        init();var lease=admitted();var renewed=coordinator(10).renew(lease);assertEquals(lease.epoch(),renewed.epoch());assertEquals(START.plusSeconds(40),renewed.expiresAt());
        assertThrows(SQLException.class,()->coordinator(10).dispatch(lease,0,REF));assertTrue(coordinator(31).acquire("run1","service2").isEmpty());
        assertThrows(SQLException.class,()->coordinator(41).renew(renewed));var successor=coordinator(41).acquire("run1","service2").orElseThrow();assertEquals(renewed.epoch()+1,successor.epoch());
    }
    @Test void cancellationNeverReleasesUnknownResourcesAndRunSlot()throws Exception {
        init();var lease=admitted();var permit=coordinator(0).dispatch(lease,0,REF).permit().orElseThrow();runs(5).cancel("run1",1);
        assertEquals(Decision.WAIT_STOP,runs(5).finishCancellation("run1",2));assertEquals(Decision.NOT_READY,coordinator(5).releaseRun(lease,2));assertEquals(WorkKind.STOP,coordinator(5).recover(lease).work().get(0).kind());
        runs(5).recordStop(2,stop(permit,DispatchObservation.DRAINED,ConnectionObservation.CLOSED));assertEquals(Decision.APPLIED,runs(5).finishCancellation("run1",3));
        assertEquals(Decision.WAIT_STOP,coordinator(5).releaseRun(lease,4));assertEquals(Decision.APPLIED,coordinator(5).release(lease,4,REF));assertEquals(Decision.APPLIED,coordinator(5).releaseRun(lease,5));assertEquals(0,coordinator(5).usage().activeRuns());assertEquals(Decision.ALREADY_APPLIED,coordinator(5).releaseRun(lease,6));
    }
    @Test void cancellationAndDispatchAreSerialized()throws Exception {
        init();var lease=admitted();var results=race(()->coordinator(0).dispatch(lease,0,REF).decision(),()->runs(0).cancel("run1",0));
        var run=runs(0).read("run1");assertTrue(results.contains(Decision.REREAD)||results.contains(Decision.CANCEL_REQUESTED));
        if(run.cancelRequested()){assertEquals(0,count(ds,"mf_reservation"));assertEquals(ExecutionState.PLANNED,runs(0).recover("run1").current().get(0).state());}
        else {assertEquals(1,count(ds,"mf_reservation"));assertEquals(Decision.CANCEL_REQUESTED,runs(0).cancel("run1",run.revision()));}
    }
    @Test void policyIsExplicitImmutableAndProbeCapacityCannotBeBorrowed()throws Exception {
        init();var c=coordinator(0);c.configure(policy(1,1));c.configure(policy(1,1));assertThrows(SQLException.class,()->c.configure(policy(2,2)));
        c.admit(RUN,List.of(PLAN));var lease=c.acquire("run1","service1").orElseThrow();assertEquals(Decision.NOT_READY,c.dispatch(lease,0,REF).decision());assertEquals(0,c.usage().applications());
        assertThrows(IllegalArgumentException.class,()->new BudgetPolicy(0,1,1,Map.of()));
    }
    @Test void releaseCommitAcknowledgementCanBeRecoveredWithoutDoubleReturningQuota()throws Exception {
        init();var lease=admitted();var permit=coordinator(0).dispatch(lease,0,REF).permit().orElseThrow();runs(5).recordStop(1,stop(permit,DispatchObservation.DRAINED,ConnectionObservation.CLOSED));
        assertThrows(SQLException.class,()->coordinator(failCommitOnSql(ds,true,"UPDATE mf_reservation SET released"),5).release(lease,2,REF));assertEquals(0,coordinator(5).usage().applications());assertEquals(Decision.ALREADY_APPLIED,coordinator(5).release(lease,3,REF));
    }
    @Test void admissionWithSubstitutedSchemaOrFilterFailsBeforeDatabaseRegistration()throws Exception {
        init();var input=PLAN.inputs().get(0);var changed=new FileStageProtocol.InputSnapshot(input.alias(),input.dataset(),input.schema(),input.batchType(),input.batch(),input.sourceVersion(),input.readinessDigest(),input.projection(),HASH,input.timezone(),input.coverage(),input.nullPartitionRequired(),input.expectedRows(),input.tokenExpiresAt(),input.manifest());
        var bad=new FileStageProtocol.Plan(PLAN.key(),PLAN.attemptId(),PLAN.releaseId(),PLAN.catalogDigest(),PLAN.resolvedSpecDigest(),DomainSnapshots.inputs(List.of(changed)).sha256(),PLAN.runtime(),PLAN.outputSchema(),List.of(changed),PLAN.expectedWriters(),PLAN.splits(),PLAN.plannedAt(),PLAN.deadline(),PLAN.runDeadline(),PLAN.retainUntil());
        assertThrows(IllegalArgumentException.class,()->coordinator(0).admit(RUN,List.of(bad)));assertEquals(0,count(ds,"mf_attempt"));
    }

    @Test void missingReservationInAnotherRunCannotManufactureGlobalCapacity()throws Exception {
        init();var lease=admitted();coordinator(0).dispatch(lease,0,REF);sql(ds,"DELETE FROM mf_reservation");
        var run2=named(RUN,"run2");var plan2=collectPlan(run2,"orders");var c=coordinator(0);c.admit(run2,List.of(plan2));var lease2=c.acquire("run2","service2").orElseThrow();
        assertThrows(SQLException.class,()->c.dispatch(lease2,0,new AttemptRef(plan2.key(),plan2.attemptId())));
    }
    @Test void replacementObjectMustDescribeTheCurrentAttemptInsteadOfAnOldPlan()throws Exception {
        init();var lease=admitted();var permit=coordinator(0).dispatch(lease,0,REF).permit().orElseThrow();var proof=stop(permit,DispatchObservation.DRAINED,ConnectionObservation.CLOSED);
        var failed=new StopEvidence(proof.ref(),proof.planDigest(),proof.identity(),proof.dispatchId(),proof.dispatch(),proof.execution(),proof.connections(),RetryReason.TRANSIENT,proof.observedAt(),proof.objectKey(),proof.objectDigest());
        runs(5).recordStop(1,failed);var key=URI.create(PLAN.stagePrefix().toString().replace("attempt1/","attempt2/")+"plan.json");
        objects.putIfAbsent(key,DomainSnapshots.plan(PLAN));var next=new PlannedAttempt(new AttemptRef(PLAN.key(),"attempt2"),StageIdentity.Executor.FLINK,key,DomainSnapshots.plan(PLAN).sha256(),PLAN.resolvedSpecDigest(),PLAN.consumedDataDigest(),PLAN.deadline());
        assertEquals(Decision.APPLIED,runs(10).retryCurrent(2,REF,next));assertThrows(java.io.IOException.class,()->coordinator(10).recover(lease));
    }
    @Test void configuredPolicyCannotRaiseFrozenRuntimeOrConnectionCeilings()throws Exception {
        init();var c=coordinator(0);c.admit(RUN,List.of(PLAN));c.configure(new BudgetPolicy(3,3,3,Map.of("postgres_local",new ConnectionBudget(100,1))));var lease=c.acquire("run1","service1").orElseThrow();
        assertEquals(Decision.NOT_READY,c.dispatch(lease,0,REF).decision());assertEquals(0,count(ds,"mf_reservation"));
    }

    @Test void boundedCoordinatorStepResumesPersistedDispatchAsObserveOnly()throws Exception {
        init();var lease=admitted();var first=coordinator(0).step(lease);assertEquals(Decision.APPLIED,first.decision());assertTrue(first.permit().isPresent());
        var successor=coordinator(31).acquire("run1","service2").orElseThrow();var second=coordinator(31).step(successor);
        assertEquals(WorkKind.OBSERVE,second.work().orElseThrow().kind());assertTrue(second.permit().isEmpty());assertEquals(1,count(ds,"mf_reservation"));
    }
    @Test void boundedCoordinatorStepFinishesCancellationAndReleasesOnlyAfterStop()throws Exception {
        init();var lease=admitted();var permit=coordinator(0).dispatch(lease,0,REF).permit().orElseThrow();runs(5).cancel("run1",1);
        assertEquals(WorkKind.STOP,coordinator(5).step(lease).work().orElseThrow().kind());assertEquals(1,coordinator(5).usage().applications());
        runs(5).recordStop(2,stop(permit,DispatchObservation.DRAINED,ConnectionObservation.CLOSED));
        assertEquals(WorkKind.RELEASE_RESOURCES,coordinator(5).step(lease).work().orElseThrow().kind());
        assertEquals(WorkKind.FINISH_CANCELLATION,coordinator(5).step(lease).work().orElseThrow().kind());assertEquals(Phase.CANCELLED,runs(5).read("run1").phase());
        assertEquals(WorkKind.RELEASE_RUN,coordinator(5).step(lease).work().orElseThrow().kind());assertEquals(0,coordinator(5).usage().activeRuns());assertTrue(coordinator(5).step(lease).work().isEmpty());
    }

    @Test void immutableObjectConflictLeavesNoRunnableRegistration()throws Exception {
        init();objects.putIfAbsent(URI.create(PLAN.stagePrefix()+"plan.json"),DomainSnapshots.catalog(RUN.catalog()));
        assertThrows(java.io.IOException.class,()->coordinator(0).admit(RUN,List.of(PLAN)));assertEquals(0,count(ds,"mf_run"));assertEquals(0,count(ds,"mf_attempt"));
    }
    @Test void activeRunSlotRemainsOccupiedBetweenStagesEvenAfterStageQuotaIsReturned()throws Exception {
        init();var run2=named(RUN,"run2");var plan2=collectPlan(run2,"orders");var c=coordinator(0);c.admit(RUN,List.of(PLAN));c.admit(run2,List.of(plan2));c.configure(new BudgetPolicy(1,2,2,Map.of("postgres_local",new ConnectionBudget(5,1))));
        var one=c.acquire("run1","service1").orElseThrow();var two=c.acquire("run2","service2").orElseThrow();var permit=c.dispatch(one,0,REF).permit().orElseThrow();
        assertEquals(Decision.NOT_READY,c.dispatch(two,0,new AttemptRef(plan2.key(),plan2.attemptId())).decision());
        runs(5).recordStop(1,stop(permit,DispatchObservation.DRAINED,ConnectionObservation.CLOSED));coordinator(5).release(one,2,REF);
        assertEquals(0,c.usage().applications());assertEquals(1,c.usage().activeRuns());assertEquals(Decision.NOT_READY,c.dispatch(two,0,new AttemptRef(plan2.key(),plan2.attemptId())).decision());
    }

    @Test void newCatalogCannotRenameAnAlreadyRegisteredPhysicalEndpoint()throws Exception {
        init();var c=coordinator(0);c.admit(RUN,List.of(PLAN));
        var old=RUN.catalog();var connections=old.connections().stream().map(p->new PlatformDefinitions.ConnectionProfile(p.ref(),"renamed_physical",p.endpoint(),p.credentials(),p.tlsRequired(),p.limits(),p.resources(),p.grants())).toList();
        var catalog=new PlatformDefinitions.Catalog(connections,old.inputs(),old.output(),old.runtime());
        var other=new ResolvedRun("run2",RUN.jobId(),RUN.jobVersion(),RUN.subject(),RUN.job(),RUN.inputVersions(),RUN.outputVersion(),catalog,RUN.acceptedAt(),RUN.runDeadline());
        assertThrows(SQLException.class,()->c.admit(other,List.of(collectPlan(other,"orders"))));assertEquals(1,count(ds,"mf_run"));
    }

    @Test void retryPlanCannotChangeFrozenRuntimeResourcesEvenWithSameResolvedDigest()throws Exception {
        init();var lease=admitted();var permit=coordinator(0).dispatch(lease,0,REF).permit().orElseThrow();var proof=stop(permit,DispatchObservation.DRAINED,ConnectionObservation.CLOSED);
        runs(5).recordStop(1,new StopEvidence(proof.ref(),proof.planDigest(),proof.identity(),proof.dispatchId(),proof.dispatch(),proof.execution(),proof.connections(),RetryReason.TRANSIENT,proof.observedAt(),proof.objectKey(),proof.objectDigest()));
        var r=PLAN.runtime();var bigger=new PlatformDefinitions.RuntimeProfile(r.ref(),r.releaseId(),r.storageScope(),new PlatformDefinitions.ComputeResources(2048,4096,4,1,4000),r.concurrency(),r.inline(),r.time(),r.retry(),r.verifyBufferBytes(),r.retention());
        var changed=new FileStageProtocol.Plan(PLAN.key(),"attempt2",PLAN.releaseId(),PLAN.catalogDigest(),PLAN.resolvedSpecDigest(),PLAN.consumedDataDigest(),bigger,PLAN.outputSchema(),PLAN.inputs(),PLAN.expectedWriters(),PLAN.splits(),START.plusSeconds(10),PLAN.deadline(),PLAN.runDeadline(),PLAN.retainUntil());
        var key=URI.create(changed.stagePrefix()+"plan.json");objects.putIfAbsent(key,DomainSnapshots.plan(changed));
        var next=new PlannedAttempt(new AttemptRef(changed.key(),changed.attemptId()),StageIdentity.Executor.FLINK,key,DomainSnapshots.plan(changed).sha256(),changed.resolvedSpecDigest(),changed.consumedDataDigest(),changed.deadline());
        assertEquals(Decision.APPLIED,runs(10).retryCurrent(2,REF,next));assertThrows(java.io.IOException.class,()->coordinator(10).dispatch(lease,3,next.ref()));assertEquals(1,count(ds,"mf_reservation"));
    }

}
