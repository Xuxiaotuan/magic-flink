package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.connectors.JdbcRunStore;
import io.github.xuxiaotuan.magicflink.control.AttemptContracts.*;
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
import static io.github.xuxiaotuan.magicflink.protocol.FileStageProtocolTest.*;
import static io.github.xuxiaotuan.magicflink.protocol.JdbcRunStoreTest.*;
import static io.github.xuxiaotuan.magicflink.protocol.JdbcTransactionTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

class JdbcAttemptRecoveryTest {
    private static final FileStageProtocol.Plan BASE_PLAN=FileStageProtocolTest.plan();
    private static final FileStageProtocol.Manifest BASE_MANIFEST=JdbcRunStoreTest.manifest();
    static FileStageProtocol.Plan plan(){return BASE_PLAN;}
    static FileStageProtocol.Manifest manifest(){return BASE_MANIFEST;}
    @TempDir Path directory;
    static final Instant NOW=CLOCK.instant();
    JdbcRunStore store(DataSource ds,long seconds){return new JdbcRunStore(ds,3,Clock.fixed(NOW.plusSeconds(seconds),ZoneOffset.UTC));}
    void collect(DataSource ds)throws Exception{
        schema(ds,"/db/metadata.sql");store(ds,0).create(new Registration("run1",plan().resolvedSpecDigest(),Phase.COLLECTING,List.of(identity(plan())),Optional.empty(),Map.of(),START.plusSeconds(1800),PlatformDefinitionsTest.runtime().retry(),PlatformDefinitionsTest.runtime().time()));
    }
    static AttemptRef ref(StageIdentity identity){return new AttemptRef(identity.stage(),identity.attemptId());}
    static PlannedAttempt replacement(StageIdentity old,String id){
        String path=old.stage().type()==StageIdentity.StageType.COLLECT?"inputs/"+old.stage().inputAlias().orElseThrow():"checks";
        return new PlannedAttempt(new AttemptRef(old.stage(),id),old.executor(),URI.create("s3://test-bucket/magic/runs/run1/"+path+"/"+id+"/plan.json"),READ_HASH,old.resolvedSpecDigest(),old.consumedDataDigest(),START.plusSeconds(1400));
    }
    static StopEvidence stopped(StageIdentity identity,DispatchObservation dispatch,ConnectionObservation connections,RetryReason reason){
        return new StopEvidence(ref(identity),identity.planDigest(),Optional.of(identity),Optional.empty(),dispatch,ExecutionObservation.FAILED,connections,reason,NOW,
                URI.create("s3://test-bucket/magic/runs/run1/stops/"+identity.attemptId()+".json"),HASH);
    }
    @Test void stopConfirmationBackoffAndHistoryProtectSwitchAndArchiveOldManifest()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var old=identity(plan());var next=replacement(old,"attempt2");var first=store(ds,0);
        assertEquals(Decision.WAIT_STOP,first.retryCurrent(0,ref(old),next));
        assertEquals(Decision.WAIT_STOP,first.recordStop(0,stopped(old,DispatchObservation.UNKNOWN,ConnectionObservation.CLOSED,RetryReason.TRANSIENT)));
        assertEquals(Decision.WAIT_STOP,first.retryCurrent(1,ref(old),next));
        assertEquals(Decision.APPLIED,first.recordStop(1,stopped(old,DispatchObservation.DRAINED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT)));
        assertEquals(Decision.NOT_READY,first.retryCurrent(2,ref(old),next));
        assertEquals(Decision.APPLIED,store(ds,5).retryCurrent(2,ref(old),next));
        var reopened=store(database(directory.resolve("metadata")),5);var history=reopened.attemptHistory(old.stage());
        assertEquals(2,history.size());assertEquals(Optional.of("attempt2"),history.get(0).replacedBy());assertEquals(ExecutionState.FAILED,history.get(0).state());
        assertEquals(ExecutionState.PLANNED,history.get(1).state());assertTrue(history.get(1).identity().isEmpty());
        assertThrows(SQLException.class,()->reopened.currentStages("run1"));
        assertEquals(Decision.ALREADY_APPLIED,reopened.retryCurrent(3,ref(old),next));
        assertEquals(Decision.ARCHIVE_ONLY,reopened.sealFile(3,manifest()));assertEquals(0,count(ds,"mf_manifest"));
        assertEquals("attempt2",reopened.recover("run1").current().get(0).ref().attemptId());
    }
    @Test void switchFailureRollsBackNewAttemptAndPointerTogether()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var old=identity(plan());var first=store(ds,0);
        first.recordStop(0,stopped(old,DispatchObservation.DRAINED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT));
        sql(ds,"ALTER TABLE mf_stage ADD CONSTRAINT reject_switch CHECK (current_attempt_id <> 'attempt2')");
        assertThrows(SQLException.class,()->store(ds,5).retryCurrent(1,ref(old),replacement(old,"attempt2")));
        assertEquals(1,count(ds,"mf_attempt"));assertEquals(Optional.empty(),first.attemptHistory(old.stage()).get(0).replacedBy());assertEquals(old,first.currentStages("run1").get(0));
    }
    @Test void concurrentSwitchesCannotConsumeTheSameRetryTwice()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var old=identity(plan());store(ds,0).recordStop(0,stopped(old,DispatchObservation.DRAINED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT));
        var results=race(()->store(ds,5).retryCurrent(1,ref(old),replacement(old,"attempt2")),()->store(ds,5).retryCurrent(1,ref(old),replacement(old,"attempt3")));
        assertEquals(1,results.stream().filter(d->d==Decision.APPLIED).count());assertEquals(1,results.stream().filter(d->d==Decision.REREAD).count());assertEquals(2,count(ds,"mf_attempt"));
    }
    @Test void nextIntentAndMaterializedPlanSurviveLostAcknowledgementWithoutRegeneratingIds()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);
        assertThrows(SQLException.class,()->new JdbcRunStore(failCommit(ds,true),3,CLOCK).sealFile(0,manifest()));
        var first=store(ds,0);var pending=first.recover("run1").intents().get(0);
        var next=new PlannedAttempt(new AttemptRef(pending.key(),"precheck1"),StageIdentity.Executor.FLINK,URI.create("s3://test-bucket/magic/runs/run1/checks/precheck1/plan.json"),HASH,plan().resolvedSpecDigest(),pending.consumedDigest(),START.plusSeconds(1400));
        assertThrows(SQLException.class,()->new JdbcRunStore(failCommit(ds,true),3,CLOCK).materializeNext(1,next));
        var recovered=first.recover("run1");assertEquals(Optional.of(next),recovered.current().stream().filter(a->a.ref().equals(next.ref())).findFirst().orElseThrow().plan());
        assertEquals(Optional.of("precheck1"),recovered.intents().get(0).firstAttemptId());assertEquals(1,count(ds,"mf_next_intent"));assertEquals(2,count(ds,"mf_attempt"));
        assertEquals(Decision.ALREADY_APPLIED,first.materializeNext(2,next));
        assertThrows(SQLException.class,()->new JdbcRunStore(failCommit(ds,true),3,CLOCK).beginDispatch(2,next.ref(),"dispatch1"));
        assertEquals(ExecutionState.SUBMITTING,first.recover("run1").current().stream().filter(a->a.ref().equals(next.ref())).findFirst().orElseThrow().state());
        assertEquals(Decision.ALREADY_APPLIED,first.beginDispatch(3,next.ref(),"dispatch1"));assertEquals(Decision.CONFLICT,first.beginDispatch(3,next.ref(),"dispatch2"));
    }
    @Test void cancelWaitsForDispatcherAndInFlightCreationEvenWhenNoResourceWasObserved()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var first=store(ds,0);first.sealFile(0,manifest());var intent=first.recover("run1").intents().get(0);
        var next=new PlannedAttempt(new AttemptRef(intent.key(),"precheck1"),StageIdentity.Executor.FLINK,URI.create("s3://test-bucket/magic/runs/run1/checks/precheck1/plan.json"),HASH,plan().resolvedSpecDigest(),intent.consumedDigest(),START.plusSeconds(1400));
        first.materializeNext(1,next);first.beginDispatch(2,next.ref(),"dispatch1");first.cancel("run1",3);
        var unknown=new StopEvidence(next.ref(),next.planDigest(),Optional.empty(),Optional.of("dispatch1"),DispatchObservation.UNKNOWN,ExecutionObservation.NOT_CREATED,ConnectionObservation.CLOSED,RetryReason.NONE,NOW,URI.create("s3://test-bucket/magic/runs/run1/stops/precheck1.json"),HASH);
        assertEquals(Decision.WAIT_STOP,first.recordStop(4,unknown));assertEquals(Decision.WAIT_STOP,first.finishCancellation("run1",5));
        var drained=new StopEvidence(unknown.ref(),unknown.planDigest(),unknown.identity(),unknown.dispatchId(),DispatchObservation.DRAINED,unknown.execution(),unknown.connections(),unknown.reason(),NOW,unknown.objectKey(),READ_HASH);
        assertEquals(Decision.APPLIED,first.recordStop(5,drained));assertEquals(Decision.WAIT_STOP,first.finishCancellation("run1",6));
        var source=identity(plan());var done=new StopEvidence(ref(source),source.planDigest(),Optional.of(source),Optional.empty(),DispatchObservation.DRAINED,ExecutionObservation.SUCCEEDED,ConnectionObservation.CLOSED,RetryReason.NONE,NOW,URI.create("s3://test-bucket/magic/runs/run1/stops/source.json"),HASH);
        assertEquals(Decision.APPLIED,first.recordStop(6,done));assertEquals(Decision.APPLIED,first.finishCancellation("run1",7));assertEquals(Phase.CANCELLED,first.read("run1").phase());
        assertEquals(Decision.ARCHIVE_ONLY,first.beginDispatch(8,next.ref(),"dispatch1"));
    }
    @Test void wrongConsumedDataAndMissingSealedPrerequisiteCannotBeRecoveredAsFreshWork()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var first=store(ds,0);first.sealFile(0,manifest());var pending=first.recover("run1").intents().get(0);
        var bad=new PlannedAttempt(new AttemptRef(pending.key(),"precheck1"),StageIdentity.Executor.INLINE,URI.create("s3://test-bucket/magic/runs/run1/checks/precheck1/plan.json"),HASH,plan().resolvedSpecDigest(),READ_HASH,START.plusSeconds(1400));
        assertEquals(Decision.CONFLICT,first.materializeNext(1,bad));assertEquals(1,count(ds,"mf_attempt"));
        sql(ds,"DELETE FROM mf_manifest");assertThrows(SQLException.class,()->first.recover("run1"));
    }
    @Test void stoppedDispatchCannotBeReopenedByReusingTheSameDispatchId()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var first=store(ds,0);first.sealFile(0,manifest());var pending=first.recover("run1").intents().get(0);
        var next=new PlannedAttempt(new AttemptRef(pending.key(),"precheck1"),StageIdentity.Executor.FLINK,URI.create("s3://test-bucket/magic/runs/run1/checks/precheck1/plan.json"),HASH,plan().resolvedSpecDigest(),pending.consumedDigest(),START.plusSeconds(1400));
        first.materializeNext(1,next);first.beginDispatch(2,next.ref(),"dispatch1");
        var proof=new StopEvidence(next.ref(),next.planDigest(),Optional.empty(),Optional.of("dispatch1"),DispatchObservation.DRAINED,ExecutionObservation.NOT_CREATED,ConnectionObservation.CLOSED,RetryReason.NONE,NOW,URI.create("s3://test-bucket/magic/runs/run1/stops/precheck1.json"),HASH);
        assertEquals(Decision.APPLIED,first.recordStop(3,proof));
        assertEquals(Decision.ARCHIVE_ONLY,first.beginDispatch(4,next.ref(),"dispatch1"));
    }
    @Test void replacementCannotBindToThePreviousAttemptsFlinkResource()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var old=identity(plan());store(ds,0).recordStop(0,stopped(old,DispatchObservation.DRAINED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT));
        var next=replacement(old,"attempt2");var first=store(ds,5);first.retryCurrent(1,ref(old),next);first.beginDispatch(2,next.ref(),"dispatch2");
        var recycled=new StageIdentity(next.ref().key(),next.ref().attemptId(),next.executor(),old.resource(),next.planDigest(),next.resolvedSpecDigest(),next.consumedDigest());
        assertEquals(Decision.CONFLICT,first.bindExecution(3,recycled,"dispatch2"));
        var fresh=new StageIdentity(recycled.stage(),recycled.attemptId(),recycled.executor(),Optional.of(new StageIdentity.FlinkResource("fresh_uid","fresh_job")),recycled.planDigest(),recycled.resolvedSpecDigest(),recycled.consumedDataDigest());
        assertEquals(Decision.APPLIED,first.bindExecution(3,fresh,"dispatch2"));assertEquals(Decision.ALREADY_APPLIED,first.bindExecution(4,fresh,"dispatch2"));
        assertEquals(Decision.CONFLICT,first.bindExecution(4,recycled,"dispatch2"));assertEquals(List.of(fresh),first.currentStages("run1"));
        var wrongStop=new StopEvidence(next.ref(),next.planDigest(),Optional.of(recycled),Optional.of("dispatch2"),DispatchObservation.DRAINED,ExecutionObservation.FAILED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT,NOW.plusSeconds(5),URI.create("s3://test-bucket/magic/runs/run1/stops/attempt2.json"),HASH);
        assertEquals(Decision.CONFLICT,first.recordStop(4,wrongStop));
    }
    @Test void failedCheckRetryKeepsOldReportAndNeedsFreshPassesForNewAttempt()throws Exception{
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var first=store(ds,0);first.create(readyRegistration());var old=checkIdentity();
        first.recordCheck(0,proof("row_compare",CheckStatus.ERROR));first.recordStop(1,stopped(old,DispatchObservation.DRAINED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT));
        var next=replacement(old,"check2");var later=store(ds,5);assertEquals(Decision.APPLIED,later.retryCurrent(2,ref(old),next));
        assertEquals(Decision.ARCHIVE_ONLY,later.recordCheck(3,proof("row_compare",CheckStatus.PASS)));assertEquals(Decision.NOT_READY,later.authorize("run1",3,"pub1"));
        assertEquals(Decision.APPLIED,later.beginDispatch(3,next.ref(),"query2"));var current=next.inlineIdentity().orElseThrow();
        assertEquals(Decision.APPLIED,later.recordCheck(4,new CheckProof(current,"row_compare",HASH,HASH,"load1",CheckStatus.PASS,URI.create("s3://test-bucket/magic/runs/run1/checks/check2/rows.json"),HASH)));
        assertEquals(Decision.APPLIED,later.recordCheck(5,new CheckProof(current,"version_identity",READ_HASH,HASH,"load1",CheckStatus.PASS,URI.create("s3://test-bucket/magic/runs/run1/checks/check2/version.json"),HASH)));
        assertEquals(Decision.APPLIED,later.authorize("run1",6,"pub1"));assertEquals(3,count(ds,"mf_check_result"));assertEquals(2,count(ds,"mf_attempt"));
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"PERMANENT","NO_BUDGET","DEADLINE","CHANGED_INPUT","CHANGED_SPEC","EXECUTOR"})
    void unsafeRetryDoesNotChangePointerOrAllocateAttempt(String scenario)throws Exception{
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var first=store(ds,0);var old=identity(plan());
        var policy=new PlatformDefinitions.RetryPolicy(scenario.equals("NO_BUDGET")?0:1,Duration.ofSeconds(5));
        first.create(new Registration("run1",old.resolvedSpecDigest(),Phase.COLLECTING,List.of(old),Optional.empty(),Map.of(),START.plusSeconds(1800),policy,PlatformDefinitionsTest.runtime().time()));
        first.recordStop(0,stopped(old,DispatchObservation.DRAINED,ConnectionObservation.CLOSED,scenario.equals("PERMANENT")?RetryReason.PERMANENT:RetryReason.TRANSIENT));
        var next=replacement(old,"attempt2");
        if(scenario.startsWith("CHANGED"))next=new PlannedAttempt(next.ref(),next.executor(),next.objectKey(),next.planDigest(),scenario.equals("CHANGED_SPEC")?HASH:next.resolvedSpecDigest(),scenario.equals("CHANGED_INPUT")?HASH:next.consumedDigest(),next.deadline());
        if(scenario.equals("EXECUTOR"))next=new PlannedAttempt(next.ref(),StageIdentity.Executor.INLINE,next.objectKey(),next.planDigest(),next.resolvedSpecDigest(),next.consumedDigest(),next.deadline());
        var later=store(ds,scenario.equals("DEADLINE")?1800:5);
        assertEquals(scenario.startsWith("CHANGED")?Decision.CONFLICT:Decision.NOT_READY,later.retryCurrent(1,ref(old),next));
        assertEquals(1,count(ds,"mf_attempt"));assertEquals(List.of(old),later.currentStages("run1"));
    }

    @Test void corruptDurableAttemptContextCannotBeRecoveredOrReadAsCurrent()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var first=store(ds,0);var old=identity(plan());
        var evidence=stopped(old,DispatchObservation.DRAINED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT);
        first.recordStop(0,evidence);
        sql(ds,"DELETE FROM mf_stop_evidence");assertThrows(SQLException.class,()->first.recover("run1"));
        sql(ds,"INSERT INTO mf_stop_evidence(content_digest,run_id,stage_type,input_alias,attempt_id,content_body) VALUES (?,?,?,?,?,?)",evidence.contentDigest(),"run1","COLLECT","orders",old.attemptId(),evidence.canonical());
        sql(ds,"UPDATE mf_run SET resolved_digest=? WHERE run_id='run1'",HASH);
        assertThrows(SQLException.class,()->first.currentStages("run1"));
    }
    @Test void definitivelyUncreatedRequestCanRetryOnlyAfterDispatcherIsDrained()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var first=store(ds,0);first.sealFile(0,manifest());var intent=first.recover("run1").intents().get(0);
        var old=new PlannedAttempt(new AttemptRef(intent.key(),"precheck1"),StageIdentity.Executor.FLINK,URI.create("s3://test-bucket/magic/runs/run1/checks/precheck1/plan.json"),HASH,plan().resolvedSpecDigest(),intent.consumedDigest(),START.plusSeconds(1400));
        first.materializeNext(1,old);first.beginDispatch(2,old.ref(),"dispatch1");
        var proof=new StopEvidence(old.ref(),old.planDigest(),Optional.empty(),Optional.of("dispatch1"),DispatchObservation.DRAINED,ExecutionObservation.NOT_CREATED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT,NOW,URI.create("s3://test-bucket/magic/runs/run1/stops/precheck1.json"),HASH);
        first.recordStop(3,proof);
        var next=new PlannedAttempt(new AttemptRef(intent.key(),"precheck2"),old.executor(),URI.create("s3://test-bucket/magic/runs/run1/checks/precheck2/plan.json"),READ_HASH,old.resolvedSpecDigest(),old.consumedDigest(),old.deadline());
        assertEquals(Decision.APPLIED,store(ds,5).retryCurrent(4,old.ref(),next));assertEquals(3,count(ds,"mf_attempt"));
        assertEquals(Optional.of("precheck1"),first.recover("run1").intents().get(0).firstAttemptId());
        var later=store(ds,5);later.beginDispatch(5,next.ref(),"dispatch2");
        later.recordStop(6,new StopEvidence(next.ref(),next.planDigest(),Optional.empty(),Optional.of("dispatch2"),DispatchObservation.DRAINED,ExecutionObservation.NOT_CREATED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT,NOW.plusSeconds(5),proof.objectKey(),READ_HASH));
        var third=new PlannedAttempt(new AttemptRef(intent.key(),"precheck3"),next.executor(),URI.create("s3://test-bucket/magic/runs/run1/checks/precheck3/plan.json"),HASH,next.resolvedSpecDigest(),next.consumedDigest(),next.deadline());
        assertEquals(Decision.NOT_READY,store(ds,10).retryCurrent(7,next.ref(),third));assertEquals(3,count(ds,"mf_attempt"));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value=ConnectionObservation.class,names={"OPEN","UNKNOWN"})
    void unclosedConnectionCannotBeUsedAsStopConfirmation(ConnectionObservation connections)throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var first=store(ds,0);var old=identity(plan());
        assertEquals(Decision.WAIT_STOP,first.recordStop(0,stopped(old,DispatchObservation.DRAINED,connections,RetryReason.TRANSIENT)));
        assertEquals(Decision.WAIT_STOP,store(ds,5).retryCurrent(1,ref(old),replacement(old,"attempt2")));
        assertTrue(first.recover("run1").current().get(0).stopDigest().isEmpty());assertEquals(1,count(ds,"mf_attempt"));
    }
    @Test void mandatoryCheckFailCannotBeRelabelledAsTransientAndRetried()throws Exception{
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var first=store(ds,0);first.create(readyRegistration());var old=checkIdentity();
        first.recordCheck(0,proof("row_compare",CheckStatus.FAIL));first.recordStop(1,stopped(old,DispatchObservation.DRAINED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT));
        assertEquals(Decision.NOT_READY,store(ds,5).retryCurrent(2,ref(old),replacement(old,"check2")));assertEquals(1,count(ds,"mf_attempt"));
    }
    @Test void inlineLimitMayCreateANewFlinkCheckAttemptWithoutChangingConsumedData()throws Exception{
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var first=store(ds,0);first.create(readyRegistration());var old=checkIdentity();
        first.recordCheck(0,proof("row_compare",CheckStatus.ERROR));first.recordStop(1,stopped(old,DispatchObservation.DRAINED,ConnectionObservation.CLOSED,RetryReason.INLINE_LIMIT));
        var original=replacement(old,"check2");var next=new PlannedAttempt(original.ref(),StageIdentity.Executor.FLINK,original.objectKey(),original.planDigest(),original.resolvedSpecDigest(),original.consumedDigest(),original.deadline());
        assertEquals(Decision.APPLIED,store(ds,5).retryCurrent(2,ref(old),next));assertTrue(first.recover("run1").current().get(0).identity().isEmpty());assertEquals(2,count(ds,"mf_attempt"));
    }

    @Test void lostSwitchAcknowledgementRecoversOneReplacementAndChargesBudgetOnce()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var old=identity(plan());store(ds,0).recordStop(0,stopped(old,DispatchObservation.DRAINED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT));var next=replacement(old,"attempt2");
        var laterClock=Clock.fixed(NOW.plusSeconds(5),ZoneOffset.UTC);
        assertThrows(SQLException.class,()->new JdbcRunStore(failCommit(ds,true),3,laterClock).retryCurrent(1,ref(old),next));
        var recovered=store(database(directory.resolve("metadata")),5);
        assertEquals(Decision.ALREADY_APPLIED,recovered.retryCurrent(2,ref(old),next));assertEquals(2,count(ds,"mf_attempt"));
        assertEquals("attempt2",recovered.recover("run1").current().get(0).ref().attemptId());
    }
    @Test void cancelAndMaterializeCannotCreateDispatchableWorkAfterCancellation()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var first=store(ds,0);first.sealFile(0,manifest());var intent=first.recover("run1").intents().get(0);
        var next=new PlannedAttempt(new AttemptRef(intent.key(),"precheck1"),StageIdentity.Executor.INLINE,URI.create("s3://test-bucket/magic/runs/run1/checks/precheck1/plan.json"),HASH,plan().resolvedSpecDigest(),intent.consumedDigest(),START.plusSeconds(1400));
        var results=race(()->first.cancel("run1",1),()->store(ds,0).materializeNext(1,next));
        assertEquals(1,results.stream().filter(d->d==Decision.REREAD).count());
        if(!first.read("run1").cancelRequested())first.cancel("run1",2);
        var revision=first.read("run1").revision();assertEquals(Decision.ARCHIVE_ONLY,first.materializeNext(revision,next));assertEquals(Decision.ARCHIVE_ONLY,first.beginDispatch(revision,next.ref(),"dispatch1"));
    }
    @Test void missingNextIntentAfterSealIsNotMistakenForNoRemainingWork()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var first=store(ds,0);first.sealFile(0,manifest());
        sql(ds,"DELETE FROM mf_next_intent");assertThrows(SQLException.class,()->first.recover("run1"));
    }

    @Test void missingCommitEvidenceMustWaitItsFrozenEvidenceWindowBeforeSwitching()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var old=identity(plan());var first=store(ds,0);
        var stopped=new StopEvidence(ref(old),old.planDigest(),Optional.of(old),Optional.empty(),DispatchObservation.DRAINED,ExecutionObservation.SUCCEEDED,ConnectionObservation.CLOSED,RetryReason.EVIDENCE_LOST,NOW,URI.create("s3://test-bucket/magic/runs/run1/stops/missing-index.json"),HASH);
        first.recordStop(0,stopped);var next=replacement(old,"attempt2");
        assertEquals(Decision.NOT_READY,store(ds,5).retryCurrent(1,ref(old),next));
        assertEquals(Decision.APPLIED,store(ds,120).retryCurrent(1,ref(old),next));assertEquals(0,count(ds,"mf_manifest"));
        assertFalse(first.attemptHistory(old.stage()).get(0).completed());
    }
    @Test void newAttemptDeadlineCannotExceedFrozenStageBudget()throws Exception{
        var ds=database(directory.resolve("metadata"));collect(ds);var old=identity(plan());store(ds,0).recordStop(0,stopped(old,DispatchObservation.DRAINED,ConnectionObservation.CLOSED,RetryReason.TRANSIENT));
        var original=replacement(old,"attempt2");var tooLong=new PlannedAttempt(original.ref(),original.executor(),original.objectKey(),original.planDigest(),original.resolvedSpecDigest(),original.consumedDigest(),START.plusSeconds(1701));
        assertEquals(Decision.NOT_READY,store(ds,5).retryCurrent(1,ref(old),tooLong));assertEquals(1,count(ds,"mf_attempt"));
    }

}
