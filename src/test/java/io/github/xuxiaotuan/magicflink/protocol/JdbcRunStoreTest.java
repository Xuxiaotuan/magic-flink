package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.connectors.JdbcRunStore;
import io.github.xuxiaotuan.magicflink.control.StageIdentity;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.*;
import java.util.*;
import static io.github.xuxiaotuan.magicflink.protocol.JdbcTransactionTestSupport.*;
import static io.github.xuxiaotuan.magicflink.protocol.FileStageProtocolTest.*;
import static org.junit.jupiter.api.Assertions.*;

class JdbcRunStoreTest {
    @TempDir Path directory;
    static final Clock CLOCK = Clock.fixed(START.plusSeconds(500), ZoneOffset.UTC);
    static final BusinessKey KEY = new BusinessKey("bank1", "postgres_result", io.github.xuxiaotuan.magicflink.spec.JobSpec.ParameterType.DATE, LocalDate.of(2026,10,18));
    static final PublicationBasis BASIS = new PublicationBasis(KEY, "load1", HASH, 0);
    static StageIdentity checkIdentity() {
        return new StageIdentity(new StageIdentity.StageKey("run1", StageIdentity.StageType.TARGET_CHECK, Optional.empty()), "check1", StageIdentity.Executor.INLINE,
                Optional.empty(), HASH, plan().resolvedSpecDigest(), READ_HASH);
    }
    static Registration readyRegistration() {
        return new Registration("run1", plan().resolvedSpecDigest(), Phase.RECONCILING_TARGET, List.of(checkIdentity()),
                Optional.of(BASIS), Map.of("row_compare", HASH, "version_identity", READ_HASH), START.plusSeconds(1800),PlatformDefinitionsTest.runtime().retry(),PlatformDefinitionsTest.runtime().time());
    }
    static CheckProof proof(String id, CheckStatus status) {
        return new CheckProof(checkIdentity(), id, id.equals("row_compare") ? HASH : READ_HASH, BASIS.candidateDigest(), BASIS.loadId(), status,
                java.net.URI.create("s3://test-bucket/magic/runs/run1/checks/" + id + ".json"), HASH);
    }
    static FileStageProtocol.Manifest manifest() {
        var p = plan(); var e = evidence(p);
        return new FileStageProtocol.Manifest("manifest1", FileStageProtocol.verify(resolved(),p,identity(p),e,accepted(p)), Optional.of(terminal(p)),
                List.of(new FileStageProtocol.VerifiedFile(e.files().get(0),2,HASH,ObjectDigests.schema(p.outputSchema()).sha256())));
    }
    @Test void sealsManifestStageAndUniqueNextIntentInOneTransactionAndReopens() throws Exception {
        var ds = database(directory.resolve("metadata")); schema(ds,"/db/metadata.sql"); var store = new JdbcRunStore(ds,3,CLOCK);
        store.create(new Registration("run1",plan().resolvedSpecDigest(),Phase.COLLECTING,List.of(identity(plan())),Optional.empty(),Map.of(),START.plusSeconds(1800),PlatformDefinitionsTest.runtime().retry(),PlatformDefinitionsTest.runtime().time()));
        assertEquals(Decision.APPLIED,store.sealFile(0,manifest()));
        var recovered = new JdbcRunStore(database(directory.resolve("metadata")),3,CLOCK);
        assertEquals(Phase.PRECHECKING,recovered.read("run1").phase());
        assertEquals(1,recovered.read("run1").revision());
        assertEquals(Decision.ALREADY_APPLIED,recovered.sealFile(1,manifest()));
        assertEquals(1,count(ds,"mf_manifest")); assertEquals(1,count(ds,"mf_next_intent"));
    }
    @Test void staleRevisionRequiresRereadAndCancellationArchivesLateManifest() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);
        store.create(new Registration("run1",plan().resolvedSpecDigest(),Phase.COLLECTING,List.of(identity(plan())),Optional.empty(),Map.of(),START.plusSeconds(1800),PlatformDefinitionsTest.runtime().retry(),PlatformDefinitionsTest.runtime().time()));
        assertEquals(Decision.REREAD,store.sealFile(2,manifest()));
        assertEquals(Decision.CANCEL_REQUESTED,store.cancel("run1",0));
        assertEquals(Decision.ARCHIVE_ONLY,store.sealFile(1,manifest()));
        assertEquals(Phase.CANCEL_REQUESTED,store.read("run1").phase());
        assertEquals(0,count(ds,"mf_manifest"));assertEquals(0,count(ds,"mf_next_intent"));assertTrue(count(ds,"mf_event")>=2);
    }
    @Test void failureCreatingNextIntentRollsBackSealAndStageCompletion() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);
        store.create(new Registration("run1",plan().resolvedSpecDigest(),Phase.COLLECTING,List.of(identity(plan())),Optional.empty(),Map.of(),START.plusSeconds(1800),PlatformDefinitionsTest.runtime().retry(),PlatformDefinitionsTest.runtime().time()));
        sql(ds,"ALTER TABLE mf_next_intent ADD CONSTRAINT reject_next CHECK (stage_type <> 'PRECHECK')");
        assertThrows(SQLException.class,()->store.sealFile(0,manifest()));
        assertEquals(0,store.read("run1").revision());assertEquals(Phase.COLLECTING,store.read("run1").phase());assertEquals(0,count(ds,"mf_manifest"));
        sql(ds,"ALTER TABLE mf_next_intent DROP CONSTRAINT reject_next");
        assertEquals(Decision.APPLIED,store.sealFile(0,manifest()));
    }
    @Test void authorizationNeedsEveryFrozenRulePassAndRejectsDifferentContext() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);store.create(readyRegistration());
        var current=checkIdentity();var old=new StageIdentity(current.stage(),"old_check",current.executor(),current.resource(),current.planDigest(),current.resolvedSpecDigest(),current.consumedDataDigest());
        assertEquals(Decision.ARCHIVE_ONLY,store.recordCheck(0,new CheckProof(old,"row_compare",HASH,HASH,"load1",CheckStatus.PASS,proof("row_compare",CheckStatus.PASS).objectKey(),HASH)));
        assertEquals(0,count(ds,"mf_check_result"));assertEquals(0,store.read("run1").revision());
        assertEquals(Decision.NOT_READY,store.authorize("run1",0,"pub1"));
        assertEquals(Decision.APPLIED,store.recordCheck(0,proof("row_compare",CheckStatus.PASS)));
        assertEquals(Decision.NOT_READY,store.authorize("run1",1,"pub1"));
        var bad=new CheckProof(checkIdentity(),"version_identity",READ_HASH,HASH,"another_load",CheckStatus.PASS,proof("version_identity",CheckStatus.PASS).objectKey(),HASH);
        assertEquals(Decision.CONFLICT,store.recordCheck(1,bad));
        assertEquals(Decision.APPLIED,store.recordCheck(1,proof("version_identity",CheckStatus.PASS)));
        assertEquals(Decision.APPLIED,store.authorize("run1",2,"pub1"));
        assertEquals("pub1",store.intent("run1").orElseThrow().request().publicationId());
        assertEquals(Phase.PUBLISHING,store.read("run1").phase());
        assertEquals(Decision.ALREADY_APPLIED,store.authorize("run1",3,"pub1"));
        assertEquals(Decision.CONFLICT,store.authorize("run1",3,"pub2"));assertEquals(1,count(ds,"mf_publication_intent"));
    }
    @Test void cancelBeforeAuthorizationBlocksItAndAuthorizationBeforeCancelIsTooLate() throws Exception {
        for(boolean cancelFirst:List.of(true,false)) {
            var ds=database(directory.resolve("metadata"+cancelFirst));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);store.create(readyRegistration());
            store.recordCheck(0,proof("row_compare",CheckStatus.PASS));store.recordCheck(1,proof("version_identity",CheckStatus.PASS));
            if(cancelFirst){assertEquals(Decision.CANCEL_REQUESTED,store.cancel("run1",2));assertEquals(Decision.ARCHIVE_ONLY,store.authorize("run1",3,"pub1"));assertTrue(store.intent("run1").isEmpty());}
            else {assertEquals(Decision.APPLIED,store.authorize("run1",2,"pub1"));assertEquals(Decision.CANCEL_TOO_LATE,store.cancel("run1",3));assertFalse(store.read("run1").cancelRequested());assertEquals(Phase.PUBLISHING,store.read("run1").phase());}
        }
    }
    @Test void twoConnectionsSerializeCancellationAgainstAuthorization() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var first=new JdbcRunStore(ds,3,CLOCK);first.create(readyRegistration());
        first.recordCheck(0,proof("row_compare",CheckStatus.PASS));first.recordCheck(1,proof("version_identity",CheckStatus.PASS));
        var second=new JdbcRunStore(ds,3,CLOCK);
        var outcomes=race(()->first.cancel("run1",2),()->second.authorize("run1",2,"pub1"));
        assertEquals(1,outcomes.stream().filter(d->d==Decision.REREAD).count());
        if(first.intent("run1").isPresent()) {assertEquals(Decision.CANCEL_TOO_LATE,first.cancel("run1",3));assertFalse(first.read("run1").cancelRequested());}
        else {assertEquals(Decision.ARCHIVE_ONLY,first.authorize("run1",3,"pub1"));assertTrue(first.read("run1").cancelRequested());}
    }
    @Test void lostAuthorizationAcknowledgementRecoversOriginalIntentAndStillRejectsCancel() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);store.create(readyRegistration());
        store.recordCheck(0,proof("row_compare",CheckStatus.PASS));store.recordCheck(1,proof("version_identity",CheckStatus.PASS));
        assertThrows(SQLException.class,()->new JdbcRunStore(failCommit(ds,true),3,CLOCK).authorize("run1",2,"pub1"));
        assertEquals("pub1",store.intent("run1").orElseThrow().request().publicationId());assertEquals(Decision.CANCEL_TOO_LATE,store.cancel("run1",3));
        assertEquals(Decision.ALREADY_APPLIED,store.authorize("run1",3,"pub1"));assertEquals(1,count(ds,"mf_publication_intent"));
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value=CheckStatus.class,names={"FAIL","ERROR","SKIPPED"})
    void anyNonPassCheckBlocksAuthorizationWithoutOverwritingEvidence(CheckStatus status) throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);store.create(readyRegistration());
        assertEquals(Decision.APPLIED,store.recordCheck(0,proof("row_compare",status)));
        assertEquals(Decision.APPLIED,store.recordCheck(1,proof("version_identity",CheckStatus.PASS)));
        assertEquals(Decision.NOT_READY,store.authorize("run1",2,"pub1"));
        assertEquals(Decision.CONFLICT,store.recordCheck(2,proof("row_compare",CheckStatus.PASS)));
        assertTrue(store.intent("run1").isEmpty());assertEquals(2,count(ds,"mf_check_result"));
    }
    @Test void expiredUnsealedFilesCannotCreateNextIntent() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");
        var store=new JdbcRunStore(ds,3,Clock.fixed(manifest().commit().plan().retainUntil().plusSeconds(1),ZoneOffset.UTC));
        store.create(new Registration("run1",plan().resolvedSpecDigest(),Phase.COLLECTING,List.of(identity(plan())),Optional.empty(),Map.of(),START.plusSeconds(1800),PlatformDefinitionsTest.runtime().retry(),PlatformDefinitionsTest.runtime().time()));
        assertEquals(Decision.NOT_READY,store.sealFile(0,manifest()));assertEquals(0,count(ds,"mf_manifest"));assertEquals(0,count(ds,"mf_next_intent"));
    }
    @Test void currentAttemptCanBeRecoveredWithAllExternalAndPlanIdentityFields() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);
        var identity=identity(plan());store.create(new Registration("run1",plan().resolvedSpecDigest(),Phase.COLLECTING,List.of(identity),Optional.empty(),Map.of(),START.plusSeconds(1800),PlatformDefinitionsTest.runtime().retry(),PlatformDefinitionsTest.runtime().time()));
        assertEquals(List.of(identity),new JdbcRunStore(database(directory.resolve("metadata")),3,CLOCK).currentStages("run1"));
        var changed=new StageIdentity(identity.stage(),"old_attempt",identity.executor(),identity.resource(),identity.planDigest(),identity.resolvedSpecDigest(),identity.consumedDataDigest());
        sql(ds,"UPDATE mf_stage SET identity_digest=? WHERE run_id='run1'",io.github.xuxiaotuan.magicflink.control.TransactionContracts.identityDigest(changed));
        assertThrows(SQLException.class,()->store.currentStages("run1"));
    }
    @Test void targetCommitLossThenMetadataCompletionPreservesOriginalAuthorization() throws Exception {
        var meta=database(directory.resolve("metadata"));schema(meta,"/db/metadata.sql");var store=new JdbcRunStore(meta,3,CLOCK);store.create(readyRegistration());
        store.recordCheck(0,proof("row_compare",CheckStatus.PASS));store.recordCheck(1,proof("version_identity",CheckStatus.PASS));store.authorize("run1",2,"pub1");
        var intent=store.intent("run1").orElseThrow();var target=database(directory.resolve("target"));schema(target,"/db/publication.sql");JdbcPublicationStoreTest.version(target,"load1","ACCEPTED");
        assertThrows(SQLException.class,()->new io.github.xuxiaotuan.magicflink.connectors.JdbcPublicationStore(failCommit(target,true),3,CLOCK).publish(intent));
        assertEquals(Decision.CANCEL_TOO_LATE,store.cancel("run1",3));
        var observed=new io.github.xuxiaotuan.magicflink.connectors.JdbcPublicationStore(target,3,CLOCK).observe(intent).orElseThrow();
        assertThrows(SQLException.class,()->new JdbcRunStore(failCommit(meta,true),3,CLOCK).confirmPublication("run1",3,observed));
        assertEquals(Phase.SUCCEEDED,store.read("run1").phase());assertEquals(Decision.ALREADY_APPLIED,store.confirmPublication("run1",4,observed));
        assertEquals(1,count(meta,"mf_publication_result"));assertEquals(1,count(target,"mf_publish_history"));
    }

    @Test void twoInputCompletionsUseRereadWithoutTreatingTheOtherInputAsStale() throws Exception {
        var run=resolved(true);var orders=collectManifest("orders",run);var customers=collectManifest("customers",run);
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);
        store.create(new Registration("run1",DomainSnapshots.run(run).sha256(),Phase.COLLECTING,List.of(orders.commit().evidence().identity(),customers.commit().evidence().identity()),Optional.empty(),Map.of(),START.plusSeconds(1800),PlatformDefinitionsTest.runtime().retry(),PlatformDefinitionsTest.runtime().time()));
        var outcomes=race(()->store.sealFile(0,orders),()->new JdbcRunStore(ds,3,CLOCK).sealFile(0,customers));
        assertEquals(1,outcomes.stream().filter(d->d==Decision.APPLIED).count());assertEquals(1,outcomes.stream().filter(d->d==Decision.REREAD).count());
        assertEquals(Phase.COLLECTING,store.read("run1").phase());assertEquals(0,count(ds,"mf_next_intent"));
        var pending=outcomes.get(0)==Decision.REREAD?orders:customers;assertEquals(Decision.APPLIED,store.sealFile(1,pending));
        assertEquals(Phase.PRECHECKING,store.read("run1").phase());assertEquals(2,count(ds,"mf_manifest"));assertEquals(1,count(ds,"mf_next_intent"));
    }
    static FileStageProtocol.Manifest collectManifest(String alias,ResolvedRun run) {
        var base=plan(run);var old=base.inputs().get(0);var definition=PlatformDefinitionsTest.dataset(alias,"source."+alias);
        var input=new FileStageProtocol.InputSnapshot(alias,definition.ref(),old.schema(),old.batchType(),old.batch(),old.sourceVersion(),old.readinessDigest(),old.projection(),DomainSnapshots.filter(definition,old.batch()).sha256(),old.timezone(),old.coverage(),old.nullPartitionRequired(),old.expectedRows(),old.tokenExpiresAt(),old.manifest());
        var p=new FileStageProtocol.Plan(new StageIdentity.StageKey("run1",StageIdentity.StageType.COLLECT,Optional.of(alias)),base.attemptId(),base.releaseId(),base.catalogDigest(),DomainSnapshots.run(run).sha256(),DomainSnapshots.inputs(List.of(input)).sha256(),base.runtime(),base.outputSchema(),List.of(input),base.expectedWriters(),base.splits().stream().map(split->new FileStageProtocol.Split(split.id(),alias,split.range())).toList(),base.plannedAt(),base.deadline(),base.runDeadline(),base.retainUntil());
        var e=evidence(p);return new FileStageProtocol.Manifest(alias+"_manifest",FileStageProtocol.verify(run,p,identity(p),e,accepted(p)),Optional.of(terminal(p)),List.of(new FileStageProtocol.VerifiedFile(e.files().get(0),2,HASH,ObjectDigests.schema(p.outputSchema()).sha256())));
    }
    @Test void authorizationDeadlineBlocksNewIntentButNeverRevokesAnExistingOne() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);store.create(readyRegistration());
        store.recordCheck(0,proof("row_compare",CheckStatus.PASS));store.recordCheck(1,proof("version_identity",CheckStatus.PASS));
        var expired=new JdbcRunStore(ds,3,Clock.fixed(START.plusSeconds(1800),ZoneOffset.UTC));
        assertEquals(Decision.NOT_READY,expired.authorize("run1",2,"pub1"));assertTrue(store.intent("run1").isEmpty());
        assertEquals(Decision.APPLIED,store.authorize("run1",2,"pub1"));
        assertEquals(Decision.ALREADY_APPLIED,expired.authorize("run1",3,"pub1"));assertEquals(Decision.CANCEL_TOO_LATE,expired.cancel("run1",3));
    }
    @Test void rejectedCommitBeforeAuthorizationLeavesNoIntentOrPublishingPhase() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);store.create(readyRegistration());
        store.recordCheck(0,proof("row_compare",CheckStatus.PASS));store.recordCheck(1,proof("version_identity",CheckStatus.PASS));
        assertThrows(SQLException.class,()->new JdbcRunStore(failCommit(ds,false),3,CLOCK).authorize("run1",2,"pub1"));
        assertTrue(store.intent("run1").isEmpty());assertEquals(Phase.RECONCILING_TARGET,store.read("run1").phase());
        assertEquals(Decision.CANCEL_REQUESTED,store.cancel("run1",2));assertEquals(Decision.ARCHIVE_ONLY,store.authorize("run1",3,"pub1"));
    }
    @Test void realDatabaseLockTimeoutDoesNotBecomeSuccessfulCancellation() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,1,CLOCK);store.create(readyRegistration());
        try(var lock=ds.getConnection()) {
            lock.setAutoCommit(false);try(var p=lock.prepareStatement("SELECT * FROM mf_run WHERE run_id='run1' FOR UPDATE");var r=p.executeQuery()){assertTrue(r.next());}
            assertThrows(SQLException.class,()->store.cancel("run1",0));lock.rollback();
        }
        assertFalse(store.read("run1").cancelRequested());assertEquals(0,store.read("run1").revision());assertTrue(store.intent("run1").isEmpty());
    }

    @Test void publicationIdCollisionAcrossRunsDoesNotPartiallyAuthorizeSecondRun() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);store.create(readyRegistration());
        store.recordCheck(0,proof("row_compare",CheckStatus.PASS));store.recordCheck(1,proof("version_identity",CheckStatus.PASS));store.authorize("run1",2,"pub1");
        var previous=checkIdentity();var other=new StageIdentity(new StageIdentity.StageKey("run2",StageIdentity.StageType.TARGET_CHECK,Optional.empty()),"check2",previous.executor(),previous.resource(),HASH,"c".repeat(64),READ_HASH);
        store.create(new Registration("run2",other.resolvedSpecDigest(),Phase.RECONCILING_TARGET,List.of(other),Optional.of(BASIS),Map.of("row_compare",HASH),START.plusSeconds(1800),PlatformDefinitionsTest.runtime().retry(),PlatformDefinitionsTest.runtime().time()));
        store.recordCheck(0,new CheckProof(other,"row_compare",HASH,HASH,"load1",CheckStatus.PASS,java.net.URI.create("s3://test-bucket/magic/runs/run2/checks/check2/report.json"),HASH));
        assertEquals(Decision.CONFLICT,store.authorize("run2",1,"pub1"));
        assertTrue(store.intent("run2").isEmpty());assertEquals(Phase.RECONCILING_TARGET,store.read("run2").phase());assertEquals(1,store.read("run2").revision());
        assertEquals(1,count(ds,"mf_publication_intent"));
    }
    @Test void historicalSuccessCanCompleteRunEvenWhenAnotherPublicationIsCurrent() throws Exception {
        var ds=database(directory.resolve("metadata"));schema(ds,"/db/metadata.sql");var store=new JdbcRunStore(ds,3,CLOCK);store.create(readyRegistration());
        store.recordCheck(0,proof("row_compare",CheckStatus.PASS));store.recordCheck(1,proof("version_identity",CheckStatus.PASS));store.authorize("run1",2,"pub1");
        var authorized=store.intent("run1").orElseThrow();var target=database(directory.resolve("target"));schema(target,"/db/publication.sql");
        JdbcPublicationStoreTest.version(target,"load1","ACCEPTED");JdbcPublicationStoreTest.version(target,"load2","ACCEPTED");
        var publisher=new io.github.xuxiaotuan.magicflink.connectors.JdbcPublicationStore(target,3,CLOCK);publisher.publish(authorized);publisher.publish(JdbcPublicationStoreTest.intent("pub2","load2",1));
        var observed=publisher.observe(authorized).orElseThrow();assertFalse(observed.currentEffective());
        assertEquals(Decision.APPLIED,store.confirmPublication("run1",3,observed));assertEquals(Phase.SUCCEEDED,store.read("run1").phase());
    }

}
