package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.connectors.JdbcPublicationStore;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import static io.github.xuxiaotuan.magicflink.protocol.JdbcTransactionTestSupport.*;
import static io.github.xuxiaotuan.magicflink.protocol.JdbcRunStoreTest.*;
import static org.junit.jupiter.api.Assertions.*;

class JdbcPublicationStoreTest {
    @TempDir Path directory;
    static AuthorizedPublication intent(String id,String load,long revision) {
        return new AuthorizedPublication(new PublicationRequest(id,"run1",KEY,load,FileStageProtocolTest.HASH,revision),CLOCK.instant());
    }
    static void version(javax.sql.DataSource ds,String load,String status) throws Exception {
        sql(ds,"INSERT INTO mf_target_version(load_id,business_key,candidate_digest,state) VALUES (?,?,?,?)",load,KEY.digest(),FileStageProtocolTest.HASH,status);
    }
    @Test void firstPublicationCreatesPointerAndHistoryAndHistoricalRetryNeverOverwritesNewerResult() throws Exception {
        var ds=database(directory.resolve("target"));schema(ds,"/db/publication.sql");version(ds,"load1","ACCEPTED");version(ds,"load2","ACCEPTED");
        var store=new JdbcPublicationStore(ds,3,CLOCK);assertEquals(0,store.readRevision(KEY));
        var first=store.publish(intent("pub1","load1",0));assertEquals(1,first.revision());assertEquals(Optional.empty(),first.previousLoadId());
        var second=store.publish(intent("pub2","load2",1));assertEquals(2,second.revision());assertEquals(Optional.of("load1"),second.previousLoadId());
        var reopened=new JdbcPublicationStore(database(directory.resolve("target")),3,CLOCK);
        assertEquals(first,reopened.publish(intent("pub1","load1",0)));assertFalse(reopened.observe(intent("pub1","load1",0)).orElseThrow().currentEffective());
        assertEquals(2,reopened.readRevision(KEY));assertEquals(2,count(ds,"mf_publish_history"));
    }
    @Test void twoFirstPublicationsCannotBothWin() throws Exception {
        var ds=database(directory.resolve("target"));schema(ds,"/db/publication.sql");version(ds,"load1","ACCEPTED");version(ds,"load2","ACCEPTED");
        var a=new JdbcPublicationStore(ds,3,CLOCK);var b=new JdbcPublicationStore(ds,3,CLOCK);
        var results=race(()->{try{a.publish(intent("pub1","load1",0));return "PUBLISHED";}catch(PublicationConflictException e){return "CONFLICT";}},
                ()->{try{b.publish(intent("pub2","load2",0));return "PUBLISHED";}catch(PublicationConflictException e){return "CONFLICT";}});
        assertEquals(1,results.stream().filter("PUBLISHED"::equals).count());assertEquals(1,results.stream().filter("CONFLICT"::equals).count());
        assertEquals(1,a.readRevision(KEY));assertEquals(1,count(ds,"mf_publish_history"));
    }
    @Test void concurrentDuplicatePublicationReturnsSameReceipt() throws Exception {
        var ds=database(directory.resolve("target"));schema(ds,"/db/publication.sql");version(ds,"load1","ACCEPTED");
        var a=new JdbcPublicationStore(ds,3,CLOCK);var b=new JdbcPublicationStore(ds,3,CLOCK);
        var results=race(()->a.publish(intent("pub1","load1",0)),()->b.publish(intent("pub1","load1",0)));
        assertEquals(results.get(0),results.get(1));assertEquals(1,count(ds,"mf_publish_history"));assertEquals(1,a.readRevision(KEY));
    }
    @Test void historyFailureRollsBackFirstPointerAndNoAcceptedVersionCannotPublish() throws Exception {
        var ds=database(directory.resolve("target"));schema(ds,"/db/publication.sql");version(ds,"load1","SEALED");var store=new JdbcPublicationStore(ds,3,CLOCK);
        assertThrows(PublicationConflictException.class,()->store.publish(intent("pub1","load1",0)));assertEquals(0,count(ds,"mf_publish_pointer"));
        sql(ds,"UPDATE mf_target_version SET state='ACCEPTED' WHERE load_id='load1'");
        sql(ds,"ALTER TABLE mf_publish_history ADD CONSTRAINT reject_history CHECK (revision < 0)");
        assertThrows(SQLException.class,()->store.publish(intent("pub1","load1",0)));assertEquals(0,count(ds,"mf_publish_pointer"));assertEquals(0,count(ds,"mf_publish_history"));
        sql(ds,"ALTER TABLE mf_publish_history DROP CONSTRAINT reject_history");assertEquals(1,store.publish(intent("pub1","load1",0)).revision());
    }
    @Test void lostResponseAfterTargetCommitIsRecoveredWithoutSecondPublication() throws Exception {
        var ds=database(directory.resolve("target"));schema(ds,"/db/publication.sql");version(ds,"load1","ACCEPTED");
        assertThrows(SQLException.class,()->new JdbcPublicationStore(failCommit(ds,true),3,CLOCK).publish(intent("pub1","load1",0)));
        var store=new JdbcPublicationStore(ds,3,CLOCK);assertTrue(store.observe(intent("pub1","load1",0)).isPresent());
        assertEquals(1,store.publish(intent("pub1","load1",0)).revision());assertEquals(1,count(ds,"mf_publish_history"));
    }
    @Test void samePublicationIdDifferentBodyConflictsAndQueryFailureIsNotRevisionZero() throws Exception {
        var ds=database(directory.resolve("target"));schema(ds,"/db/publication.sql");version(ds,"load1","ACCEPTED");version(ds,"load2","ACCEPTED");
        var store=new JdbcPublicationStore(ds,3,CLOCK);store.publish(intent("pub1","load1",0));
        assertThrows(PublicationConflictException.class,()->store.publish(intent("pub1","load2",1)));assertEquals(1,store.readRevision(KEY));
        sql(ds,"ALTER TABLE mf_publish_pointer RENAME TO unavailable_pointer");
        assertThrows(SQLException.class,()->store.readRevision(KEY));
    }
    @Test void globalPublicationIdCannotCommitForTwoDifferentBusinessKeys() throws Exception {
        var ds=database(directory.resolve("target"));schema(ds,"/db/publication.sql");version(ds,"load1","ACCEPTED");
        var otherKey=new BusinessKey("bank1","postgres_result",io.github.xuxiaotuan.magicflink.spec.JobSpec.ParameterType.DATE,java.time.LocalDate.of(2026,10,19));
        sql(ds,"INSERT INTO mf_target_version(load_id,business_key,candidate_digest,state) VALUES (?,?,?,'ACCEPTED')","load2",otherKey.digest(),FileStageProtocolTest.HASH);
        var other=new AuthorizedPublication(new PublicationRequest("pub1","run2",otherKey,"load2",FileStageProtocolTest.HASH,0),CLOCK.instant());
        var first=new JdbcPublicationStore(ds,3,CLOCK);var second=new JdbcPublicationStore(ds,3,CLOCK);
        var results=race(()->{try{first.publish(intent("pub1","load1",0));return true;}catch(PublicationConflictException e){return false;}},()->{try{second.publish(other);return true;}catch(PublicationConflictException e){return false;}});
        assertEquals(1,results.stream().filter(Boolean::booleanValue).count());assertEquals(1,count(ds,"mf_publish_pointer"));assertEquals(1,count(ds,"mf_publish_history"));
        assertEquals(1,first.readRevision(KEY)+first.readRevision(otherKey));
    }
    @Test void wrongVersionIdentityCannotPublishAndEmptyHistoryRemainsUnresolved() throws Exception {
        var ds=database(directory.resolve("target"));schema(ds,"/db/publication.sql");version(ds,"load1","ACCEPTED");
        var store=new JdbcPublicationStore(ds,3,CLOCK);assertTrue(store.observe(intent("pub1","load1",0)).isEmpty());
        var wrong=new AuthorizedPublication(new PublicationRequest("pub1","run1",KEY,"load1",FileStageProtocolTest.READ_HASH,0),CLOCK.instant());
        assertThrows(PublicationConflictException.class,()->store.publish(wrong));assertEquals(0,count(ds,"mf_publish_pointer"));assertEquals(0,count(ds,"mf_publish_history"));
    }

}
