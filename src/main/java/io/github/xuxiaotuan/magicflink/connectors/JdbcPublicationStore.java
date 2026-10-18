package io.github.xuxiaotuan.magicflink.connectors;

import io.github.xuxiaotuan.magicflink.control.TransactionContracts.*;
import javax.sql.DataSource;
import java.sql.*;
import java.time.*;
import java.util.*;

/** Target-only publication transactions; requires platform-created tables and a primary database connection. */
public final class JdbcPublicationStore {
    private final JdbcTransactions tx;
    private final Clock clock;
    public JdbcPublicationStore(DataSource source,int queryTimeoutSeconds,Clock clock){tx=new JdbcTransactions(source,queryTimeoutSeconds);this.clock=Objects.requireNonNull(clock);}
    public long readRevision(BusinessKey key)throws SQLException{
        Objects.requireNonNull(key);return tx.run(c->{try(var p=tx.prepare(c,"SELECT revision FROM mf_publish_pointer WHERE business_key=?",key.digest());var r=p.executeQuery()){return r.next()?r.getLong(1):0L;}});
    }
    public PublicationReceipt publish(AuthorizedPublication authorization)throws SQLException{
        Objects.requireNonNull(authorization);var request=authorization.request();
        try{return tx.run(c->{
            var previous=history(c,request);if(previous.isPresent())return previous.get();
            tx.update(c,"INSERT INTO mf_publish_pointer(business_key,revision,active_load_id,publication_id) VALUES (?,0,NULL,NULL) ON CONFLICT DO NOTHING",request.key().digest());
            long revision;String oldLoad;
            try(var p=tx.prepare(c,"SELECT revision,active_load_id FROM mf_publish_pointer WHERE business_key=? FOR UPDATE",request.key().digest());var r=p.executeQuery()){
                if(!r.next())throw new SQLException("initialized pointer unavailable","MF002");revision=r.getLong(1);oldLoad=r.getString(2);
            }
            previous=history(c,request);if(previous.isPresent())return previous.get();
            try(var p=tx.prepare(c,"SELECT business_key,candidate_digest,state FROM mf_target_version WHERE load_id=? FOR UPDATE",request.loadId());var r=p.executeQuery()){
                if(!r.next()||!request.key().digest().equals(r.getString(1))||!request.candidateDigest().equals(r.getString(2))||!r.getString(3).equals("ACCEPTED"))throw new PublicationConflictException();
            }
            if(revision!=request.expectedRevision())throw new PublicationConflictException();
            long next=Math.addExact(revision,1);Instant recordedAt=clock.instant();
            tx.one(tx.update(c,"UPDATE mf_publish_pointer SET revision=?,active_load_id=?,publication_id=? WHERE business_key=? AND revision=?",next,request.loadId(),request.publicationId(),request.key().digest(),revision));
            tx.one(tx.update(c,"INSERT INTO mf_publish_history(publication_id,run_id,business_key,key_payload,load_id,candidate_digest,expected_revision,request_digest,previous_load_id,revision,recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    request.publicationId(),request.runId(),request.key().digest(),request.key().canonical(),request.loadId(),request.candidateDigest(),request.expectedRevision(),request.requestDigest(),oldLoad,next,recordedAt.toString()));
            return new PublicationReceipt(request,Optional.ofNullable(oldLoad),next,recordedAt);
        });}catch(SQLException e){
            // Different business keys can race using the same global ID. The losing transaction was rolled back.
            if("23505".equals(e.getSQLState())){var committed=tx.run(c->history(c,request));if(committed.isPresent())return committed.get();}
            throw e;
        }
    }
    /** History and current pointer are observed in one SQL snapshot; no history means UNKNOWN, not safe-to-replace. */
    public Optional<PublicationObservation> observe(AuthorizedPublication authorization)throws SQLException{
        Objects.requireNonNull(authorization);var expected=authorization.request();
        return tx.run(c->{try(var p=tx.prepare(c,"SELECT h.*,p.revision AS current_revision,p.publication_id AS current_publication FROM mf_publish_history h JOIN mf_publish_pointer p ON p.business_key=h.business_key WHERE h.publication_id=?",expected.publicationId());var r=p.executeQuery()){
            if(!r.next())return Optional.empty();var receipt=receipt(r,expected);
            return Optional.of(new PublicationObservation(receipt,r.getLong("current_revision")==receipt.revision()&&expected.publicationId().equals(r.getString("current_publication"))));
        }});
    }
    private Optional<PublicationReceipt> history(Connection c,PublicationRequest expected)throws SQLException{
        try(var p=tx.prepare(c,"SELECT * FROM mf_publish_history WHERE publication_id=?",expected.publicationId());var r=p.executeQuery()){return r.next()?Optional.of(receipt(r,expected)):Optional.empty();}
    }
    private PublicationReceipt receipt(ResultSet r,PublicationRequest expected)throws SQLException{
        var actual=request(r);if(!actual.equals(expected)||!actual.requestDigest().equals(expected.requestDigest()))throw new PublicationConflictException();
        return new PublicationReceipt(actual,Optional.ofNullable(r.getString("previous_load_id")),r.getLong("revision"),Instant.parse(r.getString("recorded_at")));
    }
    static PublicationRequest request(ResultSet r)throws SQLException{
        var request=new PublicationRequest(r.getString("publication_id"),r.getString("run_id"),BusinessKey.restore(r.getString("key_payload")),r.getString("load_id"),r.getString("candidate_digest"),r.getLong("expected_revision"));
        if(!request.requestDigest().equals(r.getString("request_digest")))throw new SQLException("stored publication digest mismatch","MF004");return request;
    }
}
