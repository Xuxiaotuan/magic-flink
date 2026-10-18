package io.github.xuxiaotuan.magicflink.connectors;

import io.github.xuxiaotuan.magicflink.control.StageIdentity;
import io.github.xuxiaotuan.magicflink.control.AttemptContracts;
import io.github.xuxiaotuan.magicflink.control.AttemptContracts.*;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts.*;
import io.github.xuxiaotuan.magicflink.protocol.DomainSnapshots;
import io.github.xuxiaotuan.magicflink.protocol.FileStageProtocol;
import javax.sql.DataSource;
import java.sql.*;
import java.time.*;
import java.util.*;

/** Metadata transactions only; it never invokes Flink, object storage or the target database. */
public final class JdbcRunStore {
    private final JdbcTransactions tx;
    private final Clock clock;
    private final JdbcAttempts attempts;
    public JdbcRunStore(DataSource source,int queryTimeoutSeconds,Clock clock){tx=new JdbcTransactions(source,queryTimeoutSeconds);this.clock=Objects.requireNonNull(clock);attempts=new JdbcAttempts(tx,clock,this);}
    /** Trusted platform registration, not a public API for claiming earlier phases were executed. */
    public void create(Registration registration)throws SQLException{
        Objects.requireNonNull(registration);
        tx.run(c->{
            var b=registration.publication();
            int inserted=tx.update(c,"INSERT INTO mf_run(run_id,registration_digest,resolved_digest,phase,revision,cancel_requested,deadline,key_payload,load_id,candidate_digest,expected_revision,max_stage_retries,retry_backoff,stage_timeout,evidence_wait) VALUES (?,?,?,?,0,FALSE,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    registration.runId(),registration.contentDigest(),registration.resolvedSpecDigest(),registration.phase().name(),registration.deadline().toString(),
                    b.map(v->v.key().canonical()).orElse(null),b.map(PublicationBasis::loadId).orElse(null),b.map(PublicationBasis::candidateDigest).orElse(null),b.map(PublicationBasis::expectedRevision).orElse(null),registration.retryPolicy().maxStageRetries(),registration.retryPolicy().backoff().toString(),registration.timeLimits().stage().toString(),registration.timeLimits().evidenceWait().toString());
            var current=locked(c,registration.runId());
            if(!current.registrationDigest().equals(registration.contentDigest()))throw new SQLException("run registration conflict","MF001");
            if(inserted==1){
                for(var stage:registration.stages())attempts.register(c,registration,stage);
                for(var rule:registration.requiredChecks().entrySet())tx.one(tx.update(c,"INSERT INTO mf_required_check(run_id,check_id,rule_digest) VALUES (?,?,?)",registration.runId(),rule.getKey(),rule.getValue()));
            }
            return null;
        });
    }
    public RunView read(String runId)throws SQLException{TransactionContracts.id(runId);return tx.run(c->locked(c,runId).view());}
    public Optional<AuthorizedPublication> intent(String runId)throws SQLException{TransactionContracts.id(runId);return tx.run(c->intent(c,runId));}

    public List<StageIdentity> currentStages(String runId)throws SQLException{TransactionContracts.id(runId);return tx.run(c->{locked(c,runId);return attempts.currentBound(c,runId);});}
    public List<AttemptView> attemptHistory(StageIdentity.StageKey key)throws SQLException{return attempts.history(key);}
    public RecoverySnapshot recover(String runId)throws SQLException{return attempts.recover(runId);}
    public Decision recordStop(long revision,StopEvidence evidence)throws SQLException{return attempts.stop(revision,evidence);}
    public Decision retryCurrent(long revision,AttemptRef previous,PlannedAttempt replacement)throws SQLException{return attempts.retry(revision,previous,replacement);}
    public Decision materializeNext(long revision,PlannedAttempt next)throws SQLException{return attempts.materialize(revision,next);}
    public Decision beginDispatch(long revision,AttemptRef ref,String dispatchId)throws SQLException{return attempts.begin(revision,ref,dispatchId);}
    public Decision bindExecution(long revision,StageIdentity identity,String dispatchId)throws SQLException{return attempts.bind(revision,identity,dispatchId);}
    public Decision finishCancellation(String runId,long revision)throws SQLException{return attempts.finishCancellation(runId,revision);}
    public Decision confirmPublication(String runId,long observedRevision,PublicationObservation observation)throws SQLException{
        TransactionContracts.id(runId);Objects.requireNonNull(observation);
        return tx.run(c->{
            var run=locked(c,runId);if(run.revision()!=observedRevision)return Decision.REREAD;
            var authorization=intent(c,runId);
            if(authorization.isEmpty()||!authorization.get().request().equals(observation.receipt().request()))return Decision.CONFLICT;
            String receiptDigest=TransactionContracts.receiptDigest(observation.receipt());
            try(var p=tx.prepare(c,"SELECT receipt_digest FROM mf_publication_result WHERE run_id=?",runId);var r=p.executeQuery()){
                if(r.next()){
                    if(run.phase()!=Phase.SUCCEEDED)throw new SQLException("stored publication state mismatch","MF004");
                    return receiptDigest.equals(r.getString(1))?Decision.ALREADY_APPLIED:Decision.CONFLICT;
                }
            }
            if(run.cancelRequested()||run.phase()!=Phase.PUBLISHING)return Decision.ARCHIVE_ONLY;
            var receipt=observation.receipt();
            tx.one(tx.update(c,"INSERT INTO mf_publication_result(run_id,publication_id,request_digest,receipt_digest,previous_load_id,revision,recorded_at) VALUES (?,?,?,?,?,?,?)",
                    runId,receipt.request().publicationId(),receipt.request().requestDigest(),receiptDigest,receipt.previousLoadId().orElse(null),receipt.revision(),receipt.recordedAt().toString()));
            advance(c,run,Phase.SUCCEEDED,false);event(c,runId,observation.currentEffective()?"PUBLICATION_CONFIRMED_CURRENT":"PUBLICATION_CONFIRMED_HISTORICAL",receiptDigest);return Decision.APPLIED;
        });
    }
    public Decision sealFile(long observedRevision,FileStageProtocol.Manifest manifest)throws SQLException{
        Objects.requireNonNull(manifest);var identity=manifest.commit().evidence().identity();String content=DomainSnapshots.manifest(manifest).sha256();
        return tx.run(c->{
            var run=locked(c,identity.stage().runId());
            if(run.revision()!=observedRevision)return Decision.REREAD;
            var stage=stage(c,identity);
            if(run.cancelRequested()||!matches(run,stage,identity)){event(c,run.runId(),"LATE_MANIFEST",content);return Decision.ARCHIVE_ONLY;}
            try(var p=tx.prepare(c,"SELECT manifest_id,content_digest,identity_digest FROM mf_manifest WHERE run_id=? AND stage_type=? AND input_alias=?",run.runId(),identity.stage().type().name(),identity.stage().inputAlias().orElse(""));var r=p.executeQuery()){
                if(r.next())return stage.completed()&&r.getString(1).equals(manifest.id())&&r.getString(2).equals(content)&&r.getString(3).equals(TransactionContracts.identityDigest(identity))?Decision.ALREADY_APPLIED:Decision.CONFLICT;
            }
            if(stage.completed()||run.phase()!=TransactionContracts.phaseFor(identity.stage().type()))return Decision.ARCHIVE_ONLY;
            if(!clock.instant().isBefore(manifest.commit().plan().retainUntil()))return Decision.NOT_READY;
            tx.one(tx.update(c,"INSERT INTO mf_manifest(manifest_id,run_id,stage_type,input_alias,identity_digest,object_key,content_digest,state,sealed_at) VALUES (?,?,?,?,?,?,?,'SEALED',?)",
                    manifest.id(),run.runId(),identity.stage().type().name(),identity.stage().inputAlias().orElse(""),TransactionContracts.identityDigest(identity),manifest.commit().plan().stagePrefix()+"manifest.json",content,clock.instant().toString()));
            complete(c,identity);
            Phase next=run.phase();
            try(var p=tx.prepare(c,"SELECT COUNT(*) FROM mf_stage WHERE run_id=? AND stage_type=? AND state<>'COMPLETED'",run.runId(),identity.stage().type().name());var r=p.executeQuery()){
                r.next();if(r.getLong(1)==0){
                    var nextType=identity.stage().type()==StageIdentity.StageType.COLLECT?StageIdentity.StageType.PRECHECK:StageIdentity.StageType.RESULT_CHECK;
                    next=TransactionContracts.phaseFor(nextType);
                    List<Map<String,Object>> manifests=new ArrayList<>();
                    try(var q=tx.prepare(c,"SELECT input_alias,manifest_id,content_digest FROM mf_manifest WHERE run_id=? AND stage_type=? ORDER BY input_alias",run.runId(),identity.stage().type().name());var files=q.executeQuery()){
                        while(files.next())manifests.add(Map.of("alias",files.getString(1),"id",files.getString(2),"digest",files.getString(3)));
                    }
                    tx.one(tx.update(c,"INSERT INTO mf_next_intent(run_id,stage_type,input_alias,consumed_digest,consumed_body,cancelled) VALUES (?,?,'',?,?,FALSE)",run.runId(),nextType.name(),TransactionContracts.hash("SEALED_INPUTS",Map.of("manifests",manifests)),AttemptContracts.encode(Map.of("manifests",manifests))));
                }
            }
            advance(c,run,next,false);event(c,run.runId(),"MANIFEST_SEALED",content);return Decision.APPLIED;
        });
    }
    public Decision recordCheck(long observedRevision,CheckProof proof)throws SQLException{
        Objects.requireNonNull(proof);return tx.run(c->{
            var run=locked(c,proof.identity().stage().runId());
            if(run.revision()!=observedRevision)return Decision.REREAD;
            var stage=stage(c,proof.identity());
            if(run.cancelRequested()||!matches(run,stage,proof.identity())||run.phase()!=Phase.RECONCILING_TARGET){event(c,run.runId(),"LATE_CHECK",proof.contentDigest());return Decision.ARCHIVE_ONLY;}
            if(proof.identity().stage().type()!=StageIdentity.StageType.TARGET_CHECK||run.basis().isEmpty())return Decision.CONFLICT;
            var b=run.basis().get();String rule;
            try(var p=tx.prepare(c,"SELECT rule_digest FROM mf_required_check WHERE run_id=? AND check_id=?",run.runId(),proof.checkId());var r=p.executeQuery()){rule=r.next()?r.getString(1):null;}
            if(!proof.ruleDigest().equals(rule)||!b.loadId().equals(proof.loadId())||!b.candidateDigest().equals(proof.candidateDigest()))return Decision.CONFLICT;
            String identity=TransactionContracts.identityDigest(proof.identity());
            try(var p=tx.prepare(c,"SELECT content_digest FROM mf_check_result WHERE run_id=? AND identity_digest=? AND check_id=?",run.runId(),identity,proof.checkId());var r=p.executeQuery()){
                if(r.next())return proof.contentDigest().equals(r.getString(1))?Decision.ALREADY_APPLIED:Decision.CONFLICT;
            }
            if(stage.completed())return Decision.CONFLICT;
            tx.one(tx.update(c,"INSERT INTO mf_check_result(run_id,identity_digest,check_id,rule_digest,candidate_digest,load_id,status,content_digest,object_key,object_digest) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    run.runId(),identity,proof.checkId(),proof.ruleDigest(),proof.candidateDigest(),proof.loadId(),proof.status().name(),proof.contentDigest(),proof.objectKey().toString(),proof.objectDigest()));
            if(allPassed(c,run,identity))complete(c,proof.identity());
            advance(c,run,run.phase(),false);event(c,run.runId(),"CHECK_RECORDED",proof.contentDigest());return Decision.APPLIED;
        });
    }
    public Decision authorize(String runId,long observedRevision,String publicationId)throws SQLException{
        TransactionContracts.id(runId);TransactionContracts.id(publicationId);
        try{return tx.run(c->{
            var run=locked(c,runId);if(run.revision()!=observedRevision)return Decision.REREAD;
            var existing=intent(c,runId);
            if(existing.isPresent())return existing.get().request().publicationId().equals(publicationId)?Decision.ALREADY_APPLIED:Decision.CONFLICT;
            if(run.cancelRequested())return Decision.ARCHIVE_ONLY;
            Instant authorizedAt=clock.instant();
            if(run.phase()!=Phase.RECONCILING_TARGET||run.basis().isEmpty()||!authorizedAt.isBefore(run.deadline()))return Decision.NOT_READY;
            String identity;
            try(var p=tx.prepare(c,"SELECT * FROM mf_stage WHERE run_id=? AND stage_type='TARGET_CHECK' AND input_alias=''",runId);var r=p.executeQuery()){
                if(!r.next()||!r.getString("state").equals("COMPLETED"))return Decision.NOT_READY;
                attempts.current(c,new StageIdentity.StageKey(runId,StageIdentity.StageType.TARGET_CHECK,Optional.empty()));
                var current=storedIdentity(r);if(!current.resolvedSpecDigest().equals(run.resolvedDigest()))throw new SQLException("stored run/stage mismatch","MF004");identity=TransactionContracts.identityDigest(current);
            }
            if(!allPassed(c,run,identity))return Decision.NOT_READY;
            var b=run.basis().get();var request=new PublicationRequest(publicationId,runId,b.key(),b.loadId(),b.candidateDigest(),b.expectedRevision());
            tx.one(tx.update(c,"INSERT INTO mf_publication_intent(publication_id,run_id,key_payload,load_id,candidate_digest,expected_revision,request_digest,authorized_at) VALUES (?,?,?,?,?,?,?,?)",
                    publicationId,runId,b.key().canonical(),b.loadId(),b.candidateDigest(),b.expectedRevision(),request.requestDigest(),authorizedAt.toString()));
            advance(c,run,Phase.PUBLISHING,false);event(c,runId,"PUBLICATION_AUTHORIZED",request.requestDigest());return Decision.APPLIED;
        });}catch(SQLException error){
            // The transaction has been rolled back; a globally reused publication ID cannot authorize this run.
            if("23505".equals(error.getSQLState()))return Decision.CONFLICT;
            throw error;
        }
    }
    public Decision cancel(String runId,long observedRevision)throws SQLException{
        TransactionContracts.id(runId);return tx.run(c->{
            var run=locked(c,runId);if(run.revision()!=observedRevision)return Decision.REREAD;
            var authorized=intent(c,runId);
            if(authorized.isPresent()){event(c,runId,"CANCEL_TOO_LATE",authorized.get().request().requestDigest());return Decision.CANCEL_TOO_LATE;}
            if(run.cancelRequested())return Decision.CANCEL_REQUESTED;
            advance(c,run,Phase.CANCEL_REQUESTED,true);event(c,runId,"CANCEL_REQUESTED",run.registrationDigest());return Decision.CANCEL_REQUESTED;
        });
    }
    private boolean allPassed(Connection c,RunRow run,String identity)throws SQLException{
        var b=run.basis().orElseThrow();
        try(var p=tx.prepare(c,"SELECT COUNT(*),SUM(CASE WHEN c.status='PASS' THEN 1 ELSE 0 END) FROM mf_required_check r LEFT JOIN mf_check_result c ON c.run_id=r.run_id AND c.check_id=r.check_id AND c.rule_digest=r.rule_digest AND c.identity_digest=? AND c.candidate_digest=? AND c.load_id=? WHERE r.run_id=?",identity,b.candidateDigest(),b.loadId(),run.runId());var r=p.executeQuery()){
            r.next();return r.getLong(1)>0&&r.getLong(1)==r.getLong(2);
        }
    }
    private record StageRow(String identity,boolean completed,boolean accepting){}
    private StageRow stage(Connection c,StageIdentity identity)throws SQLException{
        var row=attempts.current(c,identity.stage());if(row==null)return null;
        return new StageRow(row.identity().map(TransactionContracts::identityDigest).orElse(null),row.completed(),row.state()==ExecutionState.RUNNING||row.state()==ExecutionState.SUCCEEDED);
    }
    private StageIdentity storedIdentity(ResultSet r)throws SQLException{
        try{
            var i=TransactionContracts.restoreIdentity(r.getString("identity_body"));
            if(!TransactionContracts.identityDigest(i).equals(r.getString("identity_digest"))||!i.stage().runId().equals(r.getString("run_id"))||!i.stage().type().name().equals(r.getString("stage_type"))||!i.stage().inputAlias().orElse("").equals(r.getString("input_alias")))throw new IllegalArgumentException("identity mismatch");
            return i;
        }catch(RuntimeException corrupt){throw new SQLException("stored stage identity corrupt","MF004");}
    }
    private boolean matches(RunRow run,StageRow stage,StageIdentity i){return stage!=null&&stage.accepting()&&run.resolvedDigest().equals(i.resolvedSpecDigest())&&Objects.equals(stage.identity(),TransactionContracts.identityDigest(i));}
    private void complete(Connection c,StageIdentity i)throws SQLException{attempts.complete(c,i);tx.one(tx.update(c,"UPDATE mf_stage SET state='COMPLETED' WHERE run_id=? AND stage_type=? AND input_alias=? AND identity_digest=? AND state='ACTIVE'",i.stage().runId(),i.stage().type().name(),i.stage().inputAlias().orElse(""),TransactionContracts.identityDigest(i)));}
    void advance(Connection c,RunRow run,Phase next,boolean cancel)throws SQLException{tx.one(tx.update(c,"UPDATE mf_run SET revision=revision+1,phase=?,cancel_requested=? WHERE run_id=? AND revision=? AND cancel_requested=FALSE",next.name(),cancel,run.runId(),run.revision()));}
    void event(Connection c,String run,String kind,String digest)throws SQLException{tx.one(tx.update(c,"INSERT INTO mf_event(event_id,run_id,event_kind,content_digest,recorded_at) VALUES (?,?,?,?,?)",UUID.randomUUID().toString(),run,kind,digest,clock.instant().toString()));}
    record RunRow(String runId,String registrationDigest,String resolvedDigest,Phase phase,long revision,boolean cancelRequested,Instant deadline,Optional<PublicationBasis> basis,int maxRetries,Duration backoff,Duration stageTimeout,Duration evidenceWait){RunView view(){return new RunView(runId,revision,phase,cancelRequested);}}
    RunRow locked(Connection c,String runId)throws SQLException{
        try(var p=tx.prepare(c,"SELECT * FROM mf_run WHERE run_id=? FOR UPDATE",runId);var r=p.executeQuery()){
            if(!r.next())throw new SQLException("unknown run","MF003");String key=r.getString("key_payload");
            var basis=key==null?Optional.<PublicationBasis>empty():Optional.of(new PublicationBasis(BusinessKey.restore(key),r.getString("load_id"),r.getString("candidate_digest"),r.getLong("expected_revision")));
            return new RunRow(runId,r.getString("registration_digest"),r.getString("resolved_digest"),Phase.valueOf(r.getString("phase")),r.getLong("revision"),r.getBoolean("cancel_requested"),Instant.parse(r.getString("deadline")),basis,r.getInt("max_stage_retries"),Duration.parse(r.getString("retry_backoff")),Duration.parse(r.getString("stage_timeout")),Duration.parse(r.getString("evidence_wait")));
        }
    }
    Optional<AuthorizedPublication> intent(Connection c,String run)throws SQLException{
        try(var p=tx.prepare(c,"SELECT * FROM mf_publication_intent WHERE run_id=?",run);var r=p.executeQuery()){
            if(!r.next())return Optional.empty();return Optional.of(new AuthorizedPublication(JdbcPublicationStore.request(r),Instant.parse(r.getString("authorized_at"))));
        }
    }
}
