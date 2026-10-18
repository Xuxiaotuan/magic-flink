package io.github.xuxiaotuan.magicflink.connectors;

import io.github.xuxiaotuan.magicflink.control.AttemptContracts;
import io.github.xuxiaotuan.magicflink.control.AttemptContracts.*;
import io.github.xuxiaotuan.magicflink.control.StageIdentity;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import static io.github.xuxiaotuan.magicflink.control.AttemptContracts.fields;

/** Shares the run transaction/lock with JdbcRunStore. It never dispatches work or infers physical stop from a lease. */
final class JdbcAttempts {
    private final JdbcTransactions tx; private final Clock clock; private final JdbcRunStore runs;
    JdbcAttempts(JdbcTransactions tx,Clock clock,JdbcRunStore runs){this.tx=tx;this.clock=clock;this.runs=runs;}
    record Row(AttemptRef ref,int ordinal,StageIdentity.Executor executor,String planDigest,String spec,String consumed,Instant deadline,Instant createdAt,
               Optional<PlannedAttempt> plan,Optional<StageIdentity> identity,ExecutionState state,boolean completed,Optional<String> dispatchId,
               Optional<Instant> dispatchAt,Optional<String> stopDigest,Optional<RetryReason> reason,Optional<Instant> stoppedAt,Optional<String> replacedBy,Optional<String> parent) {
        AttemptView view(){return new AttemptView(ref,ordinal,state,completed,identity,plan,dispatchId,stopDigest,replacedBy,createdAt);}
        String definition(){return TransactionContracts.hash("ATTEMPT_DEFINITION",fields("run",ref.key().runId(),"type",ref.key().type().name(),"alias",ref.key().inputAlias().orElse(""),"attempt",ref.attemptId(),"ordinal",ordinal,"executor",executor.name(),"plan",planDigest,"spec",spec,"consumed",consumed,"deadline",deadline,"createdAt",createdAt,"planBody",plan.map(PlannedAttempt::canonical).orElse(""),"parent",parent.orElse("")));}
    }
    void register(Connection c,Registration registration,StageIdentity identity)throws SQLException{
        var ref=new AttemptRef(identity.stage(),identity.attemptId());
        insert(c,ref,0,identity.executor(),identity.planDigest(),identity.resolvedSpecDigest(),identity.consumedDataDigest(),registration.deadline(),Optional.empty(),Optional.of(identity),Optional.empty(),ExecutionState.RUNNING);
        tx.one(tx.update(c,"INSERT INTO mf_stage(run_id,stage_type,input_alias,current_attempt_id,identity_digest,identity_body,state) VALUES (?,?,?,?,?,?,'ACTIVE')",ref.key().runId(),ref.key().type().name(),alias(ref),ref.attemptId(),TransactionContracts.identityDigest(identity),TransactionContracts.identityCanonical(identity)));
    }
    private void insert(Connection c,AttemptRef ref,int ordinal,StageIdentity.Executor executor,String digest,String spec,String consumed,Instant deadline,Optional<PlannedAttempt> plan,Optional<StageIdentity> identity,Optional<String> parent,ExecutionState state)throws SQLException{
        var row=new Row(ref,ordinal,executor,digest,spec,consumed,deadline,clock.instant(),plan,identity,state,false,Optional.empty(),Optional.empty(),Optional.empty(),Optional.empty(),Optional.empty(),Optional.empty(),parent);
        tx.one(tx.update(c,"INSERT INTO mf_attempt(run_id,stage_type,input_alias,attempt_id,ordinal,executor,plan_digest,resolved_digest,consumed_digest,deadline,created_at,plan_body,definition_digest,identity_digest,identity_body,execution_state,completion_state,parent_attempt_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'ACTIVE',?)",
                ref.key().runId(),ref.key().type().name(),alias(ref),ref.attemptId(),ordinal,executor.name(),digest,spec,consumed,deadline.toString(),row.createdAt().toString(),plan.map(PlannedAttempt::canonical).orElse(null),row.definition(),identity.map(TransactionContracts::identityDigest).orElse(null),identity.map(TransactionContracts::identityCanonical).orElse(null),state.name(),parent.orElse(null)));
    }
    Row current(Connection c,StageIdentity.StageKey key)throws SQLException{
        try(var p=tx.prepare(c,"SELECT s.*,r.resolved_digest AS expected_spec FROM mf_stage s JOIN mf_run r ON r.run_id=s.run_id WHERE s.run_id=? AND s.stage_type=? AND s.input_alias=?",key.runId(),key.type().name(),key.inputAlias().orElse(""));var r=p.executeQuery()){
            if(!r.next())return null;var row=read(c,new AttemptRef(key,r.getString("current_attempt_id")));
            if(row==null||!row.spec().equals(r.getString("expected_spec"))||!Objects.equals(row.identity().map(TransactionContracts::identityDigest).orElse(null),r.getString("identity_digest"))||!Objects.equals(row.identity().map(TransactionContracts::identityCanonical).orElse(null),r.getString("identity_body"))||row.completed()!=r.getString("state").equals("COMPLETED"))throw corrupt();return row;
        }
    }
    Row read(Connection c,AttemptRef ref)throws SQLException{
        try(var p=tx.prepare(c,"SELECT * FROM mf_attempt WHERE run_id=? AND stage_type=? AND input_alias=? AND attempt_id=?",ref.key().runId(),ref.key().type().name(),alias(ref),ref.attemptId());var r=p.executeQuery()){
            if(!r.next())return null;
            try{
                var plan=opt(r.getString("plan_body")).map(PlannedAttempt::restore);var identity=opt(r.getString("identity_body")).map(TransactionContracts::restoreIdentity);
                var row=new Row(ref,r.getInt("ordinal"),StageIdentity.Executor.valueOf(r.getString("executor")),r.getString("plan_digest"),r.getString("resolved_digest"),r.getString("consumed_digest"),Instant.parse(r.getString("deadline")),Instant.parse(r.getString("created_at")),plan,identity,ExecutionState.valueOf(r.getString("execution_state")),r.getString("completion_state").equals("COMPLETED"),opt(r.getString("dispatch_id")),opt(r.getString("dispatch_started_at")).map(Instant::parse),opt(r.getString("stop_digest")),opt(r.getString("stop_reason")).map(RetryReason::valueOf),opt(r.getString("stopped_at")).map(Instant::parse),opt(r.getString("replaced_by")),opt(r.getString("parent_attempt_id")));
                if(!row.definition().equals(r.getString("definition_digest")))throw corrupt();
                if(identity.isPresent()){
                    var i=identity.get();if(!ref.equals(new AttemptRef(i.stage(),i.attemptId()))||i.executor()!=row.executor()||!i.planDigest().equals(row.planDigest())||!i.resolvedSpecDigest().equals(row.spec())||!i.consumedDataDigest().equals(row.consumed())||!TransactionContracts.identityDigest(i).equals(r.getString("identity_digest")))throw corrupt();
                }else if(r.getString("identity_digest")!=null)throw corrupt();
                if(plan.isPresent()){var planValue=plan.get();if(!planValue.ref().equals(ref)||planValue.executor()!=row.executor()||!planValue.planDigest().equals(row.planDigest())||!planValue.resolvedSpecDigest().equals(row.spec())||!planValue.consumedDigest().equals(row.consumed())||!planValue.deadline().equals(row.deadline()))throw corrupt();}
                if(row.stopDigest().isPresent())stopProof(c,row);
                return row;
            }catch(RuntimeException invalid){throw corrupt();}
        }
    }
    private List<Row> currentRows(Connection c,String runId)throws SQLException{
        List<StageIdentity.StageKey> keys=new ArrayList<>();try(var p=tx.prepare(c,"SELECT stage_type,input_alias FROM mf_stage WHERE run_id=? ORDER BY stage_type,input_alias",runId);var r=p.executeQuery()){while(r.next())keys.add(key(runId,r.getString(1),r.getString(2)));}
        List<Row> rows=new ArrayList<>();for(var key:keys)rows.add(current(c,key));return rows;
    }
    List<StageIdentity> currentBound(Connection c,String run)throws SQLException{
        List<StageIdentity> identities=new ArrayList<>();for(var row:currentRows(c,run)){if(row.identity().isEmpty())throw new SQLException("unbound current attempt; use recovery snapshot","MF005");identities.add(row.identity().get());}return List.copyOf(identities);
    }
    List<AttemptView> history(StageIdentity.StageKey key)throws SQLException{
        return tx.run(c->{runs.locked(c,key.runId());List<String> ids=new ArrayList<>();try(var p=tx.prepare(c,"SELECT attempt_id FROM mf_attempt WHERE run_id=? AND stage_type=? AND input_alias=? ORDER BY ordinal",key.runId(),key.type().name(),key.inputAlias().orElse(""));var r=p.executeQuery()){while(r.next())ids.add(r.getString(1));}
            List<AttemptView> result=new ArrayList<>();for(var id:ids)result.add(read(c,new AttemptRef(key,id)).view());return List.copyOf(result);});
    }
    RecoverySnapshot recover(String runId)throws SQLException{
        TransactionContracts.id(runId);return tx.run(c->recover(c,runId));
    }
    RecoverySnapshot recover(Connection c,String runId)throws SQLException{
        var run=runs.locked(c,runId);List<NextIntent> intents=new ArrayList<>();
            try(var p=tx.prepare(c,"SELECT * FROM mf_next_intent WHERE run_id=? ORDER BY stage_type,input_alias",runId);var r=p.executeQuery()){
                while(r.next()){var key=key(runId,r.getString("stage_type"),r.getString("input_alias"));validateIntent(c,key,r.getString("consumed_body"),r.getString("consumed_digest"));
                    var first=opt(r.getString("first_attempt_id"));if(first.isPresent()&&read(c,new AttemptRef(key,first.get()))==null)throw corrupt();
                    intents.add(new NextIntent(key,r.getString("consumed_digest"),first,r.getBoolean("cancelled")));}
            }
            var current=currentRows(c,runId);
            if((run.phase()==Phase.PRECHECKING||run.phase()==Phase.VALIDATING_RESULT)&&intents.isEmpty()&&current.stream().anyMatch(Row::completed))throw corrupt();
            for(var row:current)if(!row.spec().equals(run.resolvedDigest()))throw corrupt();
            return new RecoverySnapshot(run.view(),current.stream().map(Row::view).toList(),intents);
    }
    Decision materialize(long revision,PlannedAttempt plan)throws SQLException{
        return tx.run(c->{var run=runs.locked(c,plan.ref().key().runId());if(run.revision()!=revision)return Decision.REREAD;if(run.cancelRequested())return Decision.ARCHIVE_ONLY;
            try(var p=tx.prepare(c,"SELECT * FROM mf_next_intent WHERE run_id=? AND stage_type=? AND input_alias=?",run.runId(),plan.ref().key().type().name(),alias(plan.ref()));var r=p.executeQuery()){
                if(!r.next())return Decision.CONFLICT;validateIntent(c,plan.ref().key(),r.getString("consumed_body"),r.getString("consumed_digest"));
                if(!r.getString("consumed_digest").equals(plan.consumedDigest())||!plan.resolvedSpecDigest().equals(run.resolvedDigest()))return Decision.CONFLICT;
                if(r.getBoolean("cancelled"))return Decision.ARCHIVE_ONLY;
                String prior=r.getString("first_attempt_id");if(prior!=null){var initial=read(c,new AttemptRef(plan.ref().key(),prior));return initial!=null&&initial.plan().equals(Optional.of(plan))?Decision.ALREADY_APPLIED:Decision.CONFLICT;}
                if(run.phase()!=TransactionContracts.phaseFor(plan.ref().key().type())||!validDeadline(run,plan))return Decision.NOT_READY;
                if(current(c,plan.ref().key())!=null)return Decision.CONFLICT;
                insertPlan(c,plan,0,Optional.empty());
                tx.one(tx.update(c,"INSERT INTO mf_stage(run_id,stage_type,input_alias,current_attempt_id,identity_digest,identity_body,state) VALUES (?,?,?,?,?,?,'ACTIVE')",run.runId(),plan.ref().key().type().name(),alias(plan.ref()),plan.ref().attemptId(),plan.inlineIdentity().map(TransactionContracts::identityDigest).orElse(null),plan.inlineIdentity().map(TransactionContracts::identityCanonical).orElse(null)));
                tx.one(tx.update(c,"UPDATE mf_next_intent SET first_attempt_id=? WHERE run_id=? AND stage_type=? AND input_alias=? AND first_attempt_id IS NULL",plan.ref().attemptId(),run.runId(),plan.ref().key().type().name(),alias(plan.ref())));
                runs.advance(c,run,run.phase(),false);runs.event(c,run.runId(),"NEXT_PLAN_MATERIALIZED",plan.planDigest());return Decision.APPLIED;
            }
        });
    }
    Decision begin(long revision,AttemptRef ref,String dispatchId)throws SQLException{
        TransactionContracts.id(dispatchId);return tx.run(c->{
            var run=runs.locked(c,ref.key().runId());
            try(var p=tx.prepare(c,"SELECT run_id FROM mf_admission WHERE run_id=?",run.runId());var r=p.executeQuery()){if(r.next())return Decision.NOT_READY;}
            return begin(c,revision,ref,dispatchId);
        });
    }
    Decision begin(Connection c,long revision,AttemptRef ref,String dispatchId)throws SQLException{
        var run=runs.locked(c,ref.key().runId());if(run.revision()!=revision)return Decision.REREAD;var row=current(c,ref.key());
            if(run.cancelRequested()||row==null||!row.ref().equals(ref)||run.phase()!=TransactionContracts.phaseFor(ref.key().type()))return Decision.ARCHIVE_ONLY;
            if(row.stopDigest().isPresent()||row.completed())return Decision.ARCHIVE_ONLY;
            if(row.dispatchId().isPresent())return row.dispatchId().get().equals(dispatchId)?Decision.ALREADY_APPLIED:Decision.CONFLICT;
            if(row.state()!=ExecutionState.PLANNED||row.stopDigest().isPresent()||!clock.instant().isBefore(row.deadline())||!clock.instant().isBefore(run.deadline()))return Decision.NOT_READY;
            tx.one(tx.update(c,"UPDATE mf_attempt SET dispatch_id=?,dispatch_started_at=?,execution_state=? WHERE run_id=? AND stage_type=? AND input_alias=? AND attempt_id=?",dispatchId,clock.instant().toString(),row.executor()==StageIdentity.Executor.FLINK?"SUBMITTING":"RUNNING",run.runId(),ref.key().type().name(),alias(ref),ref.attemptId()));
            runs.advance(c,run,run.phase(),false);runs.event(c,run.runId(),"DISPATCH_STARTED",row.planDigest());return Decision.APPLIED;
    }
    Decision bind(long revision,StageIdentity identity,String dispatchId)throws SQLException{
        return tx.run(c->{var run=runs.locked(c,identity.stage().runId());if(run.revision()!=revision)return Decision.REREAD;var row=current(c,identity.stage());var ref=new AttemptRef(identity.stage(),identity.attemptId());
            if(row==null||!row.ref().equals(ref))return Decision.ARCHIVE_ONLY;
            if(!row.dispatchId().equals(Optional.of(dispatchId))||row.executor()!=identity.executor()||!row.planDigest().equals(identity.planDigest())||!row.spec().equals(identity.resolvedSpecDigest())||!row.consumed().equals(identity.consumedDataDigest()))return Decision.CONFLICT;
            if(row.identity().isPresent())return row.identity().get().equals(identity)?Decision.ALREADY_APPLIED:Decision.CONFLICT;
            if(row.stopDigest().isPresent()||row.state()!=ExecutionState.SUBMITTING)return Decision.CONFLICT;
            try(var p=tx.prepare(c,"SELECT stage_type,input_alias,attempt_id,identity_body FROM mf_attempt WHERE run_id=? AND identity_body IS NOT NULL",run.runId());var r=p.executeQuery()){
                while(r.next()){
                    var existing=TransactionContracts.restoreIdentity(r.getString("identity_body"));
                    if(!new AttemptRef(existing.stage(),existing.attemptId()).equals(ref)&&existing.resource().isPresent()&&identity.resource().isPresent()&&existing.resource().get().crUid().equals(identity.resource().get().crUid()))return Decision.CONFLICT;
                }
            }
            tx.one(tx.update(c,"UPDATE mf_attempt SET identity_digest=?,identity_body=?,execution_state='RUNNING' WHERE run_id=? AND stage_type=? AND input_alias=? AND attempt_id=?",TransactionContracts.identityDigest(identity),TransactionContracts.identityCanonical(identity),run.runId(),identity.stage().type().name(),identity.stage().inputAlias().orElse(""),identity.attemptId()));
            tx.one(tx.update(c,"UPDATE mf_stage SET identity_digest=?,identity_body=? WHERE run_id=? AND stage_type=? AND input_alias=? AND current_attempt_id=?",TransactionContracts.identityDigest(identity),TransactionContracts.identityCanonical(identity),run.runId(),identity.stage().type().name(),identity.stage().inputAlias().orElse(""),identity.attemptId()));
            bump(c,run);runs.event(c,run.runId(),"EXECUTION_BOUND",TransactionContracts.identityDigest(identity));return Decision.APPLIED;
        });
    }
    Decision stop(long revision,StopEvidence proof)throws SQLException{
        return tx.run(c->{var run=runs.locked(c,proof.ref().key().runId());if(run.revision()!=revision)return Decision.REREAD;var row=read(c,proof.ref());
            if(row==null){runs.event(c,run.runId(),"UNKNOWN_ATTEMPT_STOP",proof.contentDigest());return Decision.ARCHIVE_ONLY;}
            if(!row.planDigest().equals(proof.planDigest())||!row.dispatchId().equals(proof.dispatchId())||!row.identity().equals(proof.identity())||proof.observedAt().isBefore(row.createdAt())||proof.observedAt().isAfter(clock.instant())||row.dispatchAt().filter(t->proof.observedAt().isBefore(t)).isPresent())return Decision.CONFLICT;
            if(proof.execution()==ExecutionObservation.NOT_CREATED&&row.executor()==StageIdentity.Executor.FLINK&&row.identity().isPresent())return Decision.CONFLICT;
            if(proof.confirmed()&&proof.execution()!=ExecutionObservation.NOT_CREATED&&row.identity().isEmpty())return Decision.CONFLICT;
            var current=current(c,proof.ref().key());
            int inserted=tx.update(c,"INSERT INTO mf_stop_evidence(content_digest,run_id,stage_type,input_alias,attempt_id,content_body) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING",proof.contentDigest(),run.runId(),proof.ref().key().type().name(),alias(proof.ref()),proof.ref().attemptId(),proof.canonical());
            if(current==null||!current.ref().equals(row.ref())){runs.event(c,run.runId(),"LATE_STOP",proof.contentDigest());return Decision.ARCHIVE_ONLY;}
            if(row.stopDigest().isPresent())return row.stopDigest().get().equals(proof.contentDigest())?Decision.ALREADY_APPLIED:Decision.CONFLICT;
            if(row.completed()&&proof.execution()!=ExecutionObservation.SUCCEEDED)return Decision.CONFLICT;
            if(inserted==0)return Decision.WAIT_STOP;
            if(proof.confirmed()){
                String state=switch(proof.execution()){case SUCCEEDED->"SUCCEEDED";case FAILED->"FAILED";case NOT_CREATED->proof.reason()==RetryReason.TRANSIENT?"FAILED":"CANCELLED";default->"CANCELLED";};
                tx.one(tx.update(c,"UPDATE mf_attempt SET stop_digest=?,stop_reason=?,stopped_at=?,execution_state=? WHERE run_id=? AND stage_type=? AND input_alias=? AND attempt_id=?",proof.contentDigest(),proof.reason().name(),proof.observedAt().toString(),state,run.runId(),proof.ref().key().type().name(),alias(proof.ref()),proof.ref().attemptId()));
            }
            bump(c,run);runs.event(c,run.runId(),proof.confirmed()?"STOP_CONFIRMED":"STOP_UNKNOWN",proof.contentDigest());return proof.confirmed()?Decision.APPLIED:Decision.WAIT_STOP;
        });
    }
    Decision retry(long revision,AttemptRef old,PlannedAttempt next)throws SQLException{
        return tx.run(c->{var run=runs.locked(c,old.key().runId());if(run.revision()!=revision)return Decision.REREAD;if(run.cancelRequested())return Decision.ARCHIVE_ONLY;var row=current(c,old.key());
            if(row==null)return Decision.ARCHIVE_ONLY;
            if(row.ref().equals(next.ref())&&row.parent().equals(Optional.of(old.attemptId())))return row.plan().equals(Optional.of(next))?Decision.ALREADY_APPLIED:Decision.CONFLICT;
            if(!row.ref().equals(old)||run.phase()!=TransactionContracts.phaseFor(old.key().type()))return Decision.ARCHIVE_ONLY;
            if(!next.ref().key().equals(old.key())||next.ref().attemptId().equals(old.attemptId())||!next.resolvedSpecDigest().equals(row.spec())||!next.consumedDigest().equals(row.consumed()))return Decision.CONFLICT;
            if(row.completed()||old.key().type()==StageIdentity.StageType.WRITE||runs.intent(c,run.runId()).isPresent())return Decision.NOT_READY;
            if(row.stopDigest().isEmpty())return Decision.WAIT_STOP;
            var proof=stopProof(c,row);boolean allowed=switch(proof.reason()){
                case TRANSIENT->row.state()==ExecutionState.FAILED&&next.executor()==row.executor();
                case EVIDENCE_LOST->Set.of(StageIdentity.StageType.COLLECT,StageIdentity.StageType.COMPUTE).contains(old.key().type())&&next.executor()==row.executor()&&!clock.instant().isBefore(proof.observedAt().plus(run.evidenceWait()));
                case INLINE_LIMIT->row.state()==ExecutionState.FAILED&&row.executor()==StageIdentity.Executor.INLINE&&next.executor()==StageIdentity.Executor.FLINK&&Set.of(StageIdentity.StageType.PRECHECK,StageIdentity.StageType.RESULT_CHECK,StageIdentity.StageType.TARGET_CHECK).contains(old.key().type());
                default->false;
            };
            try(var p=tx.prepare(c,"SELECT COUNT(*) FROM mf_check_result WHERE run_id=? AND identity_digest=? AND status='FAIL'",run.runId(),row.identity().map(TransactionContracts::identityDigest).orElse(""));var r=p.executeQuery()){r.next();if(r.getLong(1)>0)allowed=false;}
            if(!allowed||row.ordinal()>=run.maxRetries()||!validDeadline(run,next)||clock.instant().isBefore(proof.observedAt().plus(run.backoff())))return Decision.NOT_READY;
            insertPlan(c,next,row.ordinal()+1,Optional.of(old.attemptId()));
            tx.one(tx.update(c,"UPDATE mf_attempt SET replaced_by=? WHERE run_id=? AND stage_type=? AND input_alias=? AND attempt_id=? AND replaced_by IS NULL",next.ref().attemptId(),run.runId(),old.key().type().name(),alias(old),old.attemptId()));
            tx.one(tx.update(c,"UPDATE mf_stage SET current_attempt_id=?,identity_digest=?,identity_body=?,state='ACTIVE' WHERE run_id=? AND stage_type=? AND input_alias=? AND current_attempt_id=?",next.ref().attemptId(),next.inlineIdentity().map(TransactionContracts::identityDigest).orElse(null),next.inlineIdentity().map(TransactionContracts::identityCanonical).orElse(null),run.runId(),old.key().type().name(),alias(old),old.attemptId()));
            runs.advance(c,run,run.phase(),false);runs.event(c,run.runId(),"ATTEMPT_REPLACED",next.planDigest());return Decision.APPLIED;
        });
    }
    Decision finishCancellation(String runId,long revision)throws SQLException{
        return tx.run(c->finishCancellation(c,runId,revision));
    }
    Decision finishCancellation(Connection c,String runId,long revision)throws SQLException{
        var run=runs.locked(c,runId);if(run.revision()!=revision)return Decision.REREAD;if(runs.intent(c,runId).isPresent())return Decision.CANCEL_TOO_LATE;
            if(run.phase()==Phase.CANCELLED)return Decision.ALREADY_APPLIED;if(!run.cancelRequested()||run.phase()!=Phase.CANCEL_REQUESTED)return Decision.NOT_READY;
            List<AttemptRef> refs=new ArrayList<>();try(var p=tx.prepare(c,"SELECT stage_type,input_alias,attempt_id FROM mf_attempt WHERE run_id=?",runId);var r=p.executeQuery()){while(r.next())refs.add(new AttemptRef(key(runId,r.getString(1),r.getString(2)),r.getString(3)));}
            for(var ref:refs){var row=read(c,ref);if(row.stopDigest().isEmpty())return Decision.WAIT_STOP;stopProof(c,row);}
            tx.update(c,"UPDATE mf_next_intent SET cancelled=TRUE WHERE run_id=?",runId);
            tx.one(tx.update(c,"UPDATE mf_run SET phase='CANCELLED',revision=revision+1 WHERE run_id=? AND revision=? AND cancel_requested=TRUE",runId,run.revision()));runs.event(c,runId,"CANCELLATION_CONFIRMED",run.registrationDigest());return Decision.APPLIED;
    }
    private StopEvidence stopProof(Connection c,Row row)throws SQLException{
        try(var p=tx.prepare(c,"SELECT content_body FROM mf_stop_evidence WHERE content_digest=?",row.stopDigest().orElseThrow());var r=p.executeQuery()){
            if(!r.next())throw corrupt();try{var proof=StopEvidence.restore(r.getString(1));if(!proof.confirmed()||!proof.ref().equals(row.ref())||!proof.planDigest().equals(row.planDigest())||!proof.identity().equals(row.identity())||!proof.dispatchId().equals(row.dispatchId())||!proof.contentDigest().equals(row.stopDigest().get())||!row.reason().equals(Optional.of(proof.reason()))||!row.stoppedAt().equals(Optional.of(proof.observedAt())))throw corrupt();return proof;}catch(RuntimeException invalid){throw corrupt();}
        }
    }
    void complete(Connection c,StageIdentity identity)throws SQLException{
        tx.one(tx.update(c,"UPDATE mf_attempt SET completion_state='COMPLETED',execution_state='SUCCEEDED' WHERE run_id=? AND stage_type=? AND input_alias=? AND attempt_id=? AND identity_digest=? AND completion_state='ACTIVE'",identity.stage().runId(),identity.stage().type().name(),identity.stage().inputAlias().orElse(""),identity.attemptId(),TransactionContracts.identityDigest(identity)));
    }
    private void insertPlan(Connection c,PlannedAttempt p,int ordinal,Optional<String> parent)throws SQLException{insert(c,p.ref(),ordinal,p.executor(),p.planDigest(),p.resolvedSpecDigest(),p.consumedDigest(),p.deadline(),Optional.of(p),p.inlineIdentity(),parent,ExecutionState.PLANNED);}
    void initialPlan(Connection c,PlannedAttempt plan)throws SQLException{
        insertPlan(c,plan,0,Optional.empty());
        tx.one(tx.update(c,"INSERT INTO mf_stage(run_id,stage_type,input_alias,current_attempt_id,identity_digest,identity_body,state) VALUES (?,?,?,?,?,?,'ACTIVE')",plan.ref().key().runId(),plan.ref().key().type().name(),alias(plan.ref()),plan.ref().attemptId(),plan.inlineIdentity().map(TransactionContracts::identityDigest).orElse(null),plan.inlineIdentity().map(TransactionContracts::identityCanonical).orElse(null)));
    }
    private boolean validDeadline(JdbcRunStore.RunRow run,PlannedAttempt plan){Instant now=clock.instant();return !plan.deadline().isAfter(run.deadline())&&!plan.deadline().isAfter(now.plus(run.stageTimeout()))&&now.isBefore(plan.deadline())&&now.isBefore(run.deadline());}
    private void bump(Connection c,JdbcRunStore.RunRow run)throws SQLException{tx.one(tx.update(c,"UPDATE mf_run SET revision=revision+1 WHERE run_id=? AND revision=? AND cancel_requested=?",run.runId(),run.revision(),run.cancelRequested()));}
    private void validateIntent(Connection c,StageIdentity.StageKey key,String body,String digest)throws SQLException{
        var source=switch(key.type()){case PRECHECK->StageIdentity.StageType.COLLECT;case RESULT_CHECK->StageIdentity.StageType.COMPUTE;default->throw corrupt();};
        List<Map<String,Object>> files=new ArrayList<>();try(var p=tx.prepare(c,"SELECT m.* FROM mf_manifest m WHERE m.run_id=? AND m.stage_type=? ORDER BY m.input_alias",key.runId(),source.name());var r=p.executeQuery()){
            while(r.next()){var current=current(c,new StageIdentity.StageKey(key.runId(),source,source==StageIdentity.StageType.COLLECT?Optional.of(r.getString("input_alias")):Optional.empty()));if(current==null||!current.completed()||current.identity().isEmpty()||!TransactionContracts.identityDigest(current.identity().get()).equals(r.getString("identity_digest")))throw corrupt();files.add(Map.of("alias",r.getString("input_alias"),"id",r.getString("manifest_id"),"digest",r.getString("content_digest")));}
        }
        var content=Map.of("manifests",files);if(files.isEmpty()||!AttemptContracts.encode(content).equals(body)||!TransactionContracts.hash("SEALED_INPUTS",content).equals(digest))throw corrupt();
    }
    private static String alias(AttemptRef ref){return ref.key().inputAlias().orElse("");}
    private static StageIdentity.StageKey key(String run,String type,String alias){return new StageIdentity.StageKey(run,StageIdentity.StageType.valueOf(type),alias.isEmpty()?Optional.empty():Optional.of(alias));}
    private static Optional<String> opt(String value){return Optional.ofNullable(value);}
    private static SQLException corrupt(){return new SQLException("attempt/intent evidence inconsistent","MF004");}
}
