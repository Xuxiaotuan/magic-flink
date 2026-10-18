package io.github.xuxiaotuan.magicflink.connectors;

import io.github.xuxiaotuan.magicflink.control.AttemptContracts;
import io.github.xuxiaotuan.magicflink.control.AttemptContracts.*;
import io.github.xuxiaotuan.magicflink.control.CoordinatorContracts.*;
import io.github.xuxiaotuan.magicflink.control.StageIdentity;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts.*;
import io.github.xuxiaotuan.magicflink.protocol.*;
import javax.sql.DataSource;
import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.time.*;
import java.util.*;
import static io.github.xuxiaotuan.magicflink.control.AttemptContracts.fields;

/** Bounded local coordination steps. Never calls an executor, stops a process, or installs a schema. */
public final class JdbcCoordinator {
    private final JdbcTransactions tx;
    private final JdbcRunStore runs;
    private final JdbcAttempts attempts;
    private final Clock clock;
    private final Duration leaseDuration;
    private final LocalImmutableObjectStore objects;

    public JdbcCoordinator(DataSource source,int queryTimeoutSeconds,Clock clock,Duration leaseDuration,LocalImmutableObjectStore objects){
        this.tx=new JdbcTransactions(source,queryTimeoutSeconds);this.clock=Objects.requireNonNull(clock);this.objects=Objects.requireNonNull(objects);
        if(leaseDuration==null||leaseDuration.compareTo(Duration.ofSeconds(1))<0||leaseDuration.compareTo(Duration.ofSeconds(60))>0)throw new IllegalArgumentException("lease duration must be 1..60 seconds");
        this.leaseDuration=leaseDuration;this.runs=new JdbcRunStore(source,queryTimeoutSeconds,clock);this.attempts=new JdbcAttempts(tx,clock,runs);
    }
    /** Platform operation, immutable for this metadata schema lifetime. Not supplied by business YAML. */
    public void configure(BudgetPolicy policy)throws SQLException{
        Objects.requireNonNull(policy);tx.run(c->{String body=guard(c);String wanted=policy.canonical();
            if(body!=null&&!body.equals(wanted))throw new SQLException("quota policy change requires a separate controlled migration","MF001");
            if(body==null)tx.one(tx.update(c,"UPDATE mf_control_guard SET policy_body=?,policy_digest=? WHERE guard_id=1",wanted,policyDigest(wanted)));
            return null;
        });
    }
    /** All declared inputs must have ready, fixed source snapshots. Readiness probes remain a separate adapter. */
    public Decision admit(ResolvedRun run,List<FileStageProtocol.Plan> plans)throws SQLException,IOException{
        Objects.requireNonNull(run);plans=List.copyOf(plans);
        if(plans.size()!=run.inputVersions().size()||plans.stream().map(p->p.key().inputAlias().orElse("")).distinct().count()!=plans.size()
                ||!plans.stream().map(p->p.key().inputAlias().orElse("")).collect(java.util.stream.Collectors.toSet()).equals(run.inputVersions().keySet()))throw new IllegalArgumentException("exact complete initial COLLECT set required");
        var runtime=run.catalog().runtime();var frozenRun=DomainSnapshots.run(run);var frozenCatalog=DomainSnapshots.catalog(run.catalog());
        URI prefix=URI.create(runtime.storageScope()+"runs/"+run.runId()+"/");
        List<PlannedAttempt> initial=new ArrayList<>();Map<String,Object> demands=new TreeMap<>();
        Map<String,Integer> ceilings=new TreeMap<>();ceilings.put("applications",runtime.concurrency().applications());ceilings.put("inline",runtime.concurrency().checkThreads());
        run.catalog().connectionBudgets().forEach((physical,limits)->ceilings.put("connection."+physical,limits.total()-limits.probes()));
        for(var p:plans){
            if(p.key().type()!=StageIdentity.StageType.COLLECT)throw new IllegalArgumentException("initial stage must be COLLECT");
            FileStageProtocol.validatePlan(run,p);
            if(p.plannedAt().isAfter(clock.instant()))throw new IllegalArgumentException("future stage plan");
            var dataset=run.catalog().inputs().stream().filter(d->d.ref().equals(p.inputs().get(0).dataset())).findFirst().orElseThrow();
            var connection=run.catalog().connections().stream().filter(v->v.ref().equals(dataset.connection())).findFirst().orElseThrow();
            demands.put(p.key().inputAlias().orElseThrow(),Map.of("applications",1,"connection."+connection.physicalId(),dataset.read().maxConnections()));
            initial.add(new PlannedAttempt(new AttemptRef(p.key(),p.attemptId()),StageIdentity.Executor.FLINK,URI.create(p.stagePrefix()+"plan.json"),DomainSnapshots.plan(p).sha256(),frozenRun.sha256(),p.consumedDataDigest(),p.deadline()));
        }
        initial.sort(Comparator.comparing(p->p.ref().key().inputAlias().orElseThrow()));
        String body=AttemptContracts.encode(fields("run",run.runId(),"runKey",prefix+"resolved.json","runDigest",frozenRun.sha256(),"catalogKey",prefix+"catalog.json","catalogDigest",frozenCatalog.sha256(),"definitionKey",prefix+"job.json","definitionDigest",ObjectDigests.definition(run.job().spec()).sha256(),
                "acceptedAt",run.acceptedAt(),"plans",initial.stream().map(PlannedAttempt::canonical).toList(),"demands",demands,"inputConcurrency",Math.min(2,runtime.concurrency().inputsPerRun()),"ceilings",ceilings,"activeRunCeiling",runtime.concurrency().activeRuns()));
        String digest=admissionDigest(body);
        // Objects are complete before any database pointer becomes visible. Unknown I/O leaves no runnable row.
        objects.putIfAbsent(URI.create(prefix+"job.json"),ObjectDigests.definition(run.job().spec()));objects.putIfAbsent(URI.create(prefix+"resolved.json"),frozenRun);objects.putIfAbsent(URI.create(prefix+"catalog.json"),frozenCatalog);
        for(var p:plans)objects.putIfAbsent(URI.create(p.stagePrefix()+"plan.json"),DomainSnapshots.plan(p));
        return tx.run(c->{guard(c);
            for(var connection:run.catalog().connections()){
                tx.update(c,"INSERT INTO mf_physical_connection(physical_id,endpoint) VALUES (?,?) ON CONFLICT DO NOTHING",connection.physicalId(),connection.endpoint().toString());
                try(var p=tx.prepare(c,"SELECT endpoint FROM mf_physical_connection WHERE physical_id=?",connection.physicalId());var r=p.executeQuery()){
                    if(!r.next()||!connection.endpoint().toString().equals(r.getString(1)))throw new SQLException("physical connection/endpoint registration conflict","MF001");
                }
            }
            int inserted=tx.update(c,"INSERT INTO mf_run(run_id,registration_digest,resolved_digest,phase,revision,cancel_requested,deadline,max_stage_retries,retry_backoff,stage_timeout,evidence_wait) VALUES (?,?,?,'COLLECTING',0,FALSE,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    run.runId(),digest,frozenRun.sha256(),run.runDeadline().toString(),runtime.retry().maxStageRetries(),runtime.retry().backoff().toString(),runtime.time().stage().toString(),runtime.time().evidenceWait().toString());
            var current=runs.locked(c,run.runId());
            if(!current.registrationDigest().equals(digest))return Decision.CONFLICT;
            if(inserted==0){var old=admission(c,current);return old.body().equals(body)?Decision.ALREADY_APPLIED:Decision.CONFLICT;}
            if(run.acceptedAt().isAfter(clock.instant())||!clock.instant().isBefore(run.runDeadline())||initial.stream().anyMatch(p->!clock.instant().isBefore(p.deadline())))throw new SQLException("initial admission expired","MF005");
            tx.one(tx.update(c,"INSERT INTO mf_admission(run_id,accepted_at,content_body,content_digest) VALUES (?,?,?,?)",run.runId(),AttemptContracts.decode(body).get("acceptedAt"),body,digest));
            tx.one(tx.update(c,"INSERT INTO mf_coordinator_lease(run_id,epoch) VALUES (?,0)",run.runId()));
            for(var p:initial)attempts.initialPlan(c,p);
            runs.event(c,run.runId(),"INITIAL_COLLECT_ADMITTED",digest);return Decision.APPLIED;
        });
    }
    public List<String> pendingRuns(int limit)throws SQLException{
        if(limit<1||limit>1000)throw new IllegalArgumentException("bounded scan required");
        return tx.run(c->{List<String> result=new ArrayList<>();try(var p=tx.prepare(c,"SELECT a.run_id FROM mf_admission a JOIN mf_run r ON r.run_id=a.run_id WHERE r.phase NOT IN ('SUCCEEDED','CANCELLED') ORDER BY a.accepted_at,a.run_id LIMIT ?",limit);var r=p.executeQuery()){while(r.next())result.add(r.getString(1));}return List.copyOf(result);});
    }
    public Optional<Lease> acquire(String runId,String owner)throws SQLException{
        TransactionContracts.id(runId);TransactionContracts.id(owner);return tx.run(c->{runs.locked(c,runId);
            try(var p=tx.prepare(c,"SELECT * FROM mf_coordinator_lease WHERE run_id=?",runId);var r=p.executeQuery()){
                if(!r.next())throw corrupt();var previous=lease(r);Instant now=clock.instant();
                if(previous.isPresent()&&now.isBefore(previous.get().expiresAt()))return previous.get().owner().equals(owner)?previous:Optional.empty();
                long epoch=Math.addExact(r.getLong("epoch"),1);var next=new Lease(runId,owner,epoch,now.plus(leaseDuration));
                tx.one(tx.update(c,"UPDATE mf_coordinator_lease SET owner_id=?,epoch=?,expires_at=? WHERE run_id=?",owner,epoch,next.expiresAt().toString(),runId));return Optional.of(next);
            }
        });
    }
    public Lease renew(Lease expected)throws SQLException{
        return tx.run(c->{runs.locked(c,expected.runId());requireLease(c,expected);var next=new Lease(expected.runId(),expected.owner(),expected.epoch(),clock.instant().plus(leaseDuration));
            tx.one(tx.update(c,"UPDATE mf_coordinator_lease SET expires_at=? WHERE run_id=?",next.expiresAt().toString(),next.runId()));return next;});
    }
    /** Rebuilds actionable work from durable facts. No polling thread, fresh registry lookup or automatic resubmission. */
    public Snapshot recover(Lease lease)throws SQLException,IOException{
        var snapshot=tx.run(c->{var run=runs.locked(c,lease.runId());requireLease(c,lease);var admitted=admission(c,run);validateInitial(c,admitted);
            var recovery=attempts.recover(c,run.runId());var reservations=reservations(c,Optional.of(run.runId()));
            try(var p=tx.prepare(c,"SELECT stage_type,input_alias,attempt_id FROM mf_attempt WHERE run_id=? AND dispatch_id IS NOT NULL",run.runId());var r=p.executeQuery()){
                while(r.next()){var dispatched=ref(run.runId(),r.getString(1),r.getString(2),r.getString(3));if(reservations.stream().noneMatch(v->v.attempt().equals(dispatched)))throw corrupt();}
            }
            List<Work> work=new ArrayList<>();
            for(var a:recovery.current()){
                var reserved=reservations.stream().filter(v->v.attempt().equals(a.ref())&&!v.released()).findFirst();
                if(a.stopDigest().isPresent()){
                    if(reserved.isPresent())work.add(work(WorkKind.RELEASE_RESOURCES,a));
                    if(!run.cancelRequested()&&!a.completed())work.add(work(a.state()==ExecutionState.SUCCEEDED?WorkKind.RECOVER_EVIDENCE:WorkKind.RETRY_OR_ATTENTION,a));
                }else if(run.cancelRequested()||!clock.instant().isBefore(run.deadline())||a.plan().filter(p->!clock.instant().isBefore(p.deadline())).isPresent())work.add(work(WorkKind.STOP,a));
                else if(!a.completed())work.add(work(a.state()==ExecutionState.PLANNED?WorkKind.DISPATCH:WorkKind.OBSERVE,a));
                else work.add(work(WorkKind.STOP,a));
            }
            // Historical stopped attempts can still hold reservations after a safe current-pointer switch.
            for(var r:reservations)if(!r.released()&&recovery.current().stream().noneMatch(a->a.ref().equals(r.attempt())))work.add(new Work(WorkKind.RELEASE_RESOURCES,Optional.of(r.attempt()),Optional.empty()));
            if(!run.cancelRequested())for(var intent:recovery.intents())if(!intent.cancelled()&&intent.firstAttemptId().isEmpty())work.add(new Work(WorkKind.MATERIALIZE,Optional.empty(),Optional.empty()));
            if(runs.intent(c,run.runId()).isPresent()&&run.phase()!=Phase.SUCCEEDED)work.add(new Work(WorkKind.RECONCILE_PUBLICATION,Optional.empty(),Optional.empty()));
            if(Set.of(Phase.CANCELLED,Phase.SUCCEEDED).contains(run.phase())&&slotActive(c,run.runId()))work.add(new Work(WorkKind.RELEASE_RUN,Optional.empty(),Optional.empty()));
            if(run.phase()==Phase.CANCEL_REQUESTED)work.add(new Work(WorkKind.FINISH_CANCELLATION,Optional.empty(),Optional.empty()));
            work.sort(Comparator.comparingInt(w->priority(w.kind())));
            return new Snapshot(lease,recovery,reservations,work);
        });
        verifyObjects(snapshot);return snapshot;
    }
    /** One bounded coordination step; returned observation/stop/materialization work belongs to trusted adapters. */
    public Step step(Lease lease)throws SQLException,IOException{
        var snapshot=recover(lease);long revision=snapshot.recovery().run().revision();
        for(var work:snapshot.work()){
            switch(work.kind()){
                case RELEASE_RESOURCES->{return new Step(Optional.of(work),release(lease,revision,work.attempt().orElseThrow()),Optional.empty());}
                case FINISH_CANCELLATION->{
                    // Unknown stop work must be processed by the adapter before cancellation is finalized.
                    if(snapshot.work().stream().anyMatch(w->w.kind()==WorkKind.STOP))continue;
                    return new Step(Optional.of(work),finishCancellation(lease,revision),Optional.empty());
                }
                case RELEASE_RUN->{return new Step(Optional.of(work),releaseRun(lease,revision),Optional.empty());}
                case DISPATCH->{var result=dispatch(lease,revision,work.attempt().orElseThrow());if(result.decision()==Decision.NOT_READY)continue;return new Step(Optional.of(work),result.decision(),result.permit());}
                default->{return new Step(Optional.of(work),Decision.NOT_READY,Optional.empty());}
            }
        }
        return new Step(snapshot.work().stream().findFirst(),Decision.NOT_READY,Optional.empty());
    }
    private static int priority(WorkKind kind){return switch(kind){case RELEASE_RESOURCES->0;case FINISH_CANCELLATION->5;case RELEASE_RUN->2;case RECONCILE_PUBLICATION->3;case STOP->4;case RECOVER_EVIDENCE,RETRY_OR_ATTENTION->5;case MATERIALIZE->6;case DISPATCH->7;case OBSERVE->8;};}
    public DispatchResult dispatch(Lease lease,long revision,AttemptRef ref)throws SQLException,IOException{
        if(!lease.runId().equals(ref.key().runId()))throw new IllegalArgumentException("lease/run mismatch");
        // Storage I/O is outside database locks. Revision and lease are rechecked before any permit is issued.
        Snapshot snapshot;
        try{snapshot=recover(lease);}catch(SQLException changed){if("MF002".equals(changed.getSQLState()))return result(Decision.REREAD);throw changed;}
        if(snapshot.recovery().run().revision()!=revision)return result(Decision.REREAD);
        return tx.run(c->{String configured=guard(c);var run=runs.locked(c,ref.key().runId());requireLease(c,lease);
            if(run.revision()!=revision)return result(Decision.REREAD);var admitted=admission(c,run);var row=attempts.current(c,ref.key());
            if(row==null||!row.ref().equals(ref)||run.cancelRequested()||run.phase()!=TransactionContracts.phaseFor(ref.key().type())||row.completed()||row.stopDigest().isPresent())return result(Decision.ARCHIVE_ONLY);
            String dispatchId=stableDispatch(ref);
            if(row.dispatchId().isPresent())return result(row.dispatchId().get().equals(dispatchId)?Decision.ALREADY_APPLIED:Decision.CONFLICT);
            if(configured==null||row.plan().isEmpty()||row.state()!=ExecutionState.PLANNED||!clock.instant().isBefore(row.deadline())||!clock.instant().isBefore(run.deadline()))return result(Decision.NOT_READY);
            var policy=BudgetPolicy.restore(configured);var demand=demand(admitted,row);
            if(demand.isEmpty())return result(Decision.NOT_READY);
            var ceilings=(Map<?,?>)admitted.fields().get("ceilings");
            if(policy.activeRuns()>((Number)admitted.fields().get("activeRunCeiling")).intValue())return result(Decision.NOT_READY);
            for(var entry:policy.limits().entrySet())if(ceilings.containsKey(entry.getKey())&&entry.getValue()>((Number)ceilings.get(entry.getKey())).intValue())return result(Decision.NOT_READY);
            var all=reservations(c,Optional.empty());Map<String,Integer> used=totals(all);
            for(var entry:demand.entrySet())if((long)used.getOrDefault(entry.getKey(),0)+entry.getValue()>policy.limits().getOrDefault(entry.getKey(),0))return result(Decision.NOT_READY);
            boolean active=slotActive(c,run.runId());
            if(!active&&activeRuns(c)>=policy.activeRuns())return result(Decision.NOT_READY);
            if(ref.key().type()==StageIdentity.StageType.COLLECT&&all.stream().filter(r->!r.released()&&r.attempt().key().runId().equals(run.runId())&&r.attempt().key().type()==StageIdentity.StageType.COLLECT).count()>=admitted.inputConcurrency())return result(Decision.NOT_READY);
            if(all.stream().anyMatch(r->r.attempt().equals(ref)))throw corrupt();
            if(!active)tx.one(tx.update(c,"INSERT INTO mf_run_slot(run_id,released) VALUES (?,FALSE)",run.runId()));
            String body=AttemptContracts.encode(demand);
            tx.one(tx.update(c,"INSERT INTO mf_reservation(run_id,stage_type,input_alias,attempt_id,demand_body,demand_digest,released) VALUES (?,?,?,?,?,?,FALSE)",run.runId(),ref.key().type().name(),alias(ref),ref.attemptId(),body,reservationDigest(ref,body)));
            Decision started=attempts.begin(c,revision,ref,dispatchId);if(started!=Decision.APPLIED)throw corrupt();
            runs.event(c,run.runId(),"RESOURCES_RESERVED",reservationDigest(ref,body));
            return new DispatchResult(Decision.APPLIED,Optional.of(new DispatchPermit(lease,row.plan().orElseThrow(),dispatchId,"mf-"+dispatchId.substring(9),demand)));
        });
    }
    public Decision release(Lease lease,long revision,AttemptRef ref)throws SQLException{
        if(!lease.runId().equals(ref.key().runId()))throw new IllegalArgumentException("lease/run mismatch");
        return tx.run(c->{guard(c);var run=runs.locked(c,ref.key().runId());requireLease(c,lease);if(run.revision()!=revision)return Decision.REREAD;
            var reservation=reservations(c,Optional.of(run.runId())).stream().filter(r->r.attempt().equals(ref)).findFirst();
            if(reservation.isEmpty())return Decision.NOT_READY;var row=attempts.read(c,ref);if(row==null)throw corrupt();
            if(row.stopDigest().isEmpty())return Decision.WAIT_STOP;
            if(reservation.get().released())return reservation.get().releaseEvidence().equals(row.stopDigest())?Decision.ALREADY_APPLIED:Decision.CONFLICT;
            tx.one(tx.update(c,"UPDATE mf_reservation SET released=TRUE,release_digest=? WHERE run_id=? AND stage_type=? AND input_alias=? AND attempt_id=? AND released=FALSE",row.stopDigest().get(),run.runId(),ref.key().type().name(),alias(ref),ref.attemptId()));
            bump(c,run);runs.event(c,run.runId(),"RESOURCES_RELEASED",row.stopDigest().get());return Decision.APPLIED;
        });
    }
    private Decision finishCancellation(Lease lease,long revision)throws SQLException{
        return tx.run(c->{runs.locked(c,lease.runId());requireLease(c,lease);return attempts.finishCancellation(c,lease.runId(),revision);});
    }
    public Decision releaseRun(Lease lease,long revision)throws SQLException{
        return tx.run(c->{guard(c);var run=runs.locked(c,lease.runId());requireLease(c,lease);if(run.revision()!=revision)return Decision.REREAD;
            if(!Set.of(Phase.CANCELLED,Phase.SUCCEEDED).contains(run.phase()))return Decision.NOT_READY;
            if(reservations(c,Optional.of(run.runId())).stream().anyMatch(r->!r.released()))return Decision.WAIT_STOP;
            List<AttemptRef> refs=new ArrayList<>();try(var p=tx.prepare(c,"SELECT stage_type,input_alias,attempt_id FROM mf_attempt WHERE run_id=?",run.runId());var r=p.executeQuery()){while(r.next())refs.add(ref(run.runId(),r.getString(1),r.getString(2),r.getString(3)));}
            for(var ref:refs)if(attempts.read(c,ref).stopDigest().isEmpty())return Decision.WAIT_STOP;
            if(!slotActive(c,run.runId()))return Decision.ALREADY_APPLIED;
            tx.one(tx.update(c,"UPDATE mf_run_slot SET released=TRUE WHERE run_id=? AND released=FALSE",run.runId()));bump(c,run);runs.event(c,run.runId(),"RUN_SLOT_RELEASED",run.registrationDigest());return Decision.APPLIED;
        });
    }
    public Usage usage()throws SQLException{
        return tx.run(c->{guard(c);var values=totals(reservations(c,Optional.empty()));Map<String,Integer> connections=new TreeMap<>();values.forEach((key,value)->{if(key.startsWith("connection."))connections.put(key.substring(11),value);});return new Usage(activeRuns(c),values.getOrDefault("applications",0),values.getOrDefault("inline",0),connections);});
    }
    private record Admission(String body,Map<String,Object> fields,List<PlannedAttempt> initial,int inputConcurrency) {}
    private Admission admission(Connection c,JdbcRunStore.RunRow run)throws SQLException{
        try(var p=tx.prepare(c,"SELECT * FROM mf_admission WHERE run_id=?",run.runId());var r=p.executeQuery()){
            if(!r.next())throw corrupt();try{String body=r.getString("content_body");var f=AttemptContracts.decode(body);String digest=admissionDigest(body);
                if(!digest.equals(r.getString("content_digest"))||!digest.equals(run.registrationDigest())||!run.runId().equals(f.get("run"))||!run.resolvedDigest().equals(f.get("runDigest"))||!r.getString("accepted_at").equals(f.get("acceptedAt")))throw corrupt();
                var initial=((List<?>)f.get("plans")).stream().map(s->PlannedAttempt.restore((String)s)).toList();return new Admission(body,f,initial,((Number)f.get("inputConcurrency")).intValue());
            }catch(RuntimeException invalid){throw corrupt();}
        }
    }
    private void validateInitial(Connection c,Admission admission)throws SQLException{
        for(var initial:admission.initial()){var row=attempts.read(c,initial.ref());if(row==null||!row.plan().equals(Optional.of(initial)))throw corrupt();var current=attempts.current(c,initial.ref().key());if(current==null)throw corrupt();}
    }
    private void verifyObjects(Snapshot snapshot)throws SQLException,IOException{
        var admission=tx.run(c->{var run=runs.locked(c,snapshot.lease().runId());requireLease(c,snapshot.lease());if(run.revision()!=snapshot.recovery().run().revision())throw new SQLException("snapshot changed; reread","MF002");return admission(c,run);});
        var f=admission.fields();verify(URI.create((String)f.get("runKey")),(String)f.get("runDigest"),"RESOLVED_RUN_SPEC");var catalog=verify(URI.create((String)f.get("catalogKey")),(String)f.get("catalogDigest"),"PLATFORM_CATALOG");
        verify(URI.create((String)f.get("definitionKey")),(String)f.get("definitionDigest"),"JOB_DEFINITION");
        for(var initial:admission.initial())verifyPlan(initial,admission,(Map<?,?>)catalog.get("runtime"));
        for(var a:snapshot.recovery().current())if(a.plan().isPresent())verifyPlan(a.plan().get(),admission,(Map<?,?>)catalog.get("runtime"));
    }
    private void verifyPlan(PlannedAttempt p,Admission admission,Map<?,?> runtime)throws IOException{
        var content=verify(p.objectKey(),p.planDigest(),"STAGE_PLAN");
        try{
        if(!p.ref().key().runId().equals(content.get("runId"))||!p.ref().key().type().name().equals(content.get("stageType"))
                ||!alias(p.ref()).equals(content.get("inputAlias"))||!p.ref().attemptId().equals(content.get("stageAttemptId"))
                ||!p.resolvedSpecDigest().equals(content.get("resolvedSpecDigest"))||!p.consumedDigest().equals(content.get("consumedDataDigest"))
                ||!p.deadline().equals(Instant.parse((String)content.get("deadline")))||!runtime.equals(content.get("runtime"))
                ||!Objects.equals(admission.fields().get("catalogDigest"),content.get("catalogDigest"))||!Objects.equals(runtime.get("releaseId"),content.get("releaseId")))throw new IOException("immutable plan differs from current attempt/frozen runtime");
        }catch(RuntimeException malformed){throw new IOException("invalid immutable stage plan fields",malformed);}
    }
    private Map<?,?> verify(URI key,String digest,String kind)throws IOException{
        var object=objects.get(key,digest).orElseThrow(()->new IOException("required immutable coordination object missing"));
        var envelope=AttemptContracts.decode(new String(object.canonicalBytes(),java.nio.charset.StandardCharsets.UTF_8));
        if(!(kind.equals("JOB_DEFINITION")?"magic-flink-canonical-v1":"magic-flink-domain-v2").equals(envelope.get("protocol"))||!kind.equals(envelope.get("kind")))throw new IOException("coordination object protocol/kind mismatch");
        return (Map<?,?>)envelope.get("content");
    }
    private Map<String,Integer> demand(Admission admitted,JdbcAttempts.Row row)throws SQLException{
        Map<String,Integer> result=new TreeMap<>();
        switch(row.ref().key().type()){
            case COLLECT->{var raw=(Map<?,?>)((Map<?,?>)admitted.fields().get("demands")).get(alias(row.ref()));if(raw==null||row.executor()!=StageIdentity.Executor.FLINK)throw corrupt();raw.forEach((key,value)->result.put((String)key,((Number)value).intValue()));}
            case COMPUTE->{if(row.executor()!=StageIdentity.Executor.FLINK)throw corrupt();result.put("applications",1);}
            case PRECHECK,RESULT_CHECK->result.put(row.executor()==StageIdentity.Executor.FLINK?"applications":"inline",1);
            // Target write/query concurrency and loadId admission belong to the later target protocol.
            case WRITE,TARGET_CHECK->{return Map.of();}
        }
        return Map.copyOf(result);
    }
    private List<Reservation> reservations(Connection c,Optional<String> runId)throws SQLException{
        if(runId.isEmpty())validateAccounting(c);
        List<Reservation> result=new ArrayList<>();String sql="SELECT * FROM mf_reservation"+(runId.isPresent()?" WHERE run_id=?":"")+" ORDER BY run_id,stage_type,input_alias,attempt_id";
        Object[] values=runId.isPresent()?new Object[]{runId.get()}:new Object[]{};
        try(var p=tx.prepare(c,sql,values);var r=p.executeQuery()){
            while(r.next()){
                var ref=ref(r.getString("run_id"),r.getString("stage_type"),r.getString("input_alias"),r.getString("attempt_id"));String body=r.getString("demand_body");
                if(!reservationDigest(ref,body).equals(r.getString("demand_digest")))throw corrupt();var demand=new TreeMap<String,Integer>();
                try{AttemptContracts.decode(body).forEach((key,value)->{int amount=((Number)value).intValue();if(amount<1)throw new IllegalArgumentException();demand.put(key,amount);});}catch(RuntimeException invalid){throw corrupt();}
                var evidence=Optional.ofNullable(r.getString("release_digest"));
                if(r.getBoolean("released")){var row=attempts.read(c,ref);if(row==null||row.stopDigest().isEmpty()||!row.stopDigest().equals(evidence))throw corrupt();}
                result.add(new Reservation(ref,demand,r.getBoolean("released"),evidence));
            }
        }
        return List.copyOf(result);
    }
    private void validateAccounting(Connection c)throws SQLException{
        try(var p=tx.prepare(c,"SELECT COUNT(*) FROM mf_attempt a JOIN mf_admission m ON m.run_id=a.run_id LEFT JOIN mf_reservation r ON r.run_id=a.run_id AND r.stage_type=a.stage_type AND r.input_alias=a.input_alias AND r.attempt_id=a.attempt_id WHERE a.dispatch_id IS NOT NULL AND r.attempt_id IS NULL");var r=p.executeQuery()){r.next();if(r.getLong(1)>0)throw corrupt();}
        try(var p=tx.prepare(c,"SELECT COUNT(*) FROM mf_reservation r LEFT JOIN mf_run_slot s ON s.run_id=r.run_id WHERE s.run_id IS NULL OR (s.released=TRUE AND r.released=FALSE)");var r=p.executeQuery()){r.next();if(r.getLong(1)>0)throw corrupt();}
    }
    private static Map<String,Integer> totals(List<Reservation> rows){var result=new TreeMap<String,Integer>();for(var row:rows)if(!row.released())row.demand().forEach((key,value)->result.merge(key,value,Math::addExact));return result;}
    private String guard(Connection c)throws SQLException{
        try(var p=tx.prepare(c,"SELECT * FROM mf_control_guard WHERE guard_id=1 FOR UPDATE");var r=p.executeQuery()){
            if(!r.next())throw corrupt();String body=r.getString("policy_body");if(body!=null){if(!policyDigest(body).equals(r.getString("policy_digest")))throw corrupt();try{BudgetPolicy.restore(body);}catch(RuntimeException invalid){throw corrupt();}}return body;
        }
    }
    private void requireLease(Connection c,Lease expected)throws SQLException{
        try(var p=tx.prepare(c,"SELECT * FROM mf_coordinator_lease WHERE run_id=?",expected.runId());var r=p.executeQuery()){
            if(!r.next()||!lease(r).equals(Optional.of(expected))||!clock.instant().isBefore(expected.expiresAt()))throw new SQLException("coordinator lease stale; reacquire and recover","MF006");
        }
    }
    private Optional<Lease> lease(ResultSet r)throws SQLException{String owner=r.getString("owner_id");return owner==null?Optional.empty():Optional.of(new Lease(r.getString("run_id"),owner,r.getLong("epoch"),Instant.parse(r.getString("expires_at"))));}
    private boolean slotActive(Connection c,String run)throws SQLException{try(var p=tx.prepare(c,"SELECT released FROM mf_run_slot WHERE run_id=?",run);var r=p.executeQuery()){return r.next()&&!r.getBoolean(1);}}
    private int activeRuns(Connection c)throws SQLException{try(var p=tx.prepare(c,"SELECT COUNT(*) FROM mf_run_slot WHERE released=FALSE");var r=p.executeQuery()){r.next();return r.getInt(1);}}
    private void bump(Connection c,JdbcRunStore.RunRow run)throws SQLException{tx.one(tx.update(c,"UPDATE mf_run SET revision=revision+1 WHERE run_id=? AND revision=?",run.runId(),run.revision()));}
    private static Work work(WorkKind kind,AttemptView a){return new Work(kind,Optional.of(a.ref()),a.dispatchId());}
    private static DispatchResult result(Decision decision){return new DispatchResult(decision,Optional.empty());}
    private static String alias(AttemptRef ref){return ref.key().inputAlias().orElse("");}
    private static AttemptRef ref(String run,String type,String alias,String attempt){return new AttemptRef(new StageIdentity.StageKey(run,StageIdentity.StageType.valueOf(type),alias.isEmpty()?Optional.empty():Optional.of(alias)),attempt);}
    private static String admissionDigest(String body){return TransactionContracts.hash("RUN_ADMISSION_V1",AttemptContracts.decode(body));}
    private static String policyDigest(String body){return TransactionContracts.hash("QUOTA_POLICY",AttemptContracts.decode(body));}
    private static String reservationDigest(AttemptRef ref,String body){return TransactionContracts.hash("RESERVATION",fields("run",ref.key().runId(),"stage",ref.key().type().name(),"alias",alias(ref),"attempt",ref.attemptId(),"demand",AttemptContracts.decode(body)));}
    private static String stableDispatch(AttemptRef ref){return "dispatch-"+TransactionContracts.hash("DISPATCH",fields("run",ref.key().runId(),"stage",ref.key().type().name(),"alias",alias(ref),"attempt",ref.attemptId())).substring(0,40);}
    private static SQLException corrupt(){return new SQLException("coordination evidence inconsistent","MF004");}
}
