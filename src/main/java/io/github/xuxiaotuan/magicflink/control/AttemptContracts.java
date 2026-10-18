package io.github.xuxiaotuan.magicflink.control;

import io.github.xuxiaotuan.magicflink.protocol.CanonicalJson;
import io.github.xuxiaotuan.magicflink.protocol.ObjectKeys;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import static io.github.xuxiaotuan.magicflink.control.TransactionContracts.*;

/** Durable control-plane facts. External stop/dispatcher observations must come from trusted adapters. */
public final class AttemptContracts {
    private AttemptContracts() {}
    public enum ExecutionState { PLANNED, SUBMITTING, RUNNING, SUCCEEDED, FAILED, CANCELLED }
    public enum DispatchObservation { UNKNOWN, DRAINED }
    public enum ExecutionObservation { UNKNOWN, RUNNING, SUCCEEDED, FAILED, CANCELLED, NOT_CREATED }
    public enum ConnectionObservation { UNKNOWN, OPEN, CLOSED }
    public enum RetryReason { NONE, TRANSIENT, EVIDENCE_LOST, INLINE_LIMIT, PERMANENT }
    public record AttemptRef(StageIdentity.StageKey key,String attemptId) {
        public AttemptRef {Objects.requireNonNull(key);id(key.runId());key.inputAlias().ifPresent(TransactionContracts::id);id(attemptId);}
    }
    public record PlannedAttempt(AttemptRef ref,StageIdentity.Executor executor,URI objectKey,String planDigest,String resolvedSpecDigest,String consumedDigest,Instant deadline) {
        public PlannedAttempt {
            Objects.requireNonNull(ref);Objects.requireNonNull(executor);ObjectKeys.file(objectKey);digest(planDigest);digest(resolvedSpecDigest);digest(consumedDigest);Objects.requireNonNull(deadline);
            String role=switch(ref.key().type()){case COLLECT->"inputs/"+ref.key().inputAlias().orElseThrow();case COMPUTE->"compute";case WRITE->"write";default->"checks";};
            if(!objectKey.getPath().endsWith("/runs/"+ref.key().runId()+"/"+role+"/"+ref.attemptId()+"/plan.json"))throw new IllegalArgumentException("plan key must belong to this attempt");
        }
        public String canonical(){return encode(fields("run",ref.key().runId(),"type",ref.key().type().name(),"alias",ref.key().inputAlias().orElse(""),"attempt",ref.attemptId(),"executor",executor.name(),"object",objectKey.toString(),"plan",planDigest,"spec",resolvedSpecDigest,"consumed",consumedDigest,"deadline",deadline));}
        public static PlannedAttempt restore(String body){
            var m=decode(body);var key=new StageIdentity.StageKey((String)m.get("run"),StageIdentity.StageType.valueOf((String)m.get("type")),optional((String)m.get("alias")));
            var result=new PlannedAttempt(new AttemptRef(key,(String)m.get("attempt")),StageIdentity.Executor.valueOf((String)m.get("executor")),URI.create((String)m.get("object")),(String)m.get("plan"),(String)m.get("spec"),(String)m.get("consumed"),Instant.parse((String)m.get("deadline")));
            if(!result.canonical().equals(body))throw new IllegalArgumentException("plan fields mismatch");return result;
        }
        public Optional<StageIdentity> inlineIdentity(){return executor==StageIdentity.Executor.INLINE?Optional.of(new StageIdentity(ref.key(),ref.attemptId(),executor,Optional.empty(),planDigest,resolvedSpecDigest,consumedDigest)):Optional.empty();}
    }
    public record StopEvidence(AttemptRef ref,String planDigest,Optional<StageIdentity> identity,Optional<String> dispatchId,DispatchObservation dispatch,
                               ExecutionObservation execution,ConnectionObservation connections,RetryReason reason,Instant observedAt,URI objectKey,String objectDigest) {
        public StopEvidence {
            Objects.requireNonNull(ref);digest(planDigest);Objects.requireNonNull(identity);Objects.requireNonNull(dispatchId);dispatchId.ifPresent(TransactionContracts::id);
            Objects.requireNonNull(dispatch);Objects.requireNonNull(execution);Objects.requireNonNull(connections);Objects.requireNonNull(reason);Objects.requireNonNull(observedAt);ObjectKeys.file(objectKey);digest(objectDigest);
            identity.ifPresent(i->{if(!new AttemptRef(i.stage(),i.attemptId()).equals(ref)||!i.planDigest().equals(planDigest))throw new IllegalArgumentException("stop identity mismatch");});
        }
        public boolean confirmed(){return dispatch==DispatchObservation.DRAINED&&connections==ConnectionObservation.CLOSED&&Set.of(ExecutionObservation.SUCCEEDED,ExecutionObservation.FAILED,ExecutionObservation.CANCELLED,ExecutionObservation.NOT_CREATED).contains(execution);}
        public String canonical(){return encode(fields("run",ref.key().runId(),"type",ref.key().type().name(),"alias",ref.key().inputAlias().orElse(""),"attempt",ref.attemptId(),"plan",planDigest,"identity",identity.map(TransactionContracts::identityCanonical).orElse(""),"dispatchId",dispatchId.orElse(""),"dispatch",dispatch.name(),"execution",execution.name(),"connections",connections.name(),"reason",reason.name(),"observedAt",observedAt,"object",objectKey.toString(),"objectDigest",objectDigest));}
        public String contentDigest(){return hash("STOP_EVIDENCE",decode(canonical()));}
        public static StopEvidence restore(String body){
            var m=decode(body);String identity=(String)m.get("identity");
            var key=new StageIdentity.StageKey((String)m.get("run"),StageIdentity.StageType.valueOf((String)m.get("type")),optional((String)m.get("alias")));
            var result=new StopEvidence(new AttemptRef(key,(String)m.get("attempt")),(String)m.get("plan"),identity.isEmpty()?Optional.empty():Optional.of(TransactionContracts.restoreIdentity(identity)),optional((String)m.get("dispatchId")),DispatchObservation.valueOf((String)m.get("dispatch")),ExecutionObservation.valueOf((String)m.get("execution")),ConnectionObservation.valueOf((String)m.get("connections")),RetryReason.valueOf((String)m.get("reason")),Instant.parse((String)m.get("observedAt")),URI.create((String)m.get("object")),(String)m.get("objectDigest"));
            if(!result.canonical().equals(body))throw new IllegalArgumentException("stop fields mismatch");return result;
        }
    }
    public record AttemptView(AttemptRef ref,int ordinal,ExecutionState state,boolean completed,Optional<StageIdentity> identity,Optional<PlannedAttempt> plan,
                              Optional<String> dispatchId,Optional<String> stopDigest,Optional<String> replacedBy,Instant createdAt) {}
    public record NextIntent(StageIdentity.StageKey key,String consumedDigest,Optional<String> firstAttemptId,boolean cancelled) {}
    public record RecoverySnapshot(RunView run,List<AttemptView> current,List<NextIntent> intents) {
        public RecoverySnapshot {current=List.copyOf(current);intents=List.copyOf(intents);}
    }
    public static String encode(Map<String,?> fields){return new String(CanonicalJson.encode(fields),StandardCharsets.UTF_8);}
    public static Map<String,Object> decode(String body){Map<String,Object> result=new LinkedHashMap<>();((Map<?,?>)CanonicalJson.decode(body.getBytes(StandardCharsets.UTF_8))).forEach((key,value)->result.put((String)key,value));return Collections.unmodifiableMap(result);}
    public static Map<String,Object> fields(Object...pairs){Map<String,Object> m=new LinkedHashMap<>();for(int i=0;i<pairs.length;i+=2)m.put((String)pairs[i],pairs[i+1]);return m;}
    private static Optional<String> optional(String value){return value.isEmpty()?Optional.empty():Optional.of(value);}
}
