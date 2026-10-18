package io.github.xuxiaotuan.magicflink.control;

import io.github.xuxiaotuan.magicflink.control.AttemptContracts.*;
import io.github.xuxiaotuan.magicflink.control.TransactionContracts.*;
import java.time.Instant;
import java.util.*;
import static io.github.xuxiaotuan.magicflink.control.TransactionContracts.*;

/** Platform-owned quotas and durable coordination. A lease is not external-process fencing. */
public final class CoordinatorContracts {
    private CoordinatorContracts() {}
    public record ConnectionBudget(int total,int probes) {
        public ConnectionBudget {if(total<2||probes<1||probes>=total)throw new IllegalArgumentException("separate positive query/probe budgets required");}
    }
    public record BudgetPolicy(int activeRuns,int applications,int inlineChecks,Map<String,ConnectionBudget> connections) {
        public BudgetPolicy {if(activeRuns<1||applications<1||inlineChecks<1)throw new IllegalArgumentException("finite positive global quotas required");connections=Map.copyOf(connections);connections.keySet().forEach(TransactionContracts::id);}
        public Map<String,Integer> limits(){var m=new TreeMap<String,Integer>();m.put("applications",applications);m.put("inline",inlineChecks);connections.forEach((key,value)->m.put("connection."+key,value.total()-value.probes()));return Collections.unmodifiableMap(m);}
        public String canonical(){var c=new TreeMap<String,Object>();connections.forEach((key,b)->c.put(key,Map.of("total",b.total(),"probes",b.probes())));return AttemptContracts.encode(Map.of("activeRuns",activeRuns,"applications",applications,"inline",inlineChecks,"connections",c));}
        public static BudgetPolicy restore(String body){var m=AttemptContracts.decode(body);var c=new TreeMap<String,ConnectionBudget>();((Map<?,?>)m.get("connections")).forEach((key,value)->{var b=(Map<?,?>)value;c.put((String)key,new ConnectionBudget(((Number)b.get("total")).intValue(),((Number)b.get("probes")).intValue()));});var p=new BudgetPolicy(((Number)m.get("activeRuns")).intValue(),((Number)m.get("applications")).intValue(),((Number)m.get("inline")).intValue(),c);if(!p.canonical().equals(body))throw new IllegalArgumentException("noncanonical policy");return p;}
    }
    public record Lease(String runId,String owner,long epoch,Instant expiresAt) {
        public Lease {id(runId);id(owner);if(epoch<1)throw new IllegalArgumentException("positive fencing epoch required");Objects.requireNonNull(expiresAt);}
    }
    public enum WorkKind { DISPATCH, OBSERVE, STOP, RELEASE_RESOURCES, RECOVER_EVIDENCE, RETRY_OR_ATTENTION, MATERIALIZE, RECONCILE_PUBLICATION, RELEASE_RUN, FINISH_CANCELLATION }
    public record Work(WorkKind kind,Optional<AttemptRef> attempt,Optional<String> dispatchId) {}
    public record Reservation(AttemptRef attempt,Map<String,Integer> demand,boolean released,Optional<String> releaseEvidence) {
        public Reservation {demand=Map.copyOf(demand);}
    }
    public record Snapshot(Lease lease,RecoverySnapshot recovery,List<Reservation> reservations,List<Work> work) {
        public Snapshot {reservations=List.copyOf(reservations);work=List.copyOf(work);}
    }
    public record Usage(int activeRuns,int applications,int inlineChecks,Map<String,Integer> connections) {
        public Usage {connections=Map.copyOf(connections);}
    }
    /** Only APPLIED returns a one-shot permission. Unknown commit outcomes must be observed, never resent. */
    public record DispatchPermit(Lease lease,PlannedAttempt plan,String dispatchId,String resourceName,Map<String,Integer> demand) {
        public DispatchPermit {demand=Map.copyOf(demand);}
    }
    public record Step(Optional<Work> work,Decision decision,Optional<DispatchPermit> permit) {}
    public record DispatchResult(Decision decision,Optional<DispatchPermit> permit) {}
}
