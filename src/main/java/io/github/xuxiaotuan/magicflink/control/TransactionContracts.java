package io.github.xuxiaotuan.magicflink.control;

import io.github.xuxiaotuan.magicflink.protocol.CanonicalJson;
import io.github.xuxiaotuan.magicflink.protocol.ObjectKeys;
import io.github.xuxiaotuan.magicflink.spec.JobSpec.ParameterType;
import java.math.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;

/** Platform-internal transaction inputs; persisted evidence must be verified before calling the JDBC adapters. */
public final class TransactionContracts {
    private TransactionContracts() {}
    public enum Phase { COLLECTING, PRECHECKING, COMPUTING, VALIDATING_RESULT, WRITING, RECONCILING_TARGET, PUBLISHING, CANCEL_REQUESTED, SUCCEEDED, CANCELLED }
    public enum Decision { APPLIED, ALREADY_APPLIED, REREAD, ARCHIVE_ONLY, CONFLICT, NOT_READY, CANCEL_REQUESTED, CANCEL_TOO_LATE, WAIT_STOP }
    public enum CheckStatus { PASS, FAIL, ERROR, SKIPPED }
    public record BusinessKey(String tenant, String canonicalTarget, ParameterType batchType, Object batch) {
        public BusinessKey {
            id(tenant); id(canonicalTarget); Objects.requireNonNull(batchType);
            boolean valid = switch(batchType) { case DATE -> batch instanceof LocalDate; case STRING -> batch instanceof String s && !s.isBlank();
                case INTEGER -> batch instanceof BigInteger; case DECIMAL -> batch instanceof BigDecimal; case BOOLEAN -> batch instanceof Boolean; };
            if (!valid) throw new IllegalArgumentException("typed business batch required");
            if (batch instanceof BigDecimal decimal) batch = decimal.signum() == 0 ? BigDecimal.ZERO : decimal.stripTrailingZeros();
        }
        public String canonical() { return new String(CanonicalJson.encode(Map.of("tenant",tenant,"target",canonicalTarget,"batchType",batchType.name(),"batch",batch)),StandardCharsets.UTF_8); }
        public String digest() { return CanonicalJson.sha256(canonical().getBytes(StandardCharsets.UTF_8)); }
        public static BusinessKey restore(String value) {
            var map=(Map<?,?>)CanonicalJson.decode(value.getBytes(StandardCharsets.UTF_8));
            if(!map.keySet().equals(Set.of("tenant","target","batchType","batch"))) throw new IllegalArgumentException("business key fields mismatch");
            var type=ParameterType.valueOf((String)map.get("batchType")); Object raw=map.get("batch");
            Object batch=switch(type){case DATE -> LocalDate.parse((String)raw); case INTEGER -> new BigInteger(raw.toString());case DECIMAL -> new BigDecimal((String)raw);case STRING,BOOLEAN -> raw;};
            var key=new BusinessKey((String)map.get("tenant"),(String)map.get("target"),type,batch);
            if(!key.canonical().equals(value))throw new IllegalArgumentException("noncanonical business key"); return key;
        }
    }
    public record PublicationBasis(BusinessKey key, String loadId, String candidateDigest, long expectedRevision) {
        public PublicationBasis { Objects.requireNonNull(key);id(loadId);digest(candidateDigest);revision(expectedRevision); }
    }
    public record PublicationRequest(String publicationId,String runId,BusinessKey key,String loadId,String candidateDigest,long expectedRevision) {
        public PublicationRequest {id(publicationId);id(runId);Objects.requireNonNull(key);id(loadId);digest(candidateDigest);revision(expectedRevision);}
        public String requestDigest(){return hash("PUBLICATION_REQUEST",Map.of("publicationId",publicationId,"runId",runId,"businessKey",key.canonical(),"loadId",loadId,"candidateDigest",candidateDigest,"expectedRevision",expectedRevision));}
    }
    /** Only a durable metadata-store authorization may be sent to the target adapter. This DTO is not an authentication token. */
    public record AuthorizedPublication(PublicationRequest request,Instant authorizedAt) {
        public AuthorizedPublication {Objects.requireNonNull(request);Objects.requireNonNull(authorizedAt);}
    }
    public record PublicationReceipt(PublicationRequest request,Optional<String> previousLoadId,long revision,Instant recordedAt) {
        public PublicationReceipt {Objects.requireNonNull(request);Objects.requireNonNull(previousLoadId);Objects.requireNonNull(recordedAt);if(revision!=Math.addExact(request.expectedRevision(),1))throw new IllegalArgumentException("receipt revision mismatch");}
    }
    public record PublicationObservation(PublicationReceipt receipt,boolean currentEffective) {}
    public static final class PublicationConflictException extends java.sql.SQLException {
        public PublicationConflictException(){super("publication identity, accepted version or revision conflict","MF001");}
    }
    public record Registration(String runId,String resolvedSpecDigest,Phase phase,List<StageIdentity> stages,Optional<PublicationBasis> publication,Map<String,String> requiredChecks,Instant deadline,io.github.xuxiaotuan.magicflink.protocol.PlatformDefinitions.RetryPolicy retryPolicy,io.github.xuxiaotuan.magicflink.protocol.PlatformDefinitions.TimeLimits timeLimits) {
        public Registration {
            Objects.requireNonNull(timeLimits);Objects.requireNonNull(retryPolicy);id(runId);digest(resolvedSpecDigest);Objects.requireNonNull(phase);Objects.requireNonNull(publication);Objects.requireNonNull(deadline);
            stages=List.copyOf(stages);requiredChecks=Map.copyOf(requiredChecks);requiredChecks.forEach((key,value)->{id(key);digest(value);});
            if(stages.isEmpty()||stages.size()>32||requiredChecks.size()>64||new HashSet<>(stages.stream().map(StageIdentity::stage).toList()).size()!=stages.size())throw new IllegalArgumentException("unique current stages required");
            for(var stage:stages) if(!stage.stage().runId().equals(runId)||!stage.resolvedSpecDigest().equals(resolvedSpecDigest)||phaseFor(stage.stage().type())!=phase)throw new IllegalArgumentException("stage/run/phase mismatch");
            if(phase==Phase.RECONCILING_TARGET&&(stages.size()!=1||publication.isEmpty()||requiredChecks.isEmpty()))throw new IllegalArgumentException("frozen publication basis and mandatory target checks required");
            if(phase!=Phase.RECONCILING_TARGET&&(publication.isPresent()||!requiredChecks.isEmpty()))throw new IllegalArgumentException("publication context only accepted at target reconciliation");
        }
        public String contentDigest(){return hash("RUN_REGISTRATION_V2",Map.of("runId",runId,"spec",resolvedSpecDigest,"phase",phase.name(),"stages",stages.stream().map(TransactionContracts::identityDigest).sorted().toList(),"publication",publication.map(b->Map.<String,Object>of("key",b.key().canonical(),"load",b.loadId(),"candidate",b.candidateDigest(),"revision",b.expectedRevision())).orElse(Map.of()),"checks",requiredChecks,"deadline",deadline,"retry",Map.of("maxRetries",retryPolicy.maxStageRetries(),"backoff",retryPolicy.backoff().toString()),"time",Map.of("stage",timeLimits.stage().toString(),"run",timeLimits.run().toString(),"wait",timeLimits.evidenceWait().toString(),"poll",timeLimits.poll().toString())));}
    }
    public record RunView(String runId,long revision,Phase phase,boolean cancelRequested) {}
    public record CheckProof(StageIdentity identity,String checkId,String ruleDigest,String candidateDigest,String loadId,CheckStatus status,URI objectKey,String objectDigest) {
        public CheckProof {Objects.requireNonNull(identity);id(checkId);digest(ruleDigest);digest(candidateDigest);id(loadId);Objects.requireNonNull(status);ObjectKeys.file(objectKey);digest(objectDigest);}
        public String contentDigest(){return hash("CHECK_PROOF",Map.of("identity",identityDigest(identity),"id",checkId,"rule",ruleDigest,"candidate",candidateDigest,"load",loadId,"status",status.name(),"object",objectKey.toString(),"digest",objectDigest));}
    }
    public static Phase phaseFor(StageIdentity.StageType type){return switch(type){case COLLECT->Phase.COLLECTING;case PRECHECK->Phase.PRECHECKING;case COMPUTE->Phase.COMPUTING;case RESULT_CHECK->Phase.VALIDATING_RESULT;case WRITE->Phase.WRITING;case TARGET_CHECK->Phase.RECONCILING_TARGET;};}
    public static String identityDigest(StageIdentity identity){return hash("STAGE_IDENTITY",identityFields(identity));}
    public static String identityCanonical(StageIdentity identity){return new String(CanonicalJson.encode(identityFields(identity)),StandardCharsets.UTF_8);}
    public static StageIdentity restoreIdentity(String body){
        var m=(Map<?,?>)CanonicalJson.decode(body.getBytes(StandardCharsets.UTF_8));
        String alias=(String)m.get("alias");var resource=(Map<?,?>)m.get("resource");
        var identity=new StageIdentity(new StageIdentity.StageKey((String)m.get("run"),StageIdentity.StageType.valueOf((String)m.get("type")),alias.isEmpty()?Optional.empty():Optional.of(alias)),
                (String)m.get("attempt"),StageIdentity.Executor.valueOf((String)m.get("executor")),resource.isEmpty()?Optional.empty():Optional.of(new StageIdentity.FlinkResource((String)resource.get("crUid"),(String)resource.get("jobId"))),
                (String)m.get("plan"),(String)m.get("spec"),(String)m.get("consumed"));
        if(!identityCanonical(identity).equals(body))throw new IllegalArgumentException("stored stage fields mismatch");return identity;
    }
    public static String receiptDigest(PublicationReceipt receipt){return hash("PUBLICATION_RECEIPT",Map.of("request",receipt.request().requestDigest(),"previousLoad",receipt.previousLoadId().orElse(""),"revision",receipt.revision(),"recordedAt",receipt.recordedAt()));}
    private static Map<String,Object> identityFields(StageIdentity i){
        id(i.stage().runId());id(i.attemptId());i.stage().inputAlias().ifPresent(TransactionContracts::id);digest(i.planDigest());digest(i.resolvedSpecDigest());digest(i.consumedDataDigest());
        return Map.of("run",i.stage().runId(),"type",i.stage().type().name(),"alias",i.stage().inputAlias().orElse(""),"attempt",i.attemptId(),"executor",i.executor().name(),"resource",i.resource().map(r->Map.of("crUid",r.crUid(),"jobId",r.jobId())).orElse(Map.of()),"plan",i.planDigest(),"spec",i.resolvedSpecDigest(),"consumed",i.consumedDataDigest());
    }
    public static String hash(String kind,Map<String,?> body){return CanonicalJson.sha256(CanonicalJson.encode(Map.of("protocol","magic-flink-transaction-v1","kind",kind,"content",body)));}
    public static void id(String id){if(id==null||!id.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}"))throw new IllegalArgumentException("fixed platform identity required");}
    public static void digest(String digest){if(digest==null||!digest.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("SHA-256 required");}
    public static void revision(long revision){if(revision<0||revision==Long.MAX_VALUE)throw new IllegalArgumentException("revision outside supported range");}
}
