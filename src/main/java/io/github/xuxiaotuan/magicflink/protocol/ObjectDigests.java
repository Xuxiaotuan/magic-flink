package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.control.StageIdentity;
import io.github.xuxiaotuan.magicflink.spec.JobSpec;
import io.github.xuxiaotuan.magicflink.spec.SqlSchema;

import java.math.BigInteger;
import java.net.URI;
import java.time.Instant;
import java.util.*;

/** Versioned content identity, not an authority to seal or publish. Evidence must still be independently verified. */
public final class ObjectDigests {
    private ObjectDigests() {}
    public enum Kind {
        SCHEMA("columns"),
        JOB_DEFINITION("version name decimalRules parameters inputs preChecks sql postChecks output"),
        RESOLVED_RUN_SPEC("runId jobId jobVersion releaseId definitionDigest parameters inputs output"),
        STAGE_PLAN("runId stageType inputAlias stageAttemptId releaseId resolvedSpecDigest consumedDataDigest inputs schemaDigest parallelism outputPrefix plannedAt"),
        COMMIT_EVIDENCE("stageIdentity files writers splits completedAt"),
        MANIFEST("manifestId stageIdentity commitEvidenceDigest schemaDigest sourceVersions files totalRows completedAt retainUntil"),
        STAGE_RECEIPT("stageIdentity status artifactDigest completedAt");
        private final Set<String> fields;
        Kind(String fields) { this.fields = Set.of(fields.split(" ")); }
    }
    /** Stores a snapshot; neither the input map nor returned bytes can mutate the content. */
    public static final class Frozen {
        private final byte[] bytes;
        private final String digest;
        private Frozen(byte[] bytes) { this.bytes = bytes.clone(); this.digest = CanonicalJson.sha256(bytes); }
        public byte[] canonicalBytes() { return bytes.clone(); }
        public String sha256() { return digest; }
    }

    // New domain shapes have a new envelope; canonical-v1 golden vectors remain unchanged.
    static Frozen domain(String kind, Map<String, ?> content) {
        return new Frozen(CanonicalJson.encode(Map.of("protocol", "magic-flink-domain-v2", "kind", kind, "content", content)));
    }
    public static Frozen restoreCanonical(byte[] bytes, String expectedDigest) {
        digest(expectedDigest);
        if (!expectedDigest.equals(CanonicalJson.sha256(bytes))) throw new IllegalArgumentException("stored content digest mismatch");
        Object parsed = CanonicalJson.decode(bytes);
        if (!(parsed instanceof Map<?, ?> envelope) || !envelope.keySet().equals(Set.of("protocol", "kind", "content"))
                || !(envelope.get("content") instanceof Map<?, ?>) || !(envelope.get("kind") instanceof String kind))
            throw new IllegalArgumentException("invalid immutable envelope");
        if ("magic-flink-canonical-v1".equals(envelope.get("protocol"))) Kind.valueOf(kind);
        else if (!"magic-flink-domain-v2".equals(envelope.get("protocol")) || !Set.of("PLATFORM_CATALOG", "RESOLVED_RUN_SPEC", "JDBC_READ_FILTER", "CONSUMED_INPUTS", "SPLIT_READ_PLAN", "STAGE_PLAN", "COMMIT_EVIDENCE", "INPUT_MANIFEST", "CANDIDATE_MANIFEST", "STAGE_RECEIPT", "TERMINAL_OBSERVATION").contains(kind))
            throw new IllegalArgumentException("unknown immutable protocol/kind");
        return new Frozen(bytes);
    }

    public record DatasetVersion(String dataset, String version, String schemaDigest) {
        public DatasetVersion { text(dataset); text(version); digest(schemaDigest); }
        private Map<String, Object> fields(Object batch) {
            return Map.of("dataset", dataset, "version", version, "schemaDigest", schemaDigest, "batch", batch);
        }
    }

    public static Frozen freeze(Kind kind, Map<String, ?> fields) {
        Objects.requireNonNull(kind); Objects.requireNonNull(fields);
        if (!kind.fields.equals(fields.keySet())) throw new IllegalArgumentException("missing or unknown " + kind + " field");
        Map<String, Object> content = new LinkedHashMap<>(fields);
        for (var entry : content.entrySet()) {
            if (entry.getValue() == null) throw new IllegalArgumentException("null content field");
            if (entry.getKey().endsWith("Digest")) digest(entry.getValue());
        }
        if (content.containsKey("stageIdentity")) {
            if (!(content.get("stageIdentity") instanceof StageIdentity identity)) throw new IllegalArgumentException("typed StageIdentity required");
            content.put("stageIdentity", identity(identity));
        }
        for (String time : List.of("plannedAt", "completedAt", "retainUntil"))
            if (content.containsKey(time) && !(content.get(time) instanceof Instant)) throw new IllegalArgumentException("Instant required");
        switch (kind) {
            case SCHEMA -> {
                if (!(content.get("columns") instanceof List<?> columns) || columns.isEmpty()) throw new IllegalArgumentException("ordered schema columns required");
                Set<String> names = new HashSet<>();
                for (Object value : columns) {
                    if (!(value instanceof Map<?, ?> column) || !column.keySet().equals(Set.of("name", "type"))
                            || !names.add(text(column.get("name")))) throw new IllegalArgumentException("invalid schema column");
                    text(column.get("type"));
                }
            }
            case STAGE_PLAN -> {
                if (number(content.get("parallelism")) < 1) throw new IllegalArgumentException("positive parallelism required");
                text(content.get("runId")); text(content.get("stageAttemptId")); text(content.get("releaseId"));
                String stage = text(content.get("stageType"));
                StageIdentity.StageType type = StageIdentity.StageType.valueOf(stage);
                if (!(content.get("inputAlias") instanceof String alias) || (type == StageIdentity.StageType.COLLECT) == alias.isBlank())
                    throw new IllegalArgumentException("only COLLECT has an input alias");
                objectKey(text(content.get("outputPrefix")), true);
                nonemptyMap(content.get("inputs"));
            }
            case COMMIT_EVIDENCE -> normalizeCommit(content);
            case MANIFEST -> {
                text(content.get("manifestId")); nonemptyMap(content.get("sourceVersions"));
                var files = files(content.get("files"), true);
                long rows = 0;
                for (var file : files) rows = Math.addExact(rows, number(file.get("rows")));
                if (rows != number(content.get("totalRows"))) throw new IllegalArgumentException("manifest row total mismatch");
                if (((Instant)content.get("retainUntil")).isBefore((Instant)content.get("completedAt"))) throw new IllegalArgumentException("invalid retention");
                content.put("files", files);
            }
            case STAGE_RECEIPT -> {
                if (!Set.of("SUCCEEDED", "FAILED", "CANCELLED").contains(content.get("status"))) throw new IllegalArgumentException("terminal receipt required");
            }
            case RESOLVED_RUN_SPEC -> {
                text(content.get("runId")); text(content.get("jobId")); text(content.get("releaseId"));
                if (number(content.get("jobVersion")) < 1) throw new IllegalArgumentException("positive job version required");
                nonemptyMap(content.get("inputs")); nonemptyMap(content.get("output"));
                if (!(content.get("parameters") instanceof Map<?, ?>)) throw new IllegalArgumentException("typed parameters required");
            }
            case JOB_DEFINITION -> {
                if (!"v1".equals(content.get("version"))) throw new IllegalArgumentException("unknown spec protocol");
                text(content.get("name")); text(content.get("sql"));
                nonemptyMap(content.get("inputs")); nonemptyMap(content.get("output"));
            }
        }
        return new Frozen(CanonicalJson.encode(Map.of("protocol", "magic-flink-canonical-v1", "kind", kind.name(), "content", content)));
    }

    public static Frozen definition(JobSpec spec) {
        Map<String, Object> parameters = new TreeMap<>();
        spec.parameters().forEach((name, declaration) -> {
            var fields = new LinkedHashMap<String, Object>();
            fields.put("type", declaration.type().name().toLowerCase(Locale.ROOT)); fields.put("required", declaration.required());
            declaration.defaultValue().ifPresent(value -> fields.put("default", value));
            parameters.put(name, fields);
        });
        Map<String, Object> inputs = new TreeMap<>();
        spec.inputs().forEach((alias, input) -> inputs.put(alias, Map.of("dataset", input.dataset(), "batch", input.batch())));
        var fields = new LinkedHashMap<String, Object>();
        fields.put("version", "v1"); fields.put("name", spec.name());
        fields.put("decimalRules", Map.of("precision", spec.decimalRules().precision(), "scale", spec.decimalRules().scale()));
        fields.put("parameters", parameters); fields.put("inputs", inputs); fields.put("preChecks", checks(spec.preChecks()));
        fields.put("sql", spec.sql()); fields.put("postChecks", checks(spec.postChecks()));
        fields.put("output", Map.of("dataset", spec.output().dataset(), "batch", spec.output().batch(), "mode", spec.output().mode()));
        return freeze(Kind.JOB_DEFINITION, fields);
    }

    public static Frozen schema(SqlSchema schema) {
        return freeze(Kind.SCHEMA, Map.of("columns", schema.columns().stream()
                .map(column -> Map.of("name", column.name(), "type", column.type().getLogicalType().asSerializableString())).toList()));
    }

    public static Map<String, Object> typedParameters(JobSpec.BoundJob bound) {
        if (!bound.parameters().keySet().equals(bound.spec().parameters().keySet())) throw new IllegalArgumentException("parameter declaration mismatch");
        Map<String, Object> values = new TreeMap<>();
        bound.parameters().forEach((name, value) -> values.put(name, Map.of("type", bound.spec().parameters().get(name).type().name().toLowerCase(Locale.ROOT), "value", value)));
        return Collections.unmodifiableMap(values);
    }

    public static Frozen resolved(JobSpec.BoundJob bound, String runId, String jobId, long jobVersion, String releaseId,
                                  Map<String, DatasetVersion> inputs, DatasetVersion output) {
        if (!inputs.keySet().equals(bound.spec().inputs().keySet()) || !output.dataset().equals(bound.spec().output().dataset()))
            throw new IllegalArgumentException("resolved dataset aliases do not match definition");
        Map<String, Object> versions = new TreeMap<>();
        inputs.forEach((alias, version) -> {
            if (!version.dataset().equals(bound.spec().inputs().get(alias).dataset())) throw new IllegalArgumentException("dataset mismatch");
            versions.put(alias, version.fields(bound.inputBatches().get(alias)));
        });
        return freeze(Kind.RESOLVED_RUN_SPEC, Map.of("runId", runId, "jobId", jobId, "jobVersion", jobVersion, "releaseId", releaseId,
                "definitionDigest", definition(bound.spec()).sha256(), "parameters", typedParameters(bound), "inputs", versions,
                "output", output.fields(bound.outputBatch())));
    }

    private static List<Map<String, Object>> checks(List<JobSpec.Check> checks) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (var check : checks) {
            Map<String, Object> fields = new LinkedHashMap<>(); fields.put("id", check.id());
            if (check instanceof JobSpec.ReadyCheck ready) {
                fields.put("type", "batch-ready"); fields.put("inputs", ready.inputs()); fields.put("timeoutSeconds", ready.timeout().getSeconds());
            } else if (check instanceof JobSpec.ColumnsCheck columns) {
                fields.put("type", columns.rule().name().toLowerCase(Locale.ROOT).replace('_', '-')); columns.input().ifPresent(i -> fields.put("input", i)); fields.put("columns", columns.columns());
            } else if (check instanceof JobSpec.CountCheck count) {
                fields.put("type", "row-count"); count.input().ifPresent(i -> fields.put("input", i)); fields.put("min", count.min()); fields.put("max", count.max());
            } else if (check instanceof JobSpec.KeySetCheck key) {
                fields.put("type", "key-set-equal"); fields.put("expectedInput", key.expectedInput()); fields.put("columns", key.columns());
            } else if (check instanceof JobSpec.SumCheck sum) {
                fields.put("type", "sum-equal"); fields.put("expectedInput", sum.expectedInput()); fields.put("column", sum.column()); fields.put("tolerance", sum.tolerance());
            } else throw new IllegalArgumentException("unsupported check");
            result.add(fields);
        }
        return result;
    }

    static Map<String, Object> identity(StageIdentity identity) {
        var fields = new LinkedHashMap<String, Object>();
        fields.put("runId", identity.stage().runId()); fields.put("stageType", identity.stage().type().name());
        fields.put("inputAlias", identity.stage().inputAlias().orElse("")); fields.put("stageAttemptId", identity.attemptId());
        fields.put("executor", identity.executor().name());
        identity.resource().ifPresent(resource -> { fields.put("crUid", resource.crUid()); fields.put("flinkJobId", resource.jobId()); });
        digest(identity.planDigest()); digest(identity.resolvedSpecDigest()); digest(identity.consumedDataDigest());
        fields.put("planDigest", identity.planDigest()); fields.put("resolvedSpecDigest", identity.resolvedSpecDigest()); fields.put("consumedDataDigest", identity.consumedDataDigest());
        return fields;
    }

    private static void normalizeCommit(Map<String, Object> content) {
        var files = files(content.get("files"), false);
        Map<String, Long> counts = new HashMap<>();
        files.forEach(file -> counts.merge(text(file.get("writerId")), 1L, Long::sum));
        var writers = records(content.get("writers"), "writerId", Set.of("writerId", "fileCount", "ended"));
        if (writers.isEmpty()) throw new IllegalArgumentException("writer completion evidence required even for zero files");
        for (var writer : writers) {
            String id = text(writer.get("writerId"));
            if (!Boolean.TRUE.equals(writer.get("ended")) || number(writer.get("fileCount")) != counts.getOrDefault(id, 0L))
                throw new IllegalArgumentException("incomplete writer");
            counts.remove(id);
        }
        if (!counts.isEmpty()) throw new IllegalArgumentException("file writer has no end evidence");
        var splits = records(content.get("splits"), "splitId", Set.of("splitId", "evidenceDigest"));
        if (splits.isEmpty()) throw new IllegalArgumentException("explicit split completion required");
        splits.forEach(split -> digest(split.get("evidenceDigest")));
        content.put("files", files); content.put("writers", writers); content.put("splits", splits);
    }

    private static List<Map<String, Object>> files(Object raw, boolean verified) {
        var fields = verified ? Set.of("objectKey", "bytes", "writerId", "sequence", "rows", "sha256") : Set.of("objectKey", "bytes", "writerId", "sequence");
        var files = records(raw, "objectKey", fields);
        Set<List<Object>> commitIds = new HashSet<>();
        for (var file : files) {
            objectKey(text(file.get("objectKey")), false); number(file.get("bytes")); text(file.get("writerId"));
            long sequence = number(file.get("sequence"));
            if (!commitIds.add(List.of(file.get("writerId"), sequence))) throw new IllegalArgumentException("duplicate commit identity");
            if (verified) { number(file.get("rows")); digest(file.get("sha256")); }
        }
        return files;
    }
    private static List<Map<String, Object>> records(Object raw, String sortKey, Set<String> fields) {
        if (!(raw instanceof List<?> list)) throw new IllegalArgumentException("explicit list required");
        TreeMap<String, Map<String, Object>> sorted = new TreeMap<>();
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> record) || !record.keySet().equals(fields)) throw new IllegalArgumentException("invalid record fields");
            Map<String, Object> copy = new LinkedHashMap<>(); record.forEach((k, v) -> copy.put((String) k, v));
            if (sorted.putIfAbsent(text(copy.get(sortKey)), copy) != null) throw new IllegalArgumentException("duplicate record identity");
        }
        return List.copyOf(sorted.values());
    }
    private static void objectKey(String key, boolean prefix) {
        if (prefix) ObjectKeys.prefix(URI.create(key)); else ObjectKeys.file(URI.create(key));
    }
    private static void nonemptyMap(Object value) { if (!(value instanceof Map<?, ?> map) || map.isEmpty()) throw new IllegalArgumentException("nonempty version map required"); }
    private static String text(Object value) { if (!(value instanceof String s) || s.isBlank()) throw new IllegalArgumentException("nonblank identity required"); return s; }
    private static void digest(Object value) { if (!text(value).matches("[0-9a-f]{64}")) throw new IllegalArgumentException("lowercase SHA-256 required"); }
    private static long number(Object value) {
        if (!(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long || value instanceof BigInteger)) throw new IllegalArgumentException("nonnegative integer required");
        long number = new BigInteger(value.toString()).longValueExact();
        if (number < 0) throw new IllegalArgumentException("nonnegative integer required");
        return number;
    }
}
