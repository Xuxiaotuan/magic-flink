package io.github.xuxiaotuan.magicflink.protocol;

import java.util.*;
import static io.github.xuxiaotuan.magicflink.protocol.PlatformDefinitions.*;
import static io.github.xuxiaotuan.magicflink.protocol.FileStageProtocol.*;

/** Explicit field mapping: no Java serialization or reflection and no clock-dependent fields. */
public final class DomainSnapshots {
    private DomainSnapshots() {}
    private static final Comparator<WriterSlot> SLOT_ORDER = Comparator.comparing(WriterSlot::operatorId).thenComparingInt(WriterSlot::subtask);
    private static final Comparator<WriterAttempt> ATTEMPT_ORDER = Comparator.comparing(WriterAttempt::slot, SLOT_ORDER).thenComparingInt(WriterAttempt::attempt);
    public static ObjectDigests.Frozen catalog(Catalog catalog) {
        return ObjectDigests.domain("PLATFORM_CATALOG", fields(
                "connections", catalog.connections().stream().sorted(Comparator.comparing(c -> c.ref().id() + "/" + c.ref().version())).map(DomainSnapshots::connection).toList(),
                "inputs", catalog.inputs().stream().sorted(Comparator.comparing(d -> d.ref().id() + "/" + d.ref().version())).map(DomainSnapshots::dataset).toList(),
                "output", output(catalog.output()), "runtime", runtime(catalog.runtime())));
    }
    public static ObjectDigests.Frozen filter(JdbcDatasetDefinition dataset, Object batch) {
        batchValue(dataset.batchType(), batch);
        return ObjectDigests.domain("JDBC_READ_FILTER", fields("dataset", ref(dataset.ref()), "table", dataset.table(), "batchColumn", dataset.batchColumn(),
                "batch", fields("type", dataset.batchType().name(), "value", batch), "projection", dataset.read().projection(), "splitColumn", dataset.read().splitColumn(), "predicate", "BOUND_BATCH_EQUALITY"));
    }
    public static ObjectDigests.Frozen run(ResolvedRun run) {
        return ObjectDigests.domain("RESOLVED_RUN_SPEC", fields("runId", run.runId(), "jobId", run.jobId(), "jobVersion", run.jobVersion(), "subject", run.subject(),
                "definitionDigest", ObjectDigests.definition(run.job().spec()).sha256(), "parameters", ObjectDigests.typedParameters(run.job()), "batches", run.job().inputBatches(),
                "outputBatch", run.job().outputBatch(), "inputVersions", run.inputVersions().entrySet().stream().sorted(Map.Entry.comparingByKey()).map(e -> fields("alias", e.getKey(), "ref", ref(e.getValue()))).toList(),
                "outputVersion", ref(run.outputVersion()), "catalogDigest", catalog(run.catalog()).sha256(), "releaseId", run.catalog().runtime().releaseId(), "acceptedAt", run.acceptedAt(), "runDeadline", run.runDeadline()));
    }
    public static ObjectDigests.Frozen plan(Plan p) {
        return ObjectDigests.domain("STAGE_PLAN", fields("runId", p.key().runId(), "stageType", p.key().type().name(), "inputAlias", p.key().inputAlias().orElse(""),
                "stageAttemptId", p.attemptId(), "releaseId", p.releaseId(), "catalogDigest", p.catalogDigest(), "resolvedSpecDigest", p.resolvedSpecDigest(), "consumedDataDigest", p.consumedDataDigest(),
                "runtime", runtime(p.runtime()), "schema", schemaFields(p.outputSchema()), "schemaDigest", ObjectDigests.schema(p.outputSchema()).sha256(),
                "stagePrefix", p.stagePrefix().toString(), "dataPrefix", p.dataPrefix().toString(), "inputs", p.inputs().stream().sorted(Comparator.comparing(InputSnapshot::alias)).map(DomainSnapshots::input).toList(),
                "writers", p.expectedWriters().stream().sorted(SLOT_ORDER).map(DomainSnapshots::slot).toList(), "splits", p.splits().stream().sorted(Comparator.comparing(Split::id)).map(DomainSnapshots::split).toList(),
                "plannedAt", p.plannedAt(), "deadline", p.deadline(), "runDeadline", p.runDeadline(), "retainUntil", p.retainUntil()));
    }
    public static ObjectDigests.Frozen inputs(List<InputSnapshot> inputs) {
        return ObjectDigests.domain("CONSUMED_INPUTS", fields("inputs", inputs.stream().sorted(Comparator.comparing(InputSnapshot::alias)).map(DomainSnapshots::input).toList()));
    }
    public static ObjectDigests.Frozen readPlan(Plan plan, Split split) {
        return ObjectDigests.domain("SPLIT_READ_PLAN", fields("stagePlanDigest", plan(plan).sha256(), "split", split(split)));
    }
    public static ObjectDigests.Frozen terminal(TerminalObservation observation) {
        return ObjectDigests.domain("TERMINAL_OBSERVATION", fields("stageIdentity", ObjectDigests.identity(observation.identity()), "generation", observation.generation(),
                "status", observation.status().name(), "completedAt", observation.completedAt(), "observedAt", observation.observedAt()));
    }
    public static ObjectDigests.Frozen evidence(VerifiedCommit verified) {
        var e = verified.evidence(); var accepted = verified.accepted();
        return ObjectDigests.domain("COMMIT_EVIDENCE", fields("stageIdentity", ObjectDigests.identity(e.identity()), "completedAt", e.completedAt(),
                "files", e.files().stream().sorted(Comparator.comparing(f -> f.objectKey().toString())).map(DomainSnapshots::file).toList(),
                "writers", e.writers().stream().sorted(Comparator.comparing(WriterEnd::writer, ATTEMPT_ORDER)).map(w -> fields("writer", writer(w.writer()), "fileCount", w.fileCount(), "ended", true)).toList(),
                "splits", e.splits().stream().sorted(Comparator.comparing(SplitCompletion::splitId)).map(s -> fields("splitId", s.splitId(), "sourceVersion", s.sourceVersion(), "rowsRead", s.rowsRead(), "readEvidenceDigest", s.readEvidenceDigest(), "consumedReadDigest", s.consumedReadDigest())).toList(),
                "acceptedAttempts", accepted.attempts().entrySet().stream().sorted(Map.Entry.comparingByKey(SLOT_ORDER)).map(a -> fields("slot", slot(a.getKey()), "attempt", a.getValue())).toList(),
                "nativeFinalityDigest", accepted.finalityDigest()));
    }
    public static ObjectDigests.Frozen manifest(Manifest manifest) {
        var verified = manifest.commit(); var plan = verified.plan();
        return ObjectDigests.domain(plan.key().type() == io.github.xuxiaotuan.magicflink.control.StageIdentity.StageType.COLLECT ? "INPUT_MANIFEST" : "CANDIDATE_MANIFEST",
                fields("manifestId", manifest.id(), "stageIdentity", ObjectDigests.identity(verified.evidence().identity()), "releaseId", plan.releaseId(),
                        "commitEvidenceDigest", evidence(verified).sha256(), "schema", schemaFields(plan.outputSchema()), "schemaDigest", ObjectDigests.schema(plan.outputSchema()).sha256(),
                        "sourceSnapshots", plan.inputs().stream().sorted(Comparator.comparing(InputSnapshot::alias)).map(DomainSnapshots::input).toList(),
                        "files", manifest.files().stream().sorted(Comparator.comparing(f -> f.file().objectKey().toString())).map(f -> fields("commit", file(f.file()), "rows", f.rows(), "sha256", f.sha256(), "schemaDigest", f.schemaDigest())).toList(),
                        "terminal", fields("objectKey", manifest.terminal().orElseThrow().objectKey().toString(), "digest", manifest.terminal().orElseThrow().digest()),
                        "totalRows", manifest.totalRows(), "plannedAt", plan.plannedAt(), "completedAt", manifest.terminal().orElseThrow().observation().completedAt(), "retainUntil", plan.retainUntil()));
    }
    public static ObjectDigests.Frozen receipt(Manifest manifest) {
        return ObjectDigests.domain("STAGE_RECEIPT", fields("stageIdentity", ObjectDigests.identity(manifest.commit().evidence().identity()), "status", "SUCCEEDED",
                "manifestId", manifest.id(), "artifactDigest", manifest(manifest).sha256(), "commitEvidenceDigest", evidence(manifest.commit()).sha256(), "terminalDigest", manifest.terminal().orElseThrow().digest(), "completedAt", manifest.terminal().orElseThrow().observation().completedAt()));
    }
    private static Map<String, Object> connection(ConnectionProfile c) {
        return fields("ref", ref(c.ref()), "physicalId", c.physicalId(), "type", "POSTGRESQL", "endpoint", c.endpoint().toString(),
                "credentials", fields("provider", c.credentials().provider(), "reference", c.credentials().reference()), "tlsRequired", c.tlsRequired(),
                "limits", fields("total", c.limits().total(), "probes", c.limits().probes(), "fetchSize", c.limits().fetchSize(), "queryTimeout", c.limits().queryTimeout().toString()),
                "resources", c.resources().stream().sorted().toList(), "grants", c.grants().stream().map(Enum::name).sorted().toList());
    }
    private static Map<String, Object> dataset(JdbcDatasetDefinition d) {
        return fields("ref", ref(d.ref()), "connection", ref(d.connection()), "table", d.table(), "schema", schemaFields(d.schema()), "batchColumn", d.batchColumn(),
                "batchType", d.batchType().name(), "timezone", d.timezone().getId(), "ready", fields("markerTable", d.ready().markerTable(), "batchColumn", d.ready().batchColumn(),
                        "versionColumn", d.ready().versionColumn(), "rowCountColumn", d.ready().rowCountColumn(), "completedAtColumn", d.ready().completedAtColumn(), "immutableAfterReady", d.ready().immutableAfterReady()),
                "read", fields("bounded", d.read().bounded(), "stableInputVersion", d.read().stableInputVersion(), "replayable", d.read().replayable(), "projection", d.read().projection(),
                        "splitColumn", d.read().splitColumn(), "maxConnections", d.read().maxConnections()), "requiredChecks", rules(d.requiredChecks()), "allowEmpty", d.allowEmpty(), "retention", d.retention().toString(), "subjects", d.subjects().stream().sorted().toList());
    }
    private static Map<String, Object> output(OutputDatasetDefinition d) {
        return fields("ref", ref(d.ref()), "writeConnection", ref(d.writeConnection()), "publishConnection", ref(d.publishConnection()), "versionTable", d.versionTable(),
                "pointerTable", d.pointerTable(), "historyTable", d.historyTable(), "stateTable", d.stateTable(), "formalView", d.formalView(), "schema", schemaFields(d.schema()),
                "metadata", fields("batchColumn", d.metadata().batchColumn(), "loadIdColumn", d.metadata().loadIdColumn(), "publicationIdColumn", d.metadata().publicationIdColumn(), "revisionColumn", d.metadata().revisionColumn()),
                "requiredChecks", rules(d.requiredChecks()), "batchType", d.batchType().name(), "businessKeys", d.businessKeys(), "compareColumns", d.compareColumns(), "mode", d.mode(), "retention", d.retention().toString(), "subjects", d.subjects().stream().sorted().toList());
    }
    private static List<Map<String, Object>> rules(List<io.github.xuxiaotuan.magicflink.spec.JobSpec.Check> checks) {
        return checks.stream().sorted(Comparator.comparing(io.github.xuxiaotuan.magicflink.spec.JobSpec.Check::id)).map(check -> {
            if (check instanceof io.github.xuxiaotuan.magicflink.spec.JobSpec.ColumnsCheck c)
                return fields("id", c.id(), "type", c.rule().name(), "columns", c.columns());
            var c = (io.github.xuxiaotuan.magicflink.spec.JobSpec.CountCheck) check;
            return fields("id", c.id(), "type", "ROW_COUNT", "min", c.min(), "max", c.max());
        }).toList();
    }
    private static Map<String, Object> runtime(RuntimeProfile r) {
        return fields("ref", ref(r.ref()), "releaseId", r.releaseId(), "storageScope", r.storageScope().toString(),
                "compute", fields("jmMiB", r.compute().jmMiB(), "tmMiB", r.compute().tmMiB(), "slots", r.compute().slots(), "parallelism", r.compute().parallelism(), "cpuLimitMilli", r.compute().cpuLimitMilli()),
                "concurrency", fields("activeRuns", r.concurrency().activeRuns(), "applications", r.concurrency().applications(), "inputsPerRun", r.concurrency().inputsPerRun(), "checkThreads", r.concurrency().checkThreads(), "verifyThreads", r.concurrency().verifyThreads()),
                "inline", fields("rows", r.inline().rows(), "bytes", r.inline().bytes(), "timeout", r.inline().timeout().toString()),
                "time", fields("stage", r.time().stage().toString(), "run", r.time().run().toString(), "evidenceWait", r.time().evidenceWait().toString(), "poll", r.time().poll().toString()),
                "retry", fields("maxStageRetries", r.retry().maxStageRetries(), "backoff", r.retry().backoff().toString()), "verifyBufferBytes", r.verifyBufferBytes(), "retention", r.retention().toString());
    }
    private static Map<String, Object> input(InputSnapshot i) {
        var fields = fields("alias", i.alias(), "dataset", ref(i.dataset()), "schema", schemaFields(i.schema()), "schemaDigest", ObjectDigests.schema(i.schema()).sha256(),
                "batch", fields("type", i.batchType().name(), "value", i.batch()), "sourceVersion", i.sourceVersion(), "readinessDigest", i.readinessDigest(), "projection", i.projection(),
                "filterDigest", i.filterDigest(), "timezone", i.timezone().getId(), "coverage", bounds(i.coverage()), "nullPartitionRequired", i.nullPartitionRequired(), "expectedRows", i.expectedRows());
        i.tokenExpiresAt().ifPresent(time -> fields.put("tokenExpiresAt", time));
        i.manifest().ifPresent(m -> fields.put("manifest", fields("id", m.id(), "digest", m.digest(), "producer", ObjectDigests.identity(m.producer()), "schemaDigest", m.schemaDigest(), "rows", m.rows(), "retainUntil", m.retainUntil(), "files", m.files().stream().map(Object::toString).sorted().toList())));
        return fields;
    }
    private static Map<String, Object> slot(WriterSlot s) { return fields("operatorId", s.operatorId(), "subtask", s.subtask()); }
    private static Map<String, Object> writer(WriterAttempt w) { return fields("slot", slot(w.slot()), "attempt", w.attempt()); }
    private static Map<String, Object> file(CommittedFile f) { return fields("objectKey", f.objectKey().toString(), "bytes", f.bytes(), "writer", writer(f.writer()), "sequence", f.sequence()); }
    private static Map<String, Object> split(Split s) { var f = fields("id", s.id(), "inputAlias", s.inputAlias()); s.range().ifPresent(b -> f.put("range", bounds(b))); s.file().ifPresent(key -> f.put("file", key.toString())); return f; }
    private static Map<String, Object> bounds(Bounds b) { return fields("lowerInclusive", b.lowerInclusive(), "upperExclusive", b.upperExclusive()); }
    private static Map<String, Object> fields(Object... values) {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) fields.put((String) values[i], Objects.requireNonNull(values[i + 1]));
        return fields;
    }
}
