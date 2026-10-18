package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.control.StageIdentity;
import io.github.xuxiaotuan.magicflink.spec.JobSpec;
import io.github.xuxiaotuan.magicflink.spec.SqlSchema;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import static io.github.xuxiaotuan.magicflink.protocol.PlatformDefinitions.*;

/** Complete P0 file-stage contract. Trusted native observations and current-attempt CAS are separate obligations. */
public final class FileStageProtocol {
    private FileStageProtocol() {}
    public record Bounds(BigInteger lowerInclusive, BigInteger upperExclusive) {
        public Bounds { Objects.requireNonNull(lowerInclusive); Objects.requireNonNull(upperExclusive); if (lowerInclusive.compareTo(upperExclusive) > 0) throw new IllegalArgumentException("inverted range"); }
    }
    public record ManifestRef(String id, String digest, StageIdentity producer, String schemaDigest, long rows, Instant retainUntil, List<URI> files) {
        public ManifestRef {
            pathId(id); PlatformDefinitions.digest(digest); PlatformDefinitions.digest(schemaDigest); nonnegative(rows); Objects.requireNonNull(retainUntil); Objects.requireNonNull(producer);
            files = List.copyOf(files); files.forEach(ObjectKeys::file);
            if (new HashSet<>(files).size() != files.size() || (files.isEmpty() && rows != 0) || producer.stage().type() != StageIdentity.StageType.COLLECT)
                throw new IllegalArgumentException("explicit input-manifest files and COLLECT identity required");
        }
    }
    public record InputSnapshot(String alias, Ref dataset, SqlSchema schema, JobSpec.ParameterType batchType, Object batch,
                                 String sourceVersion, String readinessDigest, List<String> projection, String filterDigest, ZoneId timezone,
                                 Bounds coverage, boolean nullPartitionRequired, long expectedRows, Optional<Instant> tokenExpiresAt,
                                 Optional<ManifestRef> manifest) {
        public InputSnapshot {
            identifier(alias); Objects.requireNonNull(dataset); PlatformDefinitions.schema(schema); Objects.requireNonNull(batchType);
            batchValue(batchType, batch); text(sourceVersion); digest(readinessDigest); digest(filterDigest); Objects.requireNonNull(timezone); Objects.requireNonNull(coverage);
            projection = names(projection); projection.forEach(name -> column(schema, name)); nonnegative(expectedRows);
            Objects.requireNonNull(tokenExpiresAt); Objects.requireNonNull(manifest);
            if (coverage.lowerInclusive().equals(coverage.upperExclusive()) && expectedRows != 0 && !nullPartitionRequired) throw new IllegalArgumentException("empty coverage cannot contain nonnull records");
            if (manifest.isPresent() && (!manifest.get().schemaDigest().equals(ObjectDigests.schema(schema).sha256()) || manifest.get().rows() != expectedRows))
                throw new IllegalArgumentException("manifest reference/schema/count mismatch");
        }
    }
    public record WriterSlot(String operatorId, int subtask) {
        public WriterSlot { identifier(operatorId); nonnegative(subtask); }
    }
    public record WriterAttempt(WriterSlot slot, int attempt) {
        public WriterAttempt { Objects.requireNonNull(slot); nonnegative(attempt); }
    }
    /** Empty range/file means a NULL partition for COLLECT, or an explicit empty input for COMPUTE. */
    public record Split(String id, String inputAlias, Optional<Bounds> range, Optional<URI> file) {
        public Split(String id, String inputAlias, Optional<Bounds> range) { this(id, inputAlias, range, Optional.empty()); }
        public Split {
            identifier(id); identifier(inputAlias); Objects.requireNonNull(range); Objects.requireNonNull(file); file.ifPresent(ObjectKeys::file);
            if (range.isPresent() && file.isPresent()) throw new IllegalArgumentException("split cannot be both range and file");
        }
    }
    public record Plan(StageIdentity.StageKey key, String attemptId, String releaseId, String catalogDigest, String resolvedSpecDigest,
                        String consumedDataDigest, RuntimeProfile runtime, SqlSchema outputSchema, List<InputSnapshot> inputs,
                        List<WriterSlot> expectedWriters, List<Split> splits, Instant plannedAt, Instant deadline, Instant runDeadline, Instant retainUntil) {
        public Plan {
            Objects.requireNonNull(key); pathId(key.runId()); key.inputAlias().ifPresent(PlatformDefinitions::identifier); pathId(attemptId); text(releaseId);
            digest(catalogDigest); digest(resolvedSpecDigest); digest(consumedDataDigest); Objects.requireNonNull(runtime); PlatformDefinitions.schema(outputSchema);
            inputs = List.copyOf(inputs); expectedWriters = List.copyOf(expectedWriters); splits = List.copyOf(splits);
            Objects.requireNonNull(plannedAt); Objects.requireNonNull(deadline); Objects.requireNonNull(runDeadline); Objects.requireNonNull(retainUntil);
            if (!Set.of(StageIdentity.StageType.COLLECT, StageIdentity.StageType.COMPUTE).contains(key.type()) || !releaseId.equals(runtime.releaseId()))
                throw new IllegalArgumentException("file stage/release mismatch");
            if (!deadline.isAfter(plannedAt) || deadline.isAfter(plannedAt.plus(runtime.time().stage())) || deadline.isAfter(runDeadline)
                    || runDeadline.isAfter(plannedAt.plus(runtime.time().run())) || retainUntil.isBefore(runDeadline.plus(runtime.retention())))
                throw new IllegalArgumentException("stage time/retention budget mismatch");
            if (inputs.isEmpty() || inputs.size() > 32 || inputs.stream().map(InputSnapshot::alias).distinct().count() != inputs.size()) throw new IllegalArgumentException("unique fixed inputs required");
            if (key.type() == StageIdentity.StageType.COLLECT && (inputs.size() != 1 || !inputs.get(0).alias().equals(key.inputAlias().orElseThrow()))) throw new IllegalArgumentException("COLLECT requires its own logical input");
            for (var input : inputs) {
                if (key.type() == StageIdentity.StageType.COLLECT) {
                    if (input.manifest().isPresent() || input.tokenExpiresAt().filter(t -> t.isBefore(deadline)).isPresent()) throw new IllegalArgumentException("source version not valid for the collection budget");
                } else if (input.manifest().isEmpty() || input.manifest().get().retainUntil().isBefore(deadline)
                        || !input.manifest().get().producer().stage().runId().equals(key.runId())
                        || !input.manifest().get().producer().stage().inputAlias().orElseThrow().equals(input.alias())
                        || !input.manifest().get().producer().resolvedSpecDigest().equals(resolvedSpecDigest)) throw new IllegalArgumentException("COMPUTE requires retained same-run input manifests");
                if (key.type() == StageIdentity.StageType.COMPUTE) {
                    var producer = input.manifest().orElseThrow().producer();
                    pathId(producer.attemptId());
                    URI prefix = ObjectKeys.prefix(URI.create(runtime.storageScope() + "runs/" + key.runId() + "/inputs/" + input.alias() + "/" + producer.attemptId() + "/data/"));
                    if (input.manifest().orElseThrow().files().stream().anyMatch(file -> !ObjectKeys.contains(prefix, file)))
                        throw new IllegalArgumentException("input manifest contains files outside its producer attempt");
                }
            }
            if (expectedWriters.size() != runtime.compute().parallelism() || new HashSet<>(expectedWriters).size() != expectedWriters.size()
                    || expectedWriters.stream().map(WriterSlot::operatorId).distinct().count() != 1) throw new IllegalArgumentException("one fixed output sink with all logical writers required");
            for (int i = 0; i < runtime.compute().parallelism(); i++) {
                final int subtask = i;
                if (expectedWriters.stream().noneMatch(w -> w.subtask() == subtask)) throw new IllegalArgumentException("noncontiguous writer slots");
            }
            if (!consumedDataDigest.equals(DomainSnapshots.inputs(inputs).sha256())) throw new IllegalArgumentException("consumed inputs digest mismatch");
            validateCoverage(key.type(), inputs, splits);
        }
        public URI stagePrefix() {
            String relative = key.type() == StageIdentity.StageType.COLLECT ? "inputs/" + key.inputAlias().orElseThrow() + "/" : "compute/";
            return ObjectKeys.prefix(URI.create(runtime.storageScope() + "runs/" + key.runId() + "/" + relative + attemptId + "/"));
        }
        public URI dataPrefix() { return ObjectKeys.prefix(URI.create(stagePrefix() + "data/")); }
    }
    public record CommitId(WriterAttempt writer, long sequence) {
        public CommitId { Objects.requireNonNull(writer); nonnegative(sequence); }
    }
    public record CommittedFile(URI objectKey, long bytes, WriterAttempt writer, long sequence) {
        public CommittedFile { ObjectKeys.file(objectKey); nonnegative(bytes); Objects.requireNonNull(writer); nonnegative(sequence); }
        public CommitId id() { return new CommitId(writer, sequence); }
    }
    public record WriterEnd(WriterAttempt writer, long fileCount) {
        public WriterEnd { Objects.requireNonNull(writer); nonnegative(fileCount); }
    }
    public record SplitCompletion(String splitId, String sourceVersion, long rowsRead, String readEvidenceDigest, String consumedReadDigest) {
        public SplitCompletion { identifier(splitId); text(sourceVersion); nonnegative(rowsRead); digest(readEvidenceDigest); digest(consumedReadDigest); }
    }
    public record Evidence(StageIdentity identity, Instant completedAt, List<CommittedFile> files, List<WriterEnd> writers, List<SplitCompletion> splits) {
        public Evidence {
            Objects.requireNonNull(identity); Objects.requireNonNull(completedAt); files = List.copyOf(files); writers = List.copyOf(writers); splits = List.copyOf(splits);
            if (files.stream().map(CommittedFile::objectKey).distinct().count() != files.size() || files.stream().map(CommittedFile::id).distinct().count() != files.size()
                    || writers.stream().map(w -> w.writer().slot()).distinct().count() != writers.size() || splits.stream().map(SplitCompletion::splitId).distinct().count() != splits.size())
                throw new IllegalArgumentException("duplicate evidence identity");
        }
    }
    /** Must originate from the accepted native commit channel, not from the untrusted evidence being verified. */
    public record AcceptedCommits(StageIdentity identity, Map<WriterSlot, Integer> attempts, Set<CommitId> files, String finalityDigest) {
        public AcceptedCommits {
            Objects.requireNonNull(identity); attempts = Map.copyOf(attempts); files = Set.copyOf(files); digest(finalityDigest);
            attempts.values().forEach(FileStageProtocol::nonnegative);
        }
    }
    public static final class VerifiedCommit {
        private final Catalog catalog; private final Plan plan; private final Evidence evidence; private final AcceptedCommits accepted;
        private VerifiedCommit(Catalog catalog, Plan plan, Evidence evidence, AcceptedCommits accepted) { this.catalog = catalog; this.plan = plan; this.evidence = evidence; this.accepted = accepted; }
        public Catalog catalog() { return catalog; } public Plan plan() { return plan; } public Evidence evidence() { return evidence; } public AcceptedCommits accepted() { return accepted; }
    }
    public static VerifiedCommit verify(ResolvedRun run, Plan plan, StageIdentity expectedIdentity, Evidence evidence, AcceptedCommits accepted) {
        Objects.requireNonNull(run); Objects.requireNonNull(plan); Objects.requireNonNull(expectedIdentity); Objects.requireNonNull(evidence); Objects.requireNonNull(accepted);
        Catalog catalog = run.catalog();
        if (!plan.resolvedSpecDigest().equals(DomainSnapshots.run(run).sha256()) || !plan.key().runId().equals(run.runId()) || !plan.runDeadline().equals(run.runDeadline())
                || plan.plannedAt().isBefore(run.acceptedAt()) || !plan.catalogDigest().equals(DomainSnapshots.catalog(catalog).sha256()) || !plan.runtime().equals(catalog.runtime())
                || !plan.key().equals(expectedIdentity.stage()) || !plan.attemptId().equals(expectedIdentity.attemptId())
                || expectedIdentity.executor() != StageIdentity.Executor.FLINK || !DomainSnapshots.plan(plan).sha256().equals(expectedIdentity.planDigest())
                || !plan.resolvedSpecDigest().equals(expectedIdentity.resolvedSpecDigest()) || !plan.consumedDataDigest().equals(expectedIdentity.consumedDataDigest())
                || !expectedIdentity.equals(evidence.identity()) || !expectedIdentity.equals(accepted.identity())) throw new IllegalArgumentException("plan/configuration/external identity mismatch");
        if (evidence.completedAt().isBefore(plan.plannedAt()) || evidence.completedAt().isAfter(plan.deadline())) throw new IllegalArgumentException("commit outside fixed execution window");
        validatePlan(run, plan);
        Map<String, InputSnapshot> inputs = new HashMap<>(); plan.inputs().forEach(input -> inputs.put(input.alias(), input));
        Set<WriterSlot> expected = Set.copyOf(plan.expectedWriters());
        if (!expected.equals(accepted.attempts().keySet()) || !expected.equals(evidence.writers().stream().map(w -> w.writer().slot()).collect(java.util.stream.Collectors.toSet())))
            throw new IllegalArgumentException("final writer set differs from plan");
        Map<WriterAttempt, Long> counts = new HashMap<>(); Set<CommitId> ids = new HashSet<>();
        for (var file : evidence.files()) {
            if (!ObjectKeys.contains(plan.dataPrefix(), file.objectKey()) || !Objects.equals(accepted.attempts().get(file.writer().slot()), file.writer().attempt()))
                throw new IllegalArgumentException("foreign file prefix or unaccepted task attempt");
            ids.add(file.id()); counts.merge(file.writer(), 1L, Long::sum);
        }
        if (!ids.equals(accepted.files())) throw new IllegalArgumentException("accepted native commit set differs from evidence");
        for (var end : evidence.writers())
            if (!Objects.equals(accepted.attempts().get(end.writer().slot()), end.writer().attempt()) || end.fileCount() != counts.getOrDefault(end.writer(), 0L))
                throw new IllegalArgumentException("writer end/accepted attempt/count mismatch");
        Map<String, Split> planned = new HashMap<>(); plan.splits().forEach(split -> planned.put(split.id(), split));
        if (!planned.keySet().equals(evidence.splits().stream().map(SplitCompletion::splitId).collect(java.util.stream.Collectors.toSet()))) throw new IllegalArgumentException("completed logical split set differs from plan");
        Map<String, Long> rows = new HashMap<>();
        for (var split : evidence.splits()) {
            var input = inputs.get(planned.get(split.splitId()).inputAlias());
            if (!input.sourceVersion().equals(split.sourceVersion()) || !split.consumedReadDigest().equals(DomainSnapshots.readPlan(plan, planned.get(split.splitId())).sha256()))
                throw new IllegalArgumentException("split read a different version or read plan");
            rows.merge(input.alias(), split.rowsRead(), Math::addExact);
        }
        inputs.forEach((alias, input) -> { if (rows.getOrDefault(alias, 0L) != input.expectedRows()) throw new IllegalArgumentException("independent input control total mismatch"); });
        return new VerifiedCommit(catalog, plan, evidence, accepted);
    }
    /** Admission checks the complete plan without inventing a native execution identity or commit. */
    public static void validatePlan(ResolvedRun run, Plan plan) {
        Objects.requireNonNull(run); Objects.requireNonNull(plan);
        Catalog catalog = run.catalog();
        if (!plan.resolvedSpecDigest().equals(DomainSnapshots.run(run).sha256()) || !plan.key().runId().equals(run.runId())
                || !plan.runDeadline().equals(run.runDeadline()) || plan.plannedAt().isBefore(run.acceptedAt())
                || !plan.catalogDigest().equals(DomainSnapshots.catalog(catalog).sha256()) || !plan.runtime().equals(catalog.runtime()))
            throw new IllegalArgumentException("plan differs from frozen admission");
        Map<String, InputSnapshot> inputs = new HashMap<>(); plan.inputs().forEach(input -> inputs.put(input.alias(), input));
        if (plan.key().type() == StageIdentity.StageType.COMPUTE && !inputs.keySet().equals(run.inputVersions().keySet())) throw new IllegalArgumentException("COMPUTE omitted or substituted a resolved input");
        for (var input : plan.inputs()) {
            if (!input.dataset().equals(run.inputVersions().get(input.alias())) || !Objects.equals(input.batch(), run.job().inputBatches().get(input.alias()))) throw new IllegalArgumentException("input differs from resolved run");
            var definition = catalog.inputs().stream().filter(d -> d.ref().equals(input.dataset())).findFirst().orElseThrow(() -> new IllegalArgumentException("fixed dataset version absent"));
            var projected = new SqlSchema(definition.read().projection().stream().map(name -> column(definition.schema(), name)).toList());
            var expectedSchema = plan.key().type() == StageIdentity.StageType.COLLECT ? definition.schema() : projected;
            if (!expectedSchema.equals(input.schema()) || definition.batchType() != input.batchType() || !definition.timezone().equals(input.timezone())
                    || !definition.read().projection().equals(input.projection()) || !DomainSnapshots.filter(definition, input.batch()).sha256().equals(input.filterDigest())
                    || input.nullPartitionRequired() != (plan.key().type() == StageIdentity.StageType.COLLECT && column(definition.schema(), definition.read().splitColumn()).type().getLogicalType().isNullable()))
                throw new IllegalArgumentException("input snapshot differs from fixed dataset/read policy");
            if (input.expectedRows() == 0 && !definition.allowEmpty()) throw new IllegalArgumentException("empty batch forbidden by dataset");
            if (plan.key().type() == StageIdentity.StageType.COLLECT) {
                if (!projected.equals(plan.outputSchema())) throw new IllegalArgumentException("COLLECT output projection/schema mismatch");
            }
        }
        if (plan.key().type() == StageIdentity.StageType.COMPUTE && !plan.outputSchema().equals(catalog.output().schema())) throw new IllegalArgumentException("candidate output schema mismatch");
    }
    public record VerifiedFile(CommittedFile file, long rows, String sha256, String schemaDigest) {
        public VerifiedFile { Objects.requireNonNull(file); nonnegative(rows); digest(sha256); digest(schemaDigest); }
    }
    public enum TerminalStatus { SUCCEEDED, FAILED, CANCELLED }
    public record TerminalObservation(StageIdentity identity, long generation, TerminalStatus status, Instant completedAt, Instant observedAt) {
        public TerminalObservation {
            Objects.requireNonNull(identity); positive(generation); Objects.requireNonNull(status); Objects.requireNonNull(completedAt); Objects.requireNonNull(observedAt);
            if (identity.executor() != StageIdentity.Executor.FLINK || observedAt.isBefore(completedAt)) throw new IllegalArgumentException("invalid durable terminal observation");
        }
    }
    /** Load and verify these bytes from storage before constructing a success manifest. */
    public record StoredTerminal(TerminalObservation observation, URI objectKey, String digest) {
        public StoredTerminal {
            Objects.requireNonNull(observation); ObjectKeys.file(objectKey); PlatformDefinitions.digest(digest);
            if (!digest.equals(DomainSnapshots.terminal(observation).sha256())) throw new IllegalArgumentException("terminal bytes/reference mismatch");
        }
    }
    public record Manifest(String id, VerifiedCommit commit, Optional<StoredTerminal> terminal, List<VerifiedFile> files) {
        public Manifest {
            pathId(id); Objects.requireNonNull(commit); Objects.requireNonNull(terminal); files = List.copyOf(files);
            if (terminal.isEmpty()) throw new IllegalArgumentException("durable success terminal required before manifest");
            var t = terminal.get();
            if (t.observation().status() != TerminalStatus.SUCCEEDED || !t.observation().identity().equals(commit.evidence().identity())
                    || !t.objectKey().equals(URI.create(commit.plan().stagePrefix() + "terminal.json"))
                    || t.observation().completedAt().isBefore(commit.evidence().completedAt()) || t.observation().completedAt().isAfter(commit.plan().deadline()))
                throw new IllegalArgumentException("terminal success identity/time/reference mismatch");
            var committed = new HashSet<>(commit.evidence().files());
            if (files.size() != committed.size() || !committed.equals(files.stream().map(VerifiedFile::file).collect(java.util.stream.Collectors.toSet()))) throw new IllegalArgumentException("manifest must contain exactly the verified committed files");
            String schema = ObjectDigests.schema(commit.plan().outputSchema()).sha256(); long rows = 0;
            for (var file : files) { if (!file.schemaDigest().equals(schema)) throw new IllegalArgumentException("file schema differs from plan"); rows = Math.addExact(rows, file.rows()); }
            if (commit.plan().key().type() == StageIdentity.StageType.COLLECT && rows != commit.plan().inputs().get(0).expectedRows()) throw new IllegalArgumentException("capture file rows differ from independent control total");
        }
        public long totalRows() { return files.stream().mapToLong(VerifiedFile::rows).reduce(0L, Math::addExact); }
    }
    private static void validateCoverage(StageIdentity.StageType stage, List<InputSnapshot> inputs, List<Split> splits) {
        if (splits.isEmpty() || splits.stream().map(Split::id).distinct().count() != splits.size()) throw new IllegalArgumentException("unique planned splits required");
        Set<String> aliases = new HashSet<>(); inputs.forEach(input -> aliases.add(input.alias()));
        if (splits.stream().anyMatch(split -> !aliases.contains(split.inputAlias()))) throw new IllegalArgumentException("foreign split input");
        for (var input : inputs) {
            var own = splits.stream().filter(split -> split.inputAlias().equals(input.alias())).toList();
            if (stage == StageIdentity.StageType.COMPUTE) {
                var files = input.manifest().orElseThrow().files();
                if (input.nullPartitionRequired() || !input.coverage().equals(new Bounds(BigInteger.ZERO, BigInteger.valueOf(files.size())))) throw new IllegalArgumentException("file coverage differs from manifest");
                if (files.isEmpty()) {
                    if (own.size() != 1 || own.get(0).range().isPresent() || own.get(0).file().isPresent() || input.expectedRows() != 0) throw new IllegalArgumentException("explicit empty-file input completion required");
                } else if (own.size() != files.size() || own.stream().anyMatch(s -> s.range().isPresent() || s.file().isEmpty())
                        || !new HashSet<>(files).equals(own.stream().map(s -> s.file().orElseThrow()).collect(java.util.stream.Collectors.toSet()))) throw new IllegalArgumentException("file split coverage differs from manifest");
                continue;
            }
            if (own.stream().anyMatch(s -> s.file().isPresent())) throw new IllegalArgumentException("COLLECT cannot use file-input splits");
            long nullPartitions = own.stream().filter(split -> split.range().isEmpty()).count();
            if (nullPartitions != (input.nullPartitionRequired() ? 1 : 0)) throw new IllegalArgumentException("explicit NULL partition coverage mismatch");
            var ranges = own.stream().flatMap(split -> split.range().stream()).sorted(Comparator.comparing(Bounds::lowerInclusive)).toList();
            if (ranges.isEmpty()) throw new IllegalArgumentException("range coverage required even for empty input");
            BigInteger cursor = input.coverage().lowerInclusive();
            for (var range : ranges) {
                if (!range.lowerInclusive().equals(cursor) || (range.lowerInclusive().equals(range.upperExclusive()) && ranges.size() > 1)) throw new IllegalArgumentException("range gap/overlap/duplicate empty split");
                cursor = range.upperExclusive();
            }
            if (!cursor.equals(input.coverage().upperExclusive())) throw new IllegalArgumentException("range endpoints differ from fixed input coverage");
        }
    }
    static void batchValue(JobSpec.ParameterType type, Object value) {
        boolean valid = switch (type) { case STRING -> value instanceof String; case DATE -> value instanceof LocalDate; case INTEGER -> value instanceof BigInteger; case DECIMAL -> value instanceof BigDecimal; case BOOLEAN -> value instanceof Boolean; };
        if (!valid) throw new IllegalArgumentException("bound batch value/type mismatch");
    }
    static void pathId(String value) { if (value == null || !value.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) throw new IllegalArgumentException("invalid immutable path identity"); }
    static void nonnegative(long value) { if (value < 0) throw new IllegalArgumentException("negative evidence count"); }
}
