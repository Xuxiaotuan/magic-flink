package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.control.StageIdentity;
import io.github.xuxiaotuan.magicflink.spec.JobSpec;
import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import static io.github.xuxiaotuan.magicflink.protocol.FileStageProtocol.*;
import static io.github.xuxiaotuan.magicflink.protocol.PlatformDefinitionsTest.*;
import static org.junit.jupiter.api.Assertions.*;

class FileStageProtocolTest {
    static final String HASH = "a".repeat(64), READ_HASH = "b".repeat(64);
    static final Instant START = Instant.parse("2026-10-18T09:00:00Z");
    static final WriterSlot SLOT = new WriterSlot("file_sink", 0);
    static final WriterAttempt WRITER = new WriterAttempt(SLOT, 1);
    static PlatformDefinitions.Catalog catalog() {
        return new PlatformDefinitions.Catalog(List.of(connection(CONNECTION, Set.of(PlatformDefinitions.Grant.READ)),
                connection(output().writeConnection(), Set.of(PlatformDefinitions.Grant.READ, PlatformDefinitions.Grant.ISOLATED_WRITE)),
                connection(output().publishConnection(), Set.of(PlatformDefinitions.Grant.PUBLISH))),
                List.of(dataset("orders", "source.orders"), dataset("customers", "source.customers")), output(), runtime());
    }
    static ResolvedRun resolved() { return resolved(false); }
    static ResolvedRun resolved(boolean twoInputs) {
        var spec = new io.github.xuxiaotuan.magicflink.spec.JobSpecParser(java.time.Duration.ofHours(1), 18, 2).parse("""
                version: v1
                name: daily_orders
                parameters: {businessDate: {type: date}}
                inputs:
                  orders: {dataset: orders, batch: '${businessDate}'}
                sql: SELECT * FROM orders
                output: {dataset: enriched_orders, batch: '${businessDate}', mode: replace-batch}
                """.replace("sql: SELECT", (twoInputs ? "  customers: {dataset: customers, batch: '${businessDate}'}\n" : "") + "sql: SELECT").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var bound = new io.github.xuxiaotuan.magicflink.spec.JobSpecParser(java.time.Duration.ofHours(1), 18, 2)
                .bind(spec, Map.of("businessDate", "2026-10-18"));
        return new ResolvedRun("run1", "job1", 1, "batch_user", bound,
                (twoInputs ? Map.of("orders", dataset("orders", "source.orders").ref(), "customers", dataset("customers", "source.customers").ref()) : Map.of("orders", dataset("orders", "source.orders").ref())), output().ref(), catalog(), START, START.plusSeconds(1800));
    }
    static Plan plan() { return plan(resolved()); }
    static Plan plan(ResolvedRun run) {
        var snapshot = new InputSnapshot("orders", dataset("orders", "source.orders").ref(), SCHEMA, JobSpec.ParameterType.DATE,
                LocalDate.of(2026, 10, 18), "batch_version1", HASH, List.of("id", "amount", "business_date"), DomainSnapshots.filter(dataset("orders", "source.orders"), LocalDate.of(2026, 10, 18)).sha256(),
                java.time.ZoneId.of("Asia/Shanghai"), new Bounds(BigInteger.ZERO, BigInteger.TEN), false, 2,
                Optional.of(START.plusSeconds(3600)), Optional.empty());
        return new Plan(new StageIdentity.StageKey("run1", StageIdentity.StageType.COLLECT, Optional.of("orders")),
                "attempt1", "release1", DomainSnapshots.catalog(catalog()).sha256(), DomainSnapshots.run(run).sha256(), DomainSnapshots.inputs(List.of(snapshot)).sha256(), runtime(), SCHEMA,
                List.of(snapshot), List.of(SLOT), List.of(new Split("s0", "orders", Optional.of(new Bounds(BigInteger.ZERO, BigInteger.valueOf(5)))),
                new Split("s1", "orders", Optional.of(new Bounds(BigInteger.valueOf(5), BigInteger.TEN)))), START,
                START.plusSeconds(900), START.plusSeconds(1800), START.plusSeconds(1800 + 7 * 86400));
    }
    static StageIdentity identity(Plan plan) {
        return new StageIdentity(plan.key(), plan.attemptId(), StageIdentity.Executor.FLINK,
                Optional.of(new StageIdentity.FlinkResource("cr_uid1", "job1")), DomainSnapshots.plan(plan).sha256(), plan.resolvedSpecDigest(), plan.consumedDataDigest());
    }
    static CommittedFile file(Plan p, WriterAttempt writer, long sequence) {
        return new CommittedFile(URI.create(p.dataPrefix() + "part-" + sequence + ".parquet"), 100, writer, sequence);
    }
    static Evidence evidence(Plan p) {
        return new Evidence(identity(p), p.plannedAt().plusSeconds(60), List.of(file(p, WRITER, 0)), List.of(new WriterEnd(WRITER, 1)),
                List.of(completion(p, "s0", "batch_version1", 1), completion(p, "s1", "batch_version1", 1)));
    }
    static AcceptedCommits accepted(Plan p) {
        return new AcceptedCommits(identity(p), Map.of(SLOT, WRITER.attempt()), Set.of(new CommitId(WRITER, 0)), READ_HASH);
    }

    @Test void rejectsForgedBoundRunValuesVersionsAndAuthorization() {
        var r = resolved(); var j = r.job();
        var changed = new JobSpec.BoundJob(j.spec(), j.parameters(), Map.of("orders", LocalDate.of(2026, 10, 19)), j.outputBatch());
        assertThrows(IllegalArgumentException.class, () -> new ResolvedRun(r.runId(), r.jobId(), r.jobVersion(), r.subject(), changed,
                r.inputVersions(), r.outputVersion(), r.catalog(), r.acceptedAt(), r.runDeadline()));
        assertThrows(IllegalArgumentException.class, () -> new ResolvedRun(r.runId(), r.jobId(), r.jobVersion(), "other_user", j,
                r.inputVersions(), r.outputVersion(), r.catalog(), r.acceptedAt(), r.runDeadline()));
        assertThrows(IllegalArgumentException.class, () -> new ResolvedRun(r.runId(), r.jobId(), r.jobVersion(), r.subject(), j,
                Map.of("orders", new PlatformDefinitions.Ref("orders", "v2")), r.outputVersion(), r.catalog(), r.acceptedAt(), r.runDeadline()));
        assertThrows(IllegalArgumentException.class, () -> new ResolvedRun(r.runId(), r.jobId(), r.jobVersion(), r.subject(), j,
                r.inputVersions(), r.outputVersion(), r.catalog(), r.acceptedAt(), r.runDeadline().plusSeconds(1)));
    }

    @Test void rejectsSelfConsistentPlanForAnotherBatch() {
        var p = plan(); var i = p.inputs().get(0); var batch = LocalDate.of(2026, 10, 19);
        var input = new InputSnapshot(i.alias(), i.dataset(), i.schema(), i.batchType(), batch, i.sourceVersion(), i.readinessDigest(), i.projection(),
                DomainSnapshots.filter(dataset("orders", "source.orders"), batch).sha256(), i.timezone(), i.coverage(), i.nullPartitionRequired(), i.expectedRows(), i.tokenExpiresAt(), i.manifest());
        var changed = new Plan(p.key(), p.attemptId(), p.releaseId(), p.catalogDigest(), p.resolvedSpecDigest(), DomainSnapshots.inputs(List.of(input)).sha256(),
                p.runtime(), p.outputSchema(), List.of(input), p.expectedWriters(), p.splits(), p.plannedAt(), p.deadline(), p.runDeadline(), p.retainUntil());
        assertThrows(IllegalArgumentException.class, () -> verify(resolved(), changed, identity(changed), evidence(changed), accepted(changed)));
    }

    @Test void validatesIndependentWriterAndSplitSetsAndBuildsManifestAndReceipt() {
        var p = plan(); var e = evidence(p);
        var verified = verify(resolved(), p, identity(p), e, accepted(p));
        var manifest = new Manifest("manifest1", verified, Optional.of(terminal(p)), List.of(new VerifiedFile(e.files().get(0), 2, HASH, ObjectDigests.schema(SCHEMA).sha256())));
        assertEquals(2, manifest.totalRows());
        assertEquals(64, DomainSnapshots.manifest(manifest).sha256().length());
        assertEquals(64, DomainSnapshots.receipt(manifest).sha256().length());
        assertEquals(DomainSnapshots.manifest(manifest).sha256(), DomainSnapshots.manifest(manifest).sha256());
        assertTrue(p.dataPrefix().toString().contains("/inputs/orders/attempt1/data/"));
    }

    @Test void rejectsMissingExtraOrSubstitutedSplitsEvenWhenCountsMatch() {
        var p = plan(); var e = evidence(p);
        for (var splits : List.of(List.of(e.splits().get(0)),
                List.of(e.splits().get(0), completion(p, "other", "batch_version1", 1)),
                List.of(e.splits().get(0), completion(p, "s1", "changed_version", 1)),
                List.of(e.splits().get(0), completion(p, "s1", "batch_version1", 2)))) {
            var bad = new Evidence(e.identity(), e.completedAt(), e.files(), e.writers(), splits);
            assertThrows(IllegalArgumentException.class, () -> verify(resolved(), p, identity(p), bad, accepted(p)));
        }
    }

    @Test void rejectsFailedTaskAttemptAndDoesNotChooseLargestAttemptNumber() {
        var p = plan(); var e = evidence(p);
        var failed = new WriterAttempt(SLOT, 2);
        var bad = new Evidence(e.identity(), e.completedAt(), List.of(file(p, failed, 0)), List.of(new WriterEnd(failed, 1)), e.splits());
        assertThrows(IllegalArgumentException.class, () -> verify(resolved(), p, identity(p), bad, accepted(p)));
        assertNotNull(verify(resolved(), p, identity(p), e, accepted(p))); // attempt 1 is accepted, not maximum 2
    }

    @Test void rejectsForeignPrefixForeignResourceChangedPlanAndUnacceptedCommit() {
        var p = plan(); var e = evidence(p);
        var foreignFile = new CommittedFile(URI.create("s3://test-bucket/magic/runs/run2/part.parquet"), 100, WRITER, 0);
        assertThrows(IllegalArgumentException.class, () -> verify(resolved(), p, identity(p), new Evidence(e.identity(), e.completedAt(), List.of(foreignFile), e.writers(), e.splits()), accepted(p)));
        var changedId = new StageIdentity(p.key(), p.attemptId(), StageIdentity.Executor.FLINK, Optional.of(new StageIdentity.FlinkResource("recreated_uid", "job1")), e.identity().planDigest(), HASH, HASH);
        assertThrows(IllegalArgumentException.class, () -> verify(resolved(), p, identity(p), new Evidence(changedId, e.completedAt(), e.files(), e.writers(), e.splits()), accepted(p)));
        var missingCommit = new AcceptedCommits(e.identity(), Map.of(SLOT, 1), Set.of(), READ_HASH);
        assertThrows(IllegalArgumentException.class, () -> verify(resolved(), p, identity(p), e, missingCommit));
    }

    @Test void rejectsRangeGapsOverlapAndMissingNullPartition() {
        var p = plan();
        var gap = List.of(new Split("s0", "orders", Optional.of(new Bounds(BigInteger.ZERO, BigInteger.valueOf(4)))), p.splits().get(1));
        assertThrows(IllegalArgumentException.class, () -> copyPlan(p, p.inputs(), gap));
        var overlap = List.of(new Split("s0", "orders", Optional.of(new Bounds(BigInteger.ZERO, BigInteger.valueOf(6)))), p.splits().get(1));
        assertThrows(IllegalArgumentException.class, () -> copyPlan(p, p.inputs(), overlap));
        var i = p.inputs().get(0);
        var withNull = new InputSnapshot(i.alias(), i.dataset(), i.schema(), i.batchType(), i.batch(), i.sourceVersion(), i.readinessDigest(), i.projection(), i.filterDigest(), i.timezone(), i.coverage(), true, i.expectedRows(), i.tokenExpiresAt(), i.manifest());
        assertThrows(IllegalArgumentException.class, () -> copyPlan(p, List.of(withNull), p.splits()));
    }

    @Test void rejectsExpiredVersionUnknownProjectionAndUnverifiedFileSchemaOrRows() {
        var p = plan(); var e = evidence(p); var i = p.inputs().get(0);
        var expired = new InputSnapshot(i.alias(), i.dataset(), i.schema(), i.batchType(), i.batch(), i.sourceVersion(), i.readinessDigest(), i.projection(), i.filterDigest(), i.timezone(), i.coverage(), i.nullPartitionRequired(), i.expectedRows(), Optional.of(START.plusSeconds(10)), i.manifest());
        assertThrows(IllegalArgumentException.class, () -> copyPlan(p, List.of(expired), p.splits()));
        var verified = verify(resolved(), p, identity(p), e, accepted(p));
        assertThrows(IllegalArgumentException.class, () -> new Manifest("manifest1", verified, Optional.of(terminal(p)), List.of(new VerifiedFile(e.files().get(0), 2, HASH, READ_HASH))));
        assertThrows(IllegalArgumentException.class, () -> new Manifest("manifest1", verified, Optional.of(terminal(p)), List.of(new VerifiedFile(e.files().get(0), 1, HASH, ObjectDigests.schema(SCHEMA).sha256()))));
    }

    static SplitCompletion completion(Plan p, String id, String version, long rows) {
        String digest = p.splits().stream().filter(split -> split.id().equals(id)).findFirst()
                .map(split -> DomainSnapshots.readPlan(p, split).sha256()).orElse(HASH);
        return new SplitCompletion(id, version, rows, READ_HASH, digest);
    }
    static StoredTerminal terminal(Plan p) {
        var observed = new TerminalObservation(identity(p), 1, TerminalStatus.SUCCEEDED, p.plannedAt().plusSeconds(70), p.plannedAt().plusSeconds(80));
        return new StoredTerminal(observed, URI.create(p.stagePrefix() + "terminal.json"), DomainSnapshots.terminal(observed).sha256());
    }
    private static Plan copyPlan(Plan p, List<InputSnapshot> inputs, List<Split> splits) {
        return new Plan(p.key(), p.attemptId(), p.releaseId(), p.catalogDigest(), p.resolvedSpecDigest(), p.consumedDataDigest(), p.runtime(), p.outputSchema(), inputs, p.expectedWriters(), splits, p.plannedAt(), p.deadline(), p.runDeadline(), p.retainUntil());
    }

    @Test void commitProofAloneMustNotCreateSuccessfulManifest() {
        var p = plan(); var e = evidence(p); var verified = verify(resolved(), p, identity(p), e, accepted(p));
        assertThrows(IllegalArgumentException.class, () -> new Manifest("manifest1", verified, Optional.empty(),
                List.of(new VerifiedFile(e.files().get(0), 2, HASH, ObjectDigests.schema(SCHEMA).sha256()))));
    }

    @Test void rejectsFailedTerminalAndChangedReadPlanDigest() {
        var p = plan(); var e = evidence(p); var verified = verify(resolved(), p, identity(p), e, accepted(p));
        var observed = new TerminalObservation(identity(p), 1, TerminalStatus.FAILED, START.plusSeconds(70), START.plusSeconds(80));
        var failed = new StoredTerminal(observed, URI.create(p.stagePrefix() + "terminal.json"), DomainSnapshots.terminal(observed).sha256());
        assertThrows(IllegalArgumentException.class, () -> new Manifest("manifest1", verified, Optional.of(failed),
                List.of(new VerifiedFile(e.files().get(0), 2, HASH, ObjectDigests.schema(SCHEMA).sha256()))));
        var otherRead = new SplitCompletion("s1", "batch_version1", 1, READ_HASH, READ_HASH);
        var bad = new Evidence(e.identity(), e.completedAt(), e.files(), e.writers(), List.of(e.splits().get(0), otherRead));
        assertThrows(IllegalArgumentException.class, () -> verify(resolved(), p, identity(p), bad, accepted(p)));
    }

    @Test void computeReadsExactlyTheSealedManifestFileList() {
        var capture = plan(); var capturedEvidence = evidence(capture);
        var captured = new Manifest("manifest1", verify(resolved(), capture, identity(capture), capturedEvidence, accepted(capture)), Optional.of(terminal(capture)),
                List.of(new VerifiedFile(capturedEvidence.files().get(0), 2, HASH, ObjectDigests.schema(SCHEMA).sha256())));
        var previous = capture.inputs().get(0);
        var reference = new ManifestRef(captured.id(), DomainSnapshots.manifest(captured).sha256(), capturedEvidence.identity(), ObjectDigests.schema(SCHEMA).sha256(),
                2, capture.retainUntil(), List.of(capturedEvidence.files().get(0).objectKey()));
        var input = new InputSnapshot(previous.alias(), previous.dataset(), previous.schema(), previous.batchType(), previous.batch(), previous.sourceVersion(), previous.readinessDigest(),
                previous.projection(), previous.filterDigest(), previous.timezone(), new Bounds(BigInteger.ZERO, BigInteger.ONE), false, 2, previous.tokenExpiresAt(), Optional.of(reference));
        var key = new StageIdentity.StageKey("run1", StageIdentity.StageType.COMPUTE, Optional.empty());
        var fileSplit = new Split("f0", "orders", Optional.empty(), Optional.of(reference.files().get(0)));
        var compute = new Plan(key, "compute1", capture.releaseId(), capture.catalogDigest(), capture.resolvedSpecDigest(), DomainSnapshots.inputs(List.of(input)).sha256(),
                capture.runtime(), SCHEMA, List.of(input), List.of(SLOT), List.of(fileSplit), START.plusSeconds(120), START.plusSeconds(1020), capture.runDeadline(), capture.retainUntil());
        var e = new Evidence(identity(compute), compute.plannedAt().plusSeconds(60), List.of(file(compute, WRITER, 0)), List.of(new WriterEnd(WRITER, 1)),
                List.of(completion(compute, "f0", previous.sourceVersion(), 2)));
        var result = new Manifest("candidate1", verify(resolved(), compute, identity(compute), e, accepted(compute)), Optional.of(terminal(compute)),
                List.of(new VerifiedFile(e.files().get(0), 3, HASH, ObjectDigests.schema(SCHEMA).sha256())));
        assertEquals(3, result.totalRows()); // Join/aggregate output is not forced to equal input rows.
        assertTrue(new String(DomainSnapshots.manifest(result).canonicalBytes(), java.nio.charset.StandardCharsets.UTF_8).contains("CANDIDATE_MANIFEST"));
        URI outside = URI.create("s3://test-bucket/magic/runs/foreign/input.parquet");
        var forgedRef = new ManifestRef(reference.id(), reference.digest(), reference.producer(), reference.schemaDigest(), reference.rows(), reference.retainUntil(), List.of(outside));
        var forgedInput = new InputSnapshot(input.alias(), input.dataset(), input.schema(), input.batchType(), input.batch(), input.sourceVersion(), input.readinessDigest(), input.projection(), input.filterDigest(),
                input.timezone(), input.coverage(), false, input.expectedRows(), input.tokenExpiresAt(), Optional.of(forgedRef));
        assertThrows(IllegalArgumentException.class, () -> new Plan(compute.key(), compute.attemptId(), compute.releaseId(), compute.catalogDigest(), compute.resolvedSpecDigest(), DomainSnapshots.inputs(List.of(forgedInput)).sha256(),
                compute.runtime(), compute.outputSchema(), List.of(forgedInput), compute.expectedWriters(), List.of(new Split("f0", "orders", Optional.empty(), Optional.of(outside))),
                compute.plannedAt(), compute.deadline(), compute.runDeadline(), compute.retainUntil()));
        var foreign = new Split("f0", "orders", Optional.empty(), Optional.of(URI.create(capture.dataPrefix() + "unlisted.parquet")));
        assertThrows(IllegalArgumentException.class, () -> copyPlan(compute, compute.inputs(), List.of(foreign)));
    }

    @Test void computeCannotOmitAResolvedInputEvenWithSelfConsistentHashes() {
        var run = resolved(true); var capture = plan(run); var e = evidence(capture);
        var manifest = new Manifest("manifest1", verify(run, capture, identity(capture), e, accepted(capture)), Optional.of(terminal(capture)),
                List.of(new VerifiedFile(e.files().get(0), 2, HASH, ObjectDigests.schema(SCHEMA).sha256())));
        var i = capture.inputs().get(0);
        var reference = new ManifestRef(manifest.id(), DomainSnapshots.manifest(manifest).sha256(), identity(capture), ObjectDigests.schema(SCHEMA).sha256(), 2, capture.retainUntil(), List.of(e.files().get(0).objectKey()));
        var input = new InputSnapshot(i.alias(), i.dataset(), i.schema(), i.batchType(), i.batch(), i.sourceVersion(), i.readinessDigest(), i.projection(), i.filterDigest(), i.timezone(),
                new Bounds(BigInteger.ZERO, BigInteger.ONE), false, 2, i.tokenExpiresAt(), Optional.of(reference));
        var compute = new Plan(new StageIdentity.StageKey("run1", StageIdentity.StageType.COMPUTE, Optional.empty()), "compute1", capture.releaseId(), capture.catalogDigest(), capture.resolvedSpecDigest(),
                DomainSnapshots.inputs(List.of(input)).sha256(), capture.runtime(), SCHEMA, List.of(input), List.of(SLOT), List.of(new Split("f0", "orders", Optional.empty(), Optional.of(reference.files().get(0)))),
                START.plusSeconds(120), START.plusSeconds(1020), capture.runDeadline(), capture.retainUntil());
        var proof = new Evidence(identity(compute), START.plusSeconds(180), List.of(file(compute, WRITER, 0)), List.of(new WriterEnd(WRITER, 1)), List.of(completion(compute, "f0", i.sourceVersion(), 2)));
        var error = assertThrows(IllegalArgumentException.class, () -> verify(run, compute, identity(compute), proof, accepted(compute)));
        assertTrue(error.getMessage().contains("omitted"));
    }

    @Test void zeroOutputStillRequiresEveryWriterEnd() {
        var p = plan();
        var noFiles = new Evidence(identity(p), START.plusSeconds(60), List.of(), List.of(new WriterEnd(WRITER, 0)), evidence(p).splits());
        var nativeCommit = new AcceptedCommits(identity(p), Map.of(SLOT, 1), Set.of(), READ_HASH);
        assertNotNull(verify(resolved(), p, identity(p), noFiles, nativeCommit));
        assertThrows(IllegalArgumentException.class, () -> verify(resolved(), p, identity(p),
                new Evidence(identity(p), noFiles.completedAt(), List.of(), List.of(), noFiles.splits()), nativeCommit));
        // A zero-output writer is legitimate; losing nonempty COLLECT input records is not.
        assertThrows(IllegalArgumentException.class, () -> new Manifest("empty", verify(resolved(), p, identity(p), noFiles, nativeCommit), Optional.of(terminal(p)), List.of()));
    }

    @Test void nullPartitionMustBeExplicitInTheReadCoverage() {
        var p = plan(); var i = p.inputs().get(0);
        var input = new InputSnapshot(i.alias(), i.dataset(), i.schema(), i.batchType(), i.batch(), i.sourceVersion(), i.readinessDigest(), i.projection(), i.filterDigest(),
                i.timezone(), i.coverage(), true, i.expectedRows(), i.tokenExpiresAt(), i.manifest());
        var splits = new ArrayList<>(p.splits()); splits.add(new Split("null_rows", "orders", Optional.empty()));
        assertDoesNotThrow(() -> new Plan(p.key(), p.attemptId(), p.releaseId(), p.catalogDigest(), p.resolvedSpecDigest(), DomainSnapshots.inputs(List.of(input)).sha256(),
                p.runtime(), p.outputSchema(), List.of(input), p.expectedWriters(), splits, p.plannedAt(), p.deadline(), p.runDeadline(), p.retainUntil()));
        // The fixture has a NOT NULL split column: a forged NULL partition is rejected against the definition.
        var forged = new Plan(p.key(), p.attemptId(), p.releaseId(), p.catalogDigest(), p.resolvedSpecDigest(), DomainSnapshots.inputs(List.of(input)).sha256(),
                p.runtime(), p.outputSchema(), List.of(input), p.expectedWriters(), splits, p.plannedAt(), p.deadline(), p.runDeadline(), p.retainUntil());
        assertThrows(IllegalArgumentException.class, () -> verify(resolved(), forged, identity(forged), evidence(forged), accepted(forged)));
    }

    @Test void persistsAndReopensTheLocalEvidenceChain(@org.junit.jupiter.api.io.TempDir java.nio.file.Path root) throws Exception {
        var p = plan(); var e = evidence(p); var verified = verify(resolved(), p, identity(p), e, accepted(p));
        var store = new io.github.xuxiaotuan.magicflink.connectors.LocalImmutableObjectStore(root, p.runtime().storageScope());
        store.putIfAbsent(URI.create(p.stagePrefix() + "plan.json"), DomainSnapshots.plan(p));
        store.putIfAbsent(URI.create(p.stagePrefix() + "commit-evidence.json"), DomainSnapshots.evidence(verified));
        var success = terminal(p);
        store.putIfAbsent(success.objectKey(), DomainSnapshots.terminal(success.observation()));
        assertTrue(store.get(success.objectKey(), success.digest()).isPresent());
        var manifest = new Manifest("manifest1", verified, Optional.of(success), List.of(new VerifiedFile(e.files().get(0), 2, HASH, ObjectDigests.schema(SCHEMA).sha256())));
        var snapshot = DomainSnapshots.manifest(manifest); URI key = URI.create(p.stagePrefix() + "manifest.json");
        store.putIfAbsent(key, snapshot);
        store.putIfAbsent(URI.create(p.stagePrefix() + "receipt.json"), DomainSnapshots.receipt(manifest));
        var reopened = new io.github.xuxiaotuan.magicflink.connectors.LocalImmutableObjectStore(root, p.runtime().storageScope()).get(key, snapshot.sha256()).orElseThrow();
        assertArrayEquals(snapshot.canonicalBytes(), reopened.canonicalBytes());
        var decoded = (Map<?, ?>) CanonicalJson.decode(reopened.canonicalBytes());
        assertEquals("magic-flink-domain-v2", decoded.get("protocol"));
        assertEquals("INPUT_MANIFEST", decoded.get("kind"));
    }
}
