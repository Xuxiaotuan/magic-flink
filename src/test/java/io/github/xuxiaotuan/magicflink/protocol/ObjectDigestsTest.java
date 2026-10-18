package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.control.StageIdentity;
import io.github.xuxiaotuan.magicflink.spec.JobSpecParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static io.github.xuxiaotuan.magicflink.protocol.ObjectDigests.Kind.*;
import static org.junit.jupiter.api.Assertions.*;

class ObjectDigestsTest {
    private static final String HASH = "a".repeat(64);
    private static final Instant TIME = Instant.parse("2026-10-18T08:00:00Z");
    private static final StageIdentity ID = new StageIdentity(
            new StageIdentity.StageKey("run1", StageIdentity.StageType.COLLECT, Optional.of("orders")),
            "attempt1", StageIdentity.Executor.FLINK, Optional.of(new StageIdentity.FlinkResource("uid1", "job1")), HASH, HASH, HASH);
    private static Map<String, Object> file(String key, int sequence) {
        return new LinkedHashMap<>(Map.of("objectKey", key, "bytes", 100L, "rows", 2L, "sha256", HASH,
                "writerId", "sink/0/0", "sequence", sequence));
    }
    private static Map<String, Object> manifest() {
        return new LinkedHashMap<>(Map.of("manifestId", "manifest1", "stageIdentity", ID,
                "commitEvidenceDigest", HASH, "schemaDigest", HASH, "sourceVersions", Map.of("orders", "snapshot1"),
                "files", List.of(file("s3://test-bucket/run1/b.parquet", 1), file("s3://test-bucket/run1/a.parquet", 0)),
                "totalRows", 4L, "completedAt", TIME, "retainUntil", TIME.plusSeconds(604800)));
    }

    @Test void canonicalJsonHasAnIndependentGoldenVector() {
        assertEquals("{\"a\":\"中文\\n\\\"\\\\\",\"b\":2}", new String(CanonicalJson.encode(Map.of("b", 2, "a", "中文\n\"\\")), StandardCharsets.UTF_8));
        assertEquals("44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a", CanonicalJson.sha256(CanonicalJson.encode(Map.of())));
    }

    @Test void keyAndFileOrderDoNotChangeManifestIdentity() {
        var first = ObjectDigests.freeze(MANIFEST, manifest());
        // Independent Python stdlib JSON + hashlib vector, not computed by the implementation under test.
        assertEquals("902a5f4977fa98714b2ae2b0c321dbf77ccc047663c0d0a5cd0ca87522a4b266", first.sha256());
        var reversed = manifest();
        List<?> files = (List<?>) reversed.get("files");
        reversed.put("files", List.of(files.get(1), files.get(0)));
        Map<String, Object> reordered = new TreeMap<>(Comparator.reverseOrder()); reordered.putAll(reversed);
        assertEquals(first.sha256(), ObjectDigests.freeze(MANIFEST, reordered).sha256());
        assertArrayEquals(first.canonicalBytes(), ObjectDigests.freeze(MANIFEST, reordered).canonicalBytes());
    }

    @Test void contentIdentityChangesWithAttemptResourceOrFileContent() {
        var original = ObjectDigests.freeze(MANIFEST, manifest());
        var changed = manifest(); changed.put("manifestId", "manifest2");
        assertNotEquals(original.sha256(), ObjectDigests.freeze(MANIFEST, changed).sha256());
        changed = manifest();
        changed.put("stageIdentity", new StageIdentity(ID.stage(), "attempt2", ID.executor(), ID.resource(), HASH, HASH, HASH));
        assertNotEquals(original.sha256(), ObjectDigests.freeze(MANIFEST, changed).sha256());
        changed = manifest();
        var alteredFile = file("s3://test-bucket/run1/a.parquet", 0); alteredFile.put("sha256", "b".repeat(64));
        changed.put("files", List.of(alteredFile, file("s3://test-bucket/run1/b.parquet", 1)));
        assertNotEquals(original.sha256(), ObjectDigests.freeze(MANIFEST, changed).sha256());
    }

    @Test void frozenBytesCannotBeChangedThroughCallerCollectionsOrReturnedArrays() {
        var source = manifest(); var frozen = ObjectDigests.freeze(MANIFEST, source);
        String digest = frozen.sha256(); source.put("manifestId", "changed");
        byte[] returned = frozen.canonicalBytes(); returned[0] = 0;
        assertEquals(digest, CanonicalJson.sha256(frozen.canonicalBytes()));
    }

    @ParameterizedTest @ValueSource(strings = {"../x", "s3://test-bucket/run1/../a", "s3://test-bucket/a?token=secret", "s3://test-bucket/a#fragment", "s3://test-bucket/*.parquet", "https://evil/a"})
    void rejectsNoncanonicalOrDynamicObjectKeys(String key) {
        var bad = manifest(); bad.put("files", List.of(file(key, 0))); bad.put("totalRows", 2L);
        assertThrows(IllegalArgumentException.class, () -> ObjectDigests.freeze(MANIFEST, bad));
    }

    @Test void rejectsDuplicateKeysCommitIdentitiesWrongTotalsAndUnknownFields() {
        var a = file("s3://test-bucket/a", 0);
        for (var files : List.of(List.of(a, a), List.of(a, file("s3://test-bucket/b", 0)))) {
            var bad = manifest(); bad.put("files", files);
            assertThrows(IllegalArgumentException.class, () -> ObjectDigests.freeze(MANIFEST, bad));
        }
        var wrongTotal = manifest(); wrongTotal.put("totalRows", 5L);
        assertThrows(IllegalArgumentException.class, () -> ObjectDigests.freeze(MANIFEST, wrongTotal));
        var unknown = manifest(); unknown.put("recoveredAt", TIME.plusSeconds(1));
        assertThrows(IllegalArgumentException.class, () -> ObjectDigests.freeze(MANIFEST, unknown));
    }

    @ParameterizedTest @EnumSource(ObjectDigests.Kind.class)
    void rejectsEmptyOrIncompleteObjects(ObjectDigests.Kind kind) {
        assertThrows(IllegalArgumentException.class, () -> ObjectDigests.freeze(kind, Map.of()));
    }

    @Test void rejectsFloatingPointNullInvalidUnicodeAndUnorderedCollections() {
        for (Object value : List.of(1.0, Float.NaN, Double.POSITIVE_INFINITY, Set.of(1), "\ud800"))
            assertThrows(IllegalArgumentException.class, () -> CanonicalJson.encode(Map.of("value", value)));
        var nullMap = new HashMap<String, Object>(); nullMap.put("x", null);
        assertThrows(IllegalArgumentException.class, () -> CanonicalJson.encode(nullMap));
    }

    @Test void encodesExactDecimalsAndUtcInstantsWithoutWallClockFields() {
        String json = new String(CanonicalJson.encode(Map.of("amount", new BigDecimal("12.3400"), "completedAt", TIME)), StandardCharsets.UTF_8);
        assertEquals("{\"amount\":\"12.3400\",\"completedAt\":\"2026-10-18T08:00:00.000000000Z\"}", json);
        assertEquals(ObjectDigests.freeze(MANIFEST, manifest()).sha256(), ObjectDigests.freeze(MANIFEST, manifest()).sha256());
    }

    @Test void jobDefinitionIgnoresYamlFormattingAndResolvedRunRetainsParameterTypes() {
        var parser = new JobSpecParser(Duration.ofHours(1), 18, 2);
        String yaml = """
                version: v1
                name: daily_orders
                parameters: {businessDate: {type: date}}
                inputs: {orders: {dataset: orders_jdbc, batch: '${businessDate}'}}
                sql: SELECT * FROM orders
                output: {dataset: result_pg, batch: '${businessDate}', mode: replace-batch}
                """;
        var definition = parser.parse(yaml.getBytes(StandardCharsets.UTF_8));
        var formatted = parser.parse(("# comment\n" + yaml.replace("version: v1", "version: 'v1'")).getBytes(StandardCharsets.UTF_8));
        assertEquals(ObjectDigests.definition(definition).sha256(), ObjectDigests.definition(formatted).sha256());
        var bound = parser.bind(definition, Map.of("businessDate", "2026-10-18"));
        String typed = new String(CanonicalJson.encode(ObjectDigests.typedParameters(bound)), StandardCharsets.UTF_8);
        assertEquals("{\"businessDate\":{\"type\":\"date\",\"value\":\"2026-10-18\"}}", typed);
    }

    @Test void coversPlanCommitAndReceiptContractsWithZeroFileWriter() {
        Map<String, Object> plan = new LinkedHashMap<>();
        plan.put("runId", "run1"); plan.put("stageType", "COLLECT"); plan.put("inputAlias", "orders"); plan.put("stageAttemptId", "attempt1");
        plan.put("releaseId", "release1"); plan.put("resolvedSpecDigest", HASH); plan.put("consumedDataDigest", HASH);
        plan.put("inputs", Map.of("orders", Map.of("version", "snapshot1", "schemaDigest", HASH)));
        plan.put("schemaDigest", HASH); plan.put("parallelism", 1); plan.put("outputPrefix", "s3://test-bucket/run1/"); plan.put("plannedAt", TIME);
        assertEquals(64, ObjectDigests.freeze(STAGE_PLAN, plan).sha256().length());
        var evidence = Map.<String, Object>of("stageIdentity", ID, "files", List.of(),
                "writers", List.of(Map.of("writerId", "sink/0/0", "fileCount", 0, "ended", true)),
                "splits", List.of(Map.of("splitId", "split0", "evidenceDigest", HASH)), "completedAt", TIME);
        var commit = ObjectDigests.freeze(COMMIT_EVIDENCE, evidence);
        assertEquals(64, commit.sha256().length());
        var bad = new LinkedHashMap<>(evidence); bad.put("writers", List.of(Map.of("writerId", "sink/0/0", "fileCount", 1, "ended", true)));
        assertThrows(IllegalArgumentException.class, () -> ObjectDigests.freeze(COMMIT_EVIDENCE, bad));
        var receipt = Map.<String, Object>of("stageIdentity", ID, "status", "SUCCEEDED", "artifactDigest", HASH, "completedAt", TIME);
        assertEquals(64, ObjectDigests.freeze(STAGE_RECEIPT, receipt).sha256().length());
    }

    @Test void boundsDepthAndSizeAndFreezesResolvedDatasetVersions() {
        Object nested = "leaf";
        for (int i = 0; i < 66; i++) nested = List.of(nested);
        Object tooDeep = nested;
        assertThrows(IllegalArgumentException.class, () -> CanonicalJson.encode(tooDeep));
        assertThrows(IllegalArgumentException.class, () -> CanonicalJson.encode("x".repeat(1024 * 1024 + 1)));
        var parser = new JobSpecParser(Duration.ofHours(1), 18, 2);
        var spec = parser.parse("""
                version: v1
                name: daily_orders
                parameters: {}
                inputs: {orders: {dataset: orders_jdbc, batch: today}}
                sql: SELECT * FROM orders
                output: {dataset: result_pg, batch: today, mode: replace-batch}
                """.getBytes(StandardCharsets.UTF_8));
        var bound = parser.bind(spec, Map.of());
        var output = new ObjectDigests.DatasetVersion("result_pg", "v1", HASH);
        var inputs = Map.of("orders", new ObjectDigests.DatasetVersion("orders_jdbc", "v1", HASH));
        var first = ObjectDigests.resolved(bound, "run1", "job1", 1, "release1", inputs, output);
        var changed = Map.of("orders", new ObjectDigests.DatasetVersion("orders_jdbc", "v2", HASH));
        assertNotEquals(first.sha256(), ObjectDigests.resolved(bound, "run1", "job1", 1, "release1", changed, output).sha256());
    }

    @Test void rejectsOversizedPlainDecimalContent() {
        assertThrows(IllegalArgumentException.class, () -> CanonicalJson.encode(new BigDecimal("1e-2000000")));
    }

    @Test void schemaIdentityPreservesColumnOrderPrecisionAndNullability() {
        var a = new io.github.xuxiaotuan.magicflink.spec.SqlSchema.Column("amount", org.apache.flink.table.api.DataTypes.DECIMAL(18, 2));
        var b = new io.github.xuxiaotuan.magicflink.spec.SqlSchema.Column("id", org.apache.flink.table.api.DataTypes.BIGINT());
        var schema = new io.github.xuxiaotuan.magicflink.spec.SqlSchema(List.of(a, b));
        assertNotEquals(ObjectDigests.schema(schema).sha256(), ObjectDigests.schema(new io.github.xuxiaotuan.magicflink.spec.SqlSchema(List.of(b, a))).sha256());
        var changed = new io.github.xuxiaotuan.magicflink.spec.SqlSchema.Column("amount", org.apache.flink.table.api.DataTypes.DECIMAL(18, 1).notNull());
        assertNotEquals(ObjectDigests.schema(schema).sha256(), ObjectDigests.schema(new io.github.xuxiaotuan.magicflink.spec.SqlSchema(List.of(changed, b))).sha256());
    }
}
