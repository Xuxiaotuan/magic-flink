package io.github.xuxiaotuan.magicflink.spec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JobSpecParserTest {
    private final JobSpecParser parser = new JobSpecParser(Duration.ofHours(1), 38, 18);
    private static final String BASE = """
            version: v1
            name: daily_orders
            parameters:
              businessDate: {type: date}
            inputs:
              orders: {dataset: orders_jdbc, batch: '${businessDate}'}
            sql: SELECT * FROM orders
            output: {dataset: enriched_orders, batch: '${businessDate}', mode: replace-batch}
            """;

    private JobSpec parse(String yaml) { return parser.parse(yaml.getBytes(StandardCharsets.UTF_8)); }

    @Test void bindsDatesWithoutRewritingSql() {
        var spec = parse(BASE);
        var bound = parser.bind(spec, Map.of("businessDate", "2026-10-18"));
        assertEquals(LocalDate.of(2026, 10, 18), bound.inputBatches().get("orders"));
        assertEquals(bound.inputBatches().get("orders"), bound.outputBatch());
        assertEquals("SELECT * FROM orders", bound.spec().sql());
        assertTrue(spec.preChecks().isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> bound.parameters().clear());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "image: evil", "name: repeated", "preChecks: null", "unknown: value",
            "---\nversion: v1", "preChecks: &x []", "preChecks: *x", "preChecks: !evil []",
            "<<: {image: evil}", "? [a, b]\n: value"
    })
    void rejectsUnsafeDocumentForms(String extra) {
        assertThrows(IllegalArgumentException.class, () -> parse(BASE + extra + "\n"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{type: date, default: '2026-10-18'}",
            "{type: date, required: false}",
            "{type: date, required: false, default: 2026-10-18}",
            "{type: date, required: false, default: '2026-02-30'}",
            "{type: decimal, required: false, default: 0.01}",
            "{type: boolean, required: false, default: yes}",
            "{type: integer, required: false, default: '1'}",
            "{type: string, nullable: true}", "{type: string, required: null}", "{type: script}"
    })
    void rejectsInvalidDeclarations(String declaration) {
        assertThrows(IllegalArgumentException.class, () -> parse(BASE.replace("{type: date}", declaration)));
    }

    @Test void mergesExactDefaultsAndRejectsWrongRunValues() {
        var spec = parse(BASE.replace("{type: date}", "{type: decimal, required: false, default: '12.3400'}"));
        assertEquals(new BigDecimal("12.3400"), parser.bind(spec, Map.of()).outputBatch());
        for (Object invalid : new Object[]{12.34, 12, Map.of(), java.util.List.of(), "NaN", "1e3", "0.0000000000000000001"}) {
            assertThrows(IllegalArgumentException.class, () -> parser.bind(spec, Map.of("businessDate", invalid)));
        }
        assertThrows(IllegalArgumentException.class, () -> parser.bind(spec, Map.of("undeclared", "x")));
        assertThrows(IllegalArgumentException.class, () -> parser.bind(parse(BASE), Map.of()));
        var nullValue = new java.util.HashMap<String, Object>(); nullValue.put("businessDate", null);
        assertThrows(IllegalArgumentException.class, () -> parser.bind(spec, nullValue));
    }

    @ParameterizedTest
    @ValueSource(strings = {"${unknown}", "day_${businessDate}", "${businessDate}_end", "${}"})
    void rejectsInvalidBatchReferences(String batch) {
        assertThrows(IllegalArgumentException.class, () -> parse(BASE.replace("${businessDate}", batch)));
    }

    @Test void rejectsSqlInterpolationAndReservedAlias() {
        assertThrows(IllegalArgumentException.class, () -> parse(BASE.replace("SELECT * FROM orders", "SELECT '${businessDate}'")));
        assertThrows(IllegalArgumentException.class, () -> parse(BASE.replace("  orders:", "  job_params:")));
    }

    @Test void parsesEveryAllowedCheckAndDefaults() {
        var spec = parse(BASE + """
                preChecks:
                  - {id: ready, type: batch-ready, inputs: [orders]}
                  - {id: keys, type: unique-key, input: orders, columns: [id]}
                  - {id: count, type: row-count, input: orders, min: 0, max: 10}
                postChecks:
                  - {id: required_col, type: not-null, columns: [id]}
                  - {id: equal_keys, type: key-set-equal, expectedInput: orders, columns: [id]}
                  - {id: equal_sum, type: sum-equal, expectedInput: orders, column: amount}
                """);
        assertEquals(3, spec.preChecks().size());
        assertEquals(Duration.ofMinutes(30), ((JobSpec.ReadyCheck) spec.preChecks().get(0)).timeout());
        assertEquals(new BigDecimal("0.00"), ((JobSpec.SumCheck) spec.postChecks().get(2)).tolerance());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{id: x, type: row-compare}",
            "{id: x, type: batch-ready, inputs: []}",
            "{id: x, type: batch-ready, inputs: [orders, orders]}",
            "{id: x, type: batch-ready, inputs: [missing]}",
            "{id: x, type: batch-ready, inputs: [orders], timeout: 0s}",
            "{id: x, type: batch-ready, inputs: [orders], timeout: 61m}",
            "{id: x, type: batch-ready, inputs: [orders], timeout: 999999999999999999999h}",
            "{id: x, type: unique-key, input: orders, columns: [id, id]}",
            "{id: x, type: not-null, input: orders, columns: []}",
            "{id: x, type: row-count, input: orders, min: 2, max: 1}",
            "{id: x, type: row-count, input: orders, min: -1, max: 1}",
            "{id: x, type: row-count, input: orders, min: 0, max: 1, sql: DELETE}",
            "{id: x, type: sum-equal, expectedInput: orders, column: amount}"
    })
    void rejectsInvalidPreChecks(String check) {
        assertThrows(IllegalArgumentException.class, () -> parse(BASE + "preChecks: [" + check + "]\n"));
    }

    @Test void rejectsDuplicateIdsAcrossPhasesAndNegativeTolerance() {
        assertThrows(IllegalArgumentException.class, () -> parse(BASE + """
                preChecks: [{id: same, type: not-null, input: orders, columns: [id]}]
                postChecks: [{id: same, type: not-null, columns: [id]}]
                """));
        assertThrows(IllegalArgumentException.class, () -> parse(BASE + "postChecks: [{id: x, type: sum-equal, expectedInput: orders, column: amount, tolerance: '-0.01'}]\n"));
        assertThrows(IllegalArgumentException.class, () -> parse(BASE + "postChecks: [{id: x, type: batch-ready, inputs: [orders]}]\n"));
    }

    @Test void boundsDocumentSizeDepthInputAndCheckCounts() {
        assertThrows(IllegalArgumentException.class, () -> parser.parse(new byte[256 * 1024 + 1]));
        assertThrows(IllegalArgumentException.class, () -> parser.parse(new byte[]{(byte) 0xc3, 0x28}));
        assertThrows(IllegalArgumentException.class, () -> parse(""));
        assertThrows(IllegalArgumentException.class, () -> parse(BASE + "preChecks: " + "[".repeat(60) + "]".repeat(60)));
        StringBuilder inputs = new StringBuilder();
        for (int i = 0; i < 33; i++) inputs.append("  a").append(i).append(": {dataset: source, batch: today}\n");
        assertThrows(IllegalArgumentException.class, () -> parse(BASE.replace("  orders: {dataset: orders_jdbc, batch: '${businessDate}'}\n", inputs)));
        StringBuilder checks = new StringBuilder("preChecks:\n");
        for (int i = 0; i < 65; i++) checks.append("  - {id: c").append(i).append(", type: not-null, input: orders, columns: [id]}\n");
        assertThrows(IllegalArgumentException.class, () -> parse(BASE + checks));
    }

    @Test void bindingUsesDefinitionDecimalPolicyEvenIfServicePolicyChanges() {
        var narrowParser = new JobSpecParser(Duration.ofHours(1), 5, 2);
        var definition = narrowParser.parse(BASE.replace("{type: date}", "{type: decimal}").getBytes(StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, () -> parser.bind(definition, Map.of("businessDate", "1.234")));
        assertEquals(new BigDecimal("999.99"), parser.bind(definition, Map.of("businessDate", "999.99")).outputBatch());
        assertThrows(IllegalArgumentException.class, () -> parser.bind(definition, Map.of("businessDate", "1000.00")));
    }

    @Test void bindsAllScalarTypesAndLiteralBatches() {
        var definition = parse(BASE.replace("  businessDate: {type: date}", """
                  businessDate: {type: date}
                  enabled: {type: boolean, required: false, default: true}
                  count: {type: integer, required: false, default: 42}
                  label: {type: string, required: false, default: ''}""".indent(2).stripTrailing())
                .replace("batch: '${businessDate}', mode", "batch: fixed_batch, mode"));
        var bound = parser.bind(definition, Map.of("businessDate", "2024-02-29", "count", 100L));
        assertEquals(Boolean.TRUE, bound.parameters().get("enabled"));
        assertEquals(new java.math.BigInteger("100"), bound.parameters().get("count"));
        assertEquals("", bound.parameters().get("label"));
        assertEquals("fixed_batch", bound.outputBatch());
        assertThrows(IllegalArgumentException.class, () -> parser.bind(definition, Map.of("businessDate", "2023-02-29")));
    }

    @Test void parsesTheFullDesignExample() throws Exception {
        try (var input = getClass().getResourceAsStream("/spec/daily-orders.yaml")) {
            assertNotNull(input);
            var definition = parser.parse(input.readAllBytes());
            assertEquals(4, definition.inputs().size());
            assertEquals(4, definition.preChecks().size());
            assertEquals(3, definition.postChecks().size());
            assertEquals(4, parser.bind(definition, Map.of("businessDate", "2026-10-18")).inputBatches().size());
        }
    }
}
