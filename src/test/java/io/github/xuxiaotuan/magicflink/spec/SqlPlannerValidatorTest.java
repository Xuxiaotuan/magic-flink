package io.github.xuxiaotuan.magicflink.spec;

import org.apache.flink.table.api.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SqlPlannerValidatorTest {
    private final SqlPlannerValidator validator = new SqlPlannerValidator();
    private final JobSpecParser parser = new JobSpecParser(Duration.ofHours(1), 18, 2);
    private static final SqlSchema ORDERS = new SqlSchema(List.of(
            new SqlSchema.Column("id", DataTypes.BIGINT()),
            new SqlSchema.Column("amount", DataTypes.DECIMAL(18, 2)),
            new SqlSchema.Column("business_date", DataTypes.DATE())));
    private static final SqlSchema CUSTOMERS = new SqlSchema(List.of(
            new SqlSchema.Column("id", DataTypes.BIGINT()), new SqlSchema.Column("name", DataTypes.STRING())));
    private JobSpec job(String sql) {
        return parser.parse(("""
                version: v1
                name: daily_orders
                parameters: {businessDate: {type: date}, threshold: {type: decimal}}
                inputs:
                  orders: {dataset: orders_jdbc, batch: '${businessDate}'}
                  customers: {dataset: customers_jdbc, batch: '${businessDate}'}
                sql: |
                """ + sql.indent(2) + "output: {dataset: result_pg, batch: '${businessDate}', mode: replace-batch}\n").getBytes(StandardCharsets.UTF_8));
    }
    private SqlPlannerValidator.Report validate(String sql, SqlSchema output) {
        return validator.validate(job(sql), Map.of("orders", ORDERS, "customers", CUSTOMERS), output);
    }

    @Test void plansBoundedLeftJoinWithoutRunningAJob() {
        var output = new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.BIGINT()),
                new SqlSchema.Column("name", DataTypes.STRING())));
        var report = validate("SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON o.id=c.id", output);
        assertEquals("1.20.5", report.flinkVersion());
        assertEquals(List.of("id", "name"), report.output().columns().stream().map(SqlSchema.Column::name).toList());
        assertEquals(java.util.Set.of("orders", "customers"), report.referencedInputs());
    }

    @Test void plansCteExactAggregateAndTypedParametersWithoutRunValues() {
        var report = validate("""
                WITH selected AS (SELECT id, amount FROM orders WHERE business_date=(SELECT businessDate FROM job_params))
                SELECT id, SUM(amount) AS total FROM selected GROUP BY id
                """, new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.BIGINT()),
                new SqlSchema.Column("total", DataTypes.DECIMAL(38, 2)))));
        assertTrue(report.referencedInputs().contains("orders"));
    }

    @Test void acceptsEquiJoinWithAdditionalPredicatesAndLiteralSemicolons() {
        validate("SELECT o.id FROM orders o INNER JOIN customers c ON o.id=c.id AND c.name <> ';'", new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.BIGINT()))));
    }

    @ParameterizedTest @ValueSource(strings = {
            "CREATE TABLE bad (id BIGINT)", "INSERT INTO orders SELECT * FROM orders", "SET 'parallelism.default'='99'",
            "ADD JAR 'https://evil/jar'", "SHOW TABLES", "SELECT id FROM orders; SELECT id FROM customers",
            "SELECT id FROM other_catalog.other_db.orders", "SELECT id FROM other_db.orders", "SELECT id FROM missing",
            "SELECT o.id FROM orders o RIGHT JOIN customers c ON o.id=c.id",
            "SELECT o.id FROM orders o FULL JOIN customers c ON o.id=c.id",
            "SELECT o.id FROM orders o CROSS JOIN customers c",
            "SELECT o.id FROM orders o, customers c WHERE o.id=c.id",
            "SELECT o.id FROM orders o JOIN customers c ON o.id>c.id",
            "SELECT o.id FROM orders o JOIN customers c ON o.id=o.id",
            "SELECT RAND() AS id FROM orders", "SELECT CURRENT_TIMESTAMP AS id FROM orders",
            "SELECT UUID() AS id FROM orders", "SELECT missing_udf(id) AS id FROM orders",
            "SELECT CAST(amount AS DOUBLE) AS id FROM orders",
            "SELECT id FROM orders WHERE CAST(amount AS DOUBLE)>0",
            "SELECT id FROM orders LIMIT 1", "SELECT id FROM orders ORDER BY id LIMIT 1",
            "SELECT id FROM orders UNION ALL SELECT id FROM customers"
    })
    void rejectsForbiddenOrUnprovenCapabilities(String sql) {
        assertThrows(SqlValidationException.class, () -> validate(sql, new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.BIGINT())))));
    }

    @Test void rejectsNameTypeAndLossyOutputConversions() {
        assertThrows(SqlValidationException.class, () -> validate("SELECT unknown_col FROM orders", ORDERS));
        assertThrows(SqlValidationException.class, () -> validate("SELECT id FROM orders", new SqlSchema(List.of(new SqlSchema.Column("other", DataTypes.BIGINT())))));
        assertThrows(SqlValidationException.class, () -> validate("SELECT amount FROM orders", new SqlSchema(List.of(new SqlSchema.Column("amount", DataTypes.DECIMAL(18, 1))))));
        assertThrows(SqlValidationException.class, () -> validate("SELECT id FROM orders", new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.INT())))));
        assertThrows(SqlValidationException.class, () -> validate("SELECT id FROM orders", new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.BIGINT().notNull())))));
    }

    @Test void comparesTimestampSemanticsAndRejectsComplexAndApproximateTypes() {
        var timestamp = new SqlSchema(List.of(new SqlSchema.Column("t", DataTypes.TIMESTAMP(3))));
        assertThrows(SqlValidationException.class, () -> validator.validate(job("SELECT t FROM orders"),
                Map.of("orders", timestamp, "customers", CUSTOMERS), new SqlSchema(List.of(new SqlSchema.Column("t", DataTypes.TIMESTAMP_LTZ(3))))));
        for (var type : List.of(DataTypes.DOUBLE(), DataTypes.ARRAY(DataTypes.INT()), DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))) {
            var unsupported = new SqlSchema(List.of(new SqlSchema.Column("id", type)));
            assertThrows(SqlValidationException.class, () -> validator.validate(job("SELECT id FROM orders"), Map.of("orders", unsupported, "customers", CUSTOMERS), unsupported));
        }
    }

    @Test void doesNotRetainTablesOrCtesBetweenValidations() {
        validate("WITH previous AS (SELECT id FROM orders) SELECT id FROM previous", new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.BIGINT()))));
        assertThrows(SqlValidationException.class, () -> validate("SELECT id FROM previous", new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.BIGINT())))));
    }

    @Test void errorsHaveStableCodeAndPathWithoutSqlLiteralLeakage() {
        var error = assertThrows(SqlValidationException.class, () -> validate("SELECT secret_missing('sensitive-value') AS id FROM orders", ORDERS));
        assertEquals("sql", error.path());
        assertTrue(error.code().startsWith("SQL_"));
        assertFalse(error.getMessage().contains("sensitive-value"));
    }

    @Test void forbidsDecimalNarrowingAndFunctionsEvenInsideFoldedExpressions() {
        var narrow = new SqlSchema(List.of(new SqlSchema.Column("amount", DataTypes.DECIMAL(18, 1))));
        var error = assertThrows(SqlValidationException.class, () -> validate("SELECT CAST(amount AS DECIMAL(18,1)) AS amount FROM orders", narrow));
        assertEquals("SQL_LOSSY_CAST", error.code());
        var idOnly = new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.BIGINT())));
        var random = assertThrows(SqlValidationException.class, () -> validate("SELECT id FROM orders WHERE FALSE AND RAND()>0", idOnly));
        assertEquals("SQL_FORBIDDEN_FUNCTION", random.code());
        validate("SELECT ROUND(amount, 1) AS amount FROM orders", narrow);
    }

    @Test void rejectsDecimalToIntegerAndIntegerDowncasts() {
        var integer = new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.INT())));
        var bigint = new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.BIGINT())));
        assertEquals("SQL_LOSSY_CAST", assertThrows(SqlValidationException.class,
                () -> validate("SELECT CAST(id AS INT) AS id FROM orders", integer)).code());
        assertEquals("SQL_LOSSY_CAST", assertThrows(SqlValidationException.class,
                () -> validate("SELECT CAST(amount AS BIGINT) AS id FROM orders", bigint)).code());
    }

    @ParameterizedTest @ValueSource(strings = {
            "SELECT id FROM orders WHERE FALSE AND CAST(1 AS DOUBLE)>0",
            "SELECT id FROM orders WHERE FALSE AND CURRENT_DATE>DATE '2020-01-01'",
            "SELECT id FROM orders WHERE FALSE AND 1e0>0"
    })
    void cannotHideForbiddenExpressionsInConstantBranches(String sql) {
        assertThrows(SqlValidationException.class, () -> validate(sql, new SqlSchema(List.of(new SqlSchema.Column("id", DataTypes.BIGINT())))));
    }

    @Test void runPlanningRejectsIntegerValuesOutsideTheDeclaredSqlRepresentation() {
        var base = job("SELECT threshold FROM job_params");
        var declarations = new java.util.HashMap<>(base.parameters());
        declarations.put("threshold", new JobSpec.Parameter(JobSpec.ParameterType.INTEGER, true, java.util.Optional.empty()));
        var definition = new JobSpec(base.name(), base.decimalRules(), declarations, base.inputs(), base.preChecks(), base.sql(), base.postChecks(), base.output());
        var schemas = Map.of("orders", ORDERS, "customers", CUSTOMERS);
        var output = new SqlSchema(List.of(new SqlSchema.Column("threshold", DataTypes.DECIMAL(38, 0))));
        var tooLarge = parser.bind(definition, Map.of("businessDate", "2026-10-18", "threshold", new java.math.BigInteger("1".repeat(39))));
        assertEquals("SQL_PARAMETER_RANGE", assertThrows(SqlValidationException.class, () -> validator.validate(tooLarge, schemas, output)).code());
        var good = parser.bind(definition, Map.of("businessDate", "2026-10-18", "threshold", new java.math.BigInteger("9".repeat(38))));
        assertEquals("1.20.5", validator.validate(good, schemas, output).flinkVersion());
    }
}
