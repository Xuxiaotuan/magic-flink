package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.spec.JobSpec;
import io.github.xuxiaotuan.magicflink.spec.SqlSchema;
import org.apache.flink.table.api.DataTypes;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.time.ZoneId;
import java.util.*;

import static io.github.xuxiaotuan.magicflink.protocol.PlatformDefinitions.*;
import static org.junit.jupiter.api.Assertions.*;

class PlatformDefinitionsTest {
    static final Ref CONNECTION = new Ref("pg_reader", "v1");
    static final SqlSchema SCHEMA = new SqlSchema(List.of(
            new SqlSchema.Column("id", DataTypes.BIGINT().notNull()), new SqlSchema.Column("amount", DataTypes.DECIMAL(18, 2)),
            new SqlSchema.Column("business_date", DataTypes.DATE())));
    static final ConnectionLimits LIMITS = new ConnectionLimits(8, 2, 1000, Duration.ofSeconds(30));
    static ConnectionProfile connection(Ref ref, Set<Grant> grants) {
        return new ConnectionProfile(ref, "postgres_local", URI.create("postgresql://localhost:5432/magic"),
                new SecretRef("vault", ref.id()), true, LIMITS,
                Set.of("source.orders", "source.customers", "control.batches", "target.versions", "target.pointers", "target.history", "target.states", "target.current_orders"), grants);
    }
    static JdbcDatasetDefinition dataset(String name, String table) {
        return new JdbcDatasetDefinition(new Ref(name, "v1"), CONNECTION, table, SCHEMA, "business_date", JobSpec.ParameterType.DATE,
                ZoneId.of("Asia/Shanghai"), new ReadyPolicy("control.batches", "business_date", "source_version", "total_rows", "completed_at", true),
                new ReadPolicy(true, true, true, List.of("id", "amount", "business_date"), "id", 2),
                false, Duration.ofDays(7), Set.of("batch_user"), List.of());
    }
    static RuntimeProfile runtime() {
        return new RuntimeProfile(new Ref("p0", "v1"), "release1", URI.create("s3://test-bucket/magic/"),
                new ComputeResources(1024, 1536, 4, 1, 3250), new Concurrency(2, 2, 2, 2, 2),
                new InlineLimits(10000, 8 * 1024 * 1024, Duration.ofSeconds(30)),
                new TimeLimits(Duration.ofMinutes(15), Duration.ofMinutes(30), Duration.ofSeconds(120), Duration.ofSeconds(5)),
                new RetryPolicy(1, Duration.ofSeconds(5)), 16 * 1024 * 1024, Duration.ofDays(7));
    }
    static OutputDatasetDefinition output() {
        return new OutputDatasetDefinition(new Ref("enriched_orders", "v1"), new Ref("pg_writer", "v1"), new Ref("pg_publisher", "v1"),
                "target.versions", "target.pointers", "target.history", "target.states", "target.current_orders", SCHEMA,
                new TargetMetadata("batch_key", "load_id", "publication_id", "revision"), JobSpec.ParameterType.DATE,
                List.of("id"), List.of("id", "amount", "business_date"), "replace-batch", Duration.ofDays(7), Set.of("batch_user"), List.of());
    }

    @Test void validatesP0CatalogAndSharesPhysicalConnectionBudget() {
        var reader = connection(CONNECTION, Set.of(Grant.READ));
        var writer = connection(output().writeConnection(), Set.of(Grant.READ, Grant.ISOLATED_WRITE));
        var publisher = connection(output().publishConnection(), Set.of(Grant.PUBLISH));
        var catalog = new Catalog(List.of(reader, writer, publisher), List.of(dataset("orders", "source.orders"), dataset("customers", "source.customers")), output(), runtime());
        assertEquals(Map.of("postgres_local", LIMITS), catalog.connectionBudgets());
        catalog.authorize("batch_user");
        assertThrows(IllegalArgumentException.class, () -> catalog.authorize("someone_else"));
    }

    @Test void rejectsUnversionedRefsCredentialsAndUnsafePhysicalResources() {
        assertThrows(IllegalArgumentException.class, () -> new Ref("pg_reader", "latest"));
        assertThrows(IllegalArgumentException.class, () -> ObjectKeys.prefix(URI.create("s3://test-bucket/../")));
        assertThrows(IllegalArgumentException.class, () -> ObjectKeys.file(URI.create("s3://test-bucket/../escape.json")));
        assertThrows(IllegalArgumentException.class, () -> new SecretRef("plaintext", "password"));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionProfile(CONNECTION, "postgres_local",
                URI.create("postgresql://user:password@localhost:5432/magic"), new SecretRef("vault", "pg_reader"), true, LIMITS, Set.of("source.orders"), Set.of(Grant.READ)));
        assertThrows(IllegalArgumentException.class, () -> dataset("orders", "source.orders;DROP TABLE users"));
    }

    @Test void requiresStableBoundedReplayableSourceAndSchemaMatchedFields() {
        assertThrows(IllegalArgumentException.class, () -> new ReadPolicy(false, true, true, List.of("id"), "id", 1));
        assertThrows(IllegalArgumentException.class, () -> new ReadPolicy(true, false, true, List.of("id"), "id", 1));
        var original = dataset("orders", "source.orders");
        assertThrows(IllegalArgumentException.class, () -> new JdbcDatasetDefinition(original.ref(), CONNECTION, original.table(), SCHEMA,
                "amount", JobSpec.ParameterType.DATE, original.timezone(), original.ready(), original.read(), false, original.retention(), original.subjects(), original.requiredChecks()));
        assertThrows(IllegalArgumentException.class, () -> new JdbcDatasetDefinition(original.ref(), CONNECTION, original.table(), SCHEMA,
                original.batchColumn(), original.batchType(), original.timezone(), original.ready(),
                new ReadPolicy(true, true, true, List.of("unknown"), "id", 1), false, original.retention(), original.subjects(), original.requiredChecks()));
        assertThrows(IllegalArgumentException.class, () -> new ReadyPolicy("control.batches", "batch", "version", "rows", "finished", false));
    }

    @Test void outputMustCompareEveryBusinessColumnAndIsolateMetadata() {
        var o = output();
        assertThrows(IllegalArgumentException.class, () -> new OutputDatasetDefinition(o.ref(), o.writeConnection(), o.publishConnection(),
                o.versionTable(), o.pointerTable(), o.historyTable(), o.stateTable(), o.formalView(), SCHEMA, o.metadata(), o.batchType(),
                o.businessKeys(), List.of("id"), o.mode(), o.retention(), o.subjects(), o.requiredChecks()));
        assertThrows(IllegalArgumentException.class, () -> new TargetMetadata("batch", "load", "load", "revision"));
    }

    @Test void rejectsPrivilegeEscalationMissingConnectionAndAliasBudgetBypass() {
        var datasets = List.of(dataset("orders", "source.orders"), dataset("customers", "source.customers"));
        var reader = connection(CONNECTION, Set.of(Grant.READ));
        assertThrows(IllegalArgumentException.class, () -> new Catalog(List.of(reader), datasets, output(), runtime()));
        var writer = connection(output().writeConnection(), Set.of(Grant.READ, Grant.ISOLATED_WRITE, Grant.PUBLISH));
        var publisher = connection(output().publishConnection(), Set.of(Grant.PUBLISH));
        assertThrows(IllegalArgumentException.class, () -> new Catalog(List.of(reader, writer, publisher), datasets, output(), runtime()));
        var bypass = new ConnectionProfile(publisher.ref(), "another_budget", publisher.endpoint(), publisher.credentials(), publisher.tlsRequired(), LIMITS, publisher.resources(), publisher.grants());
        assertThrows(IllegalArgumentException.class, () -> new Catalog(List.of(reader, connection(output().writeConnection(), Set.of(Grant.READ, Grant.ISOLATED_WRITE)), bypass), datasets, output(), runtime()));
    }

    @Test void registeredRulesAreSchemaCheckedAndFrozen() {
        var d = dataset("orders", "source.orders");
        var checks = List.<JobSpec.Check>of(new JobSpec.ColumnsCheck("required_id", JobSpec.ColumnRule.NOT_NULL, Optional.empty(), List.of("id")));
        var withRule = new JdbcDatasetDefinition(d.ref(), d.connection(), d.table(), d.schema(), d.batchColumn(), d.batchType(), d.timezone(), d.ready(), d.read(), d.allowEmpty(), d.retention(), d.subjects(), checks);
        var c = FileStageProtocolTest.catalog();
        var amended = new Catalog(c.connections(), List.of(withRule, dataset("customers", "source.customers")), c.output(), c.runtime());
        assertNotEquals(DomainSnapshots.catalog(c).sha256(), DomainSnapshots.catalog(amended).sha256());
        assertThrows(IllegalArgumentException.class, () -> new JdbcDatasetDefinition(d.ref(), d.connection(), d.table(), d.schema(), d.batchColumn(), d.batchType(), d.timezone(), d.ready(), d.read(), d.allowEmpty(), d.retention(), d.subjects(),
                List.of(new JobSpec.ColumnsCheck("bad_column", JobSpec.ColumnRule.NOT_NULL, Optional.empty(), List.of("missing")))));
        assertThrows(IllegalArgumentException.class, () -> new JdbcDatasetDefinition(d.ref(), d.connection(), d.table(), d.schema(), d.batchColumn(), d.batchType(), d.timezone(), d.ready(),
                new ReadPolicy(true, true, true, List.of("amount", "business_date"), "id", 1), d.allowEmpty(), d.retention(), d.subjects(), checks));
        var o = output();
        assertThrows(IllegalArgumentException.class, () -> new OutputDatasetDefinition(o.ref(), o.writeConnection(), o.publishConnection(), o.versionTable(), o.pointerTable(), o.historyTable(), o.versionTable(), o.formalView(), o.schema(), o.metadata(), o.batchType(), o.businessKeys(), o.compareColumns(), o.mode(), o.retention(), o.subjects(), o.requiredChecks()));
    }

    @Test void checksResourceTimeoutRetentionAndRetryRelations() {
        assertThrows(IllegalArgumentException.class, () -> new ComputeResources(1024, 1536, 1, 4, 1000));
        assertThrows(IllegalArgumentException.class, () -> new TimeLimits(Duration.ofMinutes(15), Duration.ofMinutes(10), Duration.ofSeconds(120), Duration.ofSeconds(5)));
        assertThrows(IllegalArgumentException.class, () -> new TimeLimits(Duration.ofMinutes(15), Duration.ofMinutes(30), Duration.ofSeconds(5), Duration.ofSeconds(10)));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionLimits(2, 2, 1000, Duration.ofSeconds(30)));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(-1, Duration.ofSeconds(5)));
    }
}
