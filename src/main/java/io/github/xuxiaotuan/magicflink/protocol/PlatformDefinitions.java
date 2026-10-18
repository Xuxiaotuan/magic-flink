package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.spec.JobSpec;
import io.github.xuxiaotuan.magicflink.spec.SqlSchema;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.net.URI;
import java.time.Duration;
import java.time.ZoneId;
import java.util.*;

/** Platform-owned P0 PostgreSQL definitions. These declarations do not attest to live database capabilities. */
public final class PlatformDefinitions {
    private PlatformDefinitions() {}
    public record Ref(String id, String version) {
        public Ref {
            identifier(id); text(version);
            if (!version.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,63}") || Set.of("latest", "current").contains(version.toLowerCase(Locale.ROOT)))
                throw new IllegalArgumentException("immutable configuration version required");
        }
    }
    public record SecretRef(String provider, String reference) {
        public SecretRef {
            if (!Set.of("vault", "kubernetes-secret").contains(provider)) throw new IllegalArgumentException("credential reference provider required");
            if (reference == null || !reference.matches("[A-Za-z0-9_/-]{1,128}") || reference.contains("..")) throw new IllegalArgumentException("opaque credential reference required");
        }
    }
    public enum Grant { READ, ISOLATED_WRITE, PUBLISH }
    public record ConnectionLimits(int total, int probes, int fetchSize, Duration queryTimeout) {
        public ConnectionLimits { positive(total); positive(probes); positive(fetchSize); duration(queryTimeout); if (probes >= total) throw new IllegalArgumentException("probe budget must leave query connections"); }
    }
    public record ConnectionProfile(Ref ref, String physicalId, URI endpoint, SecretRef credentials, boolean tlsRequired,
                                    ConnectionLimits limits, Set<String> resources, Set<Grant> grants) {
        public ConnectionProfile {
            Objects.requireNonNull(ref); identifier(physicalId); Objects.requireNonNull(credentials); Objects.requireNonNull(limits);
            if (endpoint == null || !"postgresql".equals(endpoint.getScheme()) || endpoint.getHost() == null
                    || !endpoint.getHost().equals(endpoint.getHost().toLowerCase(Locale.ROOT)) || endpoint.getUserInfo() != null
                    || endpoint.getQuery() != null || endpoint.getFragment() != null || endpoint.getPort() < 1 || endpoint.getPort() > 65535
                    || endpoint.getPath() == null || !endpoint.getPath().matches("/[a-z][a-z0-9_]{0,62}"))
                throw new IllegalArgumentException("approved PostgreSQL endpoint without credentials/options required");
            resources = Set.copyOf(resources); grants = Set.copyOf(grants);
            if (resources.isEmpty() || grants.isEmpty()) throw new IllegalArgumentException("resource grants required");
            resources.forEach(PlatformDefinitions::table);
        }
    }
    public record ReadyPolicy(String markerTable, String batchColumn, String versionColumn, String rowCountColumn,
                               String completedAtColumn, boolean immutableAfterReady) {
        public ReadyPolicy {
            table(markerTable); List.of(batchColumn, versionColumn, rowCountColumn, completedAtColumn).forEach(PlatformDefinitions::identifier);
            if (new HashSet<>(List.of(batchColumn, versionColumn, rowCountColumn, completedAtColumn)).size() != 4 || !immutableAfterReady)
                throw new IllegalArgumentException("distinct readiness fields and immutable sealed batch required");
        }
    }
    public record ReadPolicy(boolean bounded, boolean stableInputVersion, boolean replayable, List<String> projection,
                              String splitColumn, int maxConnections) {
        public ReadPolicy {
            if (!bounded || !stableInputVersion || !replayable) throw new IllegalArgumentException("P0 requires bounded immutable replayable batches");
            projection = names(projection); identifier(splitColumn); positive(maxConnections);
        }
    }
    public record JdbcDatasetDefinition(Ref ref, Ref connection, String table, SqlSchema schema, String batchColumn,
                                         JobSpec.ParameterType batchType, ZoneId timezone, ReadyPolicy ready, ReadPolicy read,
                                         boolean allowEmpty, Duration retention, Set<String> subjects, List<JobSpec.Check> requiredChecks) {
        public JdbcDatasetDefinition {
            Objects.requireNonNull(ref); Objects.requireNonNull(connection); PlatformDefinitions.table(table); PlatformDefinitions.schema(schema); identifier(batchColumn);
            Objects.requireNonNull(batchType); Objects.requireNonNull(timezone); Objects.requireNonNull(ready); Objects.requireNonNull(read); duration(retention);
            subjects = PlatformDefinitions.subjects(subjects); requiredChecks = PlatformDefinitions.requiredChecks(requiredChecks, schema);
            if (!batchMatches(batchType, column(schema, batchColumn).type().getLogicalType().getTypeRoot())) throw new IllegalArgumentException("batch type/schema mismatch");
            read.projection().forEach(name -> column(schema, name));
            PlatformDefinitions.requiredChecks(requiredChecks, new SqlSchema(read.projection().stream().map(name -> column(schema, name)).toList()));
            LogicalTypeRoot split = column(schema, read.splitColumn()).type().getLogicalType().getTypeRoot();
            if (!Set.of(LogicalTypeRoot.TINYINT, LogicalTypeRoot.SMALLINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BIGINT, LogicalTypeRoot.DATE).contains(split))
                throw new IllegalArgumentException("P0 range splitting requires integer or date column");
        }
    }
    public record TargetMetadata(String batchColumn, String loadIdColumn, String publicationIdColumn, String revisionColumn) {
        public TargetMetadata {
            var names = List.of(batchColumn, loadIdColumn, publicationIdColumn, revisionColumn); names.forEach(PlatformDefinitions::identifier);
            if (new HashSet<>(names).size() != 4) throw new IllegalArgumentException("distinct target metadata fields required");
        }
        List<String> columns() { return List.of(batchColumn, loadIdColumn, publicationIdColumn, revisionColumn); }
    }
    public record OutputDatasetDefinition(Ref ref, Ref writeConnection, Ref publishConnection, String versionTable, String pointerTable,
                                           String historyTable, String stateTable, String formalView, SqlSchema schema, TargetMetadata metadata,
                                           JobSpec.ParameterType batchType, List<String> businessKeys, List<String> compareColumns,
                                           String mode, Duration retention, Set<String> subjects, List<JobSpec.Check> requiredChecks) {
        public OutputDatasetDefinition {
            Objects.requireNonNull(ref); Objects.requireNonNull(writeConnection); Objects.requireNonNull(publishConnection); Objects.requireNonNull(metadata); Objects.requireNonNull(batchType);
            var resources = List.of(versionTable, pointerTable, historyTable, stateTable, formalView); resources.forEach(PlatformDefinitions::table);
            if (new HashSet<>(resources).size() != 5) throw new IllegalArgumentException("target objects must be distinct");
            PlatformDefinitions.schema(schema); requiredChecks = PlatformDefinitions.requiredChecks(requiredChecks, schema); businessKeys = names(businessKeys); compareColumns = names(compareColumns); subjects = PlatformDefinitions.subjects(subjects); duration(retention);
            businessKeys.forEach(name -> column(schema, name));
            if (!compareColumns.equals(schema.columns().stream().map(SqlSchema.Column::name).toList())) throw new IllegalArgumentException("compare all business fields in schema order");
            if (!Collections.disjoint(metadata.columns(), compareColumns)) throw new IllegalArgumentException("metadata collides with business columns");
            if (!"replace-batch".equals(mode)) throw new IllegalArgumentException("P0 supports replace-batch only");
        }
    }
    public record ComputeResources(int jmMiB, int tmMiB, int slots, int parallelism, int cpuLimitMilli) {
        public ComputeResources { positive(jmMiB); positive(tmMiB); positive(slots); positive(parallelism); positive(cpuLimitMilli); if (parallelism > slots) throw new IllegalArgumentException("parallelism exceeds fixed slots"); }
    }
    public record Concurrency(int activeRuns, int applications, int inputsPerRun, int checkThreads, int verifyThreads) {
        public Concurrency { positive(activeRuns); positive(applications); positive(inputsPerRun); positive(checkThreads); positive(verifyThreads); }
    }
    public record InlineLimits(long rows, long bytes, Duration timeout) {
        public InlineLimits { positive(rows); positive(bytes); duration(timeout); }
    }
    public record TimeLimits(Duration stage, Duration run, Duration evidenceWait, Duration poll) {
        public TimeLimits {
            duration(stage); duration(run); duration(evidenceWait); duration(poll);
            if (run.compareTo(stage) < 0 || evidenceWait.compareTo(stage) > 0 || poll.compareTo(evidenceWait) > 0) throw new IllegalArgumentException("inconsistent deadlines");
        }
    }
    public record RetryPolicy(int maxStageRetries, Duration backoff) {
        public RetryPolicy { if (maxStageRetries < 0) throw new IllegalArgumentException("negative retry budget"); duration(backoff); }
    }
    public record RuntimeProfile(Ref ref, String releaseId, URI storageScope, ComputeResources compute, Concurrency concurrency,
                                  InlineLimits inline, TimeLimits time, RetryPolicy retry, long verifyBufferBytes, Duration retention) {
        public RuntimeProfile {
            Objects.requireNonNull(ref); text(releaseId); ObjectKeys.prefix(storageScope); Objects.requireNonNull(compute); Objects.requireNonNull(concurrency);
            Objects.requireNonNull(inline); Objects.requireNonNull(time); Objects.requireNonNull(retry); positive(verifyBufferBytes); duration(retention);
            if (inline.timeout().compareTo(time.stage()) > 0 || retention.compareTo(time.run().plus(time.evidenceWait())) < 0) throw new IllegalArgumentException("runtime retention/timeout mismatch");
        }
    }

    public record Catalog(List<ConnectionProfile> connections, List<JdbcDatasetDefinition> inputs, OutputDatasetDefinition output, RuntimeProfile runtime) {
        public Catalog {
            connections = List.copyOf(connections); inputs = List.copyOf(inputs); Objects.requireNonNull(output); Objects.requireNonNull(runtime);
            if (inputs.isEmpty() || inputs.size() > 32 || connections.stream().map(ConnectionProfile::ref).distinct().count() != connections.size()
                    || inputs.stream().map(JdbcDatasetDefinition::ref).distinct().count() != inputs.size()) throw new IllegalArgumentException("unique versioned definitions required");
            Map<Ref, ConnectionProfile> byRef = new HashMap<>(); Map<URI, String> endpoints = new HashMap<>();
            Map<String, ConnectionProfile> physical = new HashMap<>();
            for (var connection : connections) {
                byRef.put(connection.ref(), connection);
                String id = endpoints.putIfAbsent(connection.endpoint(), connection.physicalId());
                if (id != null && !id.equals(connection.physicalId())) throw new IllegalArgumentException("same endpoint must share one physical budget");
                var other = physical.putIfAbsent(connection.physicalId(), connection);
                if (other != null && (!other.limits().equals(connection.limits()) || !other.endpoint().equals(connection.endpoint()))) throw new IllegalArgumentException("physical connection budget/endpoint mismatch");
            }
            for (var input : inputs) {
                var connection = require(byRef, input.connection());
                if (!connection.grants().equals(Set.of(Grant.READ)) || !connection.resources().containsAll(Set.of(input.table(), input.ready().markerTable()))) throw new IllegalArgumentException("source requires readonly resource grants");
                if (input.read().maxConnections() > connection.limits().total() - connection.limits().probes()
                        || connection.limits().queryTimeout().compareTo(runtime.time().stage()) > 0 || input.retention().compareTo(runtime.retention()) < 0)
                    throw new IllegalArgumentException("source budget/retention mismatch");
            }
            var writer = require(byRef, output.writeConnection()); var publisher = require(byRef, output.publishConnection());
            if (!writer.grants().containsAll(Set.of(Grant.READ, Grant.ISOLATED_WRITE)) || writer.grants().contains(Grant.PUBLISH)
                    || !publisher.grants().equals(Set.of(Grant.PUBLISH)) || !writer.physicalId().equals(publisher.physicalId()) || writer.credentials().equals(publisher.credentials()))
                throw new IllegalArgumentException("separate write and publication credentials required");
            if (!writer.resources().containsAll(Set.of(output.versionTable(), output.stateTable())) || !publisher.resources().containsAll(Set.of(output.pointerTable(), output.historyTable(), output.versionTable(), output.stateTable(), output.formalView()))
                    || output.retention().compareTo(runtime.retention()) < 0) throw new IllegalArgumentException("target resources/retention mismatch");
        }
        public Map<String, ConnectionLimits> connectionBudgets() {
            Map<String, ConnectionLimits> result = new TreeMap<>(); connections.forEach(c -> result.put(c.physicalId(), c.limits())); return Map.copyOf(result);
        }
        public void authorize(String subject) {
            if (!output.subjects().contains(subject) || inputs.stream().anyMatch(input -> !input.subjects().contains(subject))) throw new IllegalArgumentException("dataset access denied");
        }
    }

    static List<JobSpec.Check> requiredChecks(List<JobSpec.Check> checks, SqlSchema schema) {
        var result = List.copyOf(checks); Set<String> ids = new HashSet<>();
        if (result.size() > 64) throw new IllegalArgumentException("too many registered rules");
        for (var check : result) {
            identifier(check.id()); if (!ids.add(check.id())) throw new IllegalArgumentException("duplicate registered rule");
            if (check instanceof JobSpec.ColumnsCheck c) {
                Objects.requireNonNull(c.rule());
                if (c.input().isPresent()) throw new IllegalArgumentException("dataset rules use local columns only");
                names(c.columns()).forEach(name -> column(schema, name));
            } else if (check instanceof JobSpec.CountCheck c) {
                if (c.input().isPresent() || c.min() == null || c.max() == null || c.min().signum() < 0 || c.max().compareTo(c.min()) < 0)
                    throw new IllegalArgumentException("invalid registered row-count range");
            } else throw new IllegalArgumentException("P0 registered rules support not-null, unique-key and row-count");
        }
        return result;
    }
    static Map<String, Object> ref(Ref ref) { return Map.of("id", ref.id(), "version", ref.version()); }
    static List<Map<String, Object>> schemaFields(SqlSchema schema) { return schema.columns().stream().map(c -> Map.<String, Object>of("name", c.name(), "type", c.type().getLogicalType().asSerializableString())).toList(); }
    static void schema(SqlSchema schema) {
        Objects.requireNonNull(schema);
        for (var column : schema.columns()) {
            identifier(column.name());
            switch (column.type().getLogicalType().getTypeRoot()) {
                case BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, CHAR, VARCHAR, BINARY, VARBINARY, DATE, TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> { }
                default -> throw new IllegalArgumentException("unsupported physical field type");
            }
        }
    }
    static SqlSchema.Column column(SqlSchema schema, String name) { return schema.columns().stream().filter(c -> c.name().equals(name)).findFirst().orElseThrow(() -> new IllegalArgumentException("field absent from fixed schema")); }
    static boolean batchMatches(JobSpec.ParameterType type, LogicalTypeRoot root) {
        return switch (type) {
            case DATE -> root == LogicalTypeRoot.DATE; case STRING -> root == LogicalTypeRoot.VARCHAR || root == LogicalTypeRoot.CHAR;
            case BOOLEAN -> root == LogicalTypeRoot.BOOLEAN; case DECIMAL -> root == LogicalTypeRoot.DECIMAL;
            case INTEGER -> Set.of(LogicalTypeRoot.TINYINT, LogicalTypeRoot.SMALLINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BIGINT).contains(root);
        };
    }
    static void table(String value) { if (value == null || !value.matches("[a-z][a-z0-9_]{0,62}\\.[a-z][a-z0-9_]{0,62}")) throw new IllegalArgumentException("fixed schema.table required"); }
    static void identifier(String value) { if (value == null || !value.matches("[a-z][a-z0-9_]{0,62}")) throw new IllegalArgumentException("invalid platform identifier"); }
    static void text(String value) { if (value == null || value.isBlank()) throw new IllegalArgumentException("required domain field"); }
    static void digest(String value) { if (value == null || !value.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("SHA-256 required"); }
    static void positive(long value) { if (value <= 0) throw new IllegalArgumentException("positive budget required"); }
    static void duration(Duration value) { if (value == null || value.isNegative() || value.isZero()) throw new IllegalArgumentException("positive finite duration required"); }
    static List<String> names(List<String> names) { var copy = List.copyOf(names); copy.forEach(PlatformDefinitions::identifier); if (copy.isEmpty() || new HashSet<>(copy).size() != copy.size()) throw new IllegalArgumentException("nonempty distinct fields required"); return copy; }
    static Set<String> subjects(Set<String> subjects) { var copy = Set.copyOf(subjects); if (copy.isEmpty()) throw new IllegalArgumentException("access subjects required"); copy.forEach(PlatformDefinitions::identifier); return copy; }
    private static ConnectionProfile require(Map<Ref, ConnectionProfile> connections, Ref ref) { var result = connections.get(ref); if (result == null) throw new IllegalArgumentException("versioned connection missing"); return result; }
}
