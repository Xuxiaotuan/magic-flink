package io.github.xuxiaotuan.magicflink.protocol;

import io.github.xuxiaotuan.magicflink.spec.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;
import static io.github.xuxiaotuan.magicflink.protocol.PlatformDefinitions.*;

/** Fully bound P0 admission snapshot; version selection is explicit and never reads a mutable "latest" registry. */
public record ResolvedRun(String runId, String jobId, long jobVersion, String subject, JobSpec.BoundJob job,
                           Map<String, Ref> inputVersions, Ref outputVersion, Catalog catalog, Instant acceptedAt, Instant runDeadline) {
    private static final Pattern PARAMETER = Pattern.compile("\\$\\{([A-Za-z][A-Za-z0-9_]{0,62})}");
    public ResolvedRun {
        FileStageProtocol.pathId(runId); FileStageProtocol.pathId(jobId); positive(jobVersion); identifier(subject);
        Objects.requireNonNull(job); Objects.requireNonNull(catalog); Objects.requireNonNull(outputVersion); Objects.requireNonNull(acceptedAt); Objects.requireNonNull(runDeadline);
        inputVersions = Map.copyOf(inputVersions); catalog.authorize(subject);
        if (inputVersions.size() > catalog.runtime().concurrency().inputsPerRun()) throw new IllegalArgumentException("run exceeds input budget");
        if (!runDeadline.equals(acceptedAt.plus(catalog.runtime().time().run()))) throw new IllegalArgumentException("run deadline must be fixed at admission");
        if (!inputVersions.keySet().equals(job.spec().inputs().keySet()) || !job.inputBatches().keySet().equals(inputVersions.keySet())
                || !job.parameters().keySet().equals(job.spec().parameters().keySet()) || !catalog.output().ref().equals(outputVersion)
                || !job.spec().output().dataset().equals(outputVersion.id()) || !job.spec().output().mode().equals(catalog.output().mode()))
            throw new IllegalArgumentException("resolved definition/version/parameter fields mismatch");
        job.spec().parameters().forEach((name, declaration) -> {
            Object value = job.parameters().get(name); FileStageProtocol.batchValue(declaration.type(), value);
            if (value instanceof BigDecimal decimal && (decimal.scale() > job.spec().decimalRules().scale()
                    || decimal.precision() - decimal.scale() > job.spec().decimalRules().precision() - job.spec().decimalRules().scale()))
                throw new IllegalArgumentException("parameter exceeds frozen decimal rules");
        });
        Map<String, SqlSchema> schemas = new TreeMap<>();
        for (var entry : inputVersions.entrySet()) {
            String alias = entry.getKey(); Ref ref = entry.getValue();
            if (!ref.id().equals(job.spec().inputs().get(alias).dataset())) throw new IllegalArgumentException("input dataset substituted");
            var input = catalog.inputs().stream().filter(d -> d.ref().equals(ref)).findFirst().orElseThrow(() -> new IllegalArgumentException("fixed input version absent"));
            if (!Objects.equals(bind(job.spec().inputs().get(alias).batch(), job.parameters()), job.inputBatches().get(alias))) throw new IllegalArgumentException("input batch differs from parameters");
            checkBatch(input.batchType(), job.inputBatches().get(alias), column(input.schema(), input.batchColumn()).type().getLogicalType());
            schemas.put(alias, new SqlSchema(input.read().projection().stream().map(name -> column(input.schema(), name)).toList()));
        }
        if (!Objects.equals(bind(job.spec().output().batch(), job.parameters()), job.outputBatch())) throw new IllegalArgumentException("output batch differs from parameters");
        FileStageProtocol.batchValue(catalog.output().batchType(), job.outputBatch());
        new SqlPlannerValidator().validate(job, schemas, catalog.output().schema());
        for (var check : job.spec().preChecks()) if (check instanceof JobSpec.ReadyCheck c && c.timeout().compareTo(catalog.runtime().time().stage()) > 0)
            throw new IllegalArgumentException("readiness timeout exceeds stage budget");
        checkRules(job.spec().preChecks(), schemas, catalog.output().schema(), true);
        checkRules(job.spec().postChecks(), schemas, catalog.output().schema(), false);
    }
    static void checkBatch(JobSpec.ParameterType type, Object value, org.apache.flink.table.types.logical.LogicalType sqlType) {
        FileStageProtocol.batchValue(type, value);
        if (value instanceof String s && s.isBlank()) throw new IllegalArgumentException("blank batch forbidden");
        if (value instanceof BigInteger integer) {
            int bits = switch (sqlType.getTypeRoot()) { case TINYINT -> 8; case SMALLINT -> 16; case INTEGER -> 32; case BIGINT -> 64; default -> throw new IllegalArgumentException("integer batch schema mismatch"); };
            if (integer.bitLength() >= bits) throw new IllegalArgumentException("batch integer outside physical column range");
        }
        if (value instanceof BigDecimal decimal && sqlType instanceof org.apache.flink.table.types.logical.DecimalType d
                && (decimal.scale() > d.getScale() || decimal.precision() - decimal.scale() > d.getPrecision() - d.getScale()))
            throw new IllegalArgumentException("batch decimal outside physical column capacity");
    }
    private static Object bind(String batch, Map<String, Object> values) {
        var match = PARAMETER.matcher(batch);
        if (match.matches()) return values.get(match.group(1));
        if (batch.contains("${")) throw new IllegalArgumentException("invalid batch expression");
        return batch;
    }
    private static void checkRules(List<JobSpec.Check> checks, Map<String, SqlSchema> schemas, SqlSchema output, boolean pre) {
        for (var check : checks) {
            if (check instanceof JobSpec.ColumnsCheck c) {
                SqlSchema actual = pre ? schemas.get(c.input().orElseThrow()) : output;
                if (actual == null) throw new IllegalArgumentException("unknown check input"); c.columns().forEach(name -> column(actual, name));
            } else if (check instanceof JobSpec.KeySetCheck c) {
                SqlSchema expected = schemas.get(c.expectedInput());
                if (expected == null || pre) throw new IllegalArgumentException("unknown key-set check input");
                for (String name : c.columns()) if (!column(output, name).type().getLogicalType().copy(true).equals(column(expected, name).type().getLogicalType().copy(true)))
                    throw new IllegalArgumentException("key-set check column types differ");
            } else if (check instanceof JobSpec.SumCheck c) {
                SqlSchema expected = schemas.get(c.expectedInput());
                if (expected == null || pre) throw new IllegalArgumentException("unknown sum check input");
                var left = column(output, c.column()).type().getLogicalType(); var right = column(expected, c.column()).type().getLogicalType();
                if (!left.copy(true).equals(right.copy(true)) || !Set.of(org.apache.flink.table.types.logical.LogicalTypeRoot.TINYINT,
                        org.apache.flink.table.types.logical.LogicalTypeRoot.SMALLINT, org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER,
                        org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT, org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL).contains(left.getTypeRoot()))
                    throw new IllegalArgumentException("sum check requires matching exact numeric columns");
            }
        }
    }
}
