package io.github.xuxiaotuan.magicflink.spec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Parsed v1 syntax only. Dataset authorization, Schema and SQL Planner validation are separate gates. */
public record JobSpec(String name, DecimalRules decimalRules, Map<String, Parameter> parameters, Map<String, Input> inputs,
                      List<Check> preChecks, String sql, List<Check> postChecks, Output output) {
    public JobSpec {
        java.util.Objects.requireNonNull(decimalRules, "decimalRules");
        parameters = Map.copyOf(parameters);
        inputs = Map.copyOf(inputs);
        preChecks = List.copyOf(preChecks);
        postChecks = List.copyOf(postChecks);
    }

    public enum ParameterType { STRING, BOOLEAN, INTEGER, DECIMAL, DATE }
    public record DecimalRules(int precision, int scale) {
        public DecimalRules {
            if (precision < 1 || precision > 38 || scale < 0 || scale > precision)
                throw new IllegalArgumentException("invalid decimal precision/scale");
        }
    }
    public record Parameter(ParameterType type, boolean required, Optional<Object> defaultValue) {}
    public record Input(String dataset, String batch) {}
    public record Output(String dataset, String batch, String mode) {}
    public sealed interface Check permits ReadyCheck, ColumnsCheck, CountCheck, KeySetCheck, SumCheck {
        String id();
    }
    public record ReadyCheck(String id, List<String> inputs, Duration timeout) implements Check {
        public ReadyCheck { inputs = List.copyOf(inputs); }
    }
    public enum ColumnRule { UNIQUE_KEY, NOT_NULL }
    /** Empty input denotes the candidate result in postChecks. */
    public record ColumnsCheck(String id, ColumnRule rule, Optional<String> input,
                               List<String> columns) implements Check {
        public ColumnsCheck { columns = List.copyOf(columns); }
    }
    public record CountCheck(String id, Optional<String> input, BigInteger min, BigInteger max) implements Check {}
    public record KeySetCheck(String id, String expectedInput, List<String> columns) implements Check {
        public KeySetCheck { columns = List.copyOf(columns); }
    }
    public record SumCheck(String id, String expectedInput, String column, BigDecimal tolerance) implements Check {}

    /** Typed values; this is not yet a resolved/authorized run and must not be submitted to Flink. */
    public record BoundJob(JobSpec spec, Map<String, Object> parameters,
                           Map<String, Object> inputBatches, Object outputBatch) {
        public BoundJob {
            parameters = Map.copyOf(parameters);
            inputBatches = Map.copyOf(inputBatches);
        }
    }
}
