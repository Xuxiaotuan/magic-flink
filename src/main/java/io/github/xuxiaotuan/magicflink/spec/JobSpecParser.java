package io.github.xuxiaotuan.magicflink.spec;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.events.*;
import org.yaml.snakeyaml.nodes.*;

import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Pattern;

import static io.github.xuxiaotuan.magicflink.spec.JobSpec.*;

/** No object construction from YAML, network access, SQL execution or string substitution. */
public final class JobSpecParser {
    private static final int MAX_BYTES = 256 * 1024;
    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]{0,62}");
    private static final Pattern PARAMETER = Pattern.compile("[A-Za-z][A-Za-z0-9_]{0,62}");
    private static final Pattern REFERENCE = Pattern.compile("\\$\\{([A-Za-z][A-Za-z0-9_]{0,62})}");
    private static final Set<String> RESERVED = Set.of("result", "params", "system", "job_params");
    private final Duration maxReadinessTimeout;
    private final DecimalRules decimalRules;

    /** Limits come from a platform-owned frozen policy, never from job YAML. */
    public JobSpecParser(Duration maxReadinessTimeout, int decimalPrecision, int decimalScale) {
        if (maxReadinessTimeout == null || maxReadinessTimeout.isZero() || maxReadinessTimeout.isNegative()
                || decimalPrecision < 1 || decimalPrecision > 38 || decimalScale < 0 || decimalScale > decimalPrecision) {
            throw new IllegalArgumentException("invalid parser policy");
        }
        this.maxReadinessTimeout = maxReadinessTimeout;
        this.decimalRules = new DecimalRules(decimalPrecision, decimalScale);
    }

    public JobSpec parse(byte[] document) {
        if (document == null || document.length == 0 || document.length > MAX_BYTES) throw bad("document", "size must be 1..256 KiB");
        final String text;
        try { text = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(document)).toString(); }
        catch (CharacterCodingException e) { throw bad("document", "invalid UTF-8"); }
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        options.setMaxAliasesForCollections(0);
        options.setNestingDepthLimit(32);
        options.setCodePointLimit(MAX_BYTES);
        Yaml yaml = new Yaml(options);
        Object tree;
        try {
            for (Event event : yaml.parse(new StringReader(text))) {
                if (event instanceof AliasEvent || event instanceof NodeEvent node && node.getAnchor() != null)
                    throw bad("document", "anchors and aliases are forbidden");
                if (event instanceof ScalarEvent scalar && scalar.getTag() != null
                        || event instanceof CollectionStartEvent collection && collection.getTag() != null)
                    throw bad("document", "explicit tags are forbidden");
            }
            Node node = yaml.compose(new StringReader(text)); // compose requires exactly one document
            if (node == null) throw bad("document", "empty document");
            tree = readNode(node, "$", 0);
        } catch (org.yaml.snakeyaml.error.YAMLException e) {
            // Do not echo the submitted document, which may contain sensitive literals.
            throw bad("document", "invalid YAML or exceeded nesting limit");
        }
        Map<String, Object> root = map(tree, "$", "version", "name", "parameters", "inputs", "preChecks", "sql", "postChecks", "output");
        if (!"v1".equals(string(root.get("version"), "version"))) throw bad("version", "expected v1");
        String name = name(root.get("name"), "name");
        Map<String, Parameter> parameters = parseParameters(root.get("parameters"));
        Map<String, Object> rawInputs = map(root.get("inputs"), "inputs");
        if (rawInputs.isEmpty() || rawInputs.size() > 32) throw bad("inputs", "expected 1..32 inputs");
        Map<String, Input> inputs = new LinkedHashMap<>();
        rawInputs.forEach((alias, value) -> {
            name(alias, "inputs alias");
            var input = map(value, "inputs." + alias, "dataset", "batch");
            inputs.put(alias, new Input(string(input.get("dataset"), "input.dataset"), batch(input.get("batch"), parameters)));
        });
        String sql = string(root.get("sql"), "sql");
        if (sql.contains("${")) throw bad("sql", "use typed job_params instead of interpolation");
        var output = map(root.get("output"), "output", "dataset", "batch", "mode");
        if (!"replace-batch".equals(string(output.get("mode"), "output.mode"))) throw bad("output.mode", "expected replace-batch");
        Set<String> checkIds = new HashSet<>();
        List<Check> pre = parseChecks(root.getOrDefault("preChecks", List.of()), true, inputs.keySet(), checkIds);
        List<Check> post = parseChecks(root.getOrDefault("postChecks", List.of()), false, inputs.keySet(), checkIds);
        if (pre.size() + post.size() > 64) throw bad("checks", "at most 64 checks");
        return new JobSpec(name, decimalRules, parameters, inputs, pre, sql, post,
                new Output(string(output.get("dataset"), "output.dataset"), batch(output.get("batch"), parameters), "replace-batch"));
    }

    public BoundJob bind(JobSpec spec, Map<String, ?> supplied) {
        Objects.requireNonNull(spec, "spec");
        if (supplied == null) throw bad("parameters", "missing run values");
        if (!spec.parameters().keySet().containsAll(supplied.keySet())) throw bad("parameters", "undeclared run parameter");
        Map<String, Object> values = new LinkedHashMap<>();
        spec.parameters().forEach((key, declaration) -> {
            if (supplied.containsKey(key)) values.put(key, typed(declaration.type(), supplied.get(key), "parameters." + key, spec.decimalRules()));
            else if (declaration.required()) throw bad("parameters." + key, "required run parameter missing");
            else values.put(key, declaration.defaultValue().orElseThrow(() -> bad("parameters." + key, "missing default")));
        });
        Map<String, Object> batches = new LinkedHashMap<>();
        spec.inputs().forEach((key, input) -> batches.put(key, bindBatch(input.batch(), values)));
        return new BoundJob(spec, values, batches, bindBatch(spec.output().batch(), values));
    }

    private Map<String, Parameter> parseParameters(Object raw) {
        Map<String, Parameter> result = new LinkedHashMap<>();
        map(raw, "parameters").forEach((key, value) -> {
            if (!PARAMETER.matcher(key).matches()) throw bad("parameters", "invalid parameter name");
            var item = map(value, "parameters." + key, "type", "required", "default");
            String type = string(item.get("type"), "parameter.type");
            ParameterType kind = switch (type) {
                case "string" -> ParameterType.STRING; case "boolean" -> ParameterType.BOOLEAN;
                case "integer" -> ParameterType.INTEGER; case "decimal" -> ParameterType.DECIMAL;
                case "date" -> ParameterType.DATE; default -> throw bad("parameter.type", "unsupported type");
            };
            Object flag = item.getOrDefault("required", Boolean.TRUE);
            if (!(flag instanceof Boolean)) throw bad("parameter.required", "expected boolean");
            boolean required = (Boolean) flag;
            if (required == item.containsKey("default")) throw bad("parameter.default", "required forbids default; optional requires default");
            Optional<Object> defaultValue = required ? Optional.empty() : Optional.of(typed(kind, item.get("default"), "parameter.default", decimalRules));
            result.put(key, new Parameter(kind, required, defaultValue));
        });
        return result;
    }

    private static Object typed(ParameterType type, Object value, String path, DecimalRules rules) {
        if (value == null) throw bad(path, "null is forbidden");
        return switch (type) {
            case STRING -> { if (!(value instanceof String)) throw bad(path, "expected string"); yield value; }
            case BOOLEAN -> { if (!(value instanceof Boolean)) throw bad(path, "expected boolean"); yield value; }
            case INTEGER -> {
                if (!(value instanceof BigInteger || value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte))
                    throw bad(path, "expected integer");
                yield new BigInteger(value.toString());
            }
            case DECIMAL -> decimal(value, path, rules);
            case DATE -> {
                String date = string(value, path);
                if (!date.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) throw bad(path, "expected ISO date");
                try { yield LocalDate.parse(date); }
                catch (java.time.DateTimeException e) { throw bad(path, "invalid calendar date"); }
            }
        };
    }

    private static BigDecimal decimal(Object raw, String path, DecimalRules rules) {
        String value = string(raw, path);
        if (value.length() > MAX_BYTES || !value.matches("[+-]?[0-9]+(?:\\.[0-9]+)?")) throw bad(path, "expected plain decimal string");
        BigDecimal decimal = new BigDecimal(value);
        int integerDigits = Math.max(0, decimal.precision() - decimal.scale());
        if (decimal.scale() > rules.scale() || integerDigits > rules.precision() - rules.scale())
            throw bad(path, "decimal exceeds frozen precision/scale");
        return decimal;
    }

    private List<Check> parseChecks(Object raw, boolean pre, Set<String> inputs, Set<String> ids) {
        if (!(raw instanceof List<?> list) || list.size() > 64) throw bad("checks", "expected list of at most 64");
        List<Check> result = new ArrayList<>();
        for (Object value : list) {
            var item = map(value, "check");
            String id = name(item.get("id"), "check.id");
            if (!ids.add(id)) throw bad("check.id", "duplicate id");
            String type = string(item.get("type"), "check.type");
            switch (type) {
                case "batch-ready" -> {
                    if (!pre) throw bad("check.type", "batch-ready is preChecks only");
                    fields(item, "check", "id", "type", "inputs", "timeout");
                    var aliases = strings(item.get("inputs"), "check.inputs");
                    aliases.forEach(a -> input(a, inputs));
                    result.add(new ReadyCheck(id, aliases, duration(item.getOrDefault("timeout", "30m"))));
                }
                case "unique-key", "not-null", "row-count" -> {
                    Set<String> allowed = new HashSet<>(List.of("id", "type"));
                    if (pre) allowed.add("input");
                    allowed.addAll(type.equals("row-count") ? List.of("min", "max") : List.of("columns"));
                    fields(item, "check", allowed.toArray(String[]::new));
                    Optional<String> input = pre ? Optional.of(input(item.get("input"), inputs)) : Optional.empty();
                    if (type.equals("row-count")) {
                        BigInteger min = integer(item.get("min"), "check.min");
                        BigInteger max = integer(item.get("max"), "check.max");
                        if (min.signum() < 0 || min.compareTo(max) > 0) throw bad("check", "expected 0 <= min <= max");
                        result.add(new CountCheck(id, input, min, max));
                    } else result.add(new ColumnsCheck(id, type.equals("unique-key") ? ColumnRule.UNIQUE_KEY : ColumnRule.NOT_NULL,
                            input, strings(item.get("columns"), "check.columns")));
                }
                case "key-set-equal", "sum-equal" -> {
                    if (pre) throw bad("check.type", "comparison is postChecks only");
                    if (type.equals("key-set-equal")) {
                        fields(item, "check", "id", "type", "expectedInput", "columns");
                        result.add(new KeySetCheck(id, input(item.get("expectedInput"), inputs), strings(item.get("columns"), "check.columns")));
                    } else {
                        fields(item, "check", "id", "type", "expectedInput", "column", "tolerance");
                        BigDecimal tolerance = decimal(item.getOrDefault("tolerance", "0.00"), "check.tolerance", decimalRules);
                        if (tolerance.signum() < 0) throw bad("check.tolerance", "must be nonnegative");
                        result.add(new SumCheck(id, input(item.get("expectedInput"), inputs), string(item.get("column"), "check.column"), tolerance));
                    }
                }
                default -> throw bad("check.type", "unsupported check");
            }
        }
        return List.copyOf(result);
    }

    private Duration duration(Object raw) {
        String value = string(raw, "check.timeout");
        if (!value.matches("[1-9][0-9]*[smh]")) throw bad("check.timeout", "expected positive duration in s/m/h");
        try {
            long amount = Long.parseLong(value.substring(0, value.length() - 1));
            Duration timeout = switch (value.charAt(value.length() - 1)) {
                case 's' -> Duration.ofSeconds(amount); case 'm' -> Duration.ofMinutes(amount); default -> Duration.ofHours(amount);
            };
            if (timeout.compareTo(maxReadinessTimeout) > 0) throw bad("check.timeout", "exceeds platform limit");
            return timeout;
        } catch (ArithmeticException | NumberFormatException e) { throw bad("check.timeout", "duration overflow"); }
    }

    private static String batch(Object raw, Map<String, Parameter> parameters) {
        String value = string(raw, "batch");
        if (value.contains("${")) {
            var match = REFERENCE.matcher(value);
            if (!match.matches() || !parameters.containsKey(match.group(1))) throw bad("batch", "expected one declared parameter reference");
        }
        return value;
    }

    private static Object bindBatch(String batch, Map<String, Object> values) {
        var match = REFERENCE.matcher(batch);
        return match.matches() ? Objects.requireNonNull(values.get(match.group(1))) : batch;
    }

    private static String input(Object value, Set<String> inputs) {
        String alias = string(value, "check.input");
        if (!inputs.contains(alias)) throw bad("check.input", "unknown input alias");
        return alias;
    }

    private static String name(Object value, String path) {
        String name = string(value, path);
        if (!NAME.matcher(name).matches() || RESERVED.contains(name)) throw bad(path, "invalid or reserved name");
        return name;
    }

    private static String string(Object value, String path) {
        if (!(value instanceof String s) || s.isBlank()) throw bad(path, "expected nonblank string");
        return s;
    }

    private static BigInteger integer(Object value, String path) {
        if (!(value instanceof BigInteger number)) throw bad(path, "expected integer");
        return number;
    }

    private static List<String> strings(Object value, String path) {
        if (!(value instanceof List<?> list) || list.isEmpty()) throw bad(path, "expected nonempty list");
        List<String> strings = list.stream().map(v -> string(v, path)).toList();
        if (new HashSet<>(strings).size() != strings.size()) throw bad(path, "duplicate entry");
        return strings;
    }

    private static Map<String, Object> map(Object value, String path, String... allowed) {
        if (!(value instanceof Map<?, ?> raw)) throw bad(path, "expected map");
        Map<String, Object> map = new LinkedHashMap<>();
        raw.forEach((key, item) -> map.put(string(key, path + " key"), item));
        if (allowed.length != 0) fields(map, path, allowed);
        return map;
    }

    private static void fields(Map<String, Object> map, String path, String... allowed) {
        if (!Set.of(allowed).containsAll(map.keySet())) throw bad(path, "unknown field");
    }

    private static Object readNode(Node node, String path, int depth) {
        if (depth > 32) throw bad(path, "nesting limit exceeded");
        if (node instanceof MappingNode mapping) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (NodeTuple pair : mapping.getValue()) {
                if (!(pair.getKeyNode() instanceof ScalarNode key) || !key.getTag().equals(Tag.STR)) throw bad(path, "expected string key; merge forbidden");
                String field = key.getValue();
                if (field.equals("<<") || map.containsKey(field)) throw bad(path, "merge or duplicate key");
                map.put(field, readNode(pair.getValueNode(), path + "." + field, depth + 1));
            }
            return map;
        }
        if (node instanceof SequenceNode sequence) {
            List<Object> values = new ArrayList<>();
            for (Node item : sequence.getValue()) values.add(readNode(item, path + "[]", depth + 1));
            return values;
        }
        if (node instanceof ScalarNode scalar) {
            String value = scalar.getValue();
            if (scalar.getTag().equals(Tag.STR)) return value;
            if (scalar.getTag().equals(Tag.BOOL) && (value.equals("true") || value.equals("false"))) return Boolean.valueOf(value);
            if (scalar.getTag().equals(Tag.INT) && value.matches("[+-]?(?:0|[1-9][0-9]*)")) return new BigInteger(value);
        }
        throw bad(path, "unsupported scalar: quote dates/decimals; null forbidden");
    }

    private static IllegalArgumentException bad(String path, String reason) {
        return new IllegalArgumentException(path + ": " + reason);
    }
}
