package io.github.xuxiaotuan.magicflink.protocol;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

/** Project canonical-v1, not RFC 8785: exact decimals are strings, integers remain JSON integers. */
public final class CanonicalJson {
    private static final int MAX_BYTES = 1024 * 1024;
    private static final DateTimeFormatter UTC = new DateTimeFormatterBuilder().appendInstant(9).toFormatter(Locale.ROOT);
    private CanonicalJson() {}

    public static byte[] encode(Object value) {
        StringBuilder text = new StringBuilder();
        write(value, text, 0);
        byte[] bytes = text.toString().getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_BYTES) throw new IllegalArgumentException("canonical content exceeds 1 MiB");
        return bytes;
    }
    public static String sha256(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 required by Java", e); }
    }
    private static void write(Object value, StringBuilder text, int depth) {
        if (depth > 64 || text.length() > MAX_BYTES) throw new IllegalArgumentException("canonical content limit exceeded");
        if (value instanceof String string) quote(string, text);
        else if (value instanceof Boolean || value instanceof BigInteger || value instanceof Long || value instanceof Integer
                || value instanceof Short || value instanceof Byte) text.append(value);
        else if (value instanceof BigDecimal decimal) {
            if ((long)decimal.precision() + Math.abs((long)decimal.scale()) + 3 > MAX_BYTES)
                throw new IllegalArgumentException("canonical decimal limit exceeded");
            quote(decimal.toPlainString(), text);
        }
        else if (value instanceof Instant instant) quote(UTC.format(instant), text);
        else if (value instanceof LocalDate date) quote(date.toString(), text);
        else if (value instanceof Map<?, ?> map) {
            SortedMap<String, Object> sorted = new TreeMap<>();
            map.forEach((key, item) -> {
                if (!(key instanceof String s)) throw new IllegalArgumentException("JSON keys must be strings");
                sorted.put(s, item);
            });
            text.append('{'); boolean comma = false;
            for (var entry : sorted.entrySet()) {
                if (comma) text.append(','); comma = true;
                quote(entry.getKey(), text); text.append(':'); write(entry.getValue(), text, depth + 1);
            }
            text.append('}');
        } else if (value instanceof List<?> list) {
            text.append('['); boolean comma = false;
            for (Object item : list) { if (comma) text.append(','); comma = true; write(item, text, depth + 1); }
            text.append(']');
        } else throw new IllegalArgumentException("unsupported canonical value; null, floating point and unordered collections forbidden");
    }
    private static void quote(String value, StringBuilder text) {
        if (value.length() > MAX_BYTES) throw new IllegalArgumentException("canonical string limit exceeded");
        text.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(i + 1))) throw new IllegalArgumentException("invalid Unicode");
                text.append(c).append(value.charAt(++i)); continue;
            }
            if (Character.isLowSurrogate(c)) throw new IllegalArgumentException("invalid Unicode");
            switch (c) {
                case '"' -> text.append("\\\""); case '\\' -> text.append("\\\\");
                case '\b' -> text.append("\\b"); case '\f' -> text.append("\\f");
                case '\n' -> text.append("\\n"); case '\r' -> text.append("\\r"); case '\t' -> text.append("\\t");
                default -> { if (c < 0x20) text.append(String.format(Locale.ROOT, "\\u%04x", (int)c)); else text.append(c); }
            }
        }
        text.append('"');
    }
}
