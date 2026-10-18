package io.github.xuxiaotuan.magicflink.spec;

/** Stable public error; planner exception text can contain SQL literals and is deliberately not retained. */
public final class SqlValidationException extends IllegalArgumentException {
    private final String code;
    public SqlValidationException(String code, String message) { super(message); this.code = code; }
    public String code() { return code; }
    public String path() { return "sql"; }
}
