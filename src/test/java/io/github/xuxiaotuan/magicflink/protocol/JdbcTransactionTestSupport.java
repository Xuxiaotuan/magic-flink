package io.github.xuxiaotuan.magicflink.protocol;

import org.h2.jdbcx.JdbcDataSource;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.*;
import java.nio.file.Path;
import java.sql.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

final class JdbcTransactionTestSupport {
    static DataSource database(Path path) {
        var ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:file:" + path.toAbsolutePath() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;LOCK_TIMEOUT=3000");
        ds.setUser("sa"); ds.setPassword(""); return ds;
    }
    static void schema(DataSource source, String resource) throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement();
             var input = JdbcTransactionTestSupport.class.getResourceAsStream(resource)) {
            if (input == null) throw new IllegalStateException("missing schema");
            for (var sql : new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split(";"))
                if (!sql.isBlank()) statement.execute(sql);
        }
    }
    static void sql(DataSource source, String sql, Object... values) throws SQLException {
        try (var c = source.getConnection(); var p = c.prepareStatement(sql)) {
            for (int i = 0; i < values.length; i++) p.setObject(i + 1, values[i]); p.executeUpdate();
        }
    }
    static long count(DataSource source, String table) throws SQLException {
        try (var c = source.getConnection(); var p = c.prepareStatement("SELECT COUNT(*) FROM " + table); var r = p.executeQuery()) { r.next(); return r.getLong(1); }
    }
    static <T> java.util.List<T> race(Callable<T> one, Callable<T> two) throws Exception {
        var pool = Executors.newFixedThreadPool(2); var ready = new CountDownLatch(2); var go = new CountDownLatch(1);
        try {
            var futures = new java.util.ArrayList<Future<T>>();
            for (var action : java.util.List.of(one, two)) futures.add(pool.submit(() -> { ready.countDown(); if (!go.await(5, TimeUnit.SECONDS)) throw new AssertionError("barrier timeout"); return action.call(); }));
            if (!ready.await(5, TimeUnit.SECONDS)) throw new AssertionError("workers not ready"); go.countDown();
            return java.util.List.of(futures.get(0).get(15, TimeUnit.SECONDS), futures.get(1).get(15, TimeUnit.SECONDS));
        } finally { pool.shutdownNow(); }
    }
    /** Runs all real SQL; only commit acknowledgement is lost, or commit is rejected before sending. */
    static DataSource failCommit(DataSource delegate, boolean afterCommit) {
        return new DataSource() {
            public Connection getConnection() throws SQLException {
                var real = delegate.getConnection();
                return (Connection)Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class[]{Connection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("commit")) {
                        if (afterCommit) real.commit();
                        throw new SQLException("injected commit connection failure", "08006");
                    }
                    try { return method.invoke(real, args); } catch (InvocationTargetException e) { throw e.getCause(); }
                });
            }
            public Connection getConnection(String u, String p) throws SQLException { return getConnection(); }
            public PrintWriter getLogWriter() { return null; }
            public void setLogWriter(PrintWriter p) { }
            public void setLoginTimeout(int seconds) { }
            public int getLoginTimeout() { return 0; }
            public Logger getParentLogger() { return Logger.getGlobal(); }
            public <T> T unwrap(Class<T> type) throws SQLException { throw new SQLException("unsupported"); }
            public boolean isWrapperFor(Class<?> type) { return false; }
        };
    }
}
