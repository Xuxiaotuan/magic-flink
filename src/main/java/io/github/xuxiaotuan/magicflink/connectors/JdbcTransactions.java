package io.github.xuxiaotuan.magicflink.connectors;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Objects;

/** Owns each borrowed connection. Any SQL/commit error escapes; callers must reconcile unknown outcomes. */
final class JdbcTransactions {
    private final DataSource source;
    private final int timeoutSeconds;
    JdbcTransactions(DataSource source,int timeoutSeconds){this.source=Objects.requireNonNull(source);if(timeoutSeconds<1)throw new IllegalArgumentException("bounded query timeout required");this.timeoutSeconds=timeoutSeconds;}
    @FunctionalInterface interface Work<T>{T run(Connection connection)throws SQLException;}
    <T>T run(Work<T> work)throws SQLException{
        try(var c=source.getConnection()){
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);c.setAutoCommit(false);
            try{T result=work.run(c);c.commit();return result;}
            catch(SQLException|RuntimeException e){try{c.rollback();}catch(SQLException rollback){e.addSuppressed(rollback);}throw e;}
        }
    }
    PreparedStatement prepare(Connection c,String sql,Object...values)throws SQLException{
        var p=c.prepareStatement(sql);
        try{p.setQueryTimeout(timeoutSeconds);for(int i=0;i<values.length;i++)p.setObject(i+1,values[i]);return p;}
        catch(SQLException|RuntimeException e){try{p.close();}catch(SQLException close){e.addSuppressed(close);}throw e;}
    }
    int update(Connection c,String sql,Object...values)throws SQLException{try(var p=prepare(c,sql,values)){return p.executeUpdate();}}
    void one(int changed)throws SQLException{if(changed!=1)throw new SQLException("transaction CAS invariant violated","MF002");}
}
