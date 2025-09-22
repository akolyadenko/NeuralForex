package com.trd.db;

import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.reflect.ConstructorMapper;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class Db {
    private static Jdbi niJdbi;

    public static synchronized Jdbi jdbi() {
        if (niJdbi != null) {
            return niJdbi;
        }
        niJdbi = init("jdbc:postgresql://localhost/trd", null);
        return niJdbi;
    }

    private static synchronized Jdbi init(String url, String pwd) {
        var ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername("trd");
        if (pwd != null) {
            ds.setPassword(pwd);
        }
        ds.setMaximumPoolSize(32);
        ds.setConnectionTimeout(1000);

        var jdbi = Jdbi.create(ds);
        jdbi.installPlugin(new SqlObjectPlugin());

//        jdbi.registerRowMapper(ConstructorMapper.factory(Account.class));

        jdbi.registerArrayType(String.class, "text");
        jdbi.registerArrayType(Long.class, "bigint");
        jdbi.registerArrayType(Float.class, "float4");

        return jdbi;
    }

    public static Array createLongArray(Handle h, List<Long> l) {
        try {
            return h.getConnection().createArrayOf("bigint", l.toArray(new Long[0]));
        } catch(SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Array createStringArray(Handle h, List<String> l)  {
        try {
            return h.getConnection().createArrayOf("text", l.toArray(new String[0]));
        } catch(SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Array createFloatArray(Handle h, List<Float> l)  {
        try {
            return h.getConnection().createArrayOf("float4", l.toArray(new Float[0]));
        } catch(SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> List<T> dbArrayToList(Object o) throws SQLException {
        return Arrays.asList((T[])((Array) o).getArray());
    }
}
