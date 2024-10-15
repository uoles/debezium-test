package ru.uoles.ex.debezium.db;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import ru.uoles.ex.debezium.offset.PostgreOffsetBackingStoreConfig;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * debezium-test
 * Created by Intellij IDEA.
 * Developer: uoles (Kulikov Maksim)
 * Date: 15.10.2024
 * Time: 2:36
 */
public class PostgreConnection {

    private NamedParameterJdbcTemplate template;
    private PostgreOffsetBackingStoreConfig config;

    public PostgreConnection(PostgreOffsetBackingStoreConfig config) {
        this.config = config;
    }

    private DataSource getDataSource() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl(this.config.getJdbcUrl());
        ds.setUsername(this.config.getUser());
        ds.setPassword(this.config.getPassword());
        ds.setSchema(this.config.getTableSchema());
        return ds;
    }

    private NamedParameterJdbcTemplate getTemplate() throws SQLException {
        if (Objects.isNull(template)) {
            template = new NamedParameterJdbcTemplate(getDataSource());
        }
        return template;
    }

    public void executeQuery(final String query) {
        try {
            getTemplate().getJdbcOperations().execute(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateQuery(final String query, final Map<String, Object> parameters) {
        try {
            getTemplate().execute(query, parameters, new PreparedStatementCallback() {
                @Override
                public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                    return ps.executeUpdate();
                }
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> List<T> query(String sql, RowMapper<T> rowMapper) {
        try {
            return getTemplate().query(sql, rowMapper);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() {
        try {
            return getTemplate().getJdbcTemplate().getDataSource().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
