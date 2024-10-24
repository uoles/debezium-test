package ru.uoles.ex.debezium.db;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import ru.uoles.ex.debezium.config.PropertiesConfig;

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
public class PostgreJdbcTemplate {

    private NamedParameterJdbcTemplate template;

    public PostgreJdbcTemplate() {
        getTemplate();
    }

    public DataSource dataSource() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl(PropertiesConfig.getJdbcUrl());
        ds.setUsername(PropertiesConfig.getUsername());
        ds.setPassword(PropertiesConfig.getPassword());
        ds.setSchema(PropertiesConfig.getSchema());
        return ds;
    }

    private NamedParameterJdbcTemplate getTemplate() {
        if (Objects.isNull(template)) {
            template = new NamedParameterJdbcTemplate(dataSource());
        }
        return template;
    }

    public void executeQuery(final String query) {
        getTemplate().getJdbcOperations().execute(query);
    }

    public void updateQuery(final String query, final Map<String, Object> parameters) {
        getTemplate().execute(query, parameters, new PreparedStatementCallback() {
            @Override
            public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                return ps.executeUpdate();
            }
        });
    }

    public <T> List<T> query(String sql, RowMapper<T> rowMapper) {
        return getTemplate().query(sql, rowMapper);
    }

    public <T> List<T> query(String sql, Map<String, ?> paramMap, RowMapper<T> rowMapper) {
        return getTemplate().query(sql, paramMap, rowMapper);
    }

    public Connection getConnection() {
        try {
            return getTemplate().getJdbcTemplate().getDataSource().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
