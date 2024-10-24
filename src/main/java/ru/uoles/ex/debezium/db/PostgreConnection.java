package ru.uoles.ex.debezium.db;

/**
 * debezium-test
 * Created by Intellij IDEA.
 * Developer: uoles (Kulikov Maksim)
 * Date: 23.10.2024
 * Time: 2:15
 */
public enum PostgreConnection {

    INSTANCE(new PostgreJdbcTemplate());

    private PostgreJdbcTemplate postgreJdbcTemplate;

    PostgreConnection(PostgreJdbcTemplate postgreJdbcTemplate) {
        this.postgreJdbcTemplate = postgreJdbcTemplate;
    }

    public PostgreJdbcTemplate getTemplate() {
        return postgreJdbcTemplate;
    }
}
