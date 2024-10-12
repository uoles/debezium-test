package ru.uoles.ex.config;

import io.debezium.config.Configuration;
import lombok.Getter;
import org.apache.kafka.connect.runtime.WorkerConfig;

import static ru.uoles.ex.constants.DebeziumParamsConstants.*;

/**
 * debezium-test
 * Created by Intellij IDEA.
 * Developer: uoles (Kulikov Maksim)
 * Date: 12.10.2024
 * Time: 20:33
 *
 * Source: https://review.couchbase.org/c/kafka-connect-mongo/+/202601/4/debezium-storage/
 *              debezium-storage-jdbc/src/main/java/io/debezium/storage/jdbc/offset/JdbcOffsetBackingStoreConfig.java
 */
public class JdbcOffsetBackingStoreConfig {

    private String tableName;
    @Getter
    private String tableSchema;
    @Getter
    private String tableCreate;
    @Getter
    private String tableSelect;
    @Getter
    private String tableDelete;
    @Getter
    private String tableInsert;
    @Getter
    private String jdbcUrl;
    @Getter
    private String user;
    @Getter
    private String password;

    public JdbcOffsetBackingStoreConfig(Configuration config, WorkerConfig configOriginal) {
        init(config, configOriginal);
    }

    public void init(Configuration config, WorkerConfig configOriginal) {
        this.tableName = config.getString(PROP_TABLE_NAME);
        this.tableCreate = String.format(config.getString(PROP_TABLE_DDL), tableName);
        this.tableSelect = String.format(config.getString(PROP_TABLE_SELECT), tableName);
        this.tableInsert = String.format(config.getString(PROP_TABLE_INSERT), tableName);
        this.tableDelete = String.format(config.getString(PROP_TABLE_DELETE), tableName, tableName);

        this.tableSchema = configOriginal.originalsStrings().get("offset.jdbc.schema");
        this.jdbcUrl = configOriginal.originalsStrings().get("offset.jdbc.url");
        this.user = configOriginal.originalsStrings().get("offset.jdbc.user");
        this.password = configOriginal.originalsStrings().get("offset.jdbc.password");
    }

    public String getTableName() {
        return String.join(".", tableSchema, tableName);
    }
}