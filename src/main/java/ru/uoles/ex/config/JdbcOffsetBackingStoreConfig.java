package ru.uoles.ex.config;

import static ru.uoles.ex.constants.DebeziumParamsConstants.*;

import io.debezium.config.Configuration;
import lombok.Data;
import org.apache.kafka.connect.runtime.WorkerConfig;

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
@Data
public class JdbcOffsetBackingStoreConfig {

    private String tableCreate;
    private String tableSelect;
    private String tableDelete;
    private String tableInsert;
    private String tableSchema;
    private String tableName;
    private String jdbcUrl;
    private String user;
    private String password;

    public JdbcOffsetBackingStoreConfig(Configuration config, WorkerConfig configOriginal) {
        init(config, configOriginal);
    }

    public void init(Configuration config, WorkerConfig configOriginal) {
        this.tableName = config.getString(PROP_TABLE_NAME);
        this.tableCreate = String.format(config.getString(PROP_TABLE_DDL), tableName);
        this.tableSelect = String.format(config.getString(PROP_TABLE_SELECT), tableName);
        this.tableInsert = String.format(config.getString(PROP_TABLE_INSERT), tableName);
        this.tableDelete = String.format(config.getString(PROP_TABLE_DELETE), tableName);

        this.tableSchema = configOriginal.originalsStrings().get("offset.jdbc.schema");
        this.jdbcUrl = configOriginal.originalsStrings().get("offset.jdbc.url");
        this.user = configOriginal.originalsStrings().get("offset.jdbc.user");
        this.password = configOriginal.originalsStrings().get("offset.jdbc.password");
    }

    public String getTableName() {
        return String.join(".", tableSchema, tableName);
    }
}