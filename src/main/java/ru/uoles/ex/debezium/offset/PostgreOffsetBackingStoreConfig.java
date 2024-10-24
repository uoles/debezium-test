package ru.uoles.ex.debezium.offset;

import io.debezium.config.Configuration;
import lombok.Getter;
import org.apache.kafka.connect.runtime.WorkerConfig;

import static ru.uoles.ex.debezium.offset.PostgreOffsetBackingStoreConstants.*;

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
@Getter
public class PostgreOffsetBackingStoreConfig {

    private String tableName;
    private String tableSchema;
    private String tableCreate;
    private String tableSelect;
    private String tableDelete;
    private String tableInsert;

    public PostgreOffsetBackingStoreConfig(Configuration config, WorkerConfig configOriginal) {
        init(config, configOriginal);
    }

    public void init(Configuration config, WorkerConfig configOriginal) {
        this.tableName = config.getString(PROP_TABLE_NAME);
        this.tableCreate = String.format(config.getString(PROP_TABLE_DDL), tableName);
        this.tableSelect = String.format(config.getString(PROP_TABLE_SELECT), tableName);
        this.tableInsert = String.format(config.getString(PROP_TABLE_INSERT), tableName);
        this.tableDelete = String.format(config.getString(PROP_TABLE_DELETE), tableName, tableName);
        this.tableSchema = configOriginal.originalsStrings().get("database.schema");
    }

    public String getFullTableName() {
        return String.join(".", tableSchema, tableName);
    }
}