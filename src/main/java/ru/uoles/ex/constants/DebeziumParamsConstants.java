package ru.uoles.ex.constants;

import io.debezium.config.Field;

import static io.debezium.relational.history.DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING;

/**
 * debezium-test
 * Created by Intellij IDEA.
 * Developer: uoles (Kulikov Maksim)
 * Date: 12.10.2024
 * Time: 22:00
 *
 * Source: https://review.couchbase.org/c/kafka-connect-mongo/+/202601/4/debezium-storage/
 *              debezium-storage-jdbc/src/main/java/io/debezium/storage/jdbc/offset/JdbcOffsetBackingStoreConfig.java
 */
public final class DebeziumParamsConstants {

    public static final String OFFSET_STORAGE_PREFIX = "offset.storage.";
    public static final String PROP_PREFIX = OFFSET_STORAGE_PREFIX + CONFIGURATION_FIELD_PREFIX_STRING;

    public static final Field PROP_TABLE_SCHEMA = Field.create(PROP_PREFIX + "offset.table.schema")
            .withDescription("Schema for table")
            .withDefault("dbz");

    public static final String DEFAULT_TABLE_NAME = "debezium_offset_storage";
    public static final Field PROP_TABLE_NAME = Field.create(PROP_PREFIX + "offset.table.name")
            .withDescription("Name of the table to store offsets")
            .withDefault(DEFAULT_TABLE_NAME);

    /**
     * JDBC Offset storage CREATE TABLE syntax.
     */
    public static final String DEFAULT_TABLE_DDL =
            "CREATE TABLE IF NOT EXISTS %s(id VARCHAR(36) NOT NULL, " +
                    "offset_key VARCHAR(1255), offset_val VARCHAR(1255)," +
                    "record_insert_ts TIMESTAMP NOT NULL," +
                    "record_insert_seq INTEGER NOT NULL" +
                    ")";

    /**
     * The JDBC table that will store offset information.
     * id - UUID
     * offset_key - Offset Key
     * offset_val - Offset value
     * record_insert_ts - Timestamp when the record was inserted
     * record_insert_seq - Sequence number of record
     */
    public static final Field PROP_TABLE_DDL = Field.create(PROP_PREFIX + "offset.table.ddl")
            .withDescription("Create table syntax for offset jdbc table")
            .withDefault(DEFAULT_TABLE_DDL);

    public static final String DEFAULT_TABLE_SELECT =
            "SELECT id, offset_key, offset_val " +
            " FROM %s " +
            " ORDER BY record_insert_ts desc, record_insert_seq desc ";

    public static final String DEFAULT_TABLE_DELETE =
            " DELETE FROM %s " +
            " WHERE id not in ( " +
            "   SELECT id  " +
            "   FROM %s " +
            "   ORDER BY record_insert_ts desc, record_insert_seq desc " +
            "   LIMIT 10 " +
            " )";

    public static final String DEFAULT_TABLE_INSERT =
            "INSERT INTO %s(id, offset_key, offset_val, record_insert_ts, record_insert_seq) " +
            " VALUES ( :id, :key, :value, :timestamp, :pos )";

    public static final Field PROP_TABLE_SELECT = Field.create(PROP_PREFIX + "offset.table.select")
            .withDescription("Select syntax to get offset data from jdbc table")
            .withDefault(DEFAULT_TABLE_SELECT);

    public static final Field PROP_TABLE_DELETE = Field.create(PROP_PREFIX + "offset.table.delete")
            .withDescription("Delete syntax to delete offset data from jdbc table")
            .withDefault(DEFAULT_TABLE_DELETE);

    public static final Field PROP_TABLE_INSERT = Field.create(PROP_PREFIX + "offset.table.insert")
            .withDescription("Insert syntax to add offset data to the jdbc table")
            .withDefault(DEFAULT_TABLE_INSERT);
}