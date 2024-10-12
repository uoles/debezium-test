package ru.uoles.ex.debezium;

import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.config.Configuration;
import ru.uoles.ex.config.JdbcOffsetBackingStoreConfig;

/**
 * debezium-test
 * Created by Intellij IDEA.
 * Developer: uoles (Kulikov Maksim)
 * Date: 12.10.2024
 * Time: 20:27
 *
 * Implementation of OffsetBackingStore that saves data to database table.
 * Source: https://review.couchbase.org/c/kafka-connect-mongo/+/202601/4/debezium-storage/
 *              debezium-storage-jdbc/src/main/java/io/debezium/storage/jdbc/offset/JdbcOffsetBackingStore.java
 */
public class PostgresJdbcOffsetBackingStore implements OffsetBackingStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresJdbcOffsetBackingStore.class);

    private JdbcOffsetBackingStoreConfig config;

    protected ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();
    protected ExecutorService executor;
    private final AtomicInteger recordInsertSeq = new AtomicInteger(0);
    private Connection conn;

    public PostgresJdbcOffsetBackingStore() {
    }

    public String fromByteBuffer(ByteBuffer data) {
        return (data != null) ? String.valueOf(StandardCharsets.UTF_8.decode(data.asReadOnlyBuffer())) : null;
    }

    public ByteBuffer toByteBuffer(String data) {
        return (data != null) ? ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)) : null;
    }

    @Override
    public void configure(WorkerConfig config) {
        try {
            Configuration configuration = Configuration.from(config.originalsStrings());
            this.config = new JdbcOffsetBackingStoreConfig(configuration, config);

            conn = DriverManager.getConnection(this.config.getJdbcUrl(), this.config.getUser(), this.config.getPassword());
            conn.setAutoCommit(false);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect JDBC offset backing store: " + config.originalsStrings(), e);
        }
    }

    @Override
    public synchronized void start() {
        executor = Executors.newFixedThreadPool(1, ThreadUtils.createThreadFactory(
                this.getClass().getSimpleName() + "-%d", false));

        LOGGER.info("Starting PostgresJdbcOffsetBackingStore db '{}'", config.getJdbcUrl());
        try {
            initializeTable();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create JDBC offset table: " + config.getJdbcUrl(), e);
        }
        load();
    }

    private void initializeTable() throws SQLException {
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet tableExists = dbMeta.getTables(null, null, config.getTableName(), null);

        if (tableExists.next()) {
            return;
        }

        LOGGER.info("Creating table {} to store offset", config.getTableName());
        conn.prepareStatement(config.getTableCreate()).execute();
    }

    protected void save() {
        try {
            LOGGER.debug("Saving data to state table...");

            try (PreparedStatement sqlDelete = conn.prepareStatement(config.getTableDelete())) {
                sqlDelete.executeUpdate();
                for (Map.Entry<String, String> mapEntry : data.entrySet()) {
                    Timestamp currentTs = new Timestamp(System.currentTimeMillis());
                    String key = (mapEntry.getKey() != null) ? mapEntry.getKey() : null;
                    String value = (mapEntry.getValue() != null) ? mapEntry.getValue() : null;
                    // Execute a query
                    try (PreparedStatement sql = conn.prepareStatement(config.getTableInsert())) {
                        sql.setString(1, UUID.randomUUID().toString());
                        sql.setString(2, key);
                        sql.setString(3, value);
                        sql.setTimestamp(4, currentTs);
                        sql.setInt(5, recordInsertSeq.incrementAndGet());
                        sql.executeUpdate();
                    }
                }
            }
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                // Ignore errors on rollback
            }
            throw new ConnectException(e);
        }
    }

    private void load() {
        try {
            ConcurrentHashMap<String, String> tmpData = new ConcurrentHashMap<>();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(config.getTableSelect());
            while (rs.next()) {
                String key = rs.getString("offset_key");
                String val = rs.getString("offset_val");
                tmpData.put(key, val);
            }
            data = tmpData;
        } catch (SQLException e) {
            throw new ConnectException("Failed recover records from database: " + config.getJdbcUrl(), e);
        }
    }

    private void stopExecutor() {
        if (executor != null) {
            executor.shutdown();
            // Best effort wait for any get() and set() tasks (and caller's callbacks) to complete.
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (!executor.shutdownNow().isEmpty()) {
                throw new ConnectException("Failed to stop PostgresJdbcOffsetBackingStore. Exiting without cleanly " +
                        "shutting down pending tasks and/or callbacks.");
            }
            executor = null;
        }
    }

    @Override
    public synchronized void stop() {
        stopExecutor();
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            LOGGER.error("Exception while stopping PostgresJdbcOffsetBackingStore", e);
        }
        LOGGER.info("Stopped PostgresJdbcOffsetBackingStore");
    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values, final Callback<Void> callback) {
        return executor.submit(new Callable<>() {
            @Override
            public Void call() {
                for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
                    if (entry.getKey() == null) {
                        continue;
                    }
                    data.put(fromByteBuffer(entry.getKey()), fromByteBuffer(entry.getValue()));
                }
                save();

                if (callback != null) {
                    callback.onCompletion(null, null);
                }
                return null;
            }
        });
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
        return executor.submit(new Callable<Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public Map<ByteBuffer, ByteBuffer> call() {
                Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
                for (ByteBuffer key : keys) {
                    result.put(key, toByteBuffer(data.get(fromByteBuffer(key))));
                }
                return result;
            }
        });
    }

    public Set<Map<String, Object>> connectorPartitions(String connectorName) {
        return null;
    }
}
