package ru.uoles.ex.debezium;

import io.debezium.config.Configuration;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import ru.uoles.ex.config.JdbcOffsetBackingStoreConfig;
import ru.uoles.ex.model.Offset;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
public class PostgreJdbcOffsetBackingStore implements OffsetBackingStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreJdbcOffsetBackingStore.class);

    private JdbcOffsetBackingStoreConfig config;
    private ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();
    private ExecutorService executor;
    private final AtomicInteger recordInsertSeq = new AtomicInteger(0);
    private NamedParameterJdbcTemplate template;

    public PostgreJdbcOffsetBackingStore() {
    }

    public String fromByteBuffer(ByteBuffer data) {
        return (data != null) ? String.valueOf(StandardCharsets.UTF_8.decode(data.asReadOnlyBuffer())) : null;
    }

    public ByteBuffer toByteBuffer(String data) {
        return (data != null) ? ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)) : null;
    }

    public DataSource getDataSource() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl(this.config.getJdbcUrl());
        ds.setUsername(this.config.getUser());
        ds.setPassword(this.config.getPassword());
        ds.setSchema(this.config.getTableSchema());
        return ds;
    }

    public NamedParameterJdbcTemplate getTemplate() throws SQLException {
        if (Objects.isNull(template)) {
            template = new NamedParameterJdbcTemplate(getDataSource());
        }
        return template;
    }

    @Override
    public void configure(WorkerConfig config) {
        try {
            Configuration configuration = Configuration.from(config.originalsStrings());
            this.config = new JdbcOffsetBackingStoreConfig(configuration, config);
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
        DatabaseMetaData dbMeta = getTemplate().getJdbcTemplate().getDataSource().getConnection().getMetaData();
        ResultSet tableExists = dbMeta.getTables(null, null, config.getTableName(), null);

        if (tableExists.next()) {
            return;
        }

        LOGGER.info("Creating table {} to store offset", config.getTableName());
        executeQuery(config.getTableCreate());
    }

    public void executeQuery(final String query) {
        try {
            getTemplate().getJdbcOperations().execute(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateQuery(final String query, final Offset obj) {
        try {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("id", obj.getId());
            parameters.put("key", obj.getKey());
            parameters.put("value", obj.getValue());
            parameters.put("timestamp", obj.getTimestamp());
            parameters.put("pos", obj.getPos());

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

    protected void save() {
        LOGGER.debug("Saving data to state table...");

        executeQuery(config.getTableDelete());

        for (Map.Entry<String, String> mapEntry : data.entrySet()) {
            Timestamp currentTs = new Timestamp(System.currentTimeMillis());
            String key = (mapEntry.getKey() != null) ? mapEntry.getKey() : null;
            String value = (mapEntry.getValue() != null) ? mapEntry.getValue() : null;

            String query = config.getTableInsert();
            updateQuery(
                    query,
                    new Offset(
                        UUID.randomUUID().toString(),
                        key,
                        value,
                        currentTs,
                        recordInsertSeq.incrementAndGet()
                    )
            );
        }
    }

    private void load() {
        ConcurrentHashMap<String, String> tmpData = new ConcurrentHashMap<>();
        try {
            getTemplate().query(
                    config.getTableSelect(),
                    (rs, rowNum) -> {
                        String key = rs.getString("offset_key");
                        String val = rs.getString("offset_val");
                        tmpData.put(key, val);
                        return null;
                    }
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        data = tmpData;
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
}
