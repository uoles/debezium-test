package ru.uoles.ex.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.File;
import java.io.IOException;

@Configuration
public class DebeziumConnectorConfig {

    @Bean
    public io.debezium.config.Configuration customerConnector(Environment env) throws IOException {
        var offsetStorageTempFile = File.createTempFile("offsets_", ".dat");
        return io.debezium.config.Configuration.create()
            .with("name", "customer_postgres_connector")
            .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
            .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
            .with("offset.storage.file.filename", offsetStorageTempFile.getAbsolutePath())
            .with("offset.flush.interval.ms", "60000")
            .with("database.hostname", env.getProperty("customer.datasource.host"))
            .with("database.port", env.getProperty("customer.datasource.port"))
            .with("database.user", env.getProperty("customer.datasource.username"))
            .with("database.password", env.getProperty("customer.datasource.password"))
            .with("database.dbname", env.getProperty("customer.datasource.database"))
            .with("database.server.id", "10181")
            .with("database.server.name", "customer-postgres-db-server")
            .with("database.history", "io.debezium.relational.history.MemoryDatabaseHistory")
            .with("table.include.list", "dbz.customer")
            .with("column.include.list", "dbz.customer.id,dbz.customer.email,dbz.customer.fullname")
            .with("publication.autocreate.mode", "filtered")
            .with("plugin.name", "pgoutput")
            .with("slot.name", "dbz_customerdb_listener")
            .build();
    }
}
