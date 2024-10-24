package ru.uoles.ex.debezium.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.IOException;

/**
 * debezium-test
 * Created by Intellij IDEA.
 * Developer: uoles (Kulikov Maksim)
 * Date: 20.07.2024
 * Time: 15:19
 */
@Configuration
public class DebeziumConfig {

    @Bean
    public io.debezium.config.Configuration customerConnector(Environment env) throws IOException {
        return io.debezium.config.Configuration.create()
                .with("name", "customer_postgres_connector")
                .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .with("offset.storage", "ru.uoles.ex.debezium.offset.PostgreOffsetBackingStore")
                .with("offset.flush.interval.ms", "5000")
                .with("database.hostname", PropertiesConfig.getHost())
                .with("database.port", PropertiesConfig.getPort())
                .with("database.user", PropertiesConfig.getUsername())
                .with("database.password", PropertiesConfig.getPassword())
                .with("database.dbname", PropertiesConfig.getDatabaseName())
                .with("database.schema", PropertiesConfig.getSchema())
                .with("database.server.id", "10181")
                .with("database.server.name", "customer-postgres-db-server")
                .with("database.history", "io.debezium.relational.history.MemoryDatabaseHistory")
                .with("table.include.list", "dbz.customer")
                .with("column.include.list", "dbz.customer.id,dbz.customer.email,dbz.customer.fullname")
                .with("publication.autocreate.mode", "filtered")
                .with("plugin.name", "pgoutput")
                .with("slot.name", PropertiesConfig.getSlotName())
                .build();
    }
}
