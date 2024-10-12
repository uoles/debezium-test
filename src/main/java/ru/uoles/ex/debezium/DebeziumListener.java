package ru.uoles.ex.debezium;

import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.uoles.ex.service.CustomerService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.debezium.data.Envelope.FieldName.*;
import static io.debezium.data.Envelope.Operation;
import static java.util.stream.Collectors.toMap;

@Slf4j
@Component
public class DebeziumListener {

    private final Executor executor = Executors.newSingleThreadExecutor();
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;
    private final CustomerService customerService;

    @Autowired
    public DebeziumListener(Configuration customerConnectorConfiguration, CustomerService customerService) {
        this.debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(customerConnectorConfiguration.asProperties())
                .notifying(this::handleChangeEvent)
                .build();

        this.customerService = customerService;
    }

    private void handleChangeEvent(RecordChangeEvent<SourceRecord> sourceRecordRecordChangeEvent) {
        try {
            SourceRecord sourceRecord = sourceRecordRecordChangeEvent.record();
            log.info("Key = {}, Value = {}", sourceRecord.key(), sourceRecord.value());

            Struct sourceRecordChangeValue = (Struct) sourceRecord.value();
            log.info("SourceRecordChangeValue = '{}'", sourceRecordChangeValue);

            if (Objects.nonNull(sourceRecordChangeValue)) {
                Operation operation = Operation.forCode((String) sourceRecordChangeValue.get(OPERATION));

                if (!operation.equals(Operation.READ)) {
                    Map<String, Object> dataBefore = getData((Struct) sourceRecordChangeValue.get(BEFORE));
                    log.info("--- Operation: {}", operation.name());
                    log.info("--- BEFORE. Data: {}", dataBefore);

                    Map<String, Object> dataAfter = getData((Struct) sourceRecordChangeValue.get(AFTER));
                    log.info("--- AFTER. Data: {}", dataAfter);

                    customerService.replicateData(
                            operation.equals(Operation.DELETE) ? dataBefore : dataAfter,
                            operation
                    );
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("ERROR. Processing database event exception: " + e.getMessage(), e);
        }
    }

    public Map<String, Object> getData(final Struct struct) {
        Map<String, Object> map = new HashMap<>();
        if (Objects.nonNull(struct)) {
            map = struct.schema().fields().stream()
                    .filter(o -> Objects.nonNull(struct.get(o.name())))
                    .collect(toMap(Field::name, o -> struct.get(o.name())));
        }
        return map;
    }

    @PostConstruct
    private void start() {
        this.executor.execute(debeziumEngine);
        log.info("--- DebeziumListener started.");
    }

    @PreDestroy
    private void stop() throws IOException {
        if (Objects.nonNull(this.debeziumEngine)) {
            this.debeziumEngine.close();
            log.info("--- DebeziumListener stopped.");
        }
    }
}