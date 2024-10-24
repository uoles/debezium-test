package ru.uoles.ex.debezium;

import com.google.common.collect.ImmutableMap;
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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import ru.uoles.ex.debezium.config.PropertiesConfig;
import ru.uoles.ex.debezium.constants.SlotConstants;
import ru.uoles.ex.debezium.db.PostgreConnection;
import ru.uoles.ex.debezium.db.PostgreJdbcTemplate;
import ru.uoles.ex.service.CustomerService;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.debezium.data.Envelope.FieldName.*;
import static io.debezium.data.Envelope.Operation;
import static java.util.stream.Collectors.toMap;

@Slf4j
@Component
public class DebeziumListener {

    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>());

    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;
    private final PostgreJdbcTemplate postgreJdbcTemplate = PostgreConnection.INSTANCE.getTemplate();
    private final CustomerService customerService;

    @Autowired
    public DebeziumListener(Configuration customerConnectorConfiguration, CustomerService customerService) {
        this.debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(customerConnectorConfiguration.asProperties())
                .using(this.getClass().getClassLoader())
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

    private Map<String, Object> getData(final Struct struct) {
        Map<String, Object> map = new HashMap<>();
        if (Objects.nonNull(struct)) {
            map = struct.schema().fields().stream()
                    .filter(o -> Objects.nonNull(struct.get(o.name())))
                    .collect(toMap(Field::name, o -> struct.get(o.name())));
        }
        return map;
    }

    private boolean slotIsNotActive() {
        List<Boolean> result = postgreJdbcTemplate.query(
                SlotConstants.SLOT_STATUS_SELECT,
                ImmutableMap.of(SlotConstants.SLOT_NAME_PARAM, PropertiesConfig.getSlotName()),
                (rs, rowNum) -> rs.getBoolean(SlotConstants.SLOT_ACTIVE_COLUMN)
        );

        return !CollectionUtils.isEmpty(result) && !result.get(0);
    }

    @PreDestroy
    private void stop() throws IOException {
        if (Objects.nonNull(this.debeziumEngine)) {
            this.debeziumEngine.close();
            log.info("--- DebeziumListener stopped.");
        }
    }

    @Scheduled(initialDelay = 3000, fixedDelay = 15000)
    private void execute() {
        int count = this.executor.getActiveCount();
        if (count == 0 && slotIsNotActive()) {
            executor.execute(debeziumEngine);
            log.info("--- DebeziumListener started");
        }
    }
}