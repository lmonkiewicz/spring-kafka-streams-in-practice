package pl.effectivedev.kafka.streams.inpractice.processing.delayed.processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.UUID;

public class DelayingProcessor implements Processor<String, String, String, String> {

    private static final Duration INTERVAL = Duration.ofSeconds(10);
    private KeyValueStore<String, RecordData> store;

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.store = context.getStateStore(DelayedTopologyCustomizer.STORE_NAME);
        context.schedule(
                INTERVAL,
                PunctuationType.WALL_CLOCK_TIME,
                currentTimestamp -> {
                    try(KeyValueIterator<String, RecordData> iterator = store.all()) {
                        if (iterator.hasNext()) {
                            final KeyValue<String, RecordData> storeItem = iterator.next();
                            var recordData = storeItem.value;
                            if (currentTimestamp - INTERVAL.toMillis() > recordData.timestamp) {
                                context.forward(new Record<>(recordData.getKey(), recordData.getValue(), currentTimestamp));
                                store.delete(storeItem.key);
                            }
                        }
                    }
                });
    }

    @Override
    public void process(Record<String, String> record) {
        store.put(
            UUID.randomUUID().toString(),
            new RecordData(record.timestamp(), record.key(), record.value())
        );
    }

}
