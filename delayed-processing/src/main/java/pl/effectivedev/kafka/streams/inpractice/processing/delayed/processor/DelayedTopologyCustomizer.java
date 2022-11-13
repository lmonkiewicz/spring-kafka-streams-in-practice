package pl.effectivedev.kafka.streams.inpractice.processing.delayed.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.support.serializer.JsonSerde;
import pl.effectivedev.kafka.streams.inpractice.processing.delayed.StreamConfiguration;

@Slf4j
public class DelayedTopologyCustomizer implements KafkaStreamsInfrastructureCustomizer {

    public static final String SOURCE = "topic-in-source";
    public static final String SINK = "topic-delayed-sink";
    public static final String PROCESSOR = "delaying-processor";
    public static final String STORE_NAME = "delayed";

    @Override
    public void configureTopology(Topology topology) {
        var store = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                new JsonSerde<>(RecordData.class)
        );

        topology.addSource(SOURCE, StreamConfiguration.TOPIC_IN);
        topology.addProcessor(PROCESSOR, DelayingProcessor::new, SOURCE);
        topology.addStateStore(store, PROCESSOR);
        topology.addSink(SINK, StreamConfiguration.TOPIC_DELAYED_IN, PROCESSOR);

        log.info("Customized topology: {}", topology.describe());
    }
}
