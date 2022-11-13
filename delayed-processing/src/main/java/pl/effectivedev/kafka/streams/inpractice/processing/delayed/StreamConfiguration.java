package pl.effectivedev.kafka.streams.inpractice.processing.delayed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import pl.effectivedev.kafka.streams.inpractice.processing.delayed.processor.DelayedTopologyCustomizer;

@Configuration
public class StreamConfiguration {

    public static final String TOPIC_IN = "topic-in";
    public static final String TOPIC_DELAYED_IN = "topic-delayed-in";
    public static final String TOPIC_OUT = "topic-out";

    @Bean
    StreamsBuilderFactoryBean streamsBuilderFactoryBean(KafkaProperties kafkaProperties) {
        final StreamsBuilderFactoryBean factoryBean = new StreamsBuilderFactoryBean(
            new KafkaStreamsConfiguration(kafkaProperties.buildStreamsProperties())
        );

        factoryBean.setInfrastructureCustomizer(new DelayedTopologyCustomizer());

        return factoryBean;
    }

    @Bean
    KStream<String, String> processingDSLStream(StreamsBuilder streamsBuilder) {
        var inputStream = streamsBuilder.stream(
                TOPIC_DELAYED_IN, Consumed.with(Serdes.String(), Serdes.String())
        );

        var outputStream = inputStream.mapValues(value -> "<[" + value + "]>");

        outputStream.to(TOPIC_OUT);
        return outputStream;
    }
}
