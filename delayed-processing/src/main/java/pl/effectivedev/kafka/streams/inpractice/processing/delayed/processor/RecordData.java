package pl.effectivedev.kafka.streams.inpractice.processing.delayed.processor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
class RecordData {
    long timestamp;
    String key;
    String value;
}
