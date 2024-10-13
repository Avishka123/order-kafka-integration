package com.org.order.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class Consumer implements ConsumerSeekAware {

    @Value("${seek.offset.enable}")
    private boolean offsetEnable;

    @Value("${seek.offset}")
    private long offset;

    @KafkaListener(groupId = "${group-id}", topics = "${topic.name}", autoStartup = "false")
    public void consume(ConsumerRecord<String, String> record) {
        log.info(
                "Consumed the order for (TP: {}, P: {}, OFF_SET: {}, K: {}, V: {})",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }

    @KafkaListener(groupId = "${group-id}", topics = "${topic.name}", batch = "true")
    public void consume(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {

        try {

            records.forEach(record -> {
                try {
                    log.info(
                            "Consumed the order for (TP: {}, P: {}, OFF_SET: {}, K: {}, V: {})",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());
                } catch (Exception e) {
                    log.error(
                            "Error processing order for (TP: {}, P: {}, OFF_SET: {}, K: {}, V: {}, ERR: {})",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value(), e.getMessage());
                }
            });

            log.info(
                    "Consumed the batch of order for (BATCH_SIZE: {})",
                    records.size());

        } catch (Exception e) {
            log.error(
                    "Error processing batch (ERR: {})",
                     e.getMessage());
        }finally {
            acknowledgment.acknowledge();
        }


    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback cb) {
        if (offsetEnable) {
            assignments.forEach((tp, off) -> cb.seek(tp.topic(), tp.partition(), offset));
        }
    }
}
