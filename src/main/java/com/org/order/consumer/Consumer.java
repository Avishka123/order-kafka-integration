package com.org.order.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Consumer {

  @KafkaListener(groupId = "${group-id}", topics = "${topic.name}")
  public void consume(ConsumerRecord<String, String> record) {
    log.info(
        "Consumed the order for (TP: {}, P: {}, OFF_SET: {}, K: {}, V: {})",
        record.topic(),
        record.partition(),
        record.offset(),
        record.key(),
        record.value());
  }
}
