package com.org.order.consumer;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Consumer implements ConsumerSeekAware {

  @Value("${seek.offset.enable}")
  private boolean offsetEnable;

  @Value("${seek.offset}")
  private long offset;

  //consume: concurrency attribute in Kafka allows for the creation of multiple listeners, each capable of handling incoming connections independently.
  @KafkaListener(groupId = "${group-id}", topics = "${topic.name}", concurrency = "3")
  public void consume(ConsumerRecord<String, String> record) {
    log.info(
        "Consumed the order for (TP: {}, P: {}, OFF_SET: {}, K: {}, V: {})",
        record.topic(),
        record.partition(),
        record.offset(),
        record.key(),
        record.value());
  }

  @Override
  public void onPartitionsAssigned(
      Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {

    if (offsetEnable) {
      assignments.forEach(
          (tp, off) -> {
            callback.seek(tp.topic(), tp.partition(), offset);
          });
    }
  }
}
