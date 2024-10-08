package com.org.order.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.org.order.dto.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Publisher {

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper mapper;

  @Value("${topic.name}")
  private String topicName;

  @Autowired
  public Publisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
    this.kafkaTemplate = kafkaTemplate;
    this.mapper = mapper;
  }

  public void publish(Order data) throws JsonProcessingException {

    String event = this.mapper.writeValueAsString(data);

    this.kafkaTemplate.send(this.topicName, String.valueOf(data.getId()), event);

    log.info("Successfully published the order for (T: {}, ORD_ID: {}, E: {})",this.topicName, data.getId(), event);
  }


  public void publish(Order data,int partition) throws JsonProcessingException {

    String event = this.mapper.writeValueAsString(data);

    this.kafkaTemplate.send(this.topicName,partition,String.valueOf(data.getId()),event);

    log.info("Successfully published the order for (T: {}, P: {}, ORD_ID: {}, E: {})",this.topicName,partition, data.getId(), event);

  }
}
