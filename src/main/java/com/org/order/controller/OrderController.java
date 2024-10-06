package com.org.order.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.org.order.dto.Order;
import com.org.order.publisher.Publisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "api/v1")
@Slf4j
public class OrderController {

  private final Publisher publisher;

  @Autowired
  public OrderController(Publisher publisher) {
    this.publisher = publisher;
  }

  @PostMapping(path = "/order")
  public ResponseEntity<String> publish(@RequestBody Order request) {
    log.info("Begin to publish the order: {}", request.getId());
    try {
      this.publisher.publish(request);
      log.info("Published the order: {}", request.getId());
      return new ResponseEntity<>("Successfully publish the order", HttpStatus.CREATED);
    } catch (JsonProcessingException e) {
      log.info(
          "Failed to publish the order for (ORD_ID: {}, ERR: {})", request.getId(), e.getMessage());
      return new ResponseEntity<>("Failed publish the order", HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
