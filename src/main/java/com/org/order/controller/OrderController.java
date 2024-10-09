package com.org.order.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.org.order.dto.Order;
import com.org.order.publisher.Publisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
    try {
      this.publisher.publish(request);
      return this.getOrderResponse(request.getId());
    } catch (JsonProcessingException e) {
      return this.getErrorResponse(request.getId(), e);
    }
  }

  @PostMapping(path = "/{partition}/order")
  public ResponseEntity<String> publishToPartition(
      @RequestBody Order request, @PathVariable int partition) {
    try {
      this.publisher.publish(request, partition);
      return this.getOrderResponse(request.getId());
    } catch (JsonProcessingException e) {
      return this.getErrorResponse(request.getId(), e);
    }
  }

  private ResponseEntity<String> getOrderResponse(long orderId) {
    log.info("Published the order: {}", orderId);
    return new ResponseEntity<>("Successfully publish the order", HttpStatus.CREATED);
  }

  private ResponseEntity<String> getErrorResponse(long orderId, JsonProcessingException e) {
    log.error("Failed to publish the order for (ORD_ID: {}, ERR: {})", orderId, e.getMessage());
    return new ResponseEntity<>("Failed publish the order", HttpStatus.INTERNAL_SERVER_ERROR);
  }
}
