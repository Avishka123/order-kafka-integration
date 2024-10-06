package com.org.order.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Order {
  @JsonProperty("order_id")
  private long id;

  @JsonProperty("product")
  private String productName;

  @JsonProperty("price")
  private double price;
}
