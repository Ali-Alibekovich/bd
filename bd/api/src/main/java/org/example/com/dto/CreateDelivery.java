package org.example.com.dto;

import java.time.LocalDateTime;

public class CreateDelivery {
    private String deliverymanId;
    private String orderId;
    private LocalDateTime orderDateCreated;

    // No-argument constructor
    public CreateDelivery() {}

    // All-argument constructor
    public CreateDelivery(String deliverymanId, String orderId, LocalDateTime orderDateCreated) {
        this.deliverymanId = deliverymanId;
        this.orderId = orderId;
        this.orderDateCreated = orderDateCreated;
    }

    // Getters and Setters
    public String getDeliverymanId() {
        return deliverymanId;
    }

    public void setDeliverymanId(String deliverymanId) {
        this.deliverymanId = deliverymanId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public LocalDateTime getOrderDateCreated() {
        return orderDateCreated;
    }

    public void setOrderDateCreated(LocalDateTime orderDateCreated) {
        this.orderDateCreated = orderDateCreated;
    }

    // toString method
    @Override
    public String toString() {
        return "CreateDelivery{" +
                "deliverymanId='" + deliverymanId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", orderDateCreated=" + orderDateCreated +
                '}';
    }
}
