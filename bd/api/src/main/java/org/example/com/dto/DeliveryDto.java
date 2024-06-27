package org.example.com.dto;

import java.time.LocalDateTime;

public class DeliveryDto {
    private String orderId;
    private LocalDateTime orderDateCreated;
    private String deliveryId;
    private String deliverymanId;
    private String deliveryAddress;
    private LocalDateTime deliveryTime;
    private Integer rating;
    private Long tips;

    // All-argument constructor
    public DeliveryDto(String orderId, LocalDateTime orderDateCreated, String deliveryId, String deliverymanId, String deliveryAddress, LocalDateTime deliveryTime, Integer rating, Long tips) {
        this.orderId = orderId;
        this.orderDateCreated = orderDateCreated;
        this.deliveryId = deliveryId;
        this.deliverymanId = deliverymanId;
        this.deliveryAddress = deliveryAddress;
        this.deliveryTime = deliveryTime;
        this.rating = rating;
        this.tips = tips;
    }

    // Getters and Setters
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

    public String getDeliveryId() {
        return deliveryId;
    }

    public void setDeliveryId(String deliveryId) {
        this.deliveryId = deliveryId;
    }

    public String getDeliverymanId() {
        return deliverymanId;
    }

    public void setDeliverymanId(String deliverymanId) {
        this.deliverymanId = deliverymanId;
    }

    public String getDeliveryAddress() {
        return deliveryAddress;
    }

    public void setDeliveryAddress(String deliveryAddress) {
        this.deliveryAddress = deliveryAddress;
    }

    public LocalDateTime getDeliveryTime() {
        return deliveryTime;
    }

    public void setDeliveryTime(LocalDateTime deliveryTime) {
        this.deliveryTime = deliveryTime;
    }

    public Integer getRating() {
        return rating;
    }

    public void setRating(Integer rating) {
        this.rating = rating;
    }

    public Long getTips() {
        return tips;
    }

    public void setTips(Long tips) {
        this.tips = tips;
    }

    // toString method
    @Override
    public String toString() {
        return "DeliveryDto{" +
                "orderId='" + orderId + '\'' +
                ", orderDateCreated=" + orderDateCreated +
                ", deliveryId='" + deliveryId + '\'' +
                ", deliverymanId='" + deliverymanId + '\'' +
                ", deliveryAddress='" + deliveryAddress + '\'' +
                ", deliveryTime=" + deliveryTime +
                ", rating=" + rating +
                ", tips=" + tips +
                '}';
    }
}
