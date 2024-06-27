package org.example.com.bd;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "delivery")
public class Delivery {
    @Id
    private String id;

    @ManyToOne
    @JoinColumn(name = "deliveryman_id")
    private Deliveryman deliveryman;

    @Column(name = "order_id")
    private String orderId;

    @Column(name = "order_date_created")
    private LocalDateTime orderDateCreated;

    @Column(name = "delivery_address")
    private String deliveryAddress;

    @Column(name = "delivery_time")
    private LocalDateTime deliveryTime;
    @Column(name = "rating")
    private Integer rating;
    @Column(name = "tips")
    private Long tips;

    // No-argument constructor
    public Delivery() {}

    // All-argument constructor
    public Delivery(String id, Deliveryman deliveryman, String orderId, LocalDateTime orderDateCreated, String deliveryAddress, LocalDateTime deliveryTime, Integer rating, Long tips) {
        this.id = id;
        this.deliveryman = deliveryman;
        this.orderId = orderId;
        this.orderDateCreated = orderDateCreated;
        this.deliveryAddress = deliveryAddress;
        this.deliveryTime = deliveryTime;
        this.rating = rating;
        this.tips = tips;
    }

    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Deliveryman getDeliveryman() {
        return deliveryman;
    }

    public void setDeliveryman(Deliveryman deliveryman) {
        this.deliveryman = deliveryman;
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
        return "Delivery{" +
                "id='" + id + '\'' +
                '}';
    }
}
