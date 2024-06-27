package org.example.com.bd;

import java.util.List;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

@Entity
@Table(name = "deliveryman")
public class Deliveryman {
    @Id
    private String id;

    private String name;

    @OneToMany(mappedBy = "deliveryman", cascade = CascadeType.ALL)
    private List<Delivery> deliveryList;

    // No-argument constructor
    public Deliveryman() {}

    // All-argument constructor
    public Deliveryman(String id, String name, List<Delivery> deliveryList) {
        this.id = id;
        this.name = name;
        this.deliveryList = deliveryList;
    }

    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Delivery> getDeliveryList() {
        return deliveryList;
    }

    public void setDeliveryList(List<Delivery> deliveryList) {
        this.deliveryList = deliveryList;
    }

    // toString method
    @Override
    public String toString() {
        return "Deliveryman{" +
                "id='" + id + '\'' +
                '}';
    }
}
