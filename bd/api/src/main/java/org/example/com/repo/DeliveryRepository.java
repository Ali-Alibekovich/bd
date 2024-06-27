package org.example.com.repo;

import org.example.com.bd.Delivery;
import org.springframework.data.jpa.repository.JpaRepository;


public interface DeliveryRepository extends JpaRepository<Delivery, String> {
}
