package org.example.com.repo;

import org.example.com.bd.Deliveryman;
import org.springframework.data.jpa.repository.JpaRepository;


public interface DeliveryManRepository extends JpaRepository<Deliveryman, String> {
}
