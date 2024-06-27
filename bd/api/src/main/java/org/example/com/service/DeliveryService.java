package org.example.com.service;

import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Collectors;

import net.datafaker.Faker;
import org.example.com.repo.DeliveryRepository;
import org.example.com.repo.DeliveryManRepository;
import org.example.com.dto.CreateDelivery;
import org.example.com.dto.DeliveryDto;
import org.example.com.dto.DeliverymanDto;
import org.springframework.stereotype.Service;
import org.example.com.bd.Delivery;
import org.example.com.bd.Deliveryman;

@Service
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryManRepository deliverymanRepository;

    public DeliveryService(DeliveryRepository deliveryRepository, DeliveryManRepository deliverymanRepository) {
        this.deliveryRepository = deliveryRepository;
        this.deliverymanRepository = deliverymanRepository;
    }

    public List<DeliverymanDto> getDeliverers() {
        return deliverymanRepository.findAll().stream()
                .map(deliveryman -> new DeliverymanDto(deliveryman.getId(), deliveryman.getName()))
                .toList();
    }

    public List<DeliveryDto> getDeliveryForDeliveryman(String id) {
        Deliveryman deliveryman = deliverymanRepository.getReferenceById(id);
        return deliveryman.getDeliveryList().stream()
                .map(delivery -> new DeliveryDto(
                        delivery.getOrderId(),
                        delivery.getOrderDateCreated(),
                        delivery.getId(),
                        delivery.getDeliveryman().getId(),
                        delivery.getDeliveryAddress(),
                        delivery.getDeliveryTime(),
                        delivery.getRating(),
                        delivery.getTips()
                ))
                .collect(Collectors.toList());
    }

    public DeliveryDto createDeliveryForDeliveryman(CreateDelivery createDelivery) {
        Deliveryman deliveryman = deliverymanRepository.getReferenceById(createDelivery.getDeliverymanId());
        Delivery delivery = deliveryRepository.save(createDelivery(createDelivery, deliveryman));
        return new DeliveryDto(
                delivery.getOrderId(),
                delivery.getOrderDateCreated(),
                delivery.getId(),
                delivery.getDeliveryman().getId(),
                delivery.getDeliveryAddress(),
                delivery.getDeliveryTime(),
                delivery.getRating(),
                delivery.getTips()
        );
    }

    private Delivery createDelivery(CreateDelivery createDelivery, Deliveryman deliveryman) {
        Faker faker = new Faker(new Locale("ru"));
        return new Delivery(
                Generator.generate(),
                deliveryman,
                createDelivery.getOrderId(),
                createDelivery.getOrderDateCreated(),
                faker.address().fullAddress(),
                createDelivery.getOrderDateCreated().plusMonths(randInt(30, 120)),
                randInt(1, 5),
                (long) randInt(0, 1000)
        );
    }

    private int randInt(int min, int max) {
        return new Random().nextInt((max - min) + 1) + min;
    }
}
