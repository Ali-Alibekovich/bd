package org.example.com;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import jakarta.annotation.PostConstruct;
import org.example.com.dto.CreateDelivery;
import org.example.com.dto.DeliveryDto;
import org.example.com.dto.DeliverymanDto;
import org.example.com.service.DeliveryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class Controller {

    @Autowired
    private DeliveryService deliveryService;

    @Value("${spring.datasource.url}")
    String url;

    @Value("${spring.datasource.password}")
    String username;

    @Value("${spring.datasource.username}")
    String password;

    @PostConstruct
    void init() throws IOException {
        String dml = new String(Files.readAllBytes(Paths.get("/Users/alishka/Desktop/work-itmo/bd_service/src/main/resources/init_data.sql")));
        String ddl = new String(Files.readAllBytes(Paths.get("/Users/alishka/Desktop/work-itmo/bd_service/src/main/resources/ddl.sql")));
        try(Connection db = DriverManager.getConnection(url, username, password)){
            db.setSchema("api_db");
            Statement statement = db.createStatement();
            statement.execute(ddl);
            statement.execute(dml);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    @PostMapping("/delivery")
    public DeliveryDto create(@RequestBody CreateDelivery createDelivery) {
        return deliveryService.createDeliveryForDeliveryman(createDelivery);
    }

    @GetMapping("/delivery_man")
    public List<DeliverymanDto> getDeliverers() {
        return deliveryService.getDeliverers();
    }

    @GetMapping("/delivery/{deliverymanId}")
    public List<DeliveryDto> getDeliveryForDeliveryman(@PathVariable String deliverymanId) {
        return deliveryService.getDeliveryForDeliveryman(deliverymanId);
    }
}
