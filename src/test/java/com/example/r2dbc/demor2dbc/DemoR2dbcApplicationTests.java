package com.example.r2dbc.demor2dbc;

import io.r2dbc.spi.ConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.test.context.junit4.SpringRunner;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DemoR2dbcApplicationTests {

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    ConnectionFactory connectionFactory;

    @Test
    public void contextLoads() {
        DatabaseClient client = DatabaseClient.create(connectionFactory);
        client.execute()
                .sql("CREATE TABLE reservation" +
                        "(id INT PRIMARY KEY auto_increment," +
                        "name VARCHAR(255))"
                        )
                .fetch()
                .rowsUpdated()
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();

        Flux<Void> deleteAll = this.reservationRepository.findAll().flatMap(
                r -> this.reservationRepository.deleteById(r.getId())
        );

        StepVerifier
                .create(deleteAll)
                .expectNextCount(0)
                .verifyComplete();

        Flux<Reservation> reservationFlux = Flux.just("first", "second", "third")
                .map(name -> new Reservation(null, name))
                .flatMap(r -> this.reservationRepository.save(r));

        StepVerifier
                .create(reservationFlux)
                .expectNextCount(3)
                .verifyComplete();

        final Flux<Reservation> all = this.reservationRepository.findAll();
        StepVerifier
                .create(all)
                .expectNextCount(3)
                .verifyComplete();

        final Flux<Reservation> first = this.reservationRepository.findByName("first");
        StepVerifier
                .create(first)
                .expectNextCount(1)
                .verifyComplete();

    }

}

