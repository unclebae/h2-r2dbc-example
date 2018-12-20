package com.example.r2dbc.demor2dbc;

import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.repository.query.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

@SpringBootApplication
public class DemoR2dbcApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoR2dbcApplication.class, args);
    }

}

//@Repository
//class ReservationRepository {
//
//    ConnectionFactory connectionFactory;
//
//    public ReservationRepository(ConnectionFactory connectionFactory) {
//        this.connectionFactory = connectionFactory;
//    }
//
//    Mono<Void> deleteById(Integer id) {
//        return this.connection()
//                .flatMapMany(c -> c.createStatement("delete from reservation where id = $1")
//                .bind("$1", id)
//                .execute()
//                ).then();
//    }
//
//    Flux<Reservation> findAll() {
//        return this.connection()
//                .flatMapMany(connection ->
//                        Flux.from(connection.createStatement("select * from reservation").execute())
//                .flatMap(r -> r.map( (row, rowMatadata) ->
//                        new Reservation(row.get("id", Integer.class), row.get("name", String.class)))));
//    }
//
//    Flux<Reservation> save(Reservation r) {
//        final Flux<? extends Result> flatMapMany = this.connection()
//                .flatMapMany(conn -> conn.createStatement("insert into reservation(name) value ($1)")
//                        .bind("$1", r.getName())
//                        .add()
//                        .execute()
//                );
//
//        return flatMapMany.switchMap(x -> Flux.just(new Reservation(r.getId(), r.getName())));
//    }
//
//    private Mono<Connection> connection () {
//        return Mono.from(this.connectionFactory.create());
//    }
//
//}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Reservation {
    @Id
    private Integer id;
    private String name;

}

interface ReservationRepository extends ReactiveCrudRepository<Reservation, Integer> {
    @Query("select * from reservation where name = $1")
    Flux<Reservation> findByName(String name);
}

@Configuration
@EnableR2dbcRepositories
class R2dbcConfiguration extends AbstractR2dbcConfiguration {
    private final ConnectionFactory connectionFactory;

    R2dbcConfiguration(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }


    @Override
    public ConnectionFactory connectionFactory() {
        return connectionFactory;
    }
}

@Configuration
class ConnectionFactoryConfiguration {

    @Bean
    DatabaseClient databaseClient() {
        return DatabaseClient.create(connectionFactory());
    }

    @Bean
    ConnectionFactory connectionFactory() {

        ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                .url("mem:test;DB_CLOSE_DELAY=10")
                .build());

        return connectionFactory;
    }
}
