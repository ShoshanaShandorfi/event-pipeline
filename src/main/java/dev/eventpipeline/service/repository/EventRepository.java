package dev.eventpipeline.service.repository;

import dev.eventpipeline.service.domain.Event;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EventRepository extends MongoRepository<Event, String>, EventRepositoryCustom {

    Page<Event> findByCustomerIdOrderByCreatedAtDesc(String customerId, Pageable pageable);
}