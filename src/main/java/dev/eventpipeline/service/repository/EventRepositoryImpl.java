package dev.eventpipeline.service.repository;

import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Date;
import org.bson.Document;

@Repository
public class EventRepositoryImpl implements EventRepositoryCustom {

    private final MongoTemplate mongoTemplate;

    public EventRepositoryImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Page<Event> findCustomerEvents(String customerId, Instant from, Instant to, EventStatus status, Pageable pageable) {
        Criteria criteria = Criteria.where("customerId").is(customerId);

        // createdAt is trusted and monotonic because the service sets it on receipt; it is also covered by the
        // { customerId, createdAt } index. The client-supplied event timestamp is intentionally not used for filters
        // because it can be wrong or delayed. Here from is inclusive and to is exclusive.
        if (from != null || to != null) {
            Criteria createdAtCriteria = Criteria.where("createdAt");
            if (from != null) {
                createdAtCriteria = createdAtCriteria.gte(from);
            }
            if (to != null) {
                createdAtCriteria = createdAtCriteria.lt(to);
            }
            criteria.andOperator(createdAtCriteria);
        }
        if (status != null) {
            criteria.and("status").is(status.name());
        }

        Query countQuery = Query.query(criteria);
        long total = mongoTemplate.count(countQuery, Event.class);

        Query pageQuery = Query.query(criteria)
                .with(pageable)
                .with(Sort.by(Sort.Direction.DESC, "createdAt"));
        List<Event> events = mongoTemplate.find(pageQuery, Event.class);

        return new PageImpl<>(events, pageable, total);
    }

    @Override
    public Optional<Event> claimForProcessing(String eventId, Instant now) {
        // Single atomic findAndModify: match _id AND status=RECEIVED, then set status=PROCESSING,
        // updatedAt=now and inc attempts by 1 (int, not long -- the collection validator requires attempts
        // to stay a BSON int32). Never read-then-write: this is the only thing that makes concurrent or
        // duplicate deliveries of the same message safe to race on.
        Query query = Query.query(Criteria.where("_id").is(eventId).and("status").is(EventStatus.RECEIVED.name()));
        Update update = new Update()
                .set("status", EventStatus.PROCESSING.name())
                .set("updatedAt", now)
                .inc("attempts", 1);
        Event claimed = mongoTemplate.findAndModify(query, update, FindAndModifyOptions.options().returnNew(true), Event.class);
        return Optional.ofNullable(claimed);
    }

    @Override
    public Optional<Event> markProcessed(String eventId, int claimedAttempts, Instant now) {
        Query query = ownedClaim(eventId, claimedAttempts);
        Update update = new Update()
                .set("status", EventStatus.PROCESSED.name())
                .set("processedAt", now)
                .set("updatedAt", now)
                .unset("failureReason");
        Event completed = mongoTemplate.findAndModify(query, update, FindAndModifyOptions.options().returnNew(true), Event.class);
        return Optional.ofNullable(completed);
    }

    @Override
    public Optional<Event> retryProcessing(String eventId, int claimedAttempts, String failureReason, Instant now) {
        return transitionClaim(eventId, claimedAttempts, EventStatus.RECEIVED, failureReason, now);
    }

    @Override
    public Optional<Event> markFailed(String eventId, int claimedAttempts, String failureReason, Instant now) {
        // FAILED is terminal automatically. An operator may reset it to RECEIVED (and reset attempts
        // for a fresh attempt budget); the relay then republishes it. There is deliberately no endpoint.
        return transitionClaim(eventId, claimedAttempts, EventStatus.FAILED, failureReason, now);
    }

    private Optional<Event> transitionClaim(String eventId, int claimedAttempts, EventStatus status,
                                             String failureReason, Instant now) {
        Update update = new Update().set("status", status.name()).set("failureReason", failureReason)
                .set("updatedAt", now);
        return Optional.ofNullable(mongoTemplate.findAndModify(ownedClaim(eventId, claimedAttempts), update,
                FindAndModifyOptions.options().returnNew(true), Event.class));
    }

    private Query ownedClaim(String eventId, int claimedAttempts) {
        return Query.query(Criteria.where("_id").is(eventId).and("status").is(EventStatus.PROCESSING.name())
                .and("attempts").is(claimedAttempts));
    }

    @Override
    public Optional<Event> leaseStaleReceived(Instant staleBefore, Instant now) {
        // Atomic lease: another relay cannot select this event in the same stale window, even if
        // the asynchronous publisher fails. A failed/crashed send simply becomes eligible again.
        Query query = staleEvents(EventStatus.RECEIVED, staleBefore);
        return Optional.ofNullable(mongoTemplate.findAndModify(query, new Update().set("updatedAt", now),
                FindAndModifyOptions.options().returnNew(true), Event.class));
    }

    @Override
    public Optional<Event> recoverStaleProcessing(Instant staleBefore, Instant now, int maxAttempts) {
        // Evaluate the retry budget in MongoDB, not after a read: a completion racing recovery can
        // win only once. The incremented attempts value on the next claim fences the old worker.
        Document hasBudget = new Document("$lt", List.of("$attempts", maxAttempts));
        Document status = new Document("$cond", List.of(hasBudget, EventStatus.RECEIVED.name(), EventStatus.FAILED.name()));
        Document exhausted = new Document("$concat", List.of("processing timeout after ",
                new Document("$toString", "$attempts"), " attempts"));
        Document reason = new Document("$cond", List.of(hasBudget, "recovered after processing timeout", exhausted));
        Document fields = new Document("status", status).append("updatedAt", Date.from(now)).append("failureReason", reason);
        // Only status, BSON date and string are changed; attempts remains the original BSON int32.
        AggregationUpdate update = AggregationUpdate.from(List.of(context -> new Document("$set", fields)));
        return Optional.ofNullable(mongoTemplate.findAndModify(staleEvents(EventStatus.PROCESSING, staleBefore), update,
                FindAndModifyOptions.options().returnNew(true), Event.class));
    }

    private Query staleEvents(EventStatus status, Instant staleBefore) {
        return Query.query(Criteria.where("status").is(status.name()).and("updatedAt").lt(staleBefore))
                .with(Sort.by(Sort.Direction.ASC, "updatedAt"));
    }
}
