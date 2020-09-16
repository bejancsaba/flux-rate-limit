package com.example.fluxratelimit;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class RateLimitTestV1 {
    private static final Integer BATCH_SIZE = 2;

    @Test
    public void controlViaSubscriber() {
        JobItemIdFetcher idFetcher = new JobItemIdFetcher();
        LoadStatsFetcher loadStatsFetcher = new LoadStatsFetcher();

        Flux.<List<Integer>>generate(sink -> sink.next(idFetcher.fetchNext(BATCH_SIZE)))
                .flatMap(ids -> Flux.fromIterable(ids)
                        .map(id -> new JobItemActorPv().callLegacyPv(id)).subscribeOn(Schedulers.parallel())
                )
                .map(id -> new JobItemActorPvXs().consume(id))
                .map(x -> "Batch Mapped")
                .publishOn(Schedulers.parallel())
                .subscribe(new BackpressureReadySubscriber<>(loadStatsFetcher));
    }

    class BackpressureReadySubscriber<T> extends BaseSubscriber<T> {
        private final LoadStatsFetcher loadStatsFetcher;

        BackpressureReadySubscriber(LoadStatsFetcher loadStatsFetcher) {
            this.loadStatsFetcher = loadStatsFetcher;
        }

        public void hookOnSubscribe(Subscription subscription) {
            //requested the first item on subscribe
            request(1);
        }

        public void hookOnNext(T value) {
            //process value
            //processing...
            //once processed, request a next one
            log.info("Value on next: " + value);

            if (loadStatsFetcher.canReadNextBatch()) {
                request(1);
            }
        }
    }

    class JobItemIdFetcher {
        public List<Integer> fetchNext(Integer amount) {
            Random random = new Random();
            return IntStream.range(0, amount).map(x -> random.nextInt()).boxed().collect(Collectors.toList());
        }
    }

    class JobItemActorPv {
        public Integer callLegacyPv(Integer id) {
            log.info(id + " - Legacy PV called with id");
            return id;
        }
    }

    class JobItemActorPvXs {
        public Integer consume(Integer id) {
            callPvis(id);
            callPvss(id);

            return 0;
        }

        public Integer callPvis(Integer id) {
            log.info(id + " - PVIS called with id");
            return id;
        }

        public Integer callPvss(Integer id) {
            log.info(id + " - PVSS called with id");
            return id;
        }
    }

    class LoadStatsFetcher {
        private int limit = 2;

        public boolean canReadNextBatch() {
            checkDynamoWcu();
            return limit > 0;
        }

        // @Scheduled
        public void checkDynamoWcu() {
            limit--;
        }
    }
}