package com.example.fluxratelimit;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class RateLimitTestV2 {
    private static final Integer BATCH_SIZE = 2;

    @Test
    public void emitterBasedApproachMoreReactive() throws InterruptedException {
        JobItemIdFetcher idFetcher = new JobItemIdFetcher();
        LoadStatsFetcher loadStatsFetcher = new LoadStatsFetcher();
        JobItemActorPv jobItemActorPv = new JobItemActorPv();
        JobItemActorPvXs jobItemActorPvXs = new JobItemActorPvXs();

        Flux.<Integer>create(c -> {
            c.onRequest(i -> {
                log.info("requested {}", i);
                loadStatsFetcher.updateCapacity(i);
                idFetcher.fetchNext(i).forEach(c::next);
            });
        })
                .log()
                .concatMap(id -> Mono.just(id)
                                .flatMap(jobItemActorPv::callLegacyPv)
                                .subscribeOn(Schedulers.elastic())
                        , 1)
                .flatMap(jobItemActorPvXs::consume, 1, 1)
                .publishOn(Schedulers.parallel())
                .log()
                .subscribe(new BackpressureReadySubscriber<>(loadStatsFetcher));
        Thread.sleep(1000);
    }

    class BackpressureReadySubscriber<T> extends BaseSubscriber<T> {
        private final LoadStatsFetcher loadStatsFetcher;

        BackpressureReadySubscriber(LoadStatsFetcher loadStatsFetcher) {
            this.loadStatsFetcher = loadStatsFetcher;
        }

        @Override
        public void hookOnSubscribe(Subscription subscription) {
            //requested the first item on subscribe
            log.info("initial request");
            request(BATCH_SIZE);
        }

        @Override
        public void hookOnNext(T value) {
            //process value
            //processing...
            //once processed, request a next one
            log.info("Processed#{}", value);
            if (loadStatsFetcher.canReadNextBatch()) {
                request(BATCH_SIZE);
            }
        }
    }

    class JobItemIdFetcher {
        private AtomicInteger idx = new AtomicInteger();

        public List<Integer> fetchNext(long amount) {
            return IntStream.range(0, (int) amount).map(x -> idx.incrementAndGet()).boxed().collect(Collectors.toList());
        }
    }

    class JobItemActorPv {
        public Mono<Integer> callLegacyPv(Integer id) {
            return Mono.just(id)
                    .doOnSubscribe(sub -> log.info("PV#{}", id));
        }
    }

    class JobItemActorPvXs {
        public Mono<Integer> consume(Integer id) {
            return callPvis(id).zipWith(callPvss(id), (a, b) -> id);
        }

        public Mono<Integer> callPvis(Integer id) {
            return Mono.just(id)
                    .doOnSubscribe(sub -> log.info("PVIS#{}", id));
        }

        public Mono<Integer> callPvss(Integer id) {
            return Mono.just(id)
                    .doOnSubscribe(sub -> log.info("PVSS#{}", id));
        }
    }

    class LoadStatsFetcher {
        private AtomicLong limit = new AtomicLong(3);

        public boolean canReadNextBatch() {
            return limit.get() > 0;
        }

        // @Scheduled
        public void updateCapacity(long used) {
            limit.addAndGet(-1L * used);
        }
    }
}
