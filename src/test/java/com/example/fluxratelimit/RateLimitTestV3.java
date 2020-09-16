package com.example.fluxratelimit;

import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class RateLimitTestV3 {
    private static final long BATCH_SIZE = 5;

    @Test
    public void should() throws InterruptedException {
        var idFetcher = new JobItemIdFetcher();
        var loadStatsFetcher = new LoadStatsFetcher();
        var jobItemActorPv = new JobItemActorPv();
        var jobItemActorPvXs = new JobItemActorPvXs();

        Flux.range(0, 100000)
                .flatMap(i -> idFetcher.fetchNext(BATCH_SIZE)
                        .subscribeOn(Schedulers.elastic())
                        .publishOn(Schedulers.parallel()), 2, 2)
                .doOnNext(i -> log.info("fetched {}", i))
                //.takeWhile(i -> i <= 10) // Limit the number of processed items
                .flatMap(id -> Mono.defer(() -> jobItemActorPv.callLegacyPv(id))
                        .delaySubscription(loadStatsFetcher.hasCapacity()), 2, 2)
                .flatMap(id -> Mono.defer(() -> jobItemActorPvXs.consume(id))
                        .doOnSubscribe(s -> loadStatsFetcher.consume(1)), 2, 2) // capacity adjustment for testing purposes
                .subscribe(
                        i -> log.info("Processed {}", i),
                        e -> log.error("Error in the stream", e),
                        () -> log.info("Finished")
                );

        Thread.sleep(5000);
    }

    static class LoadStatsFetcher {
        private final AtomicInteger capacity = new AtomicInteger(10);
        private final SecureRandom rng = new SecureRandom();
        private final ReplayProcessor<Integer> capacityFlux = ReplayProcessor.cacheLastOrDefault(capacity.get());

        LoadStatsFetcher() {
            // Random capacity generation for flow validation purposes
            /*Flux.interval(Duration.ofMillis(500), Duration.ofSeconds(1)).subscribe(i -> {
                int capacityToAdd = rng.nextInt(MAX_CAPACITY_ADDED) + 1;
                log.info("Adding capacity {}", capacityToAdd);
                int currentCapacity = capacity.addAndGet(capacityToAdd);
                capacityFlux.onNext(currentCapacity);
            });*/
        }

        void consume(int cap) {
            int currentCapacity = capacity.updateAndGet(current -> Math.max(current - cap, 0));
            capacityFlux.onNext(currentCapacity);
        }

        Flux<Boolean> hasCapacity() {
            return capacityFlux.doOnNext(c -> log.info("current capacity {}", c)).map(currentCapacity -> currentCapacity > 0).filter(b -> b);
        }
    }

    static class JobItemIdFetcher {
        private final AtomicInteger idx = new AtomicInteger();

        public Flux<Integer> fetchNext(long amount) {
            return Flux.fromIterable(IntStream.range(0, (int) amount).map(x -> idx.incrementAndGet()).boxed().collect(Collectors.toList()));
        }
    }

    static class JobItemActorPv {
        public Mono<Integer> callLegacyPv(Integer id) {
            return Mono.just(id)
                    .doOnSubscribe(sub -> log.info("PV#{}", id))
                    .subscribeOn(Schedulers.elastic())
                    .publishOn(Schedulers.parallel());
        }
    }

    static class JobItemActorPvXs {
        public Mono<Integer> consume(Integer id) {
            return callPvis(id).zipWith(callPvss(id), (a, b) -> id)
                    .subscribeOn(Schedulers.elastic())
                    .publishOn(Schedulers.parallel());
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
}
