package reactor.pool;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.pool.SimpleFifoPoolTest.poolableTestConfig;

class SimpleLockingPriorityPoolTest {
    private AtomicInteger releasedCount;
    private AtomicInteger newCount;

    @BeforeEach
    void setUp() {
        releasedCount = new AtomicInteger();
        newCount = new AtomicInteger();
    }

    @Test
    void pendingAreSwappedAfterResourceAllocation() {
        Scheduler scheduler = Schedulers.newParallel("poolable test allocator");

        try {
            final PoolConfig<TestUtils.PoolableTest> testConfig = poolableTestConfig(
                    0, 1,
                    Mono.defer(() -> Mono
                            .delay(Duration.ofMillis(100))
                            .thenReturn(new TestUtils.PoolableTest(newCount.incrementAndGet())))
                            .subscribeOn(scheduler),
                    pt -> releasedCount.incrementAndGet());
            SimpleLockingPriorityPool<TestUtils.PoolableTest, Integer> pool =
                    new SimpleLockingPriorityPool<TestUtils.PoolableTest, Integer>(testConfig, Comparator.naturalOrder());

            final List<Integer> order = Flux.merge(
                    pool.withPoolable(1, p -> Mono.just(1)),
                    pool.withPoolable(0, p -> Mono.just(0))).collectList().block();

            assertThat(order).containsExactly(0, 1);
        }
        finally {
            scheduler.dispose();
        }
    }

}
