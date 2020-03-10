package reactor.pool;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

/**
 * A pool that is capable to alter its behaviour when an object is borrowed based on the evaluation of supplied
 * metadata.
 * <p>
 * An example of that can be a priority based pool, that hands out objects in priority order where the priority is
 * determined from the supplied metadata.
 *
 * @param <BORROW> type of metadata which is used at borrow time.
 */
public interface MetadataHandlingPool<POOLABLE, BORROW> extends Pool<POOLABLE> {

    /** {@link Pool#acquire()} with additional metadata. */
    default Mono<PooledRef<POOLABLE>> acquire(BORROW borrowMetaData) {
        return acquire(borrowMetaData, Duration.ZERO);
    }

    /** {@link Pool#acquire(Duration)} with additional metadata. */
    Mono<PooledRef<POOLABLE>> acquire(BORROW borrowMetaData, Duration timeout);

    /** {@link Pool#withPoolable(Function)} with additional metadata. */
    default <V> Flux<V> withPoolable(BORROW borrowMetaData, Function<POOLABLE, Publisher<V>> scopeFunction) {
        return Flux.usingWhen(
                acquire(borrowMetaData),
                slot -> scopeFunction.apply(slot.poolable()),
                PooledRef::release,
                (ref, error) -> ref.release(),
                PooledRef::release);
    }
}
