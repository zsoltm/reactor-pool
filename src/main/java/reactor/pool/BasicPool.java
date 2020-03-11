package reactor.pool;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/** Basic pool operations common to all supported pool types. */
interface BasicPool extends Disposable {
    /**
     * Warms up the {@link Pool}, if needed. This typically instructs the pool to check for a minimum size and allocate
     * necessary objects when the minimum is not reached. The resulting {@link Mono} emits the number of extra resources
     * it created as a result of the {@link PoolBuilder#sizeBetween(int, int) allocation minimum}.
     * <p>
     * Note that no work nor allocation is performed until the {@link Mono} is subscribed to.
     * <p>
     * Implementations MAY include more behavior, but there is no restriction on the way this method is called by users
     * (it should be possible to call it at any time, as many times as needed or not at all).
     *
     * @apiNote this API is intended to easily reach the minimum allocated size (see {@link PoolBuilder#sizeBetween(int, int)})
     * without paying that cost on the first {@link Pool#acquire()} or {@link MetadataHandlingPool#acquire(Object)}. However,
     * implementors should also consider creating the extra resources needed to honor that minimum during the acquire,
     * as one cannot rely on the user calling {@code warmup()} consistently.
     *
     * @return a cold {@link Mono} that triggers resource warmup and emits the number of warmed up resources
     */
    Mono<Integer> warmup();

    /**
     * Shutdown the pool by:
     * <ul>
     *     <li>
     *         notifying every acquire still pending that the pool has been shut down,
     *         via a {@link RuntimeException}
     *     </li>
     *     <li>
     *         releasing each pooled resource, according to the release handler defined in
     *         the {@link PoolBuilder}
     *     </li>
     * </ul>
     * This imperative style method returns once every release handler has been started in
     * step 2, but doesn't necessarily block until full completion of said releases.
     * For a blocking alternative, use {@link #disposeLater()} and {@link Mono#block()}.
     * <p>
     * By default this is implemented as {@code .disposeLater().subscribe()}. As a result
     * failures during release could be swallowed.
     */
    @Override
    default void dispose() {
        disposeLater().subscribe();
    }

    /**
     * Returns a {@link Mono} that represents a lazy asynchronous shutdown of this {@link Pool}.
     * Shutdown doesn't happen until the {@link Mono} is {@link Mono#subscribe() subscribed}.
     * Otherwise, it performs the same steps as in the imperative counterpart, {@link #dispose()}.
     * <p>
     * If the pool has been already shut down, returns {@link Mono#empty()}. Completion of
     * the {@link Mono} indicates completion of the shutdown process.
     *
     * @return a Mono triggering the shutdown of the pool once subscribed.
     */
    Mono<Void> disposeLater();
}
