package reactor.pool;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Non lock-free implementation of MPMC priority queue. Pending Monos returned by {@link Pool#acquire()} are served in
 * priority order, based on the supplied metadata and the comparator at pool construction time.
 * <p>
 * Since it uses lock, it is intended to be used for fewer longer running processing tasks rather than many short lived
 * ones.
 */
final class SimpleLockingPriorityPool<POOLABLE, PRIORITY>
        extends SimplePool<POOLABLE, PRIORITY> implements MetadataHandlingPool<POOLABLE, PRIORITY> {

    @SuppressWarnings("rawtypes")
    private static final PriorityQueue TERMINATED = new PriorityQueue(1);

    private final ReentrantLock lock = new ReentrantLock();
    private final Comparator<Borrower<POOLABLE, PRIORITY>> borrowerComparator;

    private volatile PriorityQueue<Borrower<POOLABLE, PRIORITY>> pending;
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<SimpleLockingPriorityPool, PriorityQueue> PENDING =
            AtomicReferenceFieldUpdater.newUpdater(SimpleLockingPriorityPool.class, PriorityQueue.class, "pending");

    public SimpleLockingPriorityPool(PoolConfig<POOLABLE> poolConfig, Comparator<PRIORITY> comparator) {
        super(poolConfig);
        borrowerComparator = Comparator.comparing(borrower -> borrower.borrowMetadata, comparator);
        this.pending = new PriorityQueue<>(borrowerComparator);
    }

    @Override
    void pendingOffer(Borrower<POOLABLE, PRIORITY> pending) {
        int maxPending = poolConfig.maxPending();

        lock.lock();
        try {
            if (maxPending >= 0 && pendingCount == maxPending) {
                pending.fail(new PoolAcquirePendingLimitException(maxPending));
                return;
            }
            PENDING_COUNT.incrementAndGet(this);
            this.pending.offer(pending); //unbounded
        } finally {
            lock.unlock();
        }
    }

    @Override
    Borrower<POOLABLE, PRIORITY> pendingPoll() {
        lock.lock();
        try {
            Borrower<POOLABLE, PRIORITY> b = this.pending.poll();
            if (b != null) PENDING_COUNT.decrementAndGet(this);
            return b;
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected Borrower<POOLABLE, PRIORITY> swapPendingOptionally(Borrower<POOLABLE, PRIORITY> borrower) {
        lock.lock();
        try {
            Borrower<POOLABLE, PRIORITY> headBorrower = this.pending.peek();
            if (headBorrower == null) return borrower;
            if (borrowerComparator.compare(headBorrower, borrower) < 0) {
                this.pending.poll();
                this.pending.offer(borrower);
                return headBorrower;
            }
            return borrower;
        } finally {
            lock.unlock();
        }
    }

    @Override
    void cancelAcquire(Borrower<POOLABLE, PRIORITY> borrower) {
        lock.lock();
        try {
            if (!isDisposed()) { //ignore pool disposed
                Queue<Borrower<POOLABLE, PRIORITY>> q = this.pending;
                if (q.remove(borrower)) {
                    PENDING_COUNT.decrementAndGet(this);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isDisposed() {
        return PENDING.get(this) == TERMINATED;
    }

    @Override
    public Mono<Void> disposeLater() {
        @SuppressWarnings("unchecked")
        final Mono<Void> voidMono = super.disposeLater(PENDING, (Queue<Borrower<POOLABLE, PRIORITY>>) TERMINATED, this);
        return voidMono;
    }

    @Override
    public Mono<PooledRef<POOLABLE>> acquire(PRIORITY priority, Duration timeout) {
        return new QueueBorrowerMono<>(this, timeout, priority);
    }
}
