/*
 * Copyright (c) 2018-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.pool;

import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Non lock-free implementation of priority queue MPMC pending {@link Pool#acquire()} Monos serving them in priority
 * order, based on the supplied metadata describing priority, and the comparator supplied at pool construction time.
 *
 * See {@link SimplePool} for other characteristics of the simple pool.
 *
 * @author Simon Baslé
 */
final class SimpleLockingPriorityPool<POOLABLE, PRIORITY> extends SimplePool<POOLABLE, PRIORITY> {

    @SuppressWarnings("rawtypes")
    private static final PriorityQueue TERMINATED = new PriorityQueue(0);

    private final ReentrantLock lock = new ReentrantLock();

    private volatile PriorityQueue<Borrower<POOLABLE, PRIORITY>> pending;
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<SimpleLockingPriorityPool, PriorityQueue> PENDING =
            AtomicReferenceFieldUpdater.newUpdater(SimpleLockingPriorityPool.class, PriorityQueue.class, "pending");

    public SimpleLockingPriorityPool(PoolConfig<POOLABLE> poolConfig, Comparator<PRIORITY> comparator) {
        super(poolConfig);
        this.pending = new PriorityQueue<>(Comparator.comparing(borrower -> borrower.borrowMetadata, comparator));
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
            Queue<Borrower<POOLABLE, PRIORITY>> q = this.pending;
            Borrower<POOLABLE, PRIORITY> b = q.poll();
            if (b != null) PENDING_COUNT.decrementAndGet(this);
            return b;
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

}
