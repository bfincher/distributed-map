package com.fincher.distributedmap.client;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fincher.distributedmap.messages.Transaction;

class TransactionUpdater {

	private final Thread thread;
	private final DistributedMap<?, ?> parent;
	private final PriorityBlockingQueue<TransactionEntry> queue = new PriorityBlockingQueue<>();
	private volatile boolean stopped = false;
	private final Lock lock = new ReentrantLock();
	private final Condition condition = lock.newCondition();
	private int expectedNextMapTransactionId = 1;

	TransactionUpdater(DistributedMap<?, ?> parent) {
		this.parent = parent;
		thread = new Thread(this::run);
		thread.start();
	}

	void addTransaction(Transaction t, int mapTransactionId) {
		addTransaction(t, mapTransactionId, false);
	}

	void addTransaction(Transaction t, int mapTransactionId, boolean recordOnly) {
		lock.lock();
		try {
			queue.add(new TransactionEntry(t, mapTransactionId, recordOnly));
			condition.signal();
		} finally {
			lock.unlock();
		}
	}

	void stop() {
		stopped = true;
		thread.interrupt();
	}

	private void run() {
		lock.lock();
		try {
			while (!stopped) {
				try {
					while (queue.isEmpty()) {
						condition.await();
					}

					TransactionEntry entry = queue.peek();
					if (entry.mapTransactionId == expectedNextMapTransactionId) {
						expectedNextMapTransactionId++;
						queue.take();
						if (!entry.recordOnly) {
							parent.updateTransaction(entry.transasction, entry.mapTransactionId, entry.recordOnly);
						}
					}

				} catch (InterruptedException e) {
					// expected
				}
			}
		} finally {
			lock.unlock();
		}
	}

	private static class TransactionEntry implements Comparable<TransactionEntry> {
		final boolean recordOnly;
		final Transaction transasction;
		final int mapTransactionId;

		TransactionEntry(Transaction t, int mapTransactionId, boolean recordOnly) {
			this.transasction = t;
			this.recordOnly = recordOnly;
			this.mapTransactionId = mapTransactionId;
		}

		public int compareTo(TransactionEntry other) {
			return Integer.compare(mapTransactionId, other.mapTransactionId);
		}
	}

}
