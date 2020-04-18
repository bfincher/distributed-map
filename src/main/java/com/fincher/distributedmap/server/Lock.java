package com.fincher.distributedmap.server;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class Lock {
    private static final Duration MAX_KEY_LOCK_TIME = Duration
            .ofMillis(Integer.parseInt(System.getProperty("distributed.map.lock.timeout.millis", "1000")));

    private static Clock clock = Clock.systemUTC();

    private String uuid = null;
    private boolean isLocked = false;
    private Instant timeLockAcquired;
    private final java.util.concurrent.locks.Lock lock = new ReentrantLock();
    private final Condition unlockedCondition = lock.newCondition();

    boolean isLocked() {
        lock.lock();
        try {
            if (isLocked) {
                Duration age = Duration.between(timeLockAcquired, clock.instant());
                if (age.compareTo(MAX_KEY_LOCK_TIME) > 0) {
                    isLocked = false;
                    unlockedCondition.signalAll();
                }
            }
            return isLocked;
        } finally {
            lock.unlock();
        }
    }


    boolean isLockedBy(String uuid) {
        return isLocked() && this.uuid.equals(uuid);
    }


    void lock(String uuid) throws InterruptedException {
        lock.lock();
        try {
            while (isLocked()) {
                unlockedCondition.await();
            }
            isLocked = true;
            timeLockAcquired = clock.instant();
            this.uuid = uuid;
        } finally {
            lock.unlock();
        }
    }


    boolean lock(String uuid, Duration timeout) throws InterruptedException {
        Instant startTime = clock.instant();
        if (lock.tryLock(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
            try {
                long nanosToWait = timeout.minus(Duration.between(startTime, clock.instant())).toNanos();
                while (isLocked()) {
                    if (nanosToWait <= 0) {
                        return false;
                    }

                    nanosToWait = unlockedCondition.awaitNanos(nanosToWait);
                }
                isLocked = true;
                timeLockAcquired = clock.instant();
                this.uuid = uuid;
                return true;
            } finally {
                lock.unlock();
            }
        }
        return false;
    }


    boolean unlock(String uuid) {
        lock.lock();
        try {
            if (isLocked() && this.uuid.equals(uuid)) {
                isLocked = false;
                this.uuid = null;
                unlockedCondition.signalAll();
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }
    
    boolean updateTime(String uuid) {
        lock.lock();
        try {
            if (isLockedBy(uuid)) {
                timeLockAcquired = clock.instant();
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }
    
    
    static void setTestClock(Clock clk) {
        clock = clk;
    }

}
