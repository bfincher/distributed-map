package com.fincher.distributedmap.server;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LockTest {

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("distributed.map.lock.timeout.millis", "300");
    }


    @AfterClass
    public static void afterClass() {
        System.clearProperty("distributed.map.lock.timeout.millis");
    }


    @Test(timeout = 3000)
    public void test() throws Exception {
        final Lock lock = new Lock();
        assertFalse(lock.isLocked());
        assertFalse(lock.unlock("1"));
        lock.lock("1");
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedBy("1"));
        assertFalse(lock.isLockedBy("2"));

        assertFalse(lock.unlock("2"));

        lock.lock("1");

        Awaitility.setDefaultPollDelay(Duration.ofMillis(10));
        Awaitility.await()
                .atLeast(Duration.ofMillis(50))
                .atMost(Duration.ofMillis(150))
                .until(() -> !lock.lock("3", Duration.ofMillis(100)));
        assertFalse(lock.isLockedBy("3"));
        assertTrue(lock.isLocked());

        Awaitility.await().atMost(Duration.ofMillis(350)).until(() -> lock.lock("3", Duration.ofMillis(300)));
        assertTrue(lock.isLockedBy("3"));
    }
    
    
    @Test(timeout = 2000)
    public void testAnotherThreadHasLock() {
        Lock lock = new Lock();
        Thread thread = new Thread(() ->  {
            lock.concurrentLock.lock();
        });
        
        thread.start();
        
        Awaitility.setDefaultPollDelay(Duration.ofMillis(10));
        Awaitility.await()
                .atLeast(Duration.ofMillis(50))
                .atMost(Duration.ofMillis(150))
                .until(() -> !lock.lock("3", Duration.ofMillis(100)));
    }
    
    
    @Test(timeout = 2000)
    public void testThreaded() throws InterruptedException {
        final Lock lock = new Lock();
        lock.lock("1");
        
        Thread t = new Thread(() -> {
            try {
                lock.lock("2");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        
        t.start();
        
        Thread.sleep(10);
        
        lock.unlock("1");
        
        t.join();
        assertTrue(lock.isLockedBy("2"));    
    }
    
    
    @Test(timeout = 2000)
    public void testUpdateTime() throws InterruptedException {
        Lock lock = new Lock();
        lock.lock("1");
        
        Awaitility.await().atLeast(Duration.ofMillis(200));
        assertTrue(lock.updateTime("1"));
        Awaitility.await().atLeast(Duration.ofMillis(200)).atMost(Duration.ofMillis(400))
        .until(() -> !lock.isLockedBy("1"));
        
        lock.lock("1");
        assertFalse(lock.updateTime("2"));
        
    }

}   
