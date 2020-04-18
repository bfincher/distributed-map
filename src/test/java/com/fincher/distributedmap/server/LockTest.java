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


    @Test
    public void test() throws Exception {
        Lock lock = new Lock();
        assertFalse(lock.isLocked());
        assertFalse(lock.unlock("testUuid"));
        lock.lock("testUuid");
        assertTrue(lock.isLocked());
        assertTrue(lock.isLockedBy("testUuid"));
        assertFalse(lock.isLockedBy("otherUuid"));

        assertFalse(lock.unlock("otherUuid"));
        assertTrue(lock.unlock("testUuid"));

        lock.lock("testUuid");

        Awaitility.setDefaultPollDelay(Duration.ofMillis(10));
        Awaitility.await()
                .atLeast(Duration.ofMillis(50))
                .atMost(Duration.ofMillis(150))
                .until(() -> !lock.lock("uuid2", Duration.ofMillis(100)));
        assertFalse(lock.isLockedBy("uuid2"));
        assertTrue(lock.isLocked());
        
        Awaitility.await().atMost(Duration.ofMillis(350)).until(() -> lock.lock("uuid2", Duration.ofMillis(300)));
        assertTrue(lock.isLockedBy("uuid2"));
    }

}
