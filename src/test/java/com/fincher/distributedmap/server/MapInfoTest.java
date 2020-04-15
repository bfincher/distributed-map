package com.fincher.distributedmap.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fincher.distributedmap.Transaction;
import com.fincher.distributedmap.TransactionType;
import com.fincher.distributedmap.server.MapInfo.Lock;
import com.fincher.distributedmap.server.MapInfo.RegisteredClient;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MapInfoTest {

    private static final String TEST_MAP_NAME = "testMapName";
    private static final String TEST_KEY_TYPE = "java.lang.String";
    private static final String TEST_VALUE_TYPE = "java.lang.Integer";
    private static final String TEST_UUID = "testUuid";
    private static final String TEST_CHANNEL_ID = "testChannelId";

    private MapInfo info;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("distributed.map.lock.timeout.millis", "100");
    }

    @AfterClass
    public static void afterClass() {
        System.clearProperty("distributed.map.lock.timeout.millis");
    }

    @Before
    public void before() throws Exception {
        info = new MapInfo(TEST_MAP_NAME, TEST_KEY_TYPE, TEST_VALUE_TYPE);
        info.registerClient(TEST_UUID, 1, TEST_CHANNEL_ID, TEST_KEY_TYPE, TEST_VALUE_TYPE);
    }

    @Test
    public void testConstruct() {
        assertEquals(TEST_MAP_NAME, info.getMapName());
        assertEquals(TEST_KEY_TYPE, info.getKeyType());
        assertEquals(TEST_VALUE_TYPE, info.getValueType());
    }

    @Test
    public void testRegisterClient() {
        RegisteredClient rc = info.getClientByChannelId(TEST_CHANNEL_ID);
        assertEquals(TEST_UUID, rc.uuid);
        assertEquals(1, rc.mapTransId);
        assertEquals(TEST_CHANNEL_ID, rc.channelId);

        assertEquals(rc, info.getClientByUuid(TEST_UUID));

        // test with invalid key type
        try {
            info.registerClient("uuid2", 0, "testChannelId2", "java.lang.Long", TEST_VALUE_TYPE);
            fail("Expected an exception");
        } catch (RegistrationFailureException e) {
            String expectedMsg = "A map exists for name testMapName with a key type of java.lang.String"
                    + " that did not match this registration's key type of java.lang.Long";
            assertEquals(expectedMsg, e.getMessage());
        }

        // test with invalid value type
        try {
            info.registerClient("uuid3", 0, "testChannelId3", TEST_KEY_TYPE, "java.lang.Long");
            fail("Expected an exception");
        } catch (RegistrationFailureException e) {
            String expectedMsg = "A map exists for name testMapName with a value type of java.lang.Integer"
                    + " that did not match this registration's value type of java.lang.Long";
            assertEquals(expectedMsg, e.getMessage());
        }

        assertNull(info.getClientByChannelId("testChannelId2"));
        assertNull(info.getClientByChannelId("testChannelId3"));
        assertNull(info.getClientByUuid("uuid2"));
        assertNull(info.getClientByUuid("uuid3"));
    }

    @Test
    public void testDeRegisterClient() {
        MapInfo.Lock entry = new Lock(TEST_UUID);
        ByteString testKey = ByteString.copyFrom(new String("testKey").getBytes());
        info.keyLockMap.put(testKey, entry);

        info.mapLock = entry;

        info.deRegisterClient(TEST_UUID, TEST_CHANNEL_ID);

        assertNull(info.mapLock);
        assertNull(info.keyLockMap.get(testKey));
        assertNull(info.registeredClients.getByUuid(TEST_UUID));
        assertNull(info.registeredClients.getByChannelId(TEST_CHANNEL_ID));
    }

    @Test
    public void testCanAcquireKeyLock() throws InterruptedException {
        info.mapLock = new Lock("some other uuid");
        ByteString testKey = ByteString.copyFrom(new String("testKey").getBytes());

        // should be false because another uuid has a map lock;
        assertFalse(info.canAcquireKeyLock(testKey, TEST_UUID));

        // should pass after the lock expires
        Awaitility.await().atMost(110, TimeUnit.MILLISECONDS).until(() -> info.canAcquireKeyLock(testKey, TEST_UUID));

        info.keyLockMap.put(testKey, new Lock("someOtherUuid"));

        // should be false because another uuid has a key lock;
        assertFalse(info.canAcquireKeyLock(testKey, TEST_UUID));

        // should pass after the lock expires
        Awaitility.await().atMost(110, TimeUnit.MILLISECONDS).until(() -> info.canAcquireKeyLock(testKey, TEST_UUID));
    }

    @Test
    public void testAcquireKeyLock() throws Exception {
        ByteString testKey = ByteString.copyFrom(new String("testKey").getBytes());

        info.mapLock = new Lock("uuid3");
        // should fail since someone else has a map lock
        assertFalse(info.acquireKeyLock(testKey, TEST_UUID));
        assertNull(info.keyLockMap.get(testKey));

        Awaitility.await().atMost(110, TimeUnit.MILLISECONDS).until(() -> MapInfo.isLockOld(info.mapLock));

        // should pass now that the map lock has expired
        assertTrue(info.acquireKeyLock(testKey, TEST_UUID));
        assertEquals(TEST_UUID, info.keyLockMap.get(testKey).uuid);

        // unable to acquire since another key has the lock
        assertFalse(info.acquireKeyLock(testKey, "uuid2"));
        assertEquals(TEST_UUID, info.keyLockMap.get(testKey).uuid);

        // should pass after the lock expires
        Awaitility.await().atMost(110, TimeUnit.MILLISECONDS).until(() -> info.acquireKeyLock(testKey, "uuid2"));
        assertEquals("uuid2", info.keyLockMap.get(testKey).uuid);
    }
    
    @Test
    public void testReleaseKeyLock() {
        ByteString key = ByteString.copyFromUtf8("testKey");
        Stopwatch sw = Stopwatch.createStarted();
        info.acquireKeyLock(key, TEST_UUID);
        Lock lock = info.keyLockMap.get(key);
        assertNotNull(lock);
         
        // release should fail since this key doesn't own the lock
        assertFalse(info.releaseKeyLock(key, "uuid2"));
        lock = info.keyLockMap.get(key);
        assertNotNull(String.valueOf(sw.elapsed().toMillis()), lock);
        
        assertTrue(String.valueOf(sw.elapsed().toString()), info.releaseKeyLock(key, TEST_UUID));
        assertNull(info.keyLockMap.get(key));
    }

    @Test
    public void testCanAcquireMapLock() {
        info.mapLock = new Lock(TEST_UUID);
        assertTrue(info.canAcquireMapLock(TEST_UUID));

        info.mapLock = new Lock("uuid2");
        assertFalse(info.canAcquireMapLock(TEST_UUID));

        // should pass after the lock expires
        Awaitility.await().atMost(110, TimeUnit.MILLISECONDS).until(() -> info.canAcquireMapLock(TEST_UUID));
        assertNull(info.mapLock);

        assertTrue(info.canAcquireMapLock(TEST_UUID));
    }
    
    @Test
    public void testAcquireMapLock() {
        assertTrue(info.acquireMapLock("uuid2"));
        assertFalse(info.acquireMapLock(TEST_UUID));
        assertEquals("uuid2", info.mapLock.uuid);

        // should pass after the lock expires
        Awaitility.await().atMost(110, TimeUnit.MILLISECONDS).until(() -> info.acquireMapLock(TEST_UUID));
        assertEquals(TEST_UUID, info.mapLock.uuid);
    }
    
    @Test
    public void restReleaseMapLock() {
        info.acquireMapLock(TEST_UUID);
        assertEquals(TEST_UUID, info.mapLock.uuid);
        
        // should not release since uuid2 does not hold the lock
        assertFalse(info.releaseMapLock("uuid2"));
        assertEquals(TEST_UUID, info.mapLock.uuid);
        
        assertTrue(info.releaseMapLock(TEST_UUID));
        assertNull(info.mapLock);
    }
    
    @Test
    public void testIsLockOld() {
        Lock lock = new Lock(TEST_UUID);
        assertFalse(MapInfo.isLockOld(lock));
        
        Awaitility.await().atMost(110, TimeUnit.MILLISECONDS).until(() -> MapInfo.isLockOld(lock));
    }
    
    @Test
    public void testAddTransaction() {
        ByteString key1 = ByteString.copyFromUtf8("key1");
        ByteString value1 = ByteString.copyFromUtf8("value1");
        
        Transaction t1 = Transaction.newBuilder()
        .setKey(key1)
        .setValue(value1)
        .setTransType(TransactionType.UPDATE)
        .build();
        
        info.addTransaction(t1);
        assertEquals(t1, info.transactions.getByKey(key1).transaction);
    }
}
