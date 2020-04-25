package com.fincher.distributedmap.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fincher.distributedmap.messages.Transaction;
import com.fincher.distributedmap.messages.Transaction.TransactionType;
import com.fincher.distributedmap.server.MapInfo.RegisteredClient;
import com.fincher.distributedmap.server.MapInfo.TransactionMapEntry;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;

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
        info = new MapInfo(TEST_MAP_NAME, TEST_KEY_TYPE, TEST_VALUE_TYPE, true);
        info.registerClient(TEST_UUID, 1, TEST_CHANNEL_ID, TEST_KEY_TYPE, TEST_VALUE_TYPE, true);
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
            info.registerClient("uuid2", 0, "testChannelId2", "java.lang.Long", TEST_VALUE_TYPE, true);
            fail("Expected an exception");
        } catch (RegistrationFailureException e) {
            String expectedMsg = "A map exists for name testMapName with a key type of java.lang.String"
                    + " that did not match this registration's key type of java.lang.Long";
            assertEquals(expectedMsg, e.getMessage());
        }

        // test with invalid value type
        try {
            info.registerClient("uuid3", 0, "testChannelId3", TEST_KEY_TYPE, "java.lang.Long", true);
            fail("Expected an exception");
        } catch (RegistrationFailureException e) {
            String expectedMsg = "A map exists for name testMapName with a value type of java.lang.Integer"
                    + " that did not match this registration's value type of java.lang.Long";
            assertEquals(expectedMsg, e.getMessage());
        }
        
        // test with invalid synch setting
        try {
            info.registerClient("uuid3", 0, "testChannelId3", TEST_KEY_TYPE, TEST_VALUE_TYPE, false);
            fail("Expected an exception");
        } catch (RegistrationFailureException e) {
            String expectedMsg = "A map exists for name testMapName with a synchronization setting of true"
                    + " that did not match this registration's synchronization setting of false";
            assertEquals(expectedMsg, e.getMessage());
        }

        assertNull(info.getClientByChannelId("testChannelId2"));
        assertNull(info.getClientByChannelId("testChannelId3"));
        assertNull(info.getClientByUuid("uuid2"));
        assertNull(info.getClientByUuid("uuid3"));
    }


    @Test
    public void testDeRegisterClient() throws Exception {
        ByteString testKey = ByteString.copyFrom(new String("testKey").getBytes());
        info.acquireKeyLock(testKey, TEST_UUID);
        info.acquireMapLock(TEST_UUID);

        info.deRegisterClient(TEST_UUID, TEST_CHANNEL_ID);

        assertFalse(info.hasMapLock(TEST_UUID));
        assertFalse(info.hasKeyLock(testKey, TEST_UUID));
        assertNull(info.registeredClients.getByUuid(TEST_UUID));
        assertNull(info.registeredClients.getByChannelId(TEST_CHANNEL_ID));
    }


    @Test
    public void testCanAcquireKeyLock() throws InterruptedException {
        info.acquireMapLock("some other uuid");
        ByteString testKey = ByteString.copyFrom(new String("testKey").getBytes());

        // should be false because another uuid has a map lock;
        assertFalse(info.canAcquireKeyLock(testKey, TEST_UUID));

        // should pass after the lock expires
        Awaitility.await().atMost(110, TimeUnit.MILLISECONDS).until(() -> info.canAcquireKeyLock(testKey, TEST_UUID));
        info.releaseKeyLock(testKey, TEST_UUID);

        info.acquireKeyLock(testKey, "someOtherUuid");

        // should be false because another uuid has a key lock;
        assertFalse(info.canAcquireKeyLock(testKey, TEST_UUID));

        // should pass after the lock expires
        Awaitility.await().atMost(110, TimeUnit.MILLISECONDS).until(() -> info.canAcquireKeyLock(testKey, TEST_UUID));
    }


    @Test
    public void testAcquireKeyLock() throws Exception {
        String uuid = TEST_UUID;
        String uuid2 = "uuid2";
        String uuid3 = "uuid3";
        Duration d110 = Duration.ofMillis(110);
        ByteString testKey = ByteString.copyFrom(new String("testKey").getBytes());
        
        // test acquiring a lock when a lock entry does not exist
        assertTrue(info.acquireKeyLock(testKey, uuid));
        info.releaseKeyLock(testKey, uuid);

        info.acquireMapLock(uuid3);
        // should fail since someone else has a map lock
        assertFalse(info.acquireKeyLock(testKey, uuid));

        // should pass now that the map lock has expired
        Awaitility.await().atMost(d110).until(() -> info.acquireKeyLock(testKey, uuid));
        assertTrue(info.hasKeyLock(testKey, uuid));

        // unable to acquire since another key has the lock
        assertFalse(info.acquireKeyLock(testKey, uuid2));
        assertTrue(info.hasKeyLock(testKey, uuid));

        // should pass after the lock expires
        Awaitility.await().atMost(d110).until(() -> info.acquireKeyLock(testKey, uuid2));
        assertTrue(info.hasKeyLock(testKey, uuid2));
        assertTrue(info.canAcquireKeyLock(testKey, uuid2));
    }


    @Test
    public void testReleaseKeyLock() throws InterruptedException {
        ByteString key = ByteString.copyFromUtf8("testKey");
        
        // test releasing a lock for which no lock entry exists
        assertFalse(info.releaseKeyLock(key, TEST_UUID));
        
        Stopwatch sw = Stopwatch.createStarted();
        info.acquireKeyLock(key, TEST_UUID);
        assertTrue(info.hasKeyLock(key, TEST_UUID));

        // release should fail since this key doesn't own the lock
        assertFalse(info.releaseKeyLock(key, "uuid2"));
        assertTrue(info.hasKeyLock(key, TEST_UUID));

        assertTrue(String.valueOf(sw.elapsed().toString()), info.releaseKeyLock(key, TEST_UUID));
        assertFalse(info.hasKeyLock(key, TEST_UUID));
    }


    @Test
    public void testCanAcquireMapLock() throws InterruptedException {
        String uuid = TEST_UUID;
        String uuid2 = "uuid2";

        assertTrue(info.canAcquireMapLock(uuid));
        info.acquireMapLock(uuid);
        assertTrue(info.canAcquireMapLock(uuid));
        info.releaseMapLock(uuid);

        info.acquireMapLock(uuid2);
        assertFalse(info.canAcquireMapLock(uuid));

        // should pass after the lock expires
        Awaitility.await().atMost(110, TimeUnit.MILLISECONDS).until(() -> info.canAcquireMapLock(uuid));
        assertFalse(info.isMapLocked());

        assertTrue(info.canAcquireMapLock(uuid));
    }


    @Test
    public void testAcquireMapLock() throws InterruptedException {
        String uuid = TEST_UUID;
        String uuid2 = "uuid2";
        assertTrue(info.acquireMapLock(uuid2));
        assertFalse(info.acquireMapLock(uuid));
        assertTrue(info.hasMapLock(uuid2));

        // should pass after the lock expires
        Awaitility.await().atMost(110, TimeUnit.MILLISECONDS).until(() -> info.acquireMapLock(uuid));
        assertTrue(info.hasMapLock(uuid));
    }


    @Test
    public void restReleaseMapLock() throws InterruptedException {
        String uuid = TEST_UUID;
        String uuid2 = "uuid2";
        info.acquireMapLock(uuid);

        // should not release since uuid2 does not hold the lock
        assertFalse(info.releaseMapLock(uuid2));
        assertTrue(info.hasMapLock(uuid));

        assertTrue(info.releaseMapLock(uuid));
        assertFalse(info.isMapLocked());
        assertFalse(info.hasMapLock(uuid));
    }


    @Test
    public void testAddTransaction() {
        ByteString key1 = ByteString.copyFromUtf8("key1");
        ByteString value1 = ByteString.copyFromUtf8("value1");

        Transaction t1 = Transaction.newBuilder()
                .setKey(key1)
                .setValue(value1)
                .setTransType(Transaction.TransactionType.UPDATE)
                .build();

        info.addTransaction(t1);
        TransactionMapEntry entry = info.transactions.getByKey(key1);
        assertEquals(t1, entry.transaction);
        assertEquals(1, entry.mapTransactionId);
        assertEquals(entry, info.transactions.getByMapTransId(entry.mapTransactionId));

        ByteString key2 = ByteString.copyFromUtf8("key2");
        ByteString value2 = ByteString.copyFromUtf8("value2");
        Transaction t2 = Transaction.newBuilder()
                .setKey(key2)
                .setValue(value2)
                .setTransType(Transaction.TransactionType.UPDATE)
                .build();

        info.addTransaction(t2);
        entry = info.transactions.getByKey(key2);
        assertEquals(t2, entry.transaction);
        assertEquals(2, entry.mapTransactionId);
        assertEquals(entry, info.transactions.getByMapTransId(entry.mapTransactionId));

        ByteString value3 = ByteString.copyFromUtf8("value3");
        Transaction t3 = Transaction.newBuilder()
                .setKey(key1)
                .setValue(value3)
                .setTransType(Transaction.TransactionType.UPDATE)
                .build();

        info.addTransaction(t3);
        entry = info.transactions.getByKey(key1);
        assertEquals(t3, entry.transaction);
        assertEquals(3, entry.mapTransactionId);
        assertEquals(entry, info.transactions.getByMapTransId(entry.mapTransactionId));
        assertNull(info.transactions.getByMapTransId(1));

        Transaction t4 = Transaction.newBuilder()
                .setKey(key1)
                .setTransType(Transaction.TransactionType.DELETE)
                .build();

        info.addTransaction(t4);
        entry = info.transactions.getByKey(key1);
        assertEquals(t4, entry.transaction);
        assertEquals(4, entry.mapTransactionId);
        assertEquals(entry, info.transactions.getByMapTransId(entry.mapTransactionId));
        assertNull(info.transactions.getByMapTransId(3));
    }


    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testTransactionMapEntryEquals() {

        Transaction t1 = Transaction.newBuilder().setTransTypeValue(TransactionType.UPDATE_VALUE).build();
        Transaction t2 = t1;

        // test same pointer
        TransactionMapEntry tme1 = new TransactionMapEntry(t1, 1);
        TransactionMapEntry tme2 = tme1;
        assertTrue(tme1.equals(tme2));

        // test other is null
        assertFalse(tme1.equals(null));

        // test different class
        assertFalse(tme1.equals("hello"));

        // test different trans id
        tme2 = new TransactionMapEntry(t1, 2);
        assertFalse(tme1.equals(tme2));

        // test different transaction
        t2 = Transaction.newBuilder().setTransTypeValue(TransactionType.DELETE_VALUE).build();
        tme2 = new TransactionMapEntry(t2, 1);
        assertFalse(tme1.equals(tme2));

        // test null transaction
        tme1 = new TransactionMapEntry(null, 1);
        assertFalse(tme1.equals(tme2));

        // test is equal
        tme1 = new TransactionMapEntry(t1, 1);
        tme2 = new TransactionMapEntry(t1, 1);
        assertTrue(tme1.equals(tme2));
    }


    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testRegisteredClientEquals() {
        RegisteredClient rc1 = new RegisteredClient("uuid1", 1, "1");
        RegisteredClient rc2 = rc1;

        // test same pointer
        assertTrue(rc1.equals(rc2));

        // test other null
        assertFalse(rc1.equals(null));

        // test different class
        assertFalse(rc1.equals("other"));

        // test channel id not equals
        rc2 = new RegisteredClient("uuid1", 1, "2");
        assertFalse(rc1.equals(rc2));

        // test tid not equals
        rc2 = new RegisteredClient("uuid1", 2, "1");
        assertFalse(rc1.equals(rc2));

        // test uuid not equals
        rc2 = new RegisteredClient("uuid2", 1, "1");
        assertFalse(rc1.equals(rc2));

        // test equals
        rc2 = new RegisteredClient("uuid1", 1, "1");
        assertTrue(rc1.equals(rc2));
    }


    @Test
    public void testGetTransactionWithKey() {
        MapInfo info = new MapInfo("mapName", "key", "value", true);
        
        ByteString key1 = ByteString.copyFromUtf8("key1");
        ByteString key2 = ByteString.copyFromUtf8("key2");
        
        TransactionMapEntry tme = new TransactionMapEntry(Transaction.newBuilder().build(), 1);
        
        info.transactions.put(key1, 1, tme);
        
        assertEquals(tme.transaction, info.getTransactionWithKey(key1));
        assertNull(info.getTransactionWithKey(key2));
    }

    static class TestClock extends Clock {

        Instant instant = Instant.now();

        void tick(Duration d) {
            instant = instant.plus(d);
        }


        @Override
        public Instant instant() {
            return instant;
        }


        @Override
        public ZoneId getZone() {
            return ZoneId.systemDefault();
        }


        @Override
        public Clock withZone(ZoneId zone) {
            throw new RuntimeException("not implemented");
        }

    }
}
