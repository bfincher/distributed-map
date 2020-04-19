package com.fincher.distributedmap.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fincher.distributedmap.ClientToServerMessage;
import com.fincher.distributedmap.ClientTransactionUpdate;
import com.fincher.distributedmap.DeRegister;
import com.fincher.distributedmap.Register;
import com.fincher.distributedmap.RegisterResponse;
import com.fincher.distributedmap.ReleaseKeyLock;
import com.fincher.distributedmap.RequestKeyLock;
import com.fincher.distributedmap.RequestKeyLockResponse;
import com.fincher.distributedmap.ServerToClientMessage;
import com.fincher.distributedmap.Transaction;
import com.fincher.distributedmap.server.MapInfo.RegisteredClient;
import com.fincher.iochannel.MessageBuffer;
import com.fincher.iochannel.tcp.TcpChannelIfc;
import com.google.protobuf.ByteString;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class ServerTest {

    private static final String TEST_MAP_NAME = "testMapName";
    private static final String TEST_KEY_TYPE = "java.lang.String";
    private static final String TEST_VALUE_TYPE = "java.lang.Integer";
    private static final String TEST_UUID = "testUuid";
    private static final String TEST_CHANNEL_ID = "testChannelId";

    private TcpChannelIfc channel;
    private Server server;
    private BlockingQueue<MessageBuffer> responseMsgBuf;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("distributed.map.delete.empty.map.delay.millis", "500");
    }


    @AfterClass
    public static void afterClass() {
        System.clearProperty("distributed.map.delete.empty.map.delay.millis");
    }


    @Before
    public void before() throws Exception {
        channel = Mockito.mock(TcpChannelIfc.class);
        server = new Server(channel);

        responseMsgBuf = new LinkedBlockingQueue<>();

        Mockito.doAnswer(inv -> {
            MessageBuffer mb = inv.getArgument(0);
            mb = new MessageBuffer(mb.getBytes(), 4, mb.getBytes().length - 4);
            mb.setReceivedFromIoChannelId(inv.getArgument(1));
            responseMsgBuf.add(mb);
            return null;
        }).when(channel).send(Mockito.any(MessageBuffer.class), Mockito.anyString());
    }


    @Test
    public void testStart() throws Exception {
        server.start();
        Mockito.verify(channel, Mockito.times(1)).connect();
    }


    @Test
    public void testClose() throws Exception {
        server.close();
        Mockito.verify(channel, Mockito.times(1)).close();
    }


    @Test
    public void testRegisterNewMap() throws Exception {
        registerClient();

        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        assertEquals(TEST_KEY_TYPE, info.keyType);
        assertEquals(TEST_VALUE_TYPE, info.valueType);
        assertEquals(TEST_MAP_NAME, info.getMapName());

        RegisteredClient rc = info.registeredClients.getByChannelId(TEST_CHANNEL_ID);
        assertEquals(TEST_CHANNEL_ID, rc.channelId);
        assertEquals(0, rc.mapTransId);
        assertEquals(TEST_UUID, rc.uuid);
        assertEquals(rc, info.registeredClients.getByUuid(TEST_UUID));

        MessageBuffer mb = responseMsgBuf.poll();
        assertEquals(TEST_CHANNEL_ID, mb.getReceivedFromChannelId());

        RegisterResponse resp = ServerToClientMessage.parseFrom(mb.getBytes()).getRegisterResponse();
        assertTrue(resp.getRegistrationSuccess());
        assertEquals("", resp.getFailureReason());
        assertEquals(TEST_MAP_NAME, resp.getMapName());
    }


    @Test
    public void testRegisterInvalidKey() throws Exception {
        registerClient();

        Register reg = Register.newBuilder()
                .setMapName(TEST_MAP_NAME)
                .setKeyType(TEST_VALUE_TYPE)
                .setValueType(TEST_VALUE_TYPE).build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRegister(reg).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);

        server.handleMessage(mb);

        responseMsgBuf.poll(); // throw away the response from the first registration
        RegisterResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes()).getRegisterResponse();
        assertFalse(resp.getRegistrationSuccess());
        assertEquals(
                "A map exists for name testMapName with a key type of java.lang.String that did not match this registration's key type of java.lang.Integer",
                resp.getFailureReason());
        assertEquals(TEST_MAP_NAME, resp.getMapName());
    }


    @Test
    public void testRegisterInvalidValue() throws Exception {
        registerClient();

        Register reg = Register.newBuilder()
                .setMapName(TEST_MAP_NAME)
                .setKeyType(TEST_KEY_TYPE)
                .setValueType(TEST_KEY_TYPE).build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRegister(reg).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);

        server.handleMessage(mb);

        responseMsgBuf.poll(); // throw away the response from the first registration
        RegisterResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll(1, TimeUnit.SECONDS).getBytes())
                .getRegisterResponse();
        assertFalse(resp.getRegistrationSuccess());
        assertEquals(
                "A map exists for name testMapName with a value type of java.lang.Integer that did not match this registration's value type of java.lang.String",
                resp.getFailureReason());
        assertEquals(TEST_MAP_NAME, resp.getMapName());
    }


    @Test
    public void testRegisterSubsequentClient() throws Exception {
        registerClient();
        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);

        ByteString key1 = ByteString.copyFromUtf8("key1");
        ByteString value1 = ByteString.copyFromUtf8("value1");

        Transaction t1 = Transaction.newBuilder()
                .setKey(key1)
                .setValue(value1)
                .setTransType(Transaction.TransactionType.UPDATE)
                .build();

        info.addTransaction(t1);

        ByteString key2 = ByteString.copyFromUtf8("key2");
        ByteString value2 = ByteString.copyFromUtf8("value2");
        Transaction t2 = Transaction.newBuilder()
                .setKey(key2)
                .setValue(value2)
                .setTransType(Transaction.TransactionType.UPDATE)
                .build();

        info.addTransaction(t2);

        registerClient("uuid2", "channelid2");

        responseMsgBuf.poll(); // throw away the response from the first registration
        RegisterResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes()).getRegisterResponse();
        assertTrue(resp.getRegistrationSuccess());
        assertEquals("", resp.getFailureReason());
        assertEquals(TEST_MAP_NAME, resp.getMapName());

        ClientTransactionUpdate ctu = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getClientTransactionUpdate();
        assertEquals(TEST_MAP_NAME, ctu.getMapName());
        assertEquals(2, ctu.getTransactionsCount());
        assertEquals(2, ctu.getMapTransactionId());
        assertEquals(t1, ctu.getTransactions(0));
        assertEquals(t2, ctu.getTransactions(1));
    }


    @Test
    public void testDeRegister() throws InterruptedException {
        registerClient();
        String uuid = TEST_UUID;

        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        info.acquireMapLock(uuid);

        DeRegister dr = DeRegister.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(uuid).build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setDeRegister(dr).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        assertNull(info.registeredClients.getByUuid(uuid));
        assertNull(info.registeredClients.getByChannelId(TEST_CHANNEL_ID));
        assertFalse(info.hasMapLock(uuid));
    }
    
    
    @Test
    public void testConnectionLost() throws InterruptedException {
        registerClient(TEST_UUID, TEST_CHANNEL_ID);
        
        
        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        info.acquireMapLock(TEST_UUID);

        server.connectionLost(TEST_CHANNEL_ID);
        
        assertNull(info.registeredClients.getByUuid(TEST_UUID));
        assertNull(info.registeredClients.getByChannelId(TEST_CHANNEL_ID));
        assertFalse(info.hasMapLock(TEST_UUID));
    }


    @Test
    public void testRequestKeyLockNoMap() throws Exception {
        ByteString key = ByteString.copyFromUtf8("testKey");
        RequestKeyLock req = RequestKeyLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(TEST_UUID)
                .setKey(key)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestKeyLock(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        RequestKeyLockResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestKeyLockResponse();
        assertEquals(TEST_MAP_NAME, resp.getMapName());
        assertFalse(resp.getLockAcquired());
        assertEquals(key, resp.getKey());
    }


    @Test
    public void testRequestKeyLockMapSomeoneElseHoldsKeyLock() throws Exception {
        registerClient();

        // succesfully lock the key
        ByteString key = ByteString.copyFromUtf8("testKey");
        RequestKeyLock req = RequestKeyLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(TEST_UUID)
                .setKey(key)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestKeyLock(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        responseMsgBuf.poll(); // discard register response
        RequestKeyLockResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestKeyLockResponse();
        assertEquals(TEST_MAP_NAME, resp.getMapName());
        assertTrue(resp.getLockAcquired());
        assertEquals(key, resp.getKey());

        // request the same key lock with a different uuid
        req = RequestKeyLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid("uuid2")
                .setKey(key)
                .build();

        msg = ClientToServerMessage.newBuilder().setRequestKeyLock(req).build();
        mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId("channelid2");
        server.handleMessage(mb);

        resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes()).getRequestKeyLockResponse();
        assertEquals(TEST_MAP_NAME, resp.getMapName());
        assertFalse(resp.getLockAcquired());
        assertEquals(key, resp.getKey());
    }


    @Test
    public void testRequestKeyLockMapSomeoneElseHoldsMapLock() throws Exception {
        String uuid = TEST_UUID;
        registerClient();
        responseMsgBuf.poll(); // discard register response

        // lock the map
        server.mapInfoMap.get(TEST_MAP_NAME).acquireMapLock(uuid);

        ByteString key = ByteString.copyFromUtf8("testKey");
        RequestKeyLock req = RequestKeyLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid("uuid2")
                .setKey(key)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestKeyLock(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId("channelid2");
        server.handleMessage(mb);

        RequestKeyLockResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestKeyLockResponse();
        assertEquals(TEST_MAP_NAME, resp.getMapName());
        assertFalse(resp.getLockAcquired());
        assertEquals(key, resp.getKey());
    }


    @Test
    public void testReleaseKeyLock() throws InterruptedException {
        registerClient();
        responseMsgBuf.poll(); // discard register response
        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);

        ByteString key = ByteString.copyFromUtf8("testKey");
        info.acquireKeyLock(key, TEST_UUID);

        ReleaseKeyLock req = ReleaseKeyLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(TEST_UUID)
                .setKey(key)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setReleaseKeyLock(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);
        assertFalse(info.hasKeyLock(key, TEST_UUID));
    }


    private void registerClient() {
        registerClient(TEST_UUID, TEST_CHANNEL_ID);
    }


    private void registerClient(String uuid, String channelId) {
        Register reg = Register.newBuilder()
                .setMapName(TEST_MAP_NAME)
                .setKeyType(TEST_KEY_TYPE)
                .setValueType(TEST_VALUE_TYPE)
                .setUuid(uuid)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRegister(reg).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(channelId);

        server.handleMessage(mb);
    }

}
