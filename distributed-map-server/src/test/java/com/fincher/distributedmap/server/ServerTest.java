package com.fincher.distributedmap.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fincher.distributedmap.messages.ClientToServerMessage;
import com.fincher.distributedmap.messages.ClientTransactionUpdate;
import com.fincher.distributedmap.messages.DeRegister;
import com.fincher.distributedmap.messages.Register;
import com.fincher.distributedmap.messages.RegisterResponse;
import com.fincher.distributedmap.messages.ReleaseKeyLock;
import com.fincher.distributedmap.messages.ReleaseMapLock;
import com.fincher.distributedmap.messages.RequestKeyLock;
import com.fincher.distributedmap.messages.RequestKeyLockResponse;
import com.fincher.distributedmap.messages.RequestMapChange;
import com.fincher.distributedmap.messages.RequestMapChangeResponse;
import com.fincher.distributedmap.messages.RequestMapChangeResponse.FailureReason;
import com.fincher.distributedmap.messages.RequestMapLock;
import com.fincher.distributedmap.messages.RequestMapLockResponse;
import com.fincher.distributedmap.messages.ServerToClientMessage;
import com.fincher.distributedmap.messages.Transaction;
import com.fincher.distributedmap.messages.Transaction.TransactionType;
import com.fincher.distributedmap.server.MapInfo.RegisteredClient;

import com.fincher.iochannel.MessageBuffer;
import com.fincher.iochannel.tcp.TcpChannelIfc;
import com.fincher.iochannel.tcp.TcpServerChannel;

import com.google.protobuf.ByteString;

import java.util.List;
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
    public void testConstruct() {
        server = new Server(123);
        assertTrue(server.channel instanceof TcpServerChannel);
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
    public void testRegisterInvalidKeyNoClients() throws Exception {
        registerClient();
        responseMsgBuf.poll(); // discard register response

        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        info.deRegisterClient(TEST_UUID, TEST_CHANNEL_ID);

        Register reg = Register.newBuilder()
                .setUuid("uuid2")
                .setMapName(TEST_MAP_NAME)
                .setKeyType(TEST_VALUE_TYPE)
                .setValueType(TEST_KEY_TYPE).build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRegister(reg).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId("channel2");
        server.handleMessage(mb);

        mb = responseMsgBuf.poll();
        assertEquals("channel2", mb.getReceivedFromChannelId());

        RegisterResponse resp = ServerToClientMessage.parseFrom(mb.getBytes()).getRegisterResponse();
        assertTrue(resp.getRegistrationSuccess());
        assertEquals("", resp.getFailureReason());
        assertEquals(TEST_MAP_NAME, resp.getMapName());

        info = server.mapInfoMap.get(TEST_MAP_NAME);
        assertEquals(1, info.getNumRegisteredClients());
        assertNotNull(info.registeredClients.getByUuid("uuid2"));
        assertNotNull(info.registeredClients.getByChannelId("channel2"));
        assertEquals(TEST_VALUE_TYPE, info.keyType);
        assertEquals(TEST_KEY_TYPE, info.valueType);
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

        registerClient("uuid2", "channelid2", true);

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


    @Test(expected = IllegalArgumentException.class)
    public void testUnexpectedMessage() {
        ClientToServerMessage msg = ClientToServerMessage.newBuilder().build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);
    }


    @Test
    public void testDeRegister() throws InterruptedException {
        // test dereg with no map
        DeRegister dr = DeRegister.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid("uuid").build();
        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setDeRegister(dr).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        assertNull(server.mapInfoMap.get("TEST_MAP_NAME"));

        // now test dereg after register
        registerClient();
        String uuid = TEST_UUID;

        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        info.acquireMapLock(uuid);

        dr = DeRegister.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(uuid).build();

        msg = ClientToServerMessage.newBuilder().setDeRegister(dr).build();
        mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        assertNull(info.registeredClients.getByUuid(uuid));
        assertNull(info.registeredClients.getByChannelId(TEST_CHANNEL_ID));
        assertFalse(info.hasMapLock(uuid));
    }


    @Test
    public void testConnectionLost() throws InterruptedException {
        registerClient(TEST_UUID, TEST_CHANNEL_ID, true);

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
    public void testRequestKeyLockValuesNotEqual() throws Exception {
        registerClient();
        responseMsgBuf.poll(); // discard register response

        ByteString key = ByteString.copyFromUtf8("key1");
        ByteString value1 = ByteString.copyFromUtf8("value1");
        ByteString value2 = ByteString.copyFromUtf8("value2");

        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        Transaction t = Transaction.newBuilder().setKey(key).setValue(value1).build();

        info.transactions.put(key, 1, new MapInfo.TransactionMapEntry(t, 1));

        RequestKeyLock req = RequestKeyLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid("uuid2")
                .setKey(key)
                .setValue(value2)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestKeyLock(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId("channelid2");
        server.handleMessage(mb);

        RequestKeyLockResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestKeyLockResponse();

        assertFalse(resp.getLockAcquired());

        // try again this time with an empty value in the transaction;
        t = Transaction.newBuilder().setKey(key).setValue(ByteString.EMPTY).build();
        info.transactions.put(key, 1, new MapInfo.TransactionMapEntry(t, 1));

        req = RequestKeyLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid("uuid2")
                .setKey(key)
                .setValue(value2)
                .build();

        msg = ClientToServerMessage.newBuilder().setRequestKeyLock(req).build();
        mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId("channelid2");
        server.handleMessage(mb);

        resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestKeyLockResponse();

        assertTrue(resp.getLockAcquired());
    }
    
    
    @Test
    public void testRequestKeyLockSuccess() throws Exception {
        registerClient();
        responseMsgBuf.poll(); // discard register response

        ByteString key = ByteString.copyFromUtf8("key1");
        ByteString value = ByteString.copyFromUtf8("value1");

        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        Transaction t = Transaction.newBuilder().setKey(key).setValue(value).build();

        info.transactions.put(key, 1, new MapInfo.TransactionMapEntry(t, 1));

        RequestKeyLock req = RequestKeyLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid("uuid2")
                .setKey(key)
                .setValue(value)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestKeyLock(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId("channelid2");
        server.handleMessage(mb);

        RequestKeyLockResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestKeyLockResponse();

        assertTrue(resp.getLockAcquired());
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


    @Test
    public void testHandleRequestMapChangeMapDoesNotExist() throws Exception {
        RequestMapChange req = RequestMapChange.newBuilder().setMapName("map that does not exist").build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestMapChange(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        RequestMapChangeResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestMapChangeResponse();

        assertEquals("map that does not exist", resp.getMapName());
        assertFalse(resp.getUpdateSuccess());
        assertEquals(FailureReason.MAP_DOES_NOT_EXIST, resp.getFailureReason());
    }


    @Test
    public void testHandleRequestMapChangeNoLock() throws Exception {
        registerClient();
        responseMsgBuf.poll(); // discard register response

        RequestMapChange req = RequestMapChange.newBuilder().setMapName(TEST_MAP_NAME).setUuid(TEST_UUID).build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestMapChange(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        RequestMapChangeResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestMapChangeResponse();

        assertEquals(TEST_MAP_NAME, resp.getMapName());
        assertFalse(resp.getUpdateSuccess());
        assertEquals(FailureReason.KEY_NOT_LOCKED, resp.getFailureReason());
    }
    
    
    @Test
    public void testHandleRequestMapChangeNotRegistered() throws Exception {
        registerClient();
        responseMsgBuf.poll(); // discard register response

        RequestMapChange req = RequestMapChange.newBuilder().setMapName(TEST_MAP_NAME).setUuid("2").build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestMapChange(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        RequestMapChangeResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestMapChangeResponse();

        assertEquals(TEST_MAP_NAME, resp.getMapName());
        assertFalse(resp.getUpdateSuccess());
        assertEquals(FailureReason.CLIENT_NOT_REGISTERED, resp.getFailureReason());
    }


    @Test
    public void testHandleRequestMapChangeWithKeyLock() throws Exception {
        registerClient();
        responseMsgBuf.poll(); // discard register response

        ByteString key = ByteString.copyFromUtf8("testKey");
        ByteString value = ByteString.copyFromUtf8("testValue");
        String uuid = TEST_UUID;

        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        info.acquireKeyLock(key, uuid);

        Transaction t1 = Transaction.newBuilder().setKey(key)
                .setValue(value)
                .setTransType(TransactionType.UPDATE)
                .build();

        RequestMapChange req = RequestMapChange.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(uuid)
                .setTransaction(t1)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestMapChange(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        RequestMapChangeResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestMapChangeResponse();

        assertEquals(TEST_MAP_NAME, resp.getMapName());
        assertTrue(resp.getUpdateSuccess());
        assertEquals(FailureReason.NO_STATEMENT, resp.getFailureReason());
    }
    
    
    @Test
    public void testHandleRequestMapChangeNotSynchronized() throws Exception {
        registerClient(TEST_UUID, TEST_CHANNEL_ID, false);
        responseMsgBuf.poll(); // discard register response

        ByteString key = ByteString.copyFromUtf8("testKey");
        ByteString value = ByteString.copyFromUtf8("testValue");
        String uuid = TEST_UUID;

        Transaction t1 = Transaction.newBuilder().setKey(key)
                .setValue(value)
                .setTransType(TransactionType.UPDATE)
                .build();

        RequestMapChange req = RequestMapChange.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(uuid)
                .setTransaction(t1)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestMapChange(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        RequestMapChangeResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestMapChangeResponse();

        assertEquals(TEST_MAP_NAME, resp.getMapName());
        assertTrue(resp.getUpdateSuccess());
        assertEquals(FailureReason.NO_STATEMENT, resp.getFailureReason());
    }


    @Test
    public void testMapChangeWithOtherClients() throws Exception {
        registerClient();
        responseMsgBuf.poll(); // discard register response

        registerClient("2", "2", true);
        responseMsgBuf.poll(); // discard register response

        registerClient("3", "3", true);
        responseMsgBuf.poll(); // discard register response

        ByteString key = ByteString.copyFromUtf8("testKey");
        ByteString value = ByteString.copyFromUtf8("testValue");
        
        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        info.acquireKeyLock(key, TEST_UUID);

        Transaction t1 = Transaction.newBuilder().setKey(key)
                .setValue(value)
                .setTransType(TransactionType.UPDATE)
                .build();

        RequestMapChange req = RequestMapChange.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(TEST_UUID)
                .setTransaction(t1)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestMapChange(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        boolean foundResponse = false;
        int numClientUpdatesReceived = 0;

        while ((mb = responseMsgBuf.poll(1, TimeUnit.SECONDS)) != null) {
            
            ServerToClientMessage resp = ServerToClientMessage.parseFrom(mb.getBytes());
            
            switch (resp.getMsgCase()) {
            case CLIENTTRANSACTIONUPDATE:
                ClientTransactionUpdate ctu = resp.getClientTransactionUpdate();
                assertEquals(TEST_MAP_NAME, ctu.getMapName());
                assertEquals(1, ctu.getMapTransactionId());
                
                List<Transaction> transList = ctu.getTransactionsList();
                assertEquals(1, transList.size());
                
                assertEquals(t1, transList.get(0));
                
                numClientUpdatesReceived++;
                break;

            case REQUESTMAPCHANGERESPONSE:
                assertTrue(resp.getRequestMapChangeResponse().getUpdateSuccess());
                foundResponse = true;
                break;

            default:
                fail("Unexpected response received " + resp.getMsgCase());
            }
        }

        assertTrue(foundResponse);
        assertEquals(2, numClientUpdatesReceived);
    }


    @Test
    public void testHandleRequestMapChangeWithMapLock() throws Exception {
        registerClient();
        responseMsgBuf.poll(); // discard register response

        ByteString key = ByteString.copyFromUtf8("testKey");
        ByteString value = ByteString.copyFromUtf8("testValue");
        String uuid = TEST_UUID;

        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        info.acquireMapLock(uuid);

        Transaction t1 = Transaction.newBuilder().setKey(key)
                .setValue(value)
                .setTransType(TransactionType.UPDATE)
                .build();

        RequestMapChange req = RequestMapChange.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(uuid)
                .setTransaction(t1)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestMapChange(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        ServerToClientMessage resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes());
        System.out.println(resp.getMsgCase());
        RequestMapChangeResponse rmcr = resp.getRequestMapChangeResponse();
        System.out.println(resp);
        assertEquals(TEST_MAP_NAME, rmcr.getMapName());
        assertTrue(rmcr.getUpdateSuccess());
        assertEquals(FailureReason.NO_STATEMENT, rmcr.getFailureReason());
    }


    @Test
    public void testRequestMapLock() throws Exception {
        // test requesting a lock on a map that does not exist
        RequestMapLock req = RequestMapLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(TEST_UUID)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestMapLock(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        RequestMapLockResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestMapLockResponse();

        assertFalse(resp.getLockAcquired());

        registerClient();
        responseMsgBuf.poll(); // discard register response

        req = RequestMapLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(TEST_UUID)
                .build();

        msg = ClientToServerMessage.newBuilder().setRequestMapLock(req).build();
        mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestMapLockResponse();

        assertTrue(resp.getLockAcquired());
        assertEquals(TEST_MAP_NAME, resp.getMapName());
    }


    @Test
    public void testRequestMapLockSomeOneElseHasMapLock() throws Exception {
        registerClient();
        responseMsgBuf.poll(); // discard register response

        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        info.acquireMapLock("uuid2");

        RequestMapLock req = RequestMapLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(TEST_UUID)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestMapLock(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        RequestMapLockResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestMapLockResponse();

        assertFalse(resp.getLockAcquired());
        assertEquals(TEST_MAP_NAME, resp.getMapName());
    }


    @Test
    public void testRequestMapLockSomeOneElseHasKeyLock() throws Exception {
        registerClient();
        responseMsgBuf.poll(); // discard register response

        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        info.acquireKeyLock(ByteString.EMPTY, "uuid2");

        RequestMapLock req = RequestMapLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(TEST_UUID)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRequestMapLock(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        RequestMapLockResponse resp = ServerToClientMessage.parseFrom(responseMsgBuf.poll().getBytes())
                .getRequestMapLockResponse();

        assertFalse(resp.getLockAcquired());
        assertEquals(TEST_MAP_NAME, resp.getMapName());
    }


    @Test
    public void testReleaseMapLock() throws Exception {
        // release the lock for a map that doesn't exist
        ReleaseMapLock req = ReleaseMapLock.newBuilder().setMapName(TEST_MAP_NAME)
                .setUuid(TEST_UUID)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setReleaseMapLock(req).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(TEST_CHANNEL_ID);
        server.handleMessage(mb);

        assertNull(server.mapInfoMap.get(TEST_MAP_NAME));

        registerClient();
        MapInfo info = server.mapInfoMap.get(TEST_MAP_NAME);
        info.mapLock.lock(TEST_UUID);
        server.handleMessage(mb);
        assertNotNull(server.mapInfoMap.get(TEST_MAP_NAME));
        assertFalse(server.mapInfoMap.get(TEST_MAP_NAME).mapLock.isLockedBy(TEST_UUID));
    }


    @Test(expected = RuntimeException.class)
    public void testHandleInvalidMessage() {
        byte[] bytes = { 1, 2, 3 };
        server.handleMessage(new MessageBuffer(bytes));
    }


    private void registerClient() {
        registerClient(TEST_UUID, TEST_CHANNEL_ID, true);
    }


    private void registerClient(String uuid, String channelId, boolean isSynchronized) {
        Register reg = Register.newBuilder()
                .setMapName(TEST_MAP_NAME)
                .setKeyType(TEST_KEY_TYPE)
                .setValueType(TEST_VALUE_TYPE)
                .setUuid(uuid)
                .setIsSynchronized(isSynchronized)
                .build();

        ClientToServerMessage msg = ClientToServerMessage.newBuilder().setRegister(reg).build();
        MessageBuffer mb = new MessageBuffer(msg.toByteArray());
        mb.setReceivedFromIoChannelId(channelId);

        server.handleMessage(mb);
    }

}
