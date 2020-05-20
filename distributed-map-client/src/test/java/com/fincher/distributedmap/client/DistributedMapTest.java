package com.fincher.distributedmap.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.fincher.distributedmap.client.DistributedMap.TransformingChannel;
import com.fincher.distributedmap.messages.ClientToServerMessage;
import com.fincher.distributedmap.messages.ClientToServerMessage.MsgCase;
import com.fincher.distributedmap.messages.ClientTransactionUpdate;
import com.fincher.distributedmap.messages.RegisterResponse;
import com.fincher.distributedmap.messages.RequestMapChange;
import com.fincher.distributedmap.messages.RequestMapChangeResponse;
import com.fincher.distributedmap.messages.RequestMapChangeResponse.FailureReason;
import com.fincher.distributedmap.messages.ServerToClientMessage;
import com.fincher.distributedmap.messages.Transaction;
import com.fincher.distributedmap.messages.Transaction.TransactionType;
import com.fincher.iochannel.ChannelState;
import com.fincher.thread.ThreadPool;
import com.google.common.base.Supplier;
import com.google.protobuf.ByteString;

public class DistributedMapTest {

	private static final String MAP_NAME = "testMapName";

	private TransformingChannel channel;
	private List<ClientToServerMessage> sentMsgs;
	private DistributedMap<String, Integer> map;
	private List<ListenerEntry> listeners;
	private ThreadPool threadPool;

	@SuppressWarnings("unchecked")
	@Before
	public void before() throws Exception {
		threadPool = new ThreadPool(1);
		sentMsgs = Collections.synchronizedList(new ArrayList<>());
		listeners = Collections.synchronizedList(new ArrayList<>());

		channel = Mockito.mock(TransformingChannel.class);
		Mockito.when(channel.getState()).thenReturn(ChannelState.CONNECTED);
		Mockito.doAnswer(inv -> {
			sentMsgs.add(inv.getArgument(0));
			return null;
		}).when(channel).send(Mockito.any(ClientToServerMessage.class));

		Mockito.doAnswer(inv -> {
			ListenerEntry entry = new ListenerEntry((Consumer<ServerToClientMessage>) inv.getArgument(0),
					(Predicate<ServerToClientMessage>) inv.getArgument(1));
			listeners.add(entry);
			return null;
		}).when(channel).addTransformedMessageListener(Mockito.any(Consumer.class),
				Mockito.any(java.util.function.Predicate.class));

		Mockito.doAnswer(inv -> {
			Object o = inv.getArgument(0);
			for (Iterator<ListenerEntry> it = listeners.iterator(); it.hasNext();) {
				if (it.next().listener == o) {
					it.remove();
					return null;
				}
			}
			return null;
		}).when(channel).removeTransformedMessageListener(Mockito.any());

		Supplier<Map<String, Integer>> supplier = TreeMap::new;

		map = new DistributedMap.Builder<String, Integer>().withKeyType(String.class).withValueType(Integer.class)
				.withoutSynchronization().named(MAP_NAME).withOverridenChannel(channel).withMapImplementation(supplier)
				.build();
	}

	@After
	public void after() {
		threadPool.shutdownNow();
	}

	@Test
	public void testBuilder() {
		DistributedMap.Builder<String, Integer> builder = new DistributedMap.Builder<>();
		try {
			builder.build();
		} catch (NullPointerException e) {
			assertEquals("Map name must be set", e.getMessage());
		}

		builder.named(MAP_NAME);
		try {
			builder.build();
		} catch (NullPointerException e) {
			assertEquals("Key type must be set", e.getMessage());
		}

		builder.withKeyType(String.class);
		try {
			builder.build();
		} catch (NullPointerException e) {
			assertEquals("Value type must be set", e.getMessage());
		}

		builder.withValueType(Integer.class);
		try {
			builder.build();
		} catch (IllegalStateException e) {
			assertEquals("Server host must be set", e.getMessage());
		}

		builder.connectingToServerHost("localhost");
		try {
			builder.build();
		} catch (IllegalArgumentException e) {
			assertEquals("Server port must be between 1 and 65535 inclusive", e.getMessage());
		}

		try {
			builder.connectingToServerPort(0);
		} catch (IllegalArgumentException e) {
			assertEquals("Server port must be between 1 and 65535 inclusive", e.getMessage());
		}

		try {
			builder.connectingToServerPort(65535);
		} catch (IllegalArgumentException e) {
			assertEquals("Server port must be between 1 and 65535 inclusive", e.getMessage());
		}

		DistributedMap<String, Integer> map = builder.build();
		assertFalse(map.isSynchronized());

		try {
			builder.build();
		} catch (IllegalStateException e) {
			assertEquals("A builder can only build a map once", e.getMessage());
		}
	}

	@Test
	public void testEnsureConnection() {
		Mockito.when(channel.getState()).thenReturn(ChannelState.INITIAL);
		try {
			map.ensureConnection();
			fail("Should have got exception");
		} catch (IllegalStateException e) {
			assertEquals("Modification operations cannot be performed on the map until it is connected",
					e.getMessage());
		}

		Mockito.when(channel.getState()).thenReturn(ChannelState.CLOSED);
		try {
			map.ensureConnection();
			fail("Should have got exception");
		} catch (IllegalStateException e) {
			assertEquals("This map has been closed", e.getMessage());
		}

		Mockito.when(channel.getState()).thenReturn(ChannelState.CONNECTING);
		try {
			map.ensureConnection();
			fail("Should have got exception");
		} catch (IllegalStateException e) {
			assertEquals("Connection to server lost.  Attempting to re-connect", e.getMessage());
		}

		Mockito.when(channel.getState()).thenReturn(ChannelState.CONNECTED);
		map.ensureConnection();
	}

	@Test
	public void testConnect() throws Exception {
		Future<?> future = threadPool.submit(new Callable<Void>() {
			public Void call() throws Exception {
				map.connect();
				return null;
			}
		});

		Awaitility.await().atLeast(Duration.ofMillis(50));
		assertFalse(future.isDone());
		map.connectionEstablished("");
		answerRegistrationMessage(null);
		Awaitility.await().atMost(Duration.ofMillis(1000)).until(() -> future.isDone());
		future.get();
	}

	@Test
	public void testConnectRegFailure() throws Exception {
		Future<?> future = threadPool.submit(new Callable<Void>() {
			public Void call() throws Exception {
				try {
					map.connect();
					fail("should have got exception");
				} catch (DistributedMapException e) {
					// expected
				}
				return null;
			}
		});

		Awaitility.await().atLeast(Duration.ofMillis(50));
		assertFalse(future.isDone());
		map.connectionEstablished("");
		answerRegistrationMessage("TestFailure");
		Awaitility.await().atMost(Duration.ofMillis(1000)).until(() -> future.isDone());
	}

	@Test
	public void testClose() throws Exception {
		map.close();
		Mockito.verify(channel).close();
	}

	@Test
	public void testPutAndClear() throws Exception {
		connectAndRegister();
		testPut("key1", 1);
		testPut("key2", 2);
		assertEquals(2, map.size());

		assertEquals(Integer.valueOf(1), map.get("key1"));
		assertEquals(Integer.valueOf(2), map.get("key2"));
		assertTrue(map.containsKey("key1"));
		assertTrue(map.containsKey("key2"));
		assertTrue(map.containsValue(Integer.valueOf(1)));
		assertTrue(map.containsValue(Integer.valueOf(2)));

		Future<?> future = threadPool.submit(() -> map.clear());
		testWasDeletedAndAnswer("key1", 1);
		testWasDeletedAndAnswer("key2", 2);

		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> future.isDone());
		future.get();

		assertTrue(map.isEmpty());
	}

	@Test
	public void testKeySet() throws Exception {
		testPut("key1", 1);
		testPut("key2", 2);
		testPut("key3", 3);
		Set<String> keySet = map.keySet();
		assertEquals(3, keySet.size());
		assertFalse(keySet.isEmpty());
		assertTrue(keySet.contains("key1"));
		assertTrue(keySet.contains("key2"));
		assertTrue(keySet.contains("key3"));

		Future<?> future1 = threadPool.submit(() -> keySet.clear());
		testWasDeletedAndAnswer("key1", 1);
		testWasDeletedAndAnswer("key2", 2);
		testWasDeletedAndAnswer("key3", 3);
		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> future1.isDone());
		future1.get();

		assertTrue(map.isEmpty());
		assertTrue(keySet.isEmpty());

		testPut("key1", 1);
		testPut("key2", 2);
		testPut("key3", 3);
		Iterator<String> it = map.keySet().iterator();
		assertEquals("key1", it.next());
		assertEquals("key2", it.next());
		Future<?> future2 = threadPool.submit(() -> it.remove());

		testWasDeletedAndAnswer("key2", 2);
		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> future2.isDone());
		future2.get();
		assertEquals("key3", it.next());

		assertEquals(2, keySet.size());
		assertEquals(2, map.size());
		assertTrue(map.containsKey("key1"));
		assertTrue(map.containsKey("key3"));
		assertFalse(map.containsKey("key2"));
	}

	@Test
	public void testValues() throws Exception {
		testPut("key1", 1);
		testPut("key2", 2);
		testPut("key3", 3);
		Collection<Integer> values = map.values();
		assertEquals(3, values.size());
		assertFalse(values.isEmpty());
		assertTrue(values.contains(1));
		assertTrue(values.contains(2));
		assertTrue(values.contains(3));

		Future<?> future1 = threadPool.submit(() -> values.clear());
		testWasDeletedAndAnswer("key1", 1);
		testWasDeletedAndAnswer("key2", 2);
		testWasDeletedAndAnswer("key3", 3);
		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> future1.isDone());
		future1.get();

		assertTrue(map.isEmpty());
		assertTrue(values.isEmpty());

		testPut("key1", 1);
		testPut("key2", 2);
		testPut("key3", 3);
		Iterator<Integer> it = map.values().iterator();
		assertEquals(Integer.valueOf(1), it.next());
		assertEquals(Integer.valueOf(2), it.next());
		Future<?> future2 = threadPool.submit(() -> it.remove());

		testWasDeletedAndAnswer("key2", 2);
		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> future2.isDone());
		future2.get();
		assertEquals(Integer.valueOf(3), it.next());

		assertEquals(2, values.size());
		assertEquals(2, map.size());
		assertTrue(map.containsValue(Integer.valueOf(1)));
		assertTrue(map.containsValue(Integer.valueOf(3)));
		assertFalse(map.containsValue(Integer.valueOf(2)));
	}

	@Test
	public void testPutAll() throws Exception {
		TreeMap<String, Integer> m = new TreeMap<>();
		m.put("key1", 1);
		m.put("key2", 2);
		m.put("key3", 3);

		Future<?> future = threadPool.submit(() -> map.putAll(m));
		testWasPutAndAnswer("key1", 1);
		testWasPutAndAnswer("key2", 2);
		testWasPutAndAnswer("key3", 3);

		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> future.isDone());
		future.get();

		assertEquals(Integer.valueOf(1), map.get("key1"));
		assertEquals(Integer.valueOf(2), map.get("key2"));
		assertEquals(Integer.valueOf(3), map.get("key3"));
		assertEquals(3, map.size());
	}

	@Test
	public void testRemove() throws Exception {
		assertNull(map.remove("key1"));
		testPut("key1", 1);

		Callable<Integer> callable = () -> {
			return map.remove("key1");
		};

		Future<Integer> future = threadPool.submit(callable);
		testWasDeletedAndAnswer("key1", Integer.valueOf(1));
		Awaitility.await().atMost(Duration.ofSeconds(1));
		assertEquals(Integer.valueOf(1), future.get());
	}

	@Test
	public void testTransactionUpdate() throws Exception {
		map.handleTransactionUpdate(buildClientUpdate("key1", 1, TransactionType.UPDATE, 1, MAP_NAME));
		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> map.get("key1") == 1);
		assertEquals(1, map.size());

		map.handleTransactionUpdate(buildClientUpdate("key2", 2, TransactionType.UPDATE, 2, MAP_NAME));
		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> map.get("key2") == 2);
		assertEquals(2, map.size());
		
		map.handleTransactionUpdate(buildClientUpdate("key1", 1, TransactionType.DELETE, 3, MAP_NAME));
		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> !map.containsKey("key1"));
		assertEquals(1, map.size());
		
		// test invalid map name
		map.handleTransactionUpdate(buildClientUpdate("key3", 3, TransactionType.UPDATE, 4, "wrongName"));
		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> !map.containsKey("key3"));
		assertEquals(1, map.size());
		assertEquals(3, map.mapTransactionId.get());
		
		// test old tid
		map.handleTransactionUpdate(buildClientUpdate("key3", 3, TransactionType.UPDATE, 2, MAP_NAME));
		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> !map.containsKey("key3"));
		assertFalse(map.containsKey("key3"));
		assertEquals(1, map.size());
		assertEquals(3, map.mapTransactionId.get());
		
		//TODO test out of sequence tid
	}

	private ServerToClientMessage buildClientUpdate(String key, 
			Integer value, 
			TransactionType type, 
			int mapTid,
			String mapName)
			throws Exception {
		return ServerToClientMessage.newBuilder()
				.setClientTransactionUpdate(ClientTransactionUpdate.newBuilder().setMapName(mapName)
						.setTransaction(Transaction.newBuilder()
								.setKey(DistributedMap.serialize(key))
										.setValue(DistributedMap.serialize(value)).setTransType(type).build())
						.setMapTransactionId(mapTid).build())
				.build();
	}

	private void testWasDeletedAndAnswer(String key, Integer value) throws Exception {
		RequestMapChange req = getSentMessage(MsgCase.REQUESTMAPCHANGE).getRequestMapChange();
		assertEquals(MAP_NAME, MAP_NAME);
		assertEquals(map.uuid, req.getUuid());

		Transaction trans = req.getTransaction();
		assertEquals(key, deserialize(trans.getKey()));
		assertEquals(value, deserialize(trans.getValue()));
		assertEquals(TransactionType.DELETE, trans.getTransType());

		answerRequestMapChange(null, trans);
	}

	private void testPut(String key, Integer value) throws Exception {
		Future<?> future = threadPool.submit(() -> map.put(key, value));

		testWasPutAndAnswer(key, value);

		Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> future.isDone());
		future.get();
	}

	private void testWasPutAndAnswer(String key, Integer value) throws Exception {
		ClientToServerMessage sent = getSentMessage(MsgCase.REQUESTMAPCHANGE);

		RequestMapChange req = sent.getRequestMapChange();
		assertEquals(MAP_NAME, req.getMapName());
		assertEquals(map.uuid, req.getUuid());

		Transaction trans = req.getTransaction();
		assertEquals(key, deserialize(trans.getKey()));
		assertEquals(value, deserialize(trans.getValue()));
		assertEquals(TransactionType.UPDATE, trans.getTransType());

		answerRequestMapChange(null, trans);
	}

	private void answerRequestMapChange(FailureReason failureReason, Transaction trans) {
		RequestMapChangeResponse.Builder resp = RequestMapChangeResponse.newBuilder().setMapName(MAP_NAME)
				.setTransaction(trans);

		if (failureReason == null) {
			resp.setUpdateSuccess(true);
		} else {
			resp.setUpdateSuccess(false);
			resp.setFailureReason(failureReason);
		}

		ServerToClientMessage stc = ServerToClientMessage.newBuilder().setRequestMapChangeResponse(resp.build())
				.build();

		getListener(stc).accept(stc);
	}

	private Object deserialize(ByteString bs) throws Exception {
		try (ObjectInputStream ois = new ObjectInputStream(bs.newInput())) {
			return ois.readObject();
		}
	}

	private void connectAndRegister() throws Exception {
		Future<?> future = threadPool.submit(new Callable<Void>() {
			public Void call() throws Exception {
				map.connect();
				return null;
			}
		});

		Awaitility.await().atLeast(Duration.ofMillis(50));
		assertFalse(future.isDone());
		map.connectionEstablished("");
		answerRegistrationMessage(null);
		Awaitility.await().atMost(Duration.ofMillis(1000)).until(() -> future.isDone());
		future.get();
	}

	private ClientToServerMessage getSentMessage(MsgCase msgCase) {
		AtomicReference<ClientToServerMessage> sent = new AtomicReference<>();
		Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> {
			for (int i = 0; i < sentMsgs.size(); i++) {
				ClientToServerMessage sentMsg = sentMsgs.get(i);
				if (sentMsg.getMsgCase() == msgCase) {
					sentMsgs.remove(sentMsg);
					sent.set(sentMsg);
					return true;
				}
			}
			return false;
		});

		return sent.get();
	}

	private void answerRegistrationMessage(String failureReason) {
		ClientToServerMessage sent = getSentMessage(MsgCase.REGISTER);

		RegisterResponse.Builder resp = RegisterResponse.newBuilder().setMapName(sent.getRegister().getMapName());

		if (failureReason != null) {
			resp.setFailureReason(failureReason);
			resp.setRegistrationSuccess(false);
		} else {
			resp.setRegistrationSuccess(true);
		}

		ServerToClientMessage stc = ServerToClientMessage.newBuilder().setRegisterResponse(resp.build()).build();

		// find the listener
		getListener(stc).accept(stc);
	}

	private Consumer<ServerToClientMessage> getListener(ServerToClientMessage msg) {
		// find the listener
		synchronized (listeners) {
			Iterator<ListenerEntry> listenerIt = listeners.iterator();
			while (listenerIt.hasNext()) {
				ListenerEntry listener = listenerIt.next();
				if (listener.predicate.test(msg)) {
					listenerIt.remove();
					return listener.listener;
				}
			}
			throw new RuntimeException("Couldn't find listener");
		}
	}

	private class ListenerEntry {
		private final Consumer<ServerToClientMessage> listener;
		private final Predicate<ServerToClientMessage> predicate;

		ListenerEntry(Consumer<ServerToClientMessage> listener, Predicate<ServerToClientMessage> predicate) {
			this.listener = listener;
			this.predicate = predicate;
		}
	}

}
