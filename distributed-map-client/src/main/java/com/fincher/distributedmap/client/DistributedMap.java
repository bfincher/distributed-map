package com.fincher.distributedmap.client;

import com.fincher.distributedmap.messages.ClientToServerMessage;
import com.fincher.distributedmap.messages.ClientTransactionUpdate;
import com.fincher.distributedmap.messages.Register;
import com.fincher.distributedmap.messages.RegisterResponse;
import com.fincher.distributedmap.messages.ReleaseMapLock;
import com.fincher.distributedmap.messages.RequestKeyLock;
import com.fincher.distributedmap.messages.RequestKeyLockResponse;
import com.fincher.distributedmap.messages.RequestMapChange;
import com.fincher.distributedmap.messages.RequestMapChangeResponse;
import com.fincher.distributedmap.messages.RequestMapLock;
import com.fincher.distributedmap.messages.RequestMapLockResponse;
import com.fincher.distributedmap.messages.ServerToClientMessage;
import com.fincher.distributedmap.messages.ServerToClientMessage.MsgCase;
import com.fincher.distributedmap.messages.Transaction;
import com.fincher.distributedmap.messages.Transaction.TransactionType;

import com.fincher.iochannel.ChannelException;
import com.fincher.iochannel.MessageBuffer;
import com.fincher.iochannel.tcp.SimpleStreamIo;
import com.fincher.iochannel.tcp.TcpChannelIfc;
import com.fincher.iochannel.tcp.TcpClientChannel;
import com.fincher.iochannel.tcp.TransformingTcpChannel;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DistributedMap<K extends Serializable, V extends Serializable> implements Map<K, V>, AutoCloseable {

	private static final Logger LOG = LogManager.getLogger();

	private final Class<K> keyType;
	private final Class<V> valueType;
	protected final TransformingChannel channel;
	private final String mapName;
	private final boolean isSynchronized;
	protected final AtomicInteger mapTransactionId = new AtomicInteger(0);
	private final Lock lock = new ReentrantLock();
	private final Condition isConnectedCondition = lock.newCondition();
	private final Condition lostConnectedConditionOrMessageReceived = lock.newCondition();
	private boolean isConnected = false;
	protected final String uuid = UUID.randomUUID().toString();
	private final Map<K, V> map;
	private boolean isMapLocked = false;
	private Set<K> keysLocked = new HashSet<>();
	private final TransactionUpdater transactionUpdater = new TransactionUpdater(this);
	private final Set<Transaction> unrecordedLocalTransactions = new HashSet<>();

	public static class Builder<K extends Serializable, V extends Serializable> {
		private Class<K> keyType;
		private Class<V> valueType;
		private String mapName;
		private boolean isSynchronized;
		private String serverHost;
		private int serverPort = -1;
		private Supplier<Map<K, V>> mapSupplier;
		private TransformingChannel channel;
		private boolean hasBeenBuilt = false;

		public Builder<K, V> named(String name) {
			mapName = name;
			return this;
		}

		public Builder<K, V> withKeyType(Class<K> c) {
			keyType = c;
			return this;
		}

		public Builder<K, V> withValueType(Class<V> c) {
			valueType = c;
			return this;
		}

		public Builder<K, V> withSynchronization() {
			isSynchronized = true;
			return this;
		}

		public Builder<K, V> withoutSynchronization() {
			isSynchronized = false;
			return this;
		}

		public Builder<K, V> connectingToServerHost(String host) {
			serverHost = host;
			return this;
		}

		public Builder<K, V> connectingToServerPort(int port) {
			Preconditions.checkArgument(port > 0 && port <= 65535, "Server port must be between 1 and 65535 inclusive");
			serverPort = port;
			return this;
		}

		public Builder<K, V> withMapImplementation(Supplier<Map<K, V>> mapSupplier) {
			this.mapSupplier = mapSupplier;
			return this;
		}

		public DistributedMap<K, V> build() {
			Preconditions.checkState(!hasBeenBuilt, "A builder can only build a map once");
			Preconditions.checkNotNull(mapName, "Map name must be set");
			Preconditions.checkNotNull(keyType, "Key type must be set");
			Preconditions.checkNotNull(valueType, "Value type must be set");
			Preconditions.checkState(channel != null || serverHost != null, "Server host must be set");
			Preconditions.checkArgument(channel != null || (serverPort > 0 && serverPort <= 65535),
					"Server port must be between 1 and 65535 inclusive");

			if (channel == null) {
				TcpClientChannel tcpChannel = TcpClientChannel.createChannel(mapName + "Channel",
						new SimpleStreamIo(false), new InetSocketAddress(0),
						new InetSocketAddress(serverHost, serverPort));

				channel = new TransformingChannel(mapName + "Channel", tcpChannel);
			}

			Map<K, V> map;
			if (mapSupplier == null) {
				map = new HashMap<>();
			} else {
				map = mapSupplier.get();
			}

			DistributedMap<K, V> dm = new DistributedMap<>(mapName, isSynchronized, keyType, valueType, channel, map);
			hasBeenBuilt = true;
			return dm;
		}

		protected Builder<K, V> withOverridenChannel(TransformingChannel channel) {
			this.channel = channel;
			return this;
		}
	}

	protected DistributedMap(String mapName, boolean isSynchronized, Class<K> keyType, Class<V> valueType,
			TransformingChannel channel, Map<K, V> map) {
		this.mapName = mapName;
		this.keyType = keyType;
		this.valueType = valueType;
		this.channel = channel;
		this.isSynchronized = isSynchronized;

		channel.addConnectionEstablishedListener(this::connectionEstablished);
		channel.addConnectionLostListener(this::connectionLost);
		channel.addTransformedMessageListener(this::handleTransactionUpdate,
				msg -> msg.getMsgCase() == MsgCase.CLIENTTRANSACTIONUPDATE);

		this.map = map;
	}

	public void connect() throws IOException, InterruptedException {
		channel.connect();

		lock.lock();
		try {
			while (!isConnected) {
				isConnectedCondition.await();
			}
		} finally {
			lock.unlock();
		}

		try {
			register();
		} catch (DistributedMapException e) {
			LOG.error(e.getMessage(), e);
			close();
		}
	}

	public void close() throws IOException {
		channel.close();
	}

	@Override
	public synchronized void clear() {

		ensureConnection();

		boolean wasMapLocked = lockMap();

		try {
			for (Iterator<K> iterator = map.keySet().iterator(); iterator.hasNext();) {
				K key = iterator.next();
				requestTransaction(key, map.get(key), TransactionType.DELETE);
				iterator.remove();
			}
		} finally {
			if (wasMapLocked) {
				unlockMap();
			}
		}
	}

	@Override
	public boolean containsKey(Object o) {
		return map.containsKey(o);
	}

	@Override
	public boolean containsValue(Object o) {
		return map.containsValue(o);
	}

	@Override
	public V get(Object o) {
		return map.get(o);
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public Set<K> keySet() {
		Set<K> ks = new AbstractSet<K>() {
			public Iterator<K> iterator() {
				return new Iterator<K>() {
					private Iterator<Entry<K, V>> i = entrySet().iterator();

					public boolean hasNext() {
						return i.hasNext();
					}

					public K next() {
						return i.next().getKey();
					}

					public void remove() {
						i.remove();
					}
				};
			}

			public int size() {
				return DistributedMap.this.size();
			}

			public boolean isEmpty() {
				return DistributedMap.this.isEmpty();
			}

			public void clear() {
				DistributedMap.this.clear();
			}

			public boolean contains(Object k) {
				return DistributedMap.this.containsKey(k);
			}
		};

		return ks;
	}

	@Override
	public Collection<V> values() {
		Collection<V> c = new AbstractCollection<V>() {
			public Iterator<V> iterator() {
				return new Iterator<V>() {
					private Iterator<Entry<K, V>> i = entrySet().iterator();

					@Override
					public boolean hasNext() {
						return i.hasNext();
					}

					@Override
					public V next() {
						return i.next().getValue();
					}

					@Override
					public void remove() {
						i.remove();
					}
				};
			}

			@Override
			public int size() {
				return DistributedMap.this.size();
			}

			@Override
			public boolean isEmpty() {
				return DistributedMap.this.isEmpty();
			}

			@Override
			public void clear() {
				DistributedMap.this.clear();
			}

			@Override
			public boolean contains(Object v) {
				return DistributedMap.this.containsValue(v);
			}
		};

		return c;
	}

	@Override
	public synchronized void putAll(Map<? extends K, ? extends V> other) {
		ensureConnection();
		boolean needToUnlockMap = lockMap();
		try {
			for (Map.Entry<? extends K, ? extends V> entry : other.entrySet()) {
				put(entry.getKey(), entry.getValue());
			}
		} finally {
			if (needToUnlockMap) {
				unlockMap();
			}
		}
	}

	@Override
	public synchronized V put(K key, V value) {
		ensureConnection();
		V oldValue = map.get(key);
		boolean needToUnlockKey = lockKey(key, oldValue);

		requestTransaction(key, value, TransactionType.UPDATE);

		if (needToUnlockKey) {
			keysLocked.remove(key);
		}
		return map.put(key, value);
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new EntrySet(map.entrySet());
	}

	@Override
	public synchronized V remove(Object o) {
		ensureConnection();
		@SuppressWarnings("unchecked")
		K key = (K) o;
		V oldValue = map.get(o);
		if (oldValue != null) {
			boolean needToUnlockKey = lockKey(key, oldValue);
			try {
				requestTransaction(key, oldValue, TransactionType.DELETE);
				return map.remove(key);
			} finally {
				if (needToUnlockKey) {
					keysLocked.remove(key);
				}
			}
		}
		return null;
	}

	public boolean isSynchronized() {
		return isSynchronized;
	}

	protected void connectionEstablished(String channelId) {
		lock.lock();
		try {
			isConnected = true;
			isConnectedCondition.signal();
		} finally {
			lock.unlock();
		}
	}

	private void connectionLost(String channelId) {
		lock.lock();
		try {
			isConnected = false;
			lostConnectedConditionOrMessageReceived.signal();
		} finally {
			lock.unlock();
		}
	}

	private void register() throws ChannelException, InterruptedException {
		Register reg = Register.newBuilder().setMapName(mapName).setKeyType(keyType.getName())
				.setValueType(valueType.toString()).setIsSynchronized(isSynchronized)
				.setLastMapTransId(mapTransactionId.get()).setUuid(uuid).build();

		RegisterResponse response = rpcCall(ClientToServerMessage.newBuilder().setRegister(reg).build(), channel,
				resp -> resp.getMsgCase() == MsgCase.REGISTERRESPONSE).getRegisterResponse();

		if (!response.getRegistrationSuccess()) {
			throw new DistributedMapException(response.getFailureReason());
		}
	}

	private boolean lockMap() {

		if (isSynchronized && !isMapLocked) {
			RequestMapLock req = RequestMapLock.newBuilder().setMapName(mapName).setUuid(uuid)
					.setLatestTransId(mapTransactionId.get()).build();

			try {
				RequestMapLockResponse resp = rpcCall(ClientToServerMessage.newBuilder().setRequestMapLock(req).build(),
						channel, r -> r.getMsgCase() == MsgCase.REQUESTMAPLOCKRESPONSE).getRequestMapLockResponse();

				if (!resp.getLockAcquired()) {
					throw new DistributedMapException("Unable to obtain map lock");
				}

				isMapLocked = true;
				return true;
			} catch (ChannelException | InterruptedException e) {
				throw new DistributedMapException(e);
			}
		} else {
			return false;
		}
	}

	private void unlockMap() {
		try {
			channel.send(ClientToServerMessage.newBuilder()
					.setReleaseMapLock(ReleaseMapLock.newBuilder().setMapName(mapName).setUuid(uuid).build()).build());

			isMapLocked = false;
		} catch (ChannelException e) {
			throw new DistributedMapException(e);
		}
	}

	private boolean lockKey(K key, V value) {
		if (isSynchronized && !isMapLocked && !keysLocked.contains(key)) {
			try {
				RequestKeyLock.Builder req = RequestKeyLock.newBuilder().setKey(serialize(key)).setMapName(mapName)
						.setUuid(uuid);

				if (value != null) {
					req.setValue(serialize(value));
				}

				RequestKeyLockResponse resp = rpcCall(
						ClientToServerMessage.newBuilder().setRequestKeyLock(req.build()).build(), channel,
						r -> r.getMsgCase() == MsgCase.REQUESTKEYLOCKRESPONSE).getRequestKeyLockResponse();

				if (!resp.getLockAcquired()) {
					throw new DistributedMapException("Unable to get lock");
				}

				keysLocked.add(key);
				return true;

			} catch (IOException | InterruptedException e) {
				throw new DistributedMapException(e);
			}
		}
		return false;
	}

	private RequestMapChangeResponse requestTransaction(K key, V value, TransactionType transType) {

		try {
			Transaction.Builder builder = Transaction.newBuilder().setKey(serialize(key)).setTransType(transType);

			if (value != null) {
				builder.setValue(serialize(value));
			}
			
			Transaction trans = builder.build();

			RequestMapChange req = RequestMapChange.newBuilder().setMapName(mapName).setUuid(uuid)
					.setTransaction(trans).build();

			RequestMapChangeResponse response = rpcCall(
					ClientToServerMessage.newBuilder().setRequestMapChange(req).build(), channel,
					resp -> resp.getMsgCase() == MsgCase.REQUESTMAPCHANGERESPONSE).getRequestMapChangeResponse();

			if (response.getUpdateSuccess()) {
				unrecordedLocalTransactions.add(trans);
			} else {
				throw new DistributedMapException(response.getFailureReason().toString());
			}

			return response;
		} catch (IOException | InterruptedException e) {
			throw new DistributedMapException(e);
		}
	}

	protected static ByteString serialize(Serializable o) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream output = new ObjectOutputStream(bos);
		output.writeObject(o);
		output.flush();
		output.close();

		return ByteString.copyFrom(bos.toByteArray());
	}

	protected void ensureConnection() {
		switch (channel.getState()) {
		case INITIAL:
			throw new IllegalStateException(
					"Modification operations cannot be performed on the map until it is connected");
		case CLOSED:
			throw new IllegalStateException("This map has been closed");
		case CONNECTING:
			throw new IllegalStateException("Connection to server lost.  Attempting to re-connect");
		case CONNECTED:
			// no action necessasry
		}
	}

	private ServerToClientMessage rpcCall(ClientToServerMessage msg,
			TransformingTcpChannel<ClientToServerMessage, ServerToClientMessage> channel,
			Predicate<ServerToClientMessage> responsePredicate) throws ChannelException, InterruptedException {

		AtomicReference<ServerToClientMessage> response = new AtomicReference<>();

		Consumer<ServerToClientMessage> msgHandler = received -> {
			lock.lock();
			try {
				response.set(received);
				lostConnectedConditionOrMessageReceived.signal();
			} finally {
				lock.unlock();
			}
		};

		lock.lock();
		try {
			channel.addTransformedMessageListener(msgHandler, responsePredicate);

			channel.send(msg);
			lostConnectedConditionOrMessageReceived.await();
			if (response.get() == null) {
				throw new DistributedMapException("Connectioin lost");
			}

			return response.get();

		} finally {
			lock.unlock();
			channel.removeTransformedMessageListener(msgHandler);
		}
	}

	protected synchronized void updateTransaction(Transaction trans, int mapTID, boolean recordOnly) {
		if (!recordOnly) {
			try {
				switch (trans.getTransType()) {
				case UPDATE:
					map.put(deserialize(trans.getKey(), keyType), deserialize(trans.getValue(), valueType));
					break;

				case DELETE:
					map.remove(deserialize(trans.getKey(), Object.class));
					break;

				case UNRECOGNIZED:
					LOG.warn("Received unrecognized transaction type");
					break;
				}
			} catch (IOException | ClassNotFoundException e) {
				LOG.error(e.getMessage(), e);
			}

			mapTransactionId.set(mapTID);
		}
	}

	protected synchronized void handleTransactionUpdate(ServerToClientMessage msg) {
		ClientTransactionUpdate update = msg.getClientTransactionUpdate();
		if (!update.getMapName().equals(mapName)) {
			LOG.error("Received update for wrong map name.  Expected {} but was {}", mapName, update.getMapName());
			return;
		}

		if (update.getMapTransactionId() <= mapTransactionId.get()) {
			LOG.warn("Received older transaction id of {}.  Current transasction ID = is {}",
					update.getMapTransactionId(), mapTransactionId.get());
			return;
		}

		if (update.getMapTransactionId() != mapTransactionId.get() + 1) {
			LOG.warn("Received out of sequence transaction ID of {}.  Current transaction ID is {}",
					update.getMapTransactionId(), mapTransactionId.get());
		}

		Transaction trans = update.getTransaction();
		boolean recordOnly = unrecordedLocalTransactions.remove(trans);
		
		transactionUpdater.addTransaction(update.getTransaction(), update.getMapTransactionId(), recordOnly);
	}

	protected static class TransformingChannel
			extends TransformingTcpChannel<ClientToServerMessage, ServerToClientMessage> {
		public TransformingChannel(String id, TcpChannelIfc delegate) {
			super(id, delegate);
		}

		@Override
		public ServerToClientMessage decode(MessageBuffer mb) throws ChannelException {
			try {
				return ServerToClientMessage.parseFrom(mb.getBytes());
			} catch (InvalidProtocolBufferException e) {
				throw new ChannelException(e);
			}
		}

		@Override
		public MessageBuffer encode(ClientToServerMessage msg) {
			return new MessageBuffer(msg.toByteArray());
		}
	}

	private class EntrySet extends AbstractSet<Map.Entry<K, V>> {
		private final Set<Map.Entry<K, V>> parent;

		EntrySet(Set<Map.Entry<K, V>> parent) {
			this.parent = parent;
		}

		@Override
		public int size() {
			return parent.size();
		}

		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return new KeySetIterator(parent.iterator());
		}
	}

	private class KeySetIterator implements Iterator<Map.Entry<K, V>> {
		private final Iterator<Map.Entry<K, V>> iterator;
		private Map.Entry<K, V> prev = null;

		KeySetIterator(Iterator<Map.Entry<K, V>> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public Map.Entry<K, V> next() {
			prev = iterator.next();
			return prev;
		}

		@Override
		public void remove() {
			Preconditions.checkState(prev != null);

			boolean needToUnlockKey = lockKey(prev.getKey(), prev.getValue());

			try {
				requestTransaction(prev.getKey(), prev.getValue(), TransactionType.DELETE);
				iterator.remove();
			} finally {
				if (needToUnlockKey) {
					keysLocked.remove(prev.getKey());
				}

				prev = null;
			}
		}
	}

	private <T> T deserialize(ByteString bytes, Class<T> cls) throws IOException, ClassNotFoundException {
		try (ObjectInputStream ois = new ObjectInputStream(bytes.newInput())) {
			return cls.cast(ois.readObject());
		}
	}
}
