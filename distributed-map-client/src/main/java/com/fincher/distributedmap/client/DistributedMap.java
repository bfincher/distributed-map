package com.fincher.distributedmap.client;

import com.fincher.distributedmap.messages.ClientToServerMessage;
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

public class DistributedMap<K extends Serializable, V extends Serializable> implements Map<K, V> {

    private final Class<K> keyType;
    private final Class<V> valueType;
    private final TransformingChannel channel;
    private final String mapName;
    private final boolean isSynchronized;
    private final AtomicInteger mapTransactionId = new AtomicInteger(0);
    private final Lock lock = new ReentrantLock();
    private final Condition isConnectedCondition = lock.newCondition();
    private final Condition lostConnectedConditionOrMessageReceived = lock.newCondition();
    private boolean isConnected = false;
    private final String uuid = UUID.randomUUID().toString();
    private final Map<K, V> map = new HashMap<>();
    private boolean isMapLocked = false;
    private Set<K> keysLocked = new HashSet<>();

    public static <K extends Serializable, V extends Serializable> DistributedMap<K, V> create(String mapName,
            boolean isSynchronized,
            Class<K> keyType,
            Class<V> valueType,
            String serverHost,
            int serverPort) {
        TransformingChannel channel = new TransformingChannel("channel", TcpClientChannel.createChannel(
                "channel",
                new SimpleStreamIo(false),
                new InetSocketAddress(0),
                new InetSocketAddress(serverHost, serverPort)));

        return new DistributedMap<K, V>(mapName, isSynchronized, keyType, valueType, channel);
    }


    protected DistributedMap(String mapName,
            boolean isSynchronized,
            Class<K> keyType,
            Class<V> valueType,
            TransformingChannel channel) {
        this.mapName = mapName;
        this.keyType = keyType;
        this.valueType = valueType;
        this.channel = channel;
        this.isSynchronized = isSynchronized;
        
        channel.addConnectionEstablishedListener(this::connectionEstablished);
        channel.addConnectionLostListener(this::connectionLost);
    }


    public void connect() throws ChannelException, InterruptedException {
        channel.connect();

        lock.lock();
        try {
            while (!isConnected) {
                isConnectedCondition.await();
            }
        } finally {
            lock.unlock();
        }

        register();
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
            public boolean contains(Object k) {
                return DistributedMap.this.containsKey(k);
            }
        };

        return c;
    }


    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> other) {
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


    private void connectionEstablished(String channelId) {
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
        Register reg = Register.newBuilder()
                .setMapName(mapName)
                .setKeyType(keyType.getName())
                .setValueType(valueType.toString())
                .setIsSynchronized(isSynchronized)
                .setLastMapTransId(mapTransactionId.get())
                .setUuid(uuid)
                .build();

        RegisterResponse response = rpcCall(ClientToServerMessage.newBuilder().setRegister(reg).build(), channel,
                resp -> resp.getMsgCase() == MsgCase.REGISTERRESPONSE).getRegisterResponse();

        if (!response.getRegistrationSuccess()) {
            throw new RuntimeException(response.getFailureReason());
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
                    .setReleaseMapLock(ReleaseMapLock.newBuilder()
                            .setMapName(mapName)
                            .setUuid(uuid).build())
                    .build());

            isMapLocked = false;
        } catch (ChannelException e) {
            throw new DistributedMapException(e);
        }
    }


    private boolean lockKey(K key, V value) {
        if (isSynchronized && !isMapLocked && !keysLocked.contains(key)) {
            try {
                RequestKeyLock.Builder req = RequestKeyLock.newBuilder().setKey(serialize(key))
                        .setMapName(mapName)
                        .setUuid(uuid);

                if (value != null) {
                    req.setValue(serialize(value));
                }

                RequestKeyLockResponse resp = rpcCall(
                        ClientToServerMessage.newBuilder().setRequestKeyLock(req.build()).build(),
                        channel,
                        r -> r.getMsgCase() == MsgCase.REQUESTKEYLOCKRESPONSE)
                                .getRequestKeyLockResponse();

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
            Transaction.Builder builder = Transaction.newBuilder().setKey(serialize(key))
                    .setTransType(transType);

            if (value != null) {
                builder.setValue(serialize(value));
            }

            RequestMapChange req = RequestMapChange.newBuilder().setMapName(mapName)
                    .setUuid(uuid)
                    .setTransaction(builder.build())
                    .build();

            RequestMapChangeResponse response = rpcCall(
                    ClientToServerMessage.newBuilder().setRequestMapChange(req).build(),
                    channel,
                    resp -> resp.getMsgCase() == MsgCase.REQUESTMAPCHANGERESPONSE)
                            .getRequestMapChangeResponse();

            mapTransactionId.set(response.getMapTransactionId());

            if (!response.getUpdateSuccess()) {
                throw new DistributedMapException(response.getFailureReason().toString());
            }

            return response;
        } catch (IOException | InterruptedException e) {
            throw new DistributedMapException(e);
        }
    }


    private static ByteString serialize(Serializable o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream output = new ObjectOutputStream(bos);
        output.writeObject(o);
        output.flush();
        output.close();

        return ByteString.copyFrom(bos.toByteArray());
    }


    private void ensureConnection() {
        if (!isConnected) {
            throw new DistributedMapException("Connection to server lost");
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
}
