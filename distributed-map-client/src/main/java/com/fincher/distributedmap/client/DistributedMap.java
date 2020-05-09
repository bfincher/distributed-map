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
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DistributedMap<Key extends Serializable, Value extends Serializable> extends AbstractMap<Key, Value> {

    private final Class<Key> keyType;
    private final Class<Value> valueType;
    private final TransformingChannel channel;
    private final String mapName;
    private final boolean isSynchronized;
    private final AtomicInteger mapTransactionId = new AtomicInteger(0);
    private final Lock lock = new ReentrantLock();
    private final Condition isConnectedCondition = lock.newCondition();
    private boolean isConnected = false;
    private final String uuid = UUID.randomUUID().toString();
    private final Map<Key, Value> map = new HashMap<>();
    private boolean isMapLocked = false;
    private Set<Key> keysLocked = new HashSet<>();

    public DistributedMap(String mapName,
            boolean isSynchronized,
            Class<Key> keyType,
            Class<Value> valueType,
            String serverHost,
            int serverPort)
            throws InterruptedException, ChannelException {
        this.mapName = mapName;
        this.keyType = keyType;
        this.valueType = valueType;

        channel = new TransformingChannel("channel", TcpClientChannel.createChannel(
                "channel",
                new SimpleStreamIo(false),
                new InetSocketAddress(0),
                new InetSocketAddress(serverHost, serverPort)));

        channel.addConnectionEstablishedListener(this::connectionEstablished);
        channel.addConnectionLostListener(this::connectionLost);

        this.isSynchronized = isSynchronized;
    }


    protected DistributedMap(String mapName,
            boolean isSynchronized,
            Class<Key> keyType,
            Class<Value> valueType,
            TcpChannelIfc channel) {
        this.mapName = mapName;
        this.keyType = keyType;
        this.valueType = valueType;
        this.channel = new TransformingChannel("channel", channel);
        this.isSynchronized = isSynchronized;
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

        if (isSynchronized) {
            lockMap();
        }

        try {
            for (Iterator<Key> iterator = map.keySet().iterator(); iterator.hasNext();) {
                Key key = iterator.next();
                requestTransaction(key, map.get(key), TransactionType.DELETE);
                iterator.remove();
            }
        } finally {
            if (isSynchronized) {
                unlockMap();
            }
        }
    }


    @Override
    public synchronized Value put(Key key, Value value) {
        ensureConnection();
        Value oldValue = map.get(key);
        if (isSynchronized) {
            lockKey(key, oldValue);
        }

        requestTransaction(key, value, TransactionType.UPDATE);
        keysLocked.remove(key);
        return map.put(key, value);
    }


    @Override
    public Set<Map.Entry<Key, Value>> entrySet() {
        return new EntrySet(map.entrySet());
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

        RegisterResponse response = Rpc.call(ClientToServerMessage.newBuilder().setRegister(reg).build(), channel,
                resp -> resp.getMsgCase() == MsgCase.REGISTERRESPONSE).getRegisterResponse();

        if (!response.getRegistrationSuccess()) {
            throw new RuntimeException(response.getFailureReason());
        }
    }


    private void lockMap() {
        RequestMapLock req = RequestMapLock.newBuilder().setMapName(mapName).setUuid(uuid)
                .setLatestTransId(mapTransactionId.get()).build();

        try {
            RequestMapLockResponse resp = Rpc.call(ClientToServerMessage.newBuilder().setRequestMapLock(req).build(),
                    channel, r -> r.getMsgCase() == MsgCase.REQUESTMAPLOCKRESPONSE).getRequestMapLockResponse();

            if (!resp.getLockAcquired()) {
                throw new DistributedMapException("Unable to obtain map lock");
            }

            isMapLocked = true;
        } catch (ChannelException | InterruptedException e) {
            throw new DistributedMapException(e);
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


    private void lockKey(Key key, Value value) {
        try {
            RequestKeyLock.Builder req = RequestKeyLock.newBuilder().setKey(serialize(key))
                    .setMapName(mapName)
                    .setUuid(uuid);

            if (value != null) {
                req.setValue(serialize(value));
            }

            RequestKeyLockResponse resp = Rpc
                    .call(ClientToServerMessage.newBuilder().setRequestKeyLock(req.build()).build(),
                            channel,
                            r -> r.getMsgCase() == MsgCase.REQUESTKEYLOCKRESPONSE)
                    .getRequestKeyLockResponse();

            if (!resp.getLockAcquired()) {
                throw new DistributedMapException("Unable to get lock");
            }

            keysLocked.add(key);

        } catch (IOException | InterruptedException e) {
            throw new DistributedMapException(e);
        }
    }


    private RequestMapChangeResponse requestTransaction(Key key, Value value, TransactionType transType) {

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

            RequestMapChangeResponse response = Rpc
                    .call(ClientToServerMessage.newBuilder().setRequestMapChange(req).build(),
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

    private static class TransformingChannel
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
    
    
    private class EntrySet extends AbstractSet<Map.Entry<Key, Value>> {
        private final Set<Map.Entry<Key, Value>> parent;
        
        EntrySet(Set<Map.Entry<Key, Value>> parent) {
            this.parent = parent;
        }
        
        
        @Override
        public int size() {
            return parent.size();
        }
        
        
        @Override
        public Iterator<Map.Entry<Key, Value>> iterator() {
            return new KeySetIterator(parent.iterator());
        }
    }
    
    
    private class KeySetIterator implements Iterator<Map.Entry<Key, Value>> {
        private final Iterator<Map.Entry<Key, Value>> iterator;
        private Map.Entry<Key, Value> prev = null;
        
        KeySetIterator(Iterator<Map.Entry<Key, Value>> iterator) {
            this.iterator = iterator;
        }
        
        
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }
        
        
        @Override
        public Map.Entry<Key, Value> next() {
            prev = iterator.next();
            return prev;
        }
        
        
        @Override
        public void remove() {
            Preconditions.checkState(prev != null);
            
            boolean needToUnlockKey = false;
            if (isSynchronized) {
                if (!isMapLocked && !keysLocked.contains(prev.getKey())) {
                    lockKey(prev.getKey(), prev.getValue());
                    needToUnlockKey = true;
                }
            }
            
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
