package com.fincher.distributedmap.client;

import com.fincher.distributedmap.Utilities;
import com.fincher.distributedmap.messages.ClientToServerMessage;
import com.fincher.distributedmap.messages.Register;

import com.fincher.iochannel.ChannelException;
import com.fincher.iochannel.tcp.SimpleStreamIo;
import com.fincher.iochannel.tcp.TcpChannelIfc;
import com.fincher.iochannel.tcp.TcpClientChannel;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.HashMap;
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
    private final TcpChannelIfc channel;
    private final String mapName;
    private final boolean isSynchronized;
    private final AtomicInteger mapTransactionId = new AtomicInteger(0);
    private final Lock lock = new ReentrantLock();
    private final Condition isConnectedCondition = lock.newCondition();
    private boolean isConnected = false;
    private final String uuid = UUID.randomUUID().toString();
    private final Map<Key, Value> map = new HashMap<>();

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

        channel = TcpClientChannel.createChannel("channel", new SimpleStreamIo(false), new InetSocketAddress(0),
                new InetSocketAddress(serverHost, serverPort));

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
        this.channel = channel;
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
    public void clear() {

        // TODO implement
        if (isSynchronized) {
//            RequestMapLock.newBuilder().setMapName(mapName).set;
        }
    }


    @Override
    public Value put(Key key, Value value) {
        return null;
        // TODO implement
    }
    
    
    @Override
    public Set<Map.Entry<Key, Value>> entrySet() {
        return map.entrySet();
    }


    private void connectionEstablished(String channelId) {
        // TODO implement
    }


    private void connectionLost(String channelId) {
        // TODO implement
    }


    private void register() throws ChannelException {
        Register reg = Register.newBuilder()
                .setMapName(mapName)
                .setKeyType(keyType.getName())
                .setValueType(valueType.toString())
                .setIsSynchronized(isSynchronized)
                .setLastMapTransId(mapTransactionId.get())
                .setUuid(uuid)
                .build();

        Utilities.sendMessage(channel, ClientToServerMessage.newBuilder().setRegister(reg).build());
    }
}
