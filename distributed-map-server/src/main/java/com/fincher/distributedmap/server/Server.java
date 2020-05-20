package com.fincher.distributedmap.server;

import com.fincher.distributedmap.Utilities;
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
import com.fincher.distributedmap.server.MapInfo.RegisteredClient;
import com.fincher.distributedmap.server.MapInfo.TransactionMapEntry;
import com.fincher.iochannel.ChannelException;
import com.fincher.iochannel.MessageBuffer;
import com.fincher.iochannel.tcp.SimpleStreamIo;
import com.fincher.iochannel.tcp.TcpChannelIfc;
import com.fincher.iochannel.tcp.TcpServerChannel;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The server portion of the distributed map
 * 
 * @author Brian Fincher
 *
 */
public class Server implements Closeable {

    private static final Logger LOG = LogManager.getLogger();

    protected final TcpChannelIfc channel;

    protected final Map<String, MapInfo> mapInfoMap = Collections.synchronizedMap(new HashMap<>());

    /**
     * Constructs a new Server
     * 
     * @param serverPort The port on which this server will accept client
     *                   connections
     */
    public Server(int serverPort) {
        channel = TcpServerChannel.createChannel("DistributedMapServer", this::handleMessage,
                new SimpleStreamIo(false),
                new InetSocketAddress(serverPort));

        channel.addConnectionLostListener(this::connectionLost);
    }


    // for unit testing
    Server(TcpChannelIfc channel) {
        channel.addConnectionLostListener(this::connectionLost);
        this.channel = channel;
    }


    /**
     * Starts this server
     * 
     * @throws IOException          If an exception occurs while starting the TCP
     *                              server channel
     * @throws InterruptedException If an exception occurs while starting the TCP
     *                              server channel
     */
    public void start() throws IOException, InterruptedException {
        channel.connect();
    }


    @Override
    public void close() throws IOException {
        channel.close();
    }


    protected void handleMessage(MessageBuffer mb) {
        try {
            ClientToServerMessage wrapper = ClientToServerMessage.parseFrom(mb.getBytes());
            switch (wrapper.getMsgCase()) {
            case REGISTER:
                handleRegister(wrapper.getRegister(), mb.getReceivedFromChannelId());
                break;

            case DEREGISTER:
                handleDeRegister(wrapper.getDeRegister(), mb.getReceivedFromChannelId());
                break;

            case REQUESTKEYLOCK:
                handleRequestKeyLock(wrapper.getRequestKeyLock(), mb.getReceivedFromChannelId());
                break;

            case RELEASEKEYLOCK:
                handleReleaseKeyLock(wrapper.getReleaseKeyLock());
                break;

            case REQUESTMAPLOCK:
                handleRequestMapLock(wrapper.getRequestMapLock(), mb.getReceivedFromChannelId());
                break;

            case RELEASEMAPLOCK:
                handleReleaseMapLock(wrapper.getReleaseMapLock());
                break;

            case REQUESTMAPCHANGE:
                handleRequestMapChange(wrapper.getRequestMapChange(), mb.getReceivedFromChannelId());
                break;

            default:
                throw new IllegalArgumentException("Unexpected message: " + wrapper.getMsgCase());
            }
        } catch (InvalidProtocolBufferException | ChannelException | InterruptedException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }


    private void handleRegister(Register reg, String channelId) throws ChannelException {
        String mapName = reg.getMapName();
        String keyType = reg.getKeyType();
        String valueType = reg.getValueType();
        boolean isSynchronized = reg.getIsSynchronized();

        synchronized (mapInfoMap) {
            MapInfo info = mapInfoMap.computeIfAbsent(mapName,
                    key -> new MapInfo(mapName, keyType, valueType, isSynchronized));

            synchronized (info) {
                RegisterResponse.Builder builder = RegisterResponse.newBuilder();
                builder.setMapName(mapName);

                try {
                    info.registerClient(reg.getUuid(), 0, channelId, keyType, valueType, isSynchronized);
                    builder.setRegistrationSuccess(true);

                    Utilities.sendMessage(channel, channelId,
                            ServerToClientMessage.newBuilder().setRegisterResponse(builder.build()).build());

                    updateClient(mapName, channelId);
                } catch (RegistrationFailureException e) {
                    if (info.getNumRegisteredClients() == 0) {
                        // since there are no clientsfor this map, delete it and try again
                        LOG.info("Deleting map {} with no registered clients", mapName);
                        info.stop();
                        mapInfoMap.remove(mapName);
                        handleRegister(reg, channelId);
                        return;
                    }

                    builder.setRegistrationSuccess(false);
                    builder.setFailureReason(e.getMessage());
                    Utilities.sendMessage(channel, channelId,
                            ServerToClientMessage.newBuilder().setRegisterResponse(builder.build()).build());
                    LOG.warn(e.getMessage(), e);
                }

            }
        }
    }


    private void handleDeRegister(DeRegister msg, String channelId) {
        MapInfo info = mapInfoMap.get(msg.getMapName());
        if (info != null) {
            synchronized (info) {
                info.deRegisterClient(msg.getUuid(), channelId);
            }
        }
    }


    private void handleRequestKeyLock(RequestKeyLock msg, String channelId)
            throws ChannelException, InterruptedException {
        RequestKeyLockResponse.Builder response = RequestKeyLockResponse.newBuilder();
        ByteString key = msg.getKey();
        response.setMapName(msg.getMapName());
        response.setKey(msg.getKey());

        MapInfo info = mapInfoMap.get(msg.getMapName());
        if (info == null) {
            response.setLockAcquired(false);
        } else {
            synchronized (info) {
                Transaction oldValue = info.getTransactionWithKey(key);
                if (oldValue != null
                        && oldValue.getValue() != ByteString.EMPTY
                        && !oldValue.getValue().equals(msg.getValue())) {

                    response.setLockAcquired(false);
                } else {
                    response.setLockAcquired(info.acquireKeyLock(msg.getKey(), msg.getUuid()));
                }
            }
        }

        ServerToClientMessage wrapper = ServerToClientMessage.newBuilder().setRequestKeyLockResponse(response.build())
                .build();
        Utilities.sendMessage(channel, channelId, wrapper);
    }


    private void handleReleaseKeyLock(ReleaseKeyLock msg) {
        MapInfo info = mapInfoMap.get(msg.getMapName());
        synchronized (info) {
            info.releaseKeyLock(msg.getKey(), msg.getUuid());
        }
    }


    private void handleRequestMapLock(RequestMapLock req, String channelId)
            throws ChannelException, InterruptedException {
        RequestMapLockResponse.Builder response = RequestMapLockResponse.newBuilder();
        response.setMapName(req.getMapName());

        MapInfo info = mapInfoMap.get(req.getMapName());
        if (info == null) {
            response.setLockAcquired(false);
        } else {
            synchronized (info) {
                response.setLockAcquired(info.acquireMapLock(req.getUuid()));
            }
        }

        ServerToClientMessage wrapper = ServerToClientMessage.newBuilder().setRequestMapLockResponse(response.build())
                .build();
        Utilities.sendMessage(channel, channelId, wrapper);
    }


    private void handleReleaseMapLock(ReleaseMapLock msg) {
        MapInfo info = mapInfoMap.get(msg.getMapName());
        if (info != null) {
            synchronized (info) {
                info.releaseMapLock(msg.getUuid());
            }
        }
    }


    private void handleRequestMapChange(RequestMapChange req, String channelId) throws ChannelException {
        String mapName = req.getMapName();
        Transaction transaction = req.getTransaction();
        ByteString key = transaction.getKey();
        String uuid = req.getUuid();

        LOG.debug("handling a request for map change for map {} from uuid {}", mapName, uuid);

        RequestMapChangeResponse.Builder response = RequestMapChangeResponse.newBuilder();
        response.setMapName(mapName);
        response.setTransaction(transaction);

        MapInfo info = mapInfoMap.get(mapName);
        if (info == null) {
            response.setUpdateSuccess(false);
            response.setFailureReason(FailureReason.MAP_DOES_NOT_EXIST);
        } else {
            synchronized (info) {
                RegisteredClient rc = info.getClientByUuid(uuid);
                if (rc == null) {
                    response.setUpdateSuccess(false);
                    response.setFailureReason(FailureReason.CLIENT_NOT_REGISTERED);
                } else if (!info.isSynchronized) {
                    response.setUpdateSuccess(true);
                } else if (info.hasMapLock(uuid)) {
                    info.updateMapLock(uuid);
                    response.setUpdateSuccess(true);
                } else if (info.hasKeyLock(key, uuid)) {
                    info.releaseKeyLock(key, uuid);
                    response.setUpdateSuccess(true);
                } else {
                    response.setUpdateSuccess(false);
                    response.setFailureReason(FailureReason.KEY_NOT_LOCKED);
                }

                if (response.getUpdateSuccess()) {
                    info.addTransaction(transaction);
                    rc.mapTransId = info.getMapTransactionId();               
                    updateClientsExcept(mapName, rc.uuid);
                }
            }
        }

        ServerToClientMessage wrapper = ServerToClientMessage.newBuilder().setRequestMapChangeResponse(response.build())
                .build();
        Utilities.sendMessage(channel, channelId, wrapper);
    }


    private void updateClientsExcept(String mapName, String exceptUuid) throws ChannelException {
        MapInfo info = mapInfoMap.get(mapName);
        synchronized (info) {
            for (RegisteredClient client : info.getAllRegisteredClients()) {
                if (!client.uuid.equals(exceptUuid)) {
                    updateClient(mapName, client.channelId);
                }
            }
        }
    }


    private final void updateClient(String mapName, String channelId) throws ChannelException {
        MapInfo info = mapInfoMap.get(mapName);
        synchronized (info) {
            RegisteredClient client = info.getClientByChannelId(channelId);
            updateClient(info, client);
        }
    }


    private final void updateClient(MapInfo mapInfo, RegisteredClient client) throws ChannelException {
        synchronized (mapInfo) {
            if (client.mapTransId != mapInfo.getMapTransactionId()) {
            	for (TransactionMapEntry entry: mapInfo.getTransactionsLargerThan(client.mapTransId)) {
            		ClientTransactionUpdate.Builder builder = ClientTransactionUpdate.newBuilder();
                    builder.setMapName(mapInfo.mapName);
                    builder.setMapTransactionId(entry.mapTransactionId);
                    builder.setTransaction(entry.transaction);
                    client.mapTransId = entry.mapTransactionId;

                    ServerToClientMessage msg = ServerToClientMessage.newBuilder()
                            .setClientTransactionUpdate(builder.build()).build();
                    Utilities.sendMessage(channel, client.channelId, msg);	
            	}
            }
        }
    }


    protected void connectionLost(String channelId) {
        synchronized (mapInfoMap) {
            mapInfoMap.values().forEach(info -> {
                synchronized (info) {
                    RegisteredClient client = info.registeredClients.getByChannelId(channelId);
                    LOG.info("Connection lost on channel ID {}.  De-registering client for map {}",
                            channelId, info.mapName);

                    info.deRegisterClient(client.uuid, channelId);
                }
            });
        }
    }
}
