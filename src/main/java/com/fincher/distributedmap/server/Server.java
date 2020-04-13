package com.fincher.distributedmap.server;

import com.fincher.distributedmap.ClientToServerMessage;
import com.fincher.distributedmap.ClientTransactionUpdate;
import com.fincher.distributedmap.DeRegister;
import com.fincher.distributedmap.Register;
import com.fincher.distributedmap.RegisterResponse;
import com.fincher.distributedmap.ReleaseKeyLock;
import com.fincher.distributedmap.ReleaseMapLock;
import com.fincher.distributedmap.RequestKeyLock;
import com.fincher.distributedmap.RequestKeyLockResponse;
import com.fincher.distributedmap.RequestMapChange;
import com.fincher.distributedmap.RequestMapChangeResponse;
import com.fincher.distributedmap.RequestMapChangeResponse.FailureReason;
import com.fincher.distributedmap.RequestMapLock;
import com.fincher.distributedmap.RequestMapLockResponse;
import com.fincher.distributedmap.ServerToClientMessage;
import com.fincher.distributedmap.Transaction;
import com.fincher.distributedmap.Utilities;
import com.fincher.distributedmap.server.MapInfo.RegisteredClient;
import com.fincher.iochannel.ChannelException;
import com.fincher.iochannel.MessageBuffer;
import com.fincher.iochannel.tcp.SimpleStreamIo;
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

public class Server implements Closeable {

    private static final Logger LOG = LogManager.getLogger();

    private final TcpServerChannel channel;

    private final Map<String, MapInfo> mapInfoMap = Collections.synchronizedMap(new HashMap<>());

    public Server(int serverPort) {
        channel = TcpServerChannel.createChannel("DistributedMapServer", this::handleMessage, new SimpleStreamIo(false),
                new InetSocketAddress(serverPort));
    }

    // for unit testing
    Server(TcpServerChannel channel) {
        this.channel = channel;
    }

    public void start() throws IOException, InterruptedException {
        channel.connect();
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }

    private void handleMessage(MessageBuffer mb) {
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
        } catch (InvalidProtocolBufferException | ChannelException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void handleRegister(Register reg, String channelId) throws ChannelException {
        String mapName = reg.getMapName();
        String keyType = reg.getKeyType();
        String valueType = reg.getValueType();
        synchronized (mapInfoMap) {
            MapInfo info = mapInfoMap.computeIfAbsent(mapName, key -> new MapInfo(mapName, keyType, valueType));
            synchronized (info) {
                RegisterResponse.Builder builder = RegisterResponse.newBuilder();
                builder.setMapName(mapName);

                try {
                    info.registerClient(reg.getUuid(), 0, channelId, keyType, valueType);
                    builder.setRegistrationSuccess(true);
                    updateClient(mapName, channelId);
                } catch (RegistrationFailureException e) {
                    builder.setRegistrationSuccess(false);
                    builder.setFailureReason(e.getMessage());
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

    private void handleRequestKeyLock(RequestKeyLock msg, String channelId) throws ChannelException {
        RequestKeyLockResponse.Builder response = RequestKeyLockResponse.newBuilder();
        ByteString key = msg.getKey();
        response.setMapName(msg.getMapName());
        response.setKey(msg.getKey());

        MapInfo info = mapInfoMap.get(msg.getMapName());
        if (info == null) {
            response.setLockAcquired(false);
        } else {
            synchronized (info) {
                if (msg.getLatestTransId() != info.getLatestKeyTransactionId(key)) {
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

    private void handleRequestMapLock(RequestMapLock req, String channelId) throws ChannelException {
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

        RequestMapChangeResponse.Builder response = RequestMapChangeResponse.newBuilder();
        response.setMapName(mapName);
        response.setTransaction(transaction);

        MapInfo info = mapInfoMap.get(mapName);
        if (info == null) {
            response.setUpdateSuccess(false);
            response.setFailureReason(FailureReason.MAP_DOES_NOT_EXIST);
        } else {
            synchronized (info) {
                if (info.getLatestKeyTransactionId(key) + 1 != transaction.getKeyTransactionId()) {
                    response.setUpdateSuccess(false);
                    response.setFailureReason(FailureReason.MAP_OUT_OF_DATE);
                    updateClient(mapName, uuid);
                } else if (info.hasMapLock(uuid)) {
                    info.addTransaction(transaction);
                    info.updateMapLock();
                    response.setUpdateSuccess(true);
                } else if (info.hasKeyLock(transaction.getKey(), uuid)) {
                    info.addTransaction(transaction);
                    info.updateMapLock();
                    response.setUpdateSuccess(true);
                } else {
                    response.setUpdateSuccess(false);
                    response.setFailureReason(FailureReason.KEY_NOT_LOCKED);
                }
            }
        }

        ServerToClientMessage wrapper = ServerToClientMessage.newBuilder().setRequestMapChangeResponse(response.build())
                .build();
        Utilities.sendMessage(channel, channelId, wrapper);
    }

    private final void updateClient(String mapName, String channelId) throws ChannelException {
        MapInfo info = mapInfoMap.get(mapName);
        synchronized (info) {
            RegisteredClient client = info.getClientByChannelId(channelId);
            if (client.mapTransId != info.getMapTransactionId()) {
                ClientTransactionUpdate.Builder builder = ClientTransactionUpdate.newBuilder();
                builder.setMapName(mapName);
                builder.setMapTransactionId(info.getMapTransactionId());
                builder.getTransactionsList().addAll(info.getTransactionsLargerThen(client.mapTransId));

                ServerToClientMessage msg = ServerToClientMessage.newBuilder()
                        .setClientTransactionUpdate(builder.build()).build();
                Utilities.sendMessage(channel, channelId, msg);
            }
        }
    }

}
