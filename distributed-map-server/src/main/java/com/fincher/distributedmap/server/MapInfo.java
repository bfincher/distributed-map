package com.fincher.distributedmap.server;

import com.fincher.distributedmap.messages.Transaction;

import com.google.protobuf.ByteString;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class MapInfo {

    final String mapName;
    final String keyType;
    final String valueType;
    final boolean isSynchronized;
    private final AtomicInteger mapTransactionId = new AtomicInteger(0);
    protected final RegisteredClients registeredClients = new RegisteredClients();
    protected final Map<ByteString, Lock> keyLockMap;
    protected final Transactions transactions = new Transactions();
    protected final Lock mapLock;

    MapInfo(String mapName, String keyType, String valueType, boolean isSynchronized) {
        this.mapName = mapName;
        this.keyType = keyType;
        this.valueType = valueType;
        this.isSynchronized = isSynchronized;

        if (isSynchronized) {
            keyLockMap = new HashMap<>();
            mapLock = new Lock();
        } else {
            keyLockMap = null;
            mapLock = null;
        }
    }


    void stop() {
        registeredClients.byChannelId.clear();
        registeredClients.byUuid.clear();
        transactions.byKey.clear();
        transactions.byMapTransId.clear();

        if (keyLockMap != null) {
            keyLockMap.clear();
        }
    }


    void registerClient(String uuid, int mapTransId,
            String channelId,
            String regKeyType,
            String regValueType,
            boolean regIsSynchronized)
            throws RegistrationFailureException {

        if (!regKeyType.equals(keyType)) {
            throw new RegistrationFailureException(
                    "A map exists for name " + mapName + " with a key type of " + keyType
                            + " that did not match this registration's key type of " + regKeyType);
        }

        if (!regValueType.equals(valueType)) {
            throw new RegistrationFailureException(
                    "A map exists for name " + mapName + " with a value type of " + valueType
                            + " that did not match this registration's value type of " + regValueType);
        }

        if (regIsSynchronized != isSynchronized) {
            throw new RegistrationFailureException(
                    "A map exists for name " + mapName + " with a synchronization setting of " + isSynchronized
                            + " that did not match this registration's synchronization setting of "
                            + regIsSynchronized);
        }

        RegisteredClient entry = new RegisteredClient(uuid, mapTransId, channelId);
        registeredClients.put(uuid, channelId, entry);
    }


    void deRegisterClient(String uuid, String channelId) {
        registeredClients.remove(uuid, channelId);

        if (isSynchronized) {
            // delete any locks this client owns
            for (Iterator<Lock> it = keyLockMap.values().iterator(); it.hasNext();) {
                Lock entry = it.next();
                if (entry.isLockedBy(uuid)) {
                    entry.unlock(uuid);
                }
            }

            if (mapLock.isLockedBy(uuid)) {
                mapLock.unlock(uuid);
            }
        }
    }


    int getNumRegisteredClients() {
        return registeredClients.byUuid.size();
    }


    RegisteredClient getClientByUuid(String uuid) {
        return registeredClients.getByUuid(uuid);
    }


    RegisteredClient getClientByChannelId(String channelId) {
        return registeredClients.getByChannelId(channelId);
    }


    Collection<RegisteredClient> getAllRegisteredClients() {
        return registeredClients.byChannelId.values();
    }


    int getMapTransactionId() {
        return mapTransactionId.get();
    }


    List<TransactionMapEntry> getTransactionsLargerThan(int transactionId) {
    	return transactions.byMapTransId.tailMap(transactionId).values().stream().collect(Collectors.toUnmodifiableList());
    }


    boolean canAcquireKeyLock(ByteString key, String uuid) {
        if (!canAcquireMapLock(uuid)) {
            return false;
        }

        Lock lock = keyLockMap.computeIfAbsent(key, k -> new Lock());
        if (lock.isLocked()) {
            return lock.isLockedBy(uuid);
        }

        return true;
    }


    boolean acquireKeyLock(ByteString key, String uuid) throws InterruptedException {
        if (!canAcquireKeyLock(key, uuid)) {
            return false;
        }

        Lock lock = keyLockMap.get(key);
        lock.lock(uuid);
        return true;
    }


    boolean releaseKeyLock(ByteString key, String uuid) {
        Lock lock = keyLockMap.computeIfAbsent(key, k -> new Lock());
        return lock.unlock(uuid);
    }


    boolean canAcquireMapLock(String uuid) {
        if (mapLock.isLocked()) {
            return mapLock.isLockedBy(uuid);
        }

        for (Lock lock : keyLockMap.values()) {
            if (lock.isLocked() && !lock.isLockedBy(uuid)) {
                return false;
            }
        }

        return true;
    }


    boolean acquireMapLock(String uuid) throws InterruptedException {
        if (canAcquireMapLock(uuid)) {
            mapLock.lock(uuid);
            return true;
        }
        return false;
    }


    boolean isMapLocked() {
        return mapLock.isLocked();
    }


    boolean releaseMapLock(String uuid) {
        return mapLock.unlock(uuid);
    }


    void addTransaction(Transaction transaction) {
        ByteString transKey = transaction.getKey();

        TransactionMapEntry mapEntry = new TransactionMapEntry(transaction, mapTransactionId.incrementAndGet());
        transactions.put(transKey, mapTransactionId.get(), mapEntry);
    }


    boolean hasMapLock(String uuid) {
        return mapLock.isLockedBy(uuid);
    }


    boolean hasKeyLock(ByteString key, String uuid) {
        Lock lock = keyLockMap.computeIfAbsent(key, k -> new Lock());
        return lock.isLockedBy(uuid);
    }


    void updateMapLock(String uuid) {
        mapLock.updateTime(uuid);
    }


    String getMapName() {
        return mapName;
    }


    String getKeyType() {
        return keyType;
    }


    String getValueType() {
        return valueType;
    }

    static class RegisteredClient {
        final String uuid;
        int mapTransId;
        final String channelId;

        RegisteredClient(String uuid, Integer mapTransId, String channelId) {
            this.uuid = uuid;
            this.mapTransId = mapTransId;
            this.channelId = channelId;
        }


        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            RegisteredClient other = (RegisteredClient) obj;
            return Objects.equals(channelId, other.channelId) && mapTransId == other.mapTransId
                    && Objects.equals(uuid, other.uuid);
        }

    }

    Transaction getTransactionWithKey(ByteString key) {
        TransactionMapEntry entry = transactions.getByKey(key);

        return entry == null ? null : entry.transaction;
    }

    static class TransactionMapEntry {
        final Transaction transaction;
        final int mapTransactionId;

        TransactionMapEntry(Transaction transaction, int mapTransactionId) {
            this.transaction = transaction;
            this.mapTransactionId = mapTransactionId;
        }


        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TransactionMapEntry other = (TransactionMapEntry) obj;
            if (mapTransactionId != other.mapTransactionId) {
                return false;
            }
            if (transaction == null) {
                if (other.transaction != null) {
                    return false;
                }
            } else if (!transaction.equals(other.transaction)) {
                return false;
            }
            return true;
        }
    }

    static class Transactions {

        protected final TreeMap<Integer, TransactionMapEntry> byMapTransId = new TreeMap<>();
        protected final HashMap<ByteString, TransactionMapEntry> byKey = new HashMap<>();

        TransactionMapEntry getByMapTransId(Integer mapTransId) {
            return byMapTransId.get(mapTransId);
        }


        TransactionMapEntry getByKey(ByteString key) {
            return byKey.get(key);
        }


        void put(ByteString key, Integer mapTransactionId, TransactionMapEntry transaction) {
            byMapTransId.put(mapTransactionId, transaction);
            TransactionMapEntry prevEntry = byKey.put(key, transaction);

            // we only want 1 transaction per key
            if (prevEntry != null) {
                byMapTransId.remove(prevEntry.mapTransactionId);
            }
        }
    }

    static class RegisteredClients {

        final Map<String, RegisteredClient> byUuid = new ConcurrentHashMap<>();
        final Map<String, RegisteredClient> byChannelId = new ConcurrentHashMap<>();

        RegisteredClient getByUuid(String uuid) {
            return byUuid.get(uuid);
        }


        RegisteredClient getByChannelId(String channelId) {
            return byChannelId.get(channelId);
        }


        void put(String uuid, String channelId, RegisteredClient client) {
            byUuid.put(uuid, client);
            byChannelId.put(channelId, client);
        }


        void remove(String uuid, String channelId) {
            byUuid.remove(uuid);
            byChannelId.remove(channelId);
        }
    }
}
