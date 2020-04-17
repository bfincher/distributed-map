package com.fincher.distributedmap.server;

import com.fincher.distributedmap.Transaction;
import com.google.protobuf.ByteString;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class MapInfo {

    static final Duration MAX_KEY_LOCK_TIME = Duration
            .ofMillis(Integer.parseInt(System.getProperty("distributed.map.lock.timeout.millis", "1000")));

    final String mapName;
    final String keyType;
    final String valueType;
    private final AtomicInteger mapTransactionId = new AtomicInteger(0);
    protected final RegisteredClients registeredClients = new RegisteredClients();
    protected final Map<ByteString, Lock> keyLockMap = new HashMap<>();
    protected final Transactions transactions = new Transactions();
    protected Lock mapLock = null;

    MapInfo(String mapName, String keyType, String valueType) {
        this.mapName = mapName;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    void registerClient(String uuid, int mapTransId, String channelId, String regKeyType, String regValueType)
            throws RegistrationFailureException {

        if (!regKeyType.equals(keyType)) {
            throw new RegistrationFailureException("A map exists for name " + mapName + " with a key type of " + keyType
                    + " that did not match this registration's key type of " + regKeyType);
        }

        if (!regValueType.equals(valueType)) {
            throw new RegistrationFailureException("A map exists for name " + mapName + " with a value type of "
                    + valueType + " that did not match this registration's value type of " + regValueType);
        }

        RegisteredClient entry = new RegisteredClient(uuid, mapTransId, channelId);
        registeredClients.put(uuid, channelId, entry);
    }

    void deRegisterClient(String uuid, String channelId) {
        registeredClients.remove(uuid, channelId);

        // delete any locks this client owns
        for (Iterator<Lock> it = keyLockMap.values().iterator(); it.hasNext();) {
            Lock entry = it.next();
            if (entry.uuid.equals(uuid)) {
                it.remove();
            }
        }

        if (mapLock != null && mapLock.uuid.equals(uuid)) {
            mapLock = null;
        }
    }

    RegisteredClient getClientByUuid(String uuid) {
        return registeredClients.getByUuid(uuid);
    }

    RegisteredClient getClientByChannelId(String channelId) {
        return registeredClients.getByChannelId(channelId);
    }

    int getMapTransactionId() {
        return mapTransactionId.get();
    }

    Collection<Transaction> getTransactionsLargerThan(int transactionId) {
        return transactions.byMapTransId.tailMap(transactionId, false).values().stream().map(t -> t.transaction)
                .collect(Collectors.toUnmodifiableList());
    }

    boolean canAcquireKeyLock(ByteString key, String uuid) {
        if (!canAcquireMapLock(uuid)) {
            return false;
        }

        Lock lock = keyLockMap.get(key);
        if (lock != null) {
            // only consider the age if this client does not already own the lock
            if (!uuid.equals(lock.uuid)) {
                return isLockOld(lock);
            }
        }

        return true;
    }

    boolean acquireKeyLock(ByteString key, String uuid) {
        if (!canAcquireKeyLock(key, uuid)) {
            return false;
        }

        Lock lock = new Lock(uuid);
        keyLockMap.put(key, lock);
        return true;
    }

    boolean releaseKeyLock(ByteString key, String uuid) {
        Lock lock = keyLockMap.get(key);
        if (lock != null && lock.uuid.equals(uuid)) {
            keyLockMap.remove(key);
            return true;
        }
        return false;
    }

    boolean canAcquireMapLock(String uuid) {
        if (mapLock != null) {
            if (isLockOld(mapLock)) {
                mapLock = null;
            } else if (!mapLock.uuid.equals(uuid)) {
                return false;
            }
        }

        return true;
    }

    boolean acquireMapLock(String uuid) {
        if (canAcquireMapLock(uuid)) {
            mapLock = new Lock(uuid);
            return true;
        }
        return false;
    }

    boolean releaseMapLock(String uuid) {
        if (mapLock != null && mapLock.uuid.equals(uuid)) {
            mapLock = null;
            return true;
        }

        return false;
    }

    static boolean isLockOld(Lock lock) {
        Duration age = Duration.ofMillis(System.currentTimeMillis() - lock.timeLockAcquired);
        return age.compareTo(MAX_KEY_LOCK_TIME) > 0;
    }

    void addTransaction(Transaction transaction) {
        ByteString transKey = transaction.getKey();

        TransactionMapEntry mapEntry = new TransactionMapEntry(
                transaction,
                mapTransactionId.incrementAndGet());
        transactions.put(transKey, mapTransactionId.get(), mapEntry);
    }

    boolean hasMapLock(String uuid) {
        if (mapLock != null) {
            if (isLockOld(mapLock)) {
                mapLock = null;
                return false;
            }

            return mapLock.uuid.equals(uuid);
        }

        return false;
    }

    boolean hasKeyLock(ByteString key, String uuid) {
        Lock lock = keyLockMap.get(key);
        if (lock != null) {
            if (isLockOld(lock)) {
                keyLockMap.remove(key);
                return false;
            }

            return lock.uuid.equals(uuid);
        }

        return false;
    }

    void updateMapLock() {
        mapLock.timeLockAcquired = System.currentTimeMillis();
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

    static class Lock {
        final String uuid;
        long timeLockAcquired;

        Lock(String uuid) {
            this.uuid = uuid;
            timeLockAcquired = System.currentTimeMillis();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Lock other = (Lock) obj;
            if (timeLockAcquired != other.timeLockAcquired)
                return false;
            if (uuid == null) {
                if (other.uuid != null)
                    return false;
            } else if (!uuid.equals(other.uuid))
                return false;
            return true;
        }
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
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TransactionMapEntry other = (TransactionMapEntry) obj;
            if (mapTransactionId != other.mapTransactionId)
                return false;
            if (transaction == null) {
                if (other.transaction != null)
                    return false;
            } else if (!transaction.equals(other.transaction))
                return false;
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
