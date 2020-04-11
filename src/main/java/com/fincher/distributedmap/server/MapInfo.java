package com.fincher.distributedmap.server;

import com.fincher.distributedmap.Transaction;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class MapInfo {

    static final Duration MAX_KEY_LOCK_TIME = Duration.ofSeconds(1);

    final String mapName;
    final String keyType;
    final String valueType;
    private final AtomicInteger transactionId = new AtomicInteger(0);
    private final Map<String, RegisteredClient> registrationClientsByUuid = new ConcurrentHashMap<>();
    private final Map<String, RegisteredClient> registrationClientsByChannelId = new ConcurrentHashMap<>();
    private final TreeMap<Integer, Transaction> transactionMap = new TreeMap<>();
    private final Map<ByteString, LockEntry> keyLockMap = new HashMap<>();
    private LockEntry mapLock = null;

    MapInfo(String mapName, String keyType, String valueType) {
        this.mapName = mapName;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    void registerClient(String uuid, Integer mapTransId, String channelId, String regKeyType, String regValueType)
            throws RegistrationFailureException {

        if (!keyType.equals(this.keyType)) {
            throw new RegistrationFailureException("A map exists for name " + mapName + " with a key type of " + keyType
                    + " that did not match this registration's " + "key type of " + regKeyType);
        }

        if (!valueType.equals(this.valueType)) {
            throw new RegistrationFailureException("A map exists for name " + mapName + " with a value type of "
                    + valueType + " that did not match this registration's value type of " + regValueType);
        }

        RegisteredClient entry = new RegisteredClient(uuid, mapTransId, channelId);
        registrationClientsByUuid.put(uuid, entry);
        registrationClientsByChannelId.put(channelId, entry);
    }

    void deRegisterClient(String uuid, String channelId) {
        registrationClientsByUuid.remove(uuid);
        registrationClientsByChannelId.remove(channelId);

        // delete any locks this client owns
        for (Iterator<LockEntry> it = keyLockMap.values().iterator(); it.hasNext();) {
            LockEntry entry = it.next();
            if (entry.uuid.equals(uuid)) {
                it.remove();
            }
        }
    }

    RegisteredClient getClientByUuid(String uuid) {
        return registrationClientsByUuid.get(uuid);
    }

    RegisteredClient getClientByChannelId(String channelId) {
        return registrationClientsByChannelId.get(channelId);
    }

    int getTransactionId() {
        return transactionId.get();
    }

    Collection<Transaction> getTransactionsLargerThen(int transactionId) {
        return transactionMap.tailMap(transactionId, false).values();
    }
    
    boolean canAcquireKeyLock(ByteString key, String uuid) {
        if (!canAcquireMapLock(uuid)) {
            return false;
        }
        
        LockEntry lock = keyLockMap.get(key);
        if (lock != null) {
            // only consider the age if this client does not already own the lock
            if (!uuid.equals(lock.uuid)) {
                return !isLockOld(lock);
            }
        }
        
        return true;
    }

    boolean acquireKeyLock(ByteString key, String uuid) {
        if (!canAcquireKeyLock(key, uuid)) {
            return false;
        }
        
        LockEntry lock = new LockEntry(uuid);
        keyLockMap.put(key, lock);
        return true;
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
            mapLock = new LockEntry(uuid);
            return true;
        }
        return false;
    }

    void releaseMapLock(String uuid) {
        if (mapLock != null && mapLock.uuid.equals(uuid)) {
            mapLock = null;
        }
    }

    static boolean isLockOld(LockEntry lock) {
        Duration age = Duration.ofMillis(System.currentTimeMillis() - lock.timeLockAcquired);
        return age.compareTo(MAX_KEY_LOCK_TIME) > 0;
    }

    void releaseKeyLock(ByteString key, String uuid) {
        keyLockMap.remove(key, uuid);
    }

    void addTransaction(Transaction transaction) {
        int transId = transaction.getId();
        ByteString transKey = transaction.getKey();
        Preconditions.checkArgument(getTransactionId() + 1 == transId);

        transactionMap.put(transId, transaction);
        transactionId.incrementAndGet();

        // delete old transactions for this key
        for (Iterator<Transaction> it = transactionMap.values().iterator(); it.hasNext();) {
            Transaction oldTrans = it.next();
            if (oldTrans.getId() != transId && oldTrans.getKey().equals(transKey)) {
                it.remove();
                break; // there should only be one transaction per key
            }
        }
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
        LockEntry lock = keyLockMap.get(key);
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

    class RegisteredClient {
        final String uuid;
        Integer mapTransId;
        final String channelId;

        RegisteredClient(String uuid, Integer mapTransId, String channelId) {
            this.uuid = uuid;
            this.mapTransId = mapTransId;
            this.channelId = channelId;
        }
    }

    private static class LockEntry {
        final String uuid;
        long timeLockAcquired;

        LockEntry(String uuid) {
            this.uuid = uuid;
            timeLockAcquired = System.currentTimeMillis();
        }
    }
}
