package com.fincher.distributedmap.server;

import com.fincher.distributedmap.server.MapInfo.RegisteredClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class RegisteredClients {
    
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
