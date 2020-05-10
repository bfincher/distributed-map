package com.fincher.distributedmap.client;

import com.fincher.distributedmap.client.DistributedMap.TransformingChannel;
import com.fincher.distributedmap.messages.ClientToServerMessage;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class DistributedMapTest {
    
    private static final String MAP_NAME = "testMapName";
    
    TransformingChannel channel;
    List<ClientToServerMessage> sentMsgs;
    DistributedMap<Integer, String> map;
    
    @Before
    public void before() throws Exception {
        sentMsgs = new ArrayList<>();
        
        channel = Mockito.mock(TransformingChannel.class);
        Mockito.doAnswer(inv -> {
                sentMsgs.add(inv.getArgument(0));
                return null;
            }
        ).when(channel).send(Mockito.any(ClientToServerMessage.class));
        
        map = new DistributedMap<>(MAP_NAME, isSynchronized, keyType, valueType, channel)
    }

}
