package com.fincher.distributedmap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;

import com.fincher.iochannel.MessageBuffer;
import com.fincher.iochannel.tcp.TcpClientChannel;
import com.fincher.iochannel.tcp.TcpServerChannel;
import com.google.protobuf.GeneratedMessageV3;

public class UtilitiesTest {

    @Test
    public void testServerMessage() throws Exception {
        GeneratedMessageV3 msg = Mockito.mock(GeneratedMessageV3.class);
        byte[] msgArray = { 1, 2, 3, 4, 5 };
        Mockito.when(msg.toByteArray()).thenReturn(msgArray);

        TcpServerChannel channel = Mockito.mock(TcpServerChannel.class);
        Mockito.doAnswer(inv -> {
            MessageBuffer mb = inv.getArgument(0);
            String channelId = inv.getArgument(1);

            byte[] lengthBytes = { 0, 0, 0, 5 };
            byte[] allBytes = new MessageBuffer(lengthBytes, msgArray).getBytes();
            assertArrayEquals(allBytes, mb.getBytes());
            assertEquals("testChannelId", channelId);
            return null;
        }).when(channel).send(Mockito.any(MessageBuffer.class), Mockito.any(String.class));

        Utilities.sendMessage(channel, "testChannelId", msg);
        Mockito.verify(channel, Mockito.times(1)).send(Mockito.any(MessageBuffer.class), Mockito.any(String.class));
    }

    @Test
    public void testClientMessage() throws Exception {
        GeneratedMessageV3 msg = Mockito.mock(GeneratedMessageV3.class);
        byte[] msgArray = {1, 2, 3, 4, 5};
        Mockito.when(msg.toByteArray()).thenReturn(msgArray);
        
        TcpClientChannel channel = Mockito.mock(TcpClientChannel.class);
        Mockito.doAnswer(inv -> {
            MessageBuffer mb = inv.getArgument(0);
            
            byte[] lengthBytes = {0, 0, 0, 5};
            byte[] allBytes = new MessageBuffer(lengthBytes, msgArray).getBytes();
            assertArrayEquals(allBytes, mb.getBytes());
            return null;
        }).when(channel).send(Mockito.any(MessageBuffer.class));
        
        Utilities.sendMessage(channel, msg);
        Mockito.verify(channel, Mockito.times(1)).send(Mockito.any(MessageBuffer.class));
    }

}
