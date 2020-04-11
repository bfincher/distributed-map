package com.fincher.distributedmap;

import com.fincher.iochannel.ChannelException;
import com.fincher.iochannel.MessageBuffer;
import com.fincher.iochannel.tcp.SimpleStreamIo;
import com.fincher.iochannel.tcp.TcpClientChannel;
import com.fincher.iochannel.tcp.TcpServerChannel;
import com.google.protobuf.GeneratedMessageV3;

public class Utilities {

    public static void sendMessage(TcpServerChannel channel, String channelId, GeneratedMessageV3 msg)
            throws ChannelException {
        byte[] msgBytes = msg.toByteArray();
        byte[] lengthBytes = SimpleStreamIo.createLengthByteArray(msgBytes.length);

        channel.send(new MessageBuffer(lengthBytes, msgBytes), channelId);
    }
    
    
    public static void sendMessage(TcpClientChannel channel, GeneratedMessageV3 msg)
            throws ChannelException {
        byte[] msgBytes = msg.toByteArray();
        byte[] lengthBytes = SimpleStreamIo.createLengthByteArray(msgBytes.length);

        channel.send(new MessageBuffer(lengthBytes, msgBytes));
    }

}
