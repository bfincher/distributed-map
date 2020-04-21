package com.fincher.distributedmap;

import com.fincher.iochannel.ChannelException;
import com.fincher.iochannel.MessageBuffer;
import com.fincher.iochannel.tcp.SimpleStreamIo;
import com.fincher.iochannel.tcp.TcpChannelIfc;
import com.google.protobuf.GeneratedMessageV3;

public class Utilities {

    /**
     * Send a message on the given channel to the given channelId
     * 
     * @param channel   The channel to which the messsage should be sent
     * @param channelId The channel ID to which the message should be sent
     * @param msg       The message to be sent
     * @throws ChannelException If an exception occurs while sending
     */
    public static void sendMessage(TcpChannelIfc channel, String channelId, GeneratedMessageV3 msg)
            throws ChannelException {
        byte[] msgBytes = msg.toByteArray();
        byte[] lengthBytes = SimpleStreamIo.createLengthByteArray(msgBytes.length);

        channel.send(new MessageBuffer(lengthBytes, msgBytes), channelId);
    }
    
    
    /**
     * Send a message on the given channel to the given channelId
     * 
     * @param channel   The channel to which the messsage should be sent
     * @param msg       The message to be sent
     * @throws ChannelException If an exception occurs while sending
     */
    public static void sendMessage(TcpChannelIfc channel, GeneratedMessageV3 msg)
            throws ChannelException {
        byte[] msgBytes = msg.toByteArray();
        byte[] lengthBytes = SimpleStreamIo.createLengthByteArray(msgBytes.length);

        channel.send(new MessageBuffer(lengthBytes, msgBytes));
    }

}
