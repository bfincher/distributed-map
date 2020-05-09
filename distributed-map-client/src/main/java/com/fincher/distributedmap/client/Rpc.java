package com.fincher.distributedmap.client;

import com.fincher.distributedmap.messages.ClientToServerMessage;
import com.fincher.distributedmap.messages.ServerToClientMessage;

import com.fincher.iochannel.ChannelException;
import com.fincher.iochannel.tcp.TransformingTcpChannel;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;

class Rpc  {
    
    private Rpc() {
    }

    static ServerToClientMessage call(ClientToServerMessage msg,
            TransformingTcpChannel<ClientToServerMessage, ServerToClientMessage> channel,
            Predicate<ServerToClientMessage> responsePredicate) throws ChannelException, InterruptedException {

        AtomicReference<ServerToClientMessage> response = new AtomicReference<>();
        Lock lock = new ReentrantLock();
        Condition msgReceived = lock.newCondition();
        
        Consumer<ServerToClientMessage> msgHandler = received -> {
            lock.lock();
            try {
                response.set(received);
                msgReceived.signal();
            } finally {
                lock.unlock();
            }
        };
        
        lock.lock();
        try {
            channel.addTransformedMessageListener(msgHandler, responsePredicate);
            
            channel.send(msg);
            msgReceived.await();
            return response.get();
            
        } finally {
            lock.unlock();
            channel.removeTransformedMessageListener(msgHandler);
        }
    }

}
