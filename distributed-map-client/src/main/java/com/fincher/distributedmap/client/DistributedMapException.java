package com.fincher.distributedmap.client;

public class DistributedMapException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;

    public DistributedMapException(String msg) {
        super(msg);
    }
    
    public DistributedMapException(Throwable cause) {
        super(cause);
    }
}
