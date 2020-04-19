package com.fincher.distributedmap.server;

public class RegistrationFailureException extends Exception {

    public enum RegistrationFailureReason {
        KEY_TYPE_DOES_NOT_MATCH,
        VALUE_TYPE_DOES_NOT_MATCH
    }

    private static final long serialVersionUID = 1L;
    private final RegistrationFailureReason failureReason;

    public RegistrationFailureException(String message, RegistrationFailureReason failureReason) {
        super(message);
        this.failureReason = failureReason;
    }
    
    public RegistrationFailureReason getFailureReason() {
        return failureReason;
    }

}
