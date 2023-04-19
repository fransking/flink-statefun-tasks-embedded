package com.sbbsystems.flink.tasks.embedded.types;

public class InvalidMessageTypeException extends Exception {
    public InvalidMessageTypeException(String message) {
        super(message);
    }

    public InvalidMessageTypeException(String message, Throwable cause) {
        super(message, cause);
    }
}
