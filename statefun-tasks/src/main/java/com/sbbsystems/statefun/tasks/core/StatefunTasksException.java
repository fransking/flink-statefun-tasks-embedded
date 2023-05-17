package com.sbbsystems.statefun.tasks.core;

public class StatefunTasksException extends Exception {
    public StatefunTasksException(String message) {
        super(message);
    }

    public StatefunTasksException(String message, Throwable cause) {
        super(message, cause);
    }
}
