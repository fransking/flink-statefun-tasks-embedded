package com.sbbsystems.flink.tasks.embedded.utils;

import org.apache.flink.statefun.flink.harness.io.SerializableSupplier;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class IngressMessageSupplier {
    // static map to avoid serialization
    private static final ConcurrentHashMap<Integer, Object> messagesMap = new ConcurrentHashMap<>();

    private static final AtomicInteger nextUniqueId = new AtomicInteger(0);

    public static <T> SerializableSupplier<T> create(Collection<T> messages) {
        var id = nextUniqueId.getAndIncrement();
        messagesMap.put(id, new LinkedBlockingQueue<>(messages));
        return () -> {
            @SuppressWarnings("unchecked")
            BlockingQueue<T> queue = (BlockingQueue<T>) messagesMap.get(id);
            try {
                return queue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }
}


