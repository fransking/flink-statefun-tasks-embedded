package com.sbbsystems.statefun.tasks.utils;

import org.apache.flink.statefun.flink.harness.Harness;

public class HarnessUtils {
    public static AutoCloseable startHarnessInTheBackground(Harness harness) {
        Thread t =
                new Thread(
                        () -> {
                            try {
                                harness.start();
                            } catch (InterruptedException ignored) {
                                System.out.println("Harness Interrupted");
                            } catch (Exception exception) {
                                throw new RuntimeException(exception);
                            }
                        });
        t.setName("harness-runner");
        t.setDaemon(true);
        t.start();
        return t::interrupt;
    }
}
