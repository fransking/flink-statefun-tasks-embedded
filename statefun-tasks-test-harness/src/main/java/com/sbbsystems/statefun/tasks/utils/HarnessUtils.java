package com.sbbsystems.statefun.tasks.utils;

import org.apache.flink.statefun.flink.harness.Harness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HarnessUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HarnessUtils.class);

    public static AutoCloseable startHarnessInTheBackground(Harness harness) {
        Thread t =
                new Thread(
                        () -> {
                            try {
                                harness.start();
                            } catch (InterruptedException ignored) {
                                LOG.info("Harness Interrupted");
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
