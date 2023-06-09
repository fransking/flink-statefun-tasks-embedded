/*
 * Copyright [2023] [Frans King, Luke Ashworth]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sbbsystems.statefun.tasks.utils;

import com.sbbsystems.statefun.tasks.testmodule.IoIdentifiers;
import org.apache.flink.statefun.flink.harness.Harness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HarnessUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HarnessUtils.class);
    private static Thread harnessThread = null;

    public static synchronized void ensureHarnessThreadIsRunning() {
        if (harnessThread == null || !harnessThread.isAlive()) {
            LOG.info("Starting test harness");
            var harness = new Harness()
                    .withParallelism(5)
                    .withSupplyingIngress(IoIdentifiers.REQUEST_INGRESS, TestIngress.get())
                    .withConsumingEgress(IoIdentifiers.RESULT_EGRESS, TestEgress::addMessage)
                    .withConsumingEgress(IoIdentifiers.EVENTS_EGRESS, TestEgress::addEventMessage);

            harnessThread =
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
            harnessThread.setName("harness-runner");
            harnessThread.setDaemon(true);
            harnessThread.start();

            TestEgress.initialise(harnessThread);
        }
    }

}
