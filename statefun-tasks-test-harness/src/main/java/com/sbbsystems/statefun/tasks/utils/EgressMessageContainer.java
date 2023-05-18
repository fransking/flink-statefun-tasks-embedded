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

import org.apache.flink.statefun.flink.harness.io.SerializableSupplier;

import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class EgressMessageContainer<T> implements Serializable {
    private final SerializableSupplier<BlockingQueue<T>> messageQueueSupplier;

    public EgressMessageContainer() {
        this.messageQueueSupplier = BlockingQueueSupplier.create(Collections.emptyList());
    }

    public void addMessage(T message) {
        this.messageQueueSupplier.get().add(message);
    }

    public T getMessage(Thread harnessThread) {
        try {
            BlockingQueue<T> queue = this.messageQueueSupplier.get();
            T result = null;
            while (result == null) {
                if (!harnessThread.isAlive()) {
                    throw new RuntimeException("Harness has stopped" + harnessThread);
                }
                result = queue.poll(500, TimeUnit.MILLISECONDS);
            }
            return result;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
