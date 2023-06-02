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
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TestIngress {
    private static final BlockingQueue<TypedValue> ingressQueue = new LinkedBlockingQueue<>();

    public static void addMessage(TypedValue message) {
        ingressQueue.add(message);
    }

    public static SerializableSupplier<TypedValue> get() {
        return () -> {
            try {
                return ingressQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
