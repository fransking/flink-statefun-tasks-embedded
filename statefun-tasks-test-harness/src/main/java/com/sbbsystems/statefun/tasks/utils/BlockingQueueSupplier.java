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

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * Static dictionary containing a list per key. Useful for passing messages between components which are serialised
 * by the Flink harness.
 */
public class BlockingQueueSupplier
{
    private static final ConcurrentHashMap<Integer, Object> messagesMap = new ConcurrentHashMap<>();

    private static final AtomicInteger nextUniqueId = new AtomicInteger(0);

    private BlockingQueueSupplier() {}

    public static <T> SerializableSupplier<BlockingQueue<T>> create(Collection<T> initialMembers) {
        var id = nextUniqueId.getAndIncrement();
        messagesMap.put(id, new LinkedBlockingQueue<>(initialMembers));
        return () -> {
            @SuppressWarnings("unchecked")
            var queue = (BlockingQueue<T>)messagesMap.get(id);
            return queue;
        };
    }
}
