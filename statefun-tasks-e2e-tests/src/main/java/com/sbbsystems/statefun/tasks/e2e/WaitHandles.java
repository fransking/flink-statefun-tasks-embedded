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
package com.sbbsystems.statefun.tasks.e2e;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static java.util.Objects.isNull;

public class WaitHandles {
    public static ConcurrentHashMap<String, Semaphore> semaphores = new ConcurrentHashMap<>();

    public static void wait(String id) {
        try {
            var semaphore = semaphores.get(id);
            if (!isNull(semaphore)) {
                semaphore.acquire();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void create(String id) {
        try {
            var semaphore = new Semaphore(1);
            semaphore.acquire();
            semaphores.put(id, semaphore);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void set(String id) {
        var semaphore = semaphores.remove(id);
        if (!isNull(semaphore)) {
            semaphore.release();
        }
    }
}
