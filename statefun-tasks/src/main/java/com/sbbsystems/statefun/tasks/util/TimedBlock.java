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
package com.sbbsystems.statefun.tasks.util;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.text.MessageFormat;
import java.util.Objects;

public final class TimedBlock implements AutoCloseable {
    private final long start = System.currentTimeMillis();
    private final Logger log;
    private final String message;

    public TimedBlock(@NotNull Logger log, String message) {
        this.log = Objects.requireNonNull(log);
        this.message = message;

        log.info(message);
    }

    @Override
    public void close() {
        var duration = System.currentTimeMillis() - start;
        log.info(MessageFormat.format("{0} completed in {1} milliseconds", message, duration));
    }
}
