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

import java.text.MessageFormat;
import java.util.Objects;
import java.util.function.Consumer;

public final class TimedBlock implements AutoCloseable {
    private final long start = System.currentTimeMillis();
    private final Consumer<String> log;
    private final String message;
    private final boolean printToConsole;

    public static TimedBlock of(@NotNull Consumer<String> log, String message) {
        return new TimedBlock(Objects.requireNonNull(log), message, false);
    }

    public static TimedBlock of(@NotNull Consumer<String> log, String message, boolean printToConsole) {
        return new TimedBlock(Objects.requireNonNull(log), message, printToConsole);
    }

    public static TimedBlock of(@NotNull Consumer<String> log, String pattern, Object... arguments) {
        return TimedBlock.of(log, MessageFormat.format(pattern, arguments));
    }

    public static TimedBlock of(@NotNull Consumer<String> log, boolean printToConsole, String pattern, Object... arguments) {
        return TimedBlock.of(log, MessageFormat.format(pattern, arguments), printToConsole);
    }

    private TimedBlock(Consumer<String> log, String message, boolean printToConsole) {
        this.log = log;
        this.message = message;
        this.printToConsole = printToConsole;

        log.accept(message);

        if (printToConsole) {
            System.out.println(message);
        }
    }

    @Override
    public void close() {
        var duration = System.currentTimeMillis() - start;
        var message = MessageFormat.format("{0} completed in {1} milliseconds", this.message, duration);

        log.accept(message);

        if (printToConsole) {
            System.out.println(message);
        }
    }
}
