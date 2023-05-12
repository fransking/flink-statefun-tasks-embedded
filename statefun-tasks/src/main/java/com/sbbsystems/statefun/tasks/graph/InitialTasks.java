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
package com.sbbsystems.statefun.tasks.graph;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnmodifiableView;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class InitialTasks implements Iterable<Task> {
    private final List<Task> tasks;
    private final boolean predecessorIsEmptyGroup;
    private final int maxParallelism;

    public static InitialTasks of(List<Task> tasks, boolean predecessorIsEmptyGroup, int maxParallelism) {
        return new InitialTasks(Collections.unmodifiableList(tasks), predecessorIsEmptyGroup, maxParallelism);
    }

    public static InitialTasks of(Task task, boolean predecessorIsEmptyGroup) {
        return new InitialTasks(List.of(task), predecessorIsEmptyGroup, 0);
    }

    private InitialTasks(List<Task> tasks, boolean predecessorIsEmptyGroup, int maxParallelism) {
        this.tasks = tasks;
        this.maxParallelism = maxParallelism;
        this.predecessorIsEmptyGroup = predecessorIsEmptyGroup;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public boolean isPredecessorIsEmptyGroup() {
        return predecessorIsEmptyGroup;
    }

    @UnmodifiableView
    public List<Task> getTasks() {
        return tasks;
    }

    @NotNull
    @UnmodifiableView
    @Override
    public Iterator<Task> iterator() {
        return tasks.iterator();
    }
}
