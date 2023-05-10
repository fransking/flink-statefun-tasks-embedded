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

import com.sbbsystems.statefun.tasks.types.TaskEntry;
import com.sbbsystems.statefun.tasks.util.Id;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class PipelineGraph {

    private final Map<String, Task> tasks;
    private final PersistedTable<String, TaskEntry> taskEntries;
    private final Entry head;

    @SuppressWarnings("unused")
    private PipelineGraph() {
        this.tasks = new HashMap<>();
        this.taskEntries = PersistedTable.of(Id.generate(), String.class, TaskEntry.class);
        this.head = null;
    }

    public PipelineGraph(
            @NotNull Map<String, Task> tasks,
            @NotNull PersistedTable<String, TaskEntry> taskEntries,
            @Nullable Entry head) {
        this.tasks = Objects.requireNonNull(tasks);
        this.taskEntries = Objects.requireNonNull(taskEntries);
        this.head = head;
    }

    public static PipelineGraph from(
            @NotNull Map<String, Task> tasks,
            @NotNull PersistedTable<String, TaskEntry> taskEntries,
            @Nullable Entry head) {
        return new PipelineGraph(tasks, taskEntries, head);
    }

    public TaskEntry getTaskEntry(String id) {
        return taskEntries.get(id);
    }

    public Task getTask(String id) {
        return tasks.get(id);
    }

    public Iterable<Entry> getEntries() {
        return () -> EntryIterator.from(head);
    }

    public Entry getHead() {
        return head;
    }
}
