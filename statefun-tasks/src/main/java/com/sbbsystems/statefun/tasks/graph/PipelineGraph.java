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

import com.sbbsystems.statefun.tasks.types.GroupEntry;
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
    private final PersistedTable<String, GroupEntry> groupEntries;
    private final Entry head;

    @SuppressWarnings("unused")
    private PipelineGraph() {
        this.tasks = new HashMap<>();
        this.taskEntries = PersistedTable.of(Id.generate(), String.class, TaskEntry.class);
        this.groupEntries = PersistedTable.of(Id.generate(), String.class, GroupEntry.class);
        this.head = null;
    }

    public PipelineGraph(
            @NotNull Map<String, Task> tasks,
            @NotNull PersistedTable<String, TaskEntry> taskEntries,
            @NotNull PersistedTable<String, GroupEntry> groupEntries,
            @Nullable Entry head) {
        this.tasks = Objects.requireNonNull(tasks);
        this.taskEntries = Objects.requireNonNull(taskEntries);
        this.groupEntries = Objects.requireNonNull(groupEntries);
        this.head = head;
    }

    public static PipelineGraph from(
            @NotNull Map<String, Task> tasks,
            @NotNull PersistedTable<String, TaskEntry> taskEntries,
            @NotNull PersistedTable<String, GroupEntry> groupEntries,
            @Nullable Entry head) {
        return new PipelineGraph(tasks, taskEntries, groupEntries, head);
    }

    public TaskEntry getTaskEntry(String id) {
        return taskEntries.get(id);
    }

    public GroupEntry getGroupEntry(String id) {
        return groupEntries.get(id);
    }

    public Task getTask(String id) {
        return tasks.get(id);
    }

    public Iterable<Task> getTasks() {
        return getTasks(head);
    }

    public Iterable<Task> getTasks(Entry from) {
        return () -> TasksIterator.from(from);
    }

    public @Nullable Entry getHead() {
        return head;
    }

    public InitialTasks getInitialTasks()
            throws InvalidGraphException {

        return InitialTasksCollector.of(this).collectFrom(getHead());
    }

    public InitialTasks getInitialTasks(Entry entry)
            throws InvalidGraphException {

        return InitialTasksCollector.of(this).collectFrom(entry);
    }

    public Entry getNextStep(Entry from) {
        Entry next = null;

        if (from instanceof Group) {
            var groupEntry = getGroupEntry(from.getId());
            if (groupEntry.remaining <= 0) {
                next = from.getNext();
            }
        } else {
            next = from.getNext();
        }

        if (Objects.isNull(next) && !Objects.isNull(from.getParentGroup())) {
            // we reached the end of this chain - check if parent group is complete and continue from there
            return this.getNextStep(from.getParentGroup());
        }

        return next;
    }
}
