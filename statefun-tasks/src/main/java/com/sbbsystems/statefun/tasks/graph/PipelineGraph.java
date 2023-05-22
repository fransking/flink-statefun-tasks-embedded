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

import com.sbbsystems.statefun.tasks.PipelineFunctionState;
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
    private final Map<String, GroupEntry> updatedGroupEntries = new HashMap<>();
    private final Map<String, TaskEntry> updatedTaskEntries = new HashMap<>();
    private final Entry head;
    private final Entry tail;

//    @SuppressWarnings("unused")
//    private PipelineGraph() {
//        this.tasks = new HashMap<>();
//        this.taskEntries = PersistedTable.of(Id.generate(), String.class, TaskEntry.class);
//        this.groupEntries = PersistedTable.of(Id.generate(), String.class, GroupEntry.class);
//        this.head = null;
//    }

    private PipelineGraph(
            @NotNull Map<String, Task> tasks,
            @NotNull PersistedTable<String, TaskEntry> taskEntries,
            @NotNull PersistedTable<String, GroupEntry> groupEntries,
            @Nullable Entry head,
            @Nullable Entry tail) {
        this.tasks = Objects.requireNonNull(tasks);
        this.taskEntries = Objects.requireNonNull(taskEntries);
        this.groupEntries = Objects.requireNonNull(groupEntries);
        this.head = head;
        this.tail = tail;
    }

    public static PipelineGraph from(
            @NotNull Map<String, Task> tasks,
            @NotNull PersistedTable<String, TaskEntry> taskEntries,
            @NotNull PersistedTable<String, GroupEntry> groupEntries,
            @Nullable Entry head,
            @Nullable Entry tail) {
        return new PipelineGraph(tasks, taskEntries, groupEntries, head, tail);
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

    public @Nullable Entry getTail() {
        return tail;
    }

    public InitialTasks getInitialTasks()
            throws InvalidGraphException {

        return InitialTasksCollector.of(this).collectFrom(getHead());
    }

    public InitialTasks getInitialTasks(Entry entry)
            throws InvalidGraphException {

        return InitialTasksCollector.of(this).collectFrom(entry);
    }

    public @NotNull NextStep getNextStep(Entry from, boolean isTaskException)
            throws InvalidGraphException {

            return NextStepGenerator.of(this).getNextStep(from, isTaskException);
    }

    public Entry getNextEntry(Entry from) {
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
            return this.getNextEntry(from.getParentGroup());
        }

        return next;
    }

    public void markComplete(Entry entry) {
        if (entry instanceof Task) {
            var taskEntry = getTaskEntry(entry.getId());
            taskEntry.complete = true;
            updatedTaskEntries.put(entry.getId(), taskEntry);
        }

        if (Objects.isNull(entry.getNext()) && !Objects.isNull(entry.getParentGroup())) {
            var groupEntry = getGroupEntry(entry.getParentGroup().getId());
            groupEntry.remaining--;
            updatedGroupEntries.put(entry.getParentGroup().getId(), groupEntry);

            if (groupEntry.remaining == 0) {
                markComplete(entry.getParentGroup());
            }
        }
    }

    public void saveUpdatedState(PipelineFunctionState state) {
        updatedGroupEntries.forEach((k, v) -> state.getGroupEntries().set(k, v));
        updatedTaskEntries.forEach((k, v) -> state.getTaskEntries().set(k, v));
    }

    public void saveState(PipelineFunctionState state) {
        saveUpdatedState(state);
        state.setHead(head);
        state.setTail(tail);
        state.setTasks(MapOfTasks.from(tasks));
    }
}
