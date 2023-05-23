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
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class PipelineGraph {

    private final Map<String, Entry> entries;
    private final PersistedTable<String, TaskEntry> taskEntries;
    private final PersistedTable<String, GroupEntry> groupEntries;
    private final Map<String, GroupEntry> updatedGroupEntries = new HashMap<>();
    private final Map<String, TaskEntry> updatedTaskEntries = new HashMap<>();
    private final String head;
    private final String tail;

    private PipelineGraph(
            @NotNull Map<String, Entry> entries,
            @NotNull PersistedTable<String, TaskEntry> taskEntries,
            @NotNull PersistedTable<String, GroupEntry> groupEntries,
            @Nullable String head,
            @Nullable String tail) {
        this.entries = Objects.requireNonNull(entries);
        this.taskEntries = Objects.requireNonNull(taskEntries);
        this.groupEntries = Objects.requireNonNull(groupEntries);
        this.head = head;
        this.tail = tail;
    }

    public static PipelineGraph from(
            @NotNull Map<String, Entry> entries,
            @NotNull PersistedTable<String, TaskEntry> taskEntries,
            @NotNull PersistedTable<String, GroupEntry> groupEntries,
            @Nullable String head,
            @Nullable String tail) {
        return new PipelineGraph(entries, taskEntries, groupEntries, head, tail);
    }

    public TaskEntry getTaskEntry(String id) {
        return updatedTaskEntries.getOrDefault(id, taskEntries.get(id));
    }

    public void updateTaskEntry(TaskEntry entry) {
        updatedTaskEntries.put(entry.uid, entry);
    }

    public GroupEntry getGroupEntry(String id) {
        return updatedGroupEntries.getOrDefault(id, groupEntries.get(id));
    }

    public void updateGroupEntry(GroupEntry entry) {
        updatedGroupEntries.put(entry.groupId, entry);
    }

    public Task getTask(String id) {
        return (Task) getEntry(id);
    }

    public Group getGroup(String id) {
        return (Group) getEntry(id);
    }

    public Entry getEntry(String id) {
        return entries.get(id);
    }

    public Iterable<Task> getTasks() {
        return getTasks(getHead());
    }

    public Iterable<Task> getTasks(Entry from) {
        return () -> TasksIterator.from(from);
    }

    public @Nullable Entry getHead() {
        return getEntry(head);
    }

    public @Nullable Entry getTail() {
        return getEntry(tail);
    }

    public InitialTasks getInitialTasks()
            throws InvalidGraphException {

        return InitialTasksCollector.of(this).collectFrom(getHead());
    }

    public InitialTasks getInitialTasks(Entry entry)
            throws InvalidGraphException {

        return InitialTasksCollector.of(this).collectFrom(entry);
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
            // we reached the end of this chain return parent group if we have one
            return from.getParentGroup();
        }

        return next;
    }

    public void markComplete(Entry entry) {
        if (entry instanceof Task) {
            var taskEntry = getTaskEntry(entry.getId());
            taskEntry.complete = true;
            updateTaskEntry(taskEntry);
        }

        if (Objects.isNull(entry.getNext()) && !Objects.isNull(entry.getParentGroup())) {
            var groupEntry = getGroupEntry(entry.getParentGroup().getId());
            groupEntry.remaining = Math.max(0, groupEntry.remaining - 1);
            updateGroupEntry(groupEntry);

            if (groupEntry.remaining == 0) {
                markComplete(entry.getParentGroup());
            }
        }
    }

    public void saveUpdatedState(PipelineFunctionState state) {
        updatedGroupEntries.forEach((k, v) -> state.getGroupEntries().set(k, v));
        updatedTaskEntries.forEach((k, v) -> state.getTaskEntries().set(k, v));
        updatedTaskEntries.clear();
        updatedGroupEntries.clear();
    }

    public void saveState(PipelineFunctionState state) {
        saveUpdatedState(state);
        state.setHead(head);
        state.setTail(tail);
        state.setEntries(MapOfEntries.from(entries));
    }
}
