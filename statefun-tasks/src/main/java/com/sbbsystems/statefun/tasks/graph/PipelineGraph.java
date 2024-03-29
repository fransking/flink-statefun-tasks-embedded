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

import com.google.common.collect.Iterables;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.types.GroupEntry;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public final class PipelineGraph {
    private final Map<String, GroupEntry> updatedGroupEntries = new HashMap<>();
    private final PipelineFunctionState state;
    private final MapOfEntries entries;

    private PipelineGraph(PipelineFunctionState state) {
        this.state = state;
        this.entries = state.getEntries();
    }

    public static PipelineGraph from(@NotNull PipelineFunctionState state) {
        return new PipelineGraph(requireNonNull(state));
    }

    public TaskEntry getTaskEntry(String id) {
        return state.getTaskEntries().get(id);
    }

    public GroupEntry getGroupEntry(String id) {
        return updatedGroupEntries.getOrDefault(id, state.getGroupEntries().get(id));
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
        return entries.getItems().get(id);
    }

    public Iterable<Task> getTasks() {
        return getTasks(getHead());
    }

    public Iterable<Task> getTasks(Entry from) {
        return () -> TasksIterator.from(from);
    }

    public Stream<TaskEntry> getTaskEntries() {
        return StreamSupport.stream(getTasks().spliterator(), false).map(t -> getTaskEntry(t.getId()));
    }

    public @Nullable Entry getHead() {
        return getEntry(state.getHead());
    }

    public @Nullable Entry getTail() {
        return getEntry(state.getTail());
    }

    public @Nullable Task getFinally() {
        var tail = getTail();

        return (tail instanceof Task && ((Task) tail).isFinally())
                ? (Task) tail
                : null;
    }

    public Stream<Task> getInitialTasks(Entry entry, List<Task> skippedTasks) {
        return getInitialTasks(entry, skippedTasks, false);
    }

    public Stream<Task> getInitialTasks(Entry entry, List<Task> skippedTasks, boolean exceptionally) {
        return StreamSupport.stream(getInitialTasksIterator(entry, skippedTasks, exceptionally).spliterator(), false);
    }

    private Iterable<Task> getInitialTasksIterator(Entry entry, List<Task> skippedTasks, boolean exceptionally) {
        // deal with empty groups
        while (entry instanceof Group && entry.isEmpty()) {
            entry = entry.getNext();
        }

        if (entry instanceof Task) {
            var task = (Task) entry;

            // skip tasks that don't match our current execution state
            if (!task.isFinally() && task.isExceptionally() != exceptionally) {
                markComplete(task);
                skippedTasks.add(task);
                return getInitialTasksIterator(task.getNext(), skippedTasks, exceptionally);
            }

            return () -> Collections.singletonList(task).iterator();
        }

        if (entry instanceof Group) {
            var group = (Group) entry;
            Iterable<Task> iterable = Collections::emptyIterator;

            for (var groupEntry: group.getItems()) {
                iterable = Iterables.concat(iterable, getInitialTasksIterator(groupEntry, skippedTasks, exceptionally));
            }

            return iterable;
        }

        return Collections::emptyIterator;
    }

    public Entry getNextEntry(Entry from) {
        var next = from.getNext();

        if (isNull(next) && !isNull(from.getParentGroup())) {
            // we reached the end of this chain return parent group if we have one
            return from.getParentGroup();
        }

        return next;
    }

    public void markComplete(Entry entry) {
        if (entry instanceof Group) {
            // if we are a group then mark group as complete
            var groupEntry = getGroupEntry(entry.getId());
            groupEntry.remaining = 0;
            updateGroupEntry(groupEntry);
        }

        if (isNull(entry.getNext()) && !isNull(entry.getParentGroup())) {
            // if we are the end of a chain in a parent group then decrement the parent group remaining count
            var groupEntry = getGroupEntry(entry.getParentGroup().getId());
            groupEntry.remaining = Math.max(0, groupEntry.remaining - 1);
            updateGroupEntry(groupEntry);
        }
    }

    public void markHasException(Group group) {
        var groupEntry = getGroupEntry(group.getId());
        groupEntry.hasException = true;
        updateGroupEntry(groupEntry);
    }

    public boolean hasException(GroupEntry groupEntry) {
        return groupEntry.hasException;
    }

    public boolean isComplete(String groupId) {
        return isComplete(getGroupEntry(groupId));
    }

    public boolean isComplete(GroupEntry groupEntry) {
        return groupEntry.remaining == 0;
    }

    public void saveUpdatedState() {
        updatedGroupEntries.forEach((k, v) -> state.getGroupEntries().set(k, v));
        updatedGroupEntries.clear();
    }

    public boolean isFinally(Entry entry) {
        return entry instanceof Task && ((Task) entry).isFinally();
    }

    public void saveState() {
        saveUpdatedState();

        // ensure write back to Flink state
        state.setHead(state.getHead());
        state.setTail(state.getTail());
        state.setEntries(entries);
    }
}
