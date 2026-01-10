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
package com.sbbsystems.statefun.tasks.graph.v2;

import com.google.common.collect.Iterables;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.types.GroupEntry;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.sbbsystems.statefun.tasks.graph.v2.GraphEntry.isTask;
import static com.sbbsystems.statefun.tasks.graph.v2.GraphEntry.isGroup;
import static com.sbbsystems.statefun.tasks.graph.v2.GraphEntry.getNext;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public final class PipelineGraph {
    private final Map<String, GroupEntry> updatedGroupEntries = new HashMap<>();
    private final PipelineFunctionState state;
    private final MapOfGraphEntries entries;

    private PipelineGraph(PipelineFunctionState state) {
        this.state = state;
        this.entries = state.getGraphEntries();
    }

    public static PipelineGraph from(@NotNull PipelineFunctionState state) {
        return new PipelineGraph(requireNonNull(state));
    }

    public Map<String, GraphEntry> getEntries() {
        return entries.getItems();
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

    public GraphEntry getEntry(String id) {
        return getEntries().get(id);
    }

    public Iterable<GraphEntry> getTasks() {
        return getTasks(getHead());
    }

    public Iterable<GraphEntry> getTasks(GraphEntry from) {
        return () -> GraphEntryIterator.from(from, getEntries());
    }

    public Stream<TaskEntry> getTaskEntries() {
        return StreamSupport.stream(getTasks().spliterator(), false).map(t -> getTaskEntry(t.getId()));
    }

    public @Nullable GraphEntry getHead() {
        return getEntry(state.getHead());
    }

    public @Nullable GraphEntry getTail() {
        return getEntry(state.getTail());
    }

    public boolean isFinally(GraphEntry entry) {
        return isTask(entry) && entry.isFinally();
    }

    public @Nullable GraphEntry getFinally() {
        var tail = getTail();
        return (isFinally(tail)) ? tail : null;
    }

    public Stream<GraphEntry> getInitialTasks(GraphEntry entry, List<GraphEntry> skippedTasks) {
        return getInitialTasks(entry, skippedTasks, false);
    }

    public Stream<GraphEntry> getInitialTasks(GraphEntry entry, List<GraphEntry> skippedTasks, boolean exceptionally) {
        return StreamSupport.stream(getInitialTasksIterator(entry, skippedTasks, exceptionally).spliterator(), false);
    }

    private Iterable<GraphEntry> getInitialTasksIterator(GraphEntry entry, List<GraphEntry> skippedTasks, boolean exceptionally) {
        // deal with empty groups
        while (isGroup(entry) && entry.isEmpty(getEntries())) {
            entry = getEntry(entry.getNextId());
        }

        if (isTask(entry)) {
            var task = entry;

            // skip tasks that don't match our current execution state
            if (!task.isFinally() && task.isExceptionally() != exceptionally) {
                markComplete(task);
                skippedTasks.add(task);
                return getInitialTasksIterator(getNext(task, getEntries()), skippedTasks, exceptionally);
            }

            return () -> Collections.singletonList(task).iterator();
        }

        if (isGroup(entry)) {
            Iterable<GraphEntry> iterable = Collections::emptyIterator;

            for (var entryId: entry.getIdsInGroup()) {
                GraphEntry groupEntry = getEntries().get(entryId);
                iterable = Iterables.concat(iterable, getInitialTasksIterator(groupEntry, skippedTasks, exceptionally));
            }

            return iterable;
        }

        return Collections::emptyIterator;
    }

    public GraphEntry getNextEntry(GraphEntry from) {
        var next = getNext(from, getEntries());

        if (isNull(next) && !isNull(from.getParentGroupId())) {
            // we reached the end of this chain return parent group if we have one
            return getEntry(from.getParentGroupId());
        }

        return next;
    }

    public void markComplete(GraphEntry entry) {
        if (isGroup(entry)) {
            // if we are a group then mark group as complete
            var groupEntry = getGroupEntry(entry.getId());
            groupEntry.remaining = 0;
            updateGroupEntry(groupEntry);
        }

        if (isNull(getNext(entry, getEntries())) && !isNull(entry.getParentGroupId())) {
            // if we are the end of a chain in a parent group then decrement the parent group remaining count
            var groupEntry = getGroupEntry(entry.getParentGroupId());
            groupEntry.remaining = Math.max(0, groupEntry.remaining - 1);
            updateGroupEntry(groupEntry);
        }
    }

    public void markHasException(GraphEntry group) {
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

    public void saveState() {
        saveUpdatedState();

        // ensure write back to Flink state
        state.setHead(state.getHead());
        state.setTail(state.getTail());
        state.setGraphEntries(entries);
    }
}
