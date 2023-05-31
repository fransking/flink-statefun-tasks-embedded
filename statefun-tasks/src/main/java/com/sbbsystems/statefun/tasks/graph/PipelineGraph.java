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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public final class PipelineGraph {

    private final Map<String, GroupEntry> updatedGroupEntries = new HashMap<>();
    private final PipelineFunctionState state;

    private PipelineGraph(PipelineFunctionState state) {
        this.state = state;
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
        return state.getEntries().getItems().get(id);
    }

    public Iterable<Task> getTasks() {
        return getTasks(getHead());
    }

    public Iterable<Task> getTasks(Entry from) {
        return () -> TasksIterator.from(from);
    }

    public @Nullable Entry getHead() {
        return getEntry(state.getHead());
    }

    public @Nullable Entry getTail() {
        return getEntry(state.getTail());
    }

    public Stream<Task> getInitialTasks() {
        return getInitialTasks(getHead());
    }

    public Stream<Task> getInitialTasks(Entry entry) {

        if (entry instanceof Task) {
            return Stream.of((Task) entry);
        }

        if (entry instanceof Group) {
            var group = (Group) entry;
            var stream = Stream.<Task>of();

            for (var groupEntry: group.getItems()) {
                stream = Stream.concat(stream, getInitialTasks(groupEntry));
            }

            return stream;
        }

        return Stream.empty();
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
            var groupEntry = getGroupEntry(entry.getId());
            groupEntry.remaining = Math.max(0, groupEntry.remaining - 1);
            updateGroupEntry(groupEntry);
        }

        if (isNull(entry.getNext()) && !isNull(entry.getParentGroup())) {
            var groupEntry = getGroupEntry(entry.getParentGroup().getId());
            groupEntry.remaining = Math.max(0, groupEntry.remaining - 1);
            updateGroupEntry(groupEntry);

            if (isComplete(groupEntry)) {
                markComplete(entry.getParentGroup());
            }
        }
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
        state.setEntries(state.getEntries());
    }
}
