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
package com.sbbsystems.statefun.tasks;

import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.MapOfEntries;
import com.sbbsystems.statefun.tasks.pipeline.GroupDeferredTasksState;
import com.sbbsystems.statefun.tasks.types.GroupEntry;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;

public final class PipelineFunctionState {
    @Persisted
    private final PersistedTable<String, TaskEntry> taskEntries = PersistedTable.of("taskEntries", String.class, TaskEntry.class);

    @Persisted
    private final PersistedTable<String, GroupEntry> groupEntries = PersistedTable.of("groupEntries", String.class, GroupEntry.class);

    @Persisted
    private final PersistedValue<MapOfEntries> entries = PersistedValue.of("entries", MapOfEntries.class);

    @Persisted
    private final PersistedValue<String> head = PersistedValue.of("head", String.class);

    @Persisted
    private final PersistedValue<String> tail = PersistedValue.of("tail", String.class);

    @Persisted
    private final PersistedValue<Integer> status = PersistedValue.of("status", Integer.class);

    @Persisted
    private final PersistedTable<String, GroupDeferredTasksState> deferredTasks = PersistedTable.of("deferredTasksMap", String.class, GroupDeferredTasksState.class);

    private MapOfEntries cachedEntries = null;

    private final Map<String, GroupDeferredTasksState> cachedDeferredTasks = new HashMap<>();

    public static PipelineFunctionState newInstance() {
        return new PipelineFunctionState();
    }

    private PipelineFunctionState() {
    }

    public PersistedTable<String, TaskEntry> getTaskEntries() {
        return taskEntries;
    }

    public PersistedTable<String, GroupEntry> getGroupEntries() {
        return groupEntries;
    }

    public MapOfEntries getEntries() {
        if (!Objects.isNull(cachedEntries)) {
            return cachedEntries;
        }

        cachedEntries = entries.getOrDefault(new MapOfEntries());
        return cachedEntries;
    }

    public void setEntries(MapOfEntries tasks) {
        cachedEntries = tasks;
        this.entries.set(cachedEntries);
    }

    @NotNull
    public GroupDeferredTasksState getDeferredTasks(String groupId) {
        var groupCachedTasks = cachedDeferredTasks.getOrDefault(groupId, null);
        if (groupCachedTasks != null) {
            return groupCachedTasks;
        }
        var groupTasks = deferredTasks.get(groupId);
        if (groupTasks == null) {
            groupTasks = GroupDeferredTasksState.of(emptyList());
        }
        cachedDeferredTasks.put(groupId, groupTasks);
        return groupTasks;
    }

    public void cacheDeferredTasks(String groupId, GroupDeferredTasksState deferredTasks) {
        cachedDeferredTasks.put(groupId, deferredTasks);
    }

    public void saveDeferredTasks() {
        cachedDeferredTasks.forEach(deferredTasks::set);
    }

    public String getHead() {
        return head.get();
    }

    public void setHead(String head) {
        this.head.set(head);
    }

    public String getTail() {
        return tail.get();
    }

    public void setTail(String tail) {
        this.tail.set(tail);
    }

    public TaskStatus.Status getStatus() {
        var status = this.status.getOrDefault(0);
        return TaskStatus.Status.forNumber(status);
    }

    public void setStatus(TaskStatus.Status status) {
        this.status.set(status.getNumber());
    }

    public void reset() {
        taskEntries.clear();
        groupEntries.clear();
        entries.clear();
        head.clear();
        tail.clear();
        status.clear();
    }
}
