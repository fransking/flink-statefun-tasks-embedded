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

import com.sbbsystems.statefun.tasks.graph.MapOfEntries;
import com.sbbsystems.statefun.tasks.types.GroupEntry;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;

import java.util.Objects;

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

    private MapOfEntries cachedEntries = null;

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

    public void reset() {
        taskEntries.clear();
        groupEntries.clear();
        entries.clear();
        head.clear();
        tail.clear();
    }
}
