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

import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.PipelineEntry;
import com.sbbsystems.statefun.tasks.types.GroupEntry;
import com.sbbsystems.statefun.tasks.types.GroupEntryBuilder;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import com.sbbsystems.statefun.tasks.types.TaskEntryBuilder;
import com.sbbsystems.statefun.tasks.util.Id;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.MessageFormat;
import java.util.*;

public final class PipelineGraphBuilder {
    private Map<String, Entry> entries;
    private PersistedTable<String, TaskEntry> taskEntries;
    private PersistedTable<String, GroupEntry> groupEntries;
    private Pipeline pipelineProto;
    private String head;
    private String tail;

    private PipelineGraphBuilder() {
        entries = new HashMap<>();
        taskEntries = PersistedTable.of(Id.generate(), String.class, TaskEntry.class);
        groupEntries = PersistedTable.of(Id.generate(), String.class, GroupEntry.class);
    }

    public static PipelineGraphBuilder newInstance() {
        return new PipelineGraphBuilder();
    }

    public PipelineGraphBuilder withEntries(@NotNull Map<String, Entry> entries) {
        this.entries = Objects.requireNonNull(entries);
        return this;
    }

    public PipelineGraphBuilder withTaskEntries(@NotNull PersistedTable<String, TaskEntry> taskEntries) {
        this.taskEntries = Objects.requireNonNull(taskEntries);
        return this;
    }

    public PipelineGraphBuilder withGroupEntries(@NotNull PersistedTable<String, GroupEntry> groupEntries) {
        this.groupEntries = Objects.requireNonNull(groupEntries);
        return this;
    }

    public PipelineGraphBuilder withHead(@Nullable String head) {
        this.head = head;
        return this;
    }

    public PipelineGraphBuilder withTail(@Nullable String tail) {
        this.tail = tail;
        return this;
    }

    public PipelineGraphBuilder fromProto(@NotNull Pipeline pipelineProto) {
        this.pipelineProto = Objects.requireNonNull(pipelineProto);
        return this;
    }

    public PipelineGraph build()
            throws InvalidGraphException {
        if (!Objects.isNull(pipelineProto)) {
            //build graph from protobuf
            var headEntry = buildGraph(pipelineProto);
            head = Objects.isNull(headEntry) ? null : headEntry.getId();
        }
        //else we use existing state as passed to the builder
        return PipelineGraph.from(entries, taskEntries, groupEntries, head, tail);
    }

    private Entry buildGraph(Pipeline pipelineProto)
            throws InvalidGraphException {
        return buildGraph(pipelineProto, null);
    }

    private Entry buildGraph(Pipeline pipelineProto, Group parentGroup)
            throws InvalidGraphException {

        Entry head = null;
        Entry current = null;

        for (PipelineEntry entry: pipelineProto.getEntriesList()) {
            Entry next = null;

            if (entry.hasTaskEntry()) {
                var taskEntry = entry.getTaskEntry();
                next = Task.of(taskEntry.getUid(), taskEntry.getIsExceptionally());

                if (entries.containsKey(next.getId())) {
                    throw new InvalidGraphException(MessageFormat.format("Duplicate task uid {0}", next.getId()));
                }

                entries.put(next.getId(), next);
                taskEntries.set(next.getId(), TaskEntryBuilder.fromProto(taskEntry));

            } else if (entry.hasGroupEntry()) {
                var groupEntry = entry.getGroupEntry();
                next = Group.of(groupEntry.getGroupId());

                if (entries.containsKey(next.getId())) {
                    throw new InvalidGraphException(MessageFormat.format("Duplicate group id {0}", next.getId()));
                }

                entries.put(next.getId(), next);
                groupEntries.set(next.getId(), GroupEntryBuilder.fromProto(groupEntry));

                var group = (Group) next;
                for (Pipeline pipelineInGroupProto : groupEntry.getGroupList()) {
                    group.addEntry(this.buildGraph(pipelineInGroupProto, group));
                }
            }

            if (!Objects.isNull(next)) {
                next.setParentGroup(parentGroup);

                if (head == null) {
                    head = next;
                } else {
                    current.setNext(next);
                    next.setPrevious(current);
                }

                current = next;

                if (Objects.isNull(parentGroup)) {
                    // keep track of tail node in the main chain
                    tail = current.getId();
                }
            }
        }

        return head;
    }
}
