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
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import com.sbbsystems.statefun.tasks.types.TaskEntryBuilder;
import com.sbbsystems.statefun.tasks.util.Id;
import org.apache.flink.statefun.sdk.state.PersistedTable;

import java.util.Objects;

public final class PipelineGraphBuilder {
    private PersistedTable<String, TaskEntry> taskLookup;
    private Pipeline pipelineProto;

    private PipelineGraphBuilder() {
        taskLookup = PersistedTable.of(Id.generate(), String.class, TaskEntry.class);
        pipelineProto = Pipeline.getDefaultInstance();
    }

    public static PipelineGraphBuilder newInstance() {
        return new PipelineGraphBuilder();
    }

    public PipelineGraphBuilder withTaskLookup(PersistedTable<String, TaskEntry> tasksByUid) {
        this.taskLookup = tasksByUid;
        return this;
    }

    public PipelineGraphBuilder fromProto(Pipeline pipelineProto) {
        this.pipelineProto = pipelineProto;
        return this;
    }

    public PipelineGraph build() {
        var head = Objects.isNull(pipelineProto) ? null : buildGraph(pipelineProto);
        return PipelineGraph.from(taskLookup, head);
    }

    private Entry buildGraph(Pipeline pipelineProto) {
        return buildGraph(pipelineProto,null);
    }

    private Entry buildGraph(Pipeline pipelineProto, Group parentGroup) {
        Entry head = null;
        Entry current = null;

        for (PipelineEntry entry: pipelineProto.getEntriesList()) {
            Entry next = null;

            if (entry.hasTaskEntry()) {
                var taskEntry = entry.getTaskEntry();
                next = Task.of(taskEntry.getUid());
                taskLookup.set(next.getId(), TaskEntryBuilder.fromProto(taskEntry));

            } else if (entry.hasGroupEntry()) {
                var groupEntry = entry.getGroupEntry();
                next = Group.of(groupEntry.getGroupId());

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
                }

                current = next;
            }
        }

        return head;
    }
}
