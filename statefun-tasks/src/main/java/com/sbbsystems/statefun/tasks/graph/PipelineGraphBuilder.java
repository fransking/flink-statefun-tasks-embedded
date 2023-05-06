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
import com.sbbsystems.statefun.tasks.types.Task;
import com.sbbsystems.statefun.tasks.types.TaskBuilder;
import org.apache.flink.statefun.sdk.state.PersistedTable;

public final class PipelineGraphBuilder {
    private PersistedTable<String, Task> taskLookup = PersistedTable.of("taskLookup", String.class, Task.class);
    private Chain entries;
    private Pipeline pipelineProto = Pipeline.getDefaultInstance();

    public PipelineGraphBuilder withTaskLookup(PersistedTable<String, Task> tasksByUid) {
        this.taskLookup = tasksByUid;
        return this;
    }

    public PipelineGraphBuilder withEntries(Chain entries) {
        this.entries = entries;
        return this;
    }

    public PipelineGraphBuilder fromProto(Pipeline pipelineProto) {
        this.pipelineProto = pipelineProto;
        return this;
    }

    public PipelineGraph build() {
        if (entries == null) {
            entries = new Chain();
            addTasks(entries, pipelineProto);
        }

        return new PipelineGraph(taskLookup, entries);
    }

    private void addTasks(Chain entries, Pipeline pipelineProto) {
        pipelineProto.getEntriesList().forEach(entry -> {
            if (entry.hasTaskEntry()) {
                var taskEntry = entry.getTaskEntry();

                var taskId = new TaskId(taskEntry.getUid());
                entries.add(taskId);
                taskLookup.set(taskId.getId(), TaskBuilder.fromProto(taskEntry));

            } else if (entry.hasGroupEntry()) {
                var groupEntry = entry.getGroupEntry();
                var grouping = new Grouping(groupEntry.getGroupId());
                entries.add(grouping);

                for (Pipeline pipelineInGroupProto : groupEntry.getGroupList()) {
                    var chainInGroup = new Chain();
                    grouping.getEntries().add(chainInGroup);

                    this.addTasks(chainInGroup, pipelineInGroupProto);
                }
            }
        });
    }
}
