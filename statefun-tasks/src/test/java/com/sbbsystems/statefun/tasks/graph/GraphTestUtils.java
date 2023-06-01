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
import com.sbbsystems.statefun.tasks.generated.TaskEntry;
import com.sbbsystems.statefun.tasks.types.GroupEntry;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class GraphTestUtils {
    public static Pipeline buildPipelineFromTemplate(List<?> template) {
        var pipeline = Pipeline.newBuilder();

        template.forEach(item -> {
            if (item instanceof List<?>) {
                var groupEntry = com.sbbsystems.statefun.tasks.generated.GroupEntry.newBuilder()
                        .setGroupId(String.valueOf(UUID.randomUUID()));

                ((List<?>) item).forEach(groupItem -> {
                    if (groupItem instanceof List<?>) {
                        var group = buildPipelineFromTemplate((List<?>) groupItem);
                        groupEntry.addGroup(group);
                    }
                });

                pipeline.addEntries(PipelineEntry.newBuilder().setGroupEntry(groupEntry));
            } else {
                var taskEntry = TaskEntry.newBuilder()
                        .setTaskId(String.valueOf(item))
                        .setUid(String.valueOf(item))
                        .build();

                pipeline.addEntries(PipelineEntry.newBuilder().setTaskEntry(taskEntry));
            }
        });

        return pipeline.build();
    }

    public static PipelineGraph fromTemplate(List<?> template) {
        var pipeline = buildPipelineFromTemplate(template);
        var builder = PipelineGraphBuilder.newInstance().fromProto(pipeline);

        try {
            return builder.build();
        } catch (InvalidGraphException e) {
            throw new RuntimeException(e);
        }
    }

    public static GroupEntry getGroupContainingTask(String taskId, PipelineGraph graph) {
        var taskA = graph.getTask(taskId);
        var groupId = Objects.requireNonNull(taskA.getParentGroup()).getId();
        return graph.getGroupEntry(groupId);
    }

}
