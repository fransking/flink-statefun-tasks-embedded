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

import com.sbbsystems.statefun.tasks.types.GroupEntry;

import java.util.List;
import java.util.Objects;

public class GraphTestUtils {
    public static PipelineGraph fromTemplate(List<?> template) {
        var pipeline = PipelineGraphBuilderTests.buildPipelineFromTemplate(template);
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
