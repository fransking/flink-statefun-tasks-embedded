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

package com.sbbsystems.statefun.tasks.groupaggregation;

import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.graph.Group;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.graph.Task;

import java.text.MessageFormat;
import java.util.ArrayList;

public class GroupResultPropagatorImpl implements GroupResultPropagator {

    private final GroupResultAggregator aggregator;

    private GroupResultPropagatorImpl(GroupResultAggregator aggregator) {
        this.aggregator = aggregator;
    }

    public static GroupResultPropagatorImpl newInstance(GroupResultAggregator aggregator) {
        return new GroupResultPropagatorImpl(aggregator);
    }

    @Override
    public TaskResultOrException propagateGroupResult(Group group, PipelineGraph graph, PipelineFunctionState state, String invocationId) {
        var currentGroup = group;
        while (currentGroup.getParentGroup() != null && graph.getGroupEntry(currentGroup.getParentGroup().getId()).remaining == 0) {
            currentGroup = currentGroup.getParentGroup();
        }
        return aggregateChildResults(currentGroup, graph, state, invocationId);
    }

    private TaskResultOrException aggregateChildResults(Group group, PipelineGraph graph, PipelineFunctionState state, String invocationId) {
        var aggregatedResultList = new ArrayList<TaskResultOrException>();
        for (var entry : group.getItems()) {
            if (entry instanceof Task) {
                var taskResult = state.getIntermediateGroupResults().get(entry.getId());
                if (taskResult == null) {
                    throw new RuntimeException(MessageFormat.format("No intermediate task result in state for {0}", entry.getId()));
                }
                aggregatedResultList.add(taskResult);
            } else if (entry instanceof Group) {
                aggregatedResultList.add(aggregateChildResults((Group) entry, graph, state, invocationId));
            } else {
                throw new RuntimeException("Unexpected entry type");
            }
        }
        var groupEntry = graph.getGroupEntry(group.getId());

        return aggregator.aggregateResult(group.getId(), invocationId, aggregatedResultList, groupEntry.returnExceptions);
    }
}
