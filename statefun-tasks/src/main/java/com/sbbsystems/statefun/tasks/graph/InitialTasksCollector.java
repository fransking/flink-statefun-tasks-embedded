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

import java.util.LinkedList;
import java.util.Objects;

public class InitialTasksCollector {

    private final PipelineGraph graph;
    private int maxParallelism;

    public static InitialTasksCollector of(PipelineGraph graph) {
        return new InitialTasksCollector(graph);
    }

    private InitialTasksCollector(PipelineGraph graph) {
        this.graph = graph;
    }

    public InitialTasks collectFrom(Entry entry)
            throws InvalidGraphException {

        return collect(entry, false);
    }

    private InitialTasks collect(Entry entry, boolean predecessorIsEmptyGroup)
            throws InvalidGraphException {

        InitialTasks initialTasks = null;

        if (entry instanceof Task) {
            initialTasks = InitialTasks.of((Task) entry, predecessorIsEmptyGroup);

        } else if (entry instanceof Group) {
            var group = (Group) entry;

            // update min maxParallelism value
            maxParallelism = getMinMaxParallelism(maxParallelism, group);

            // skip over empty groups e.g. [[]] -> a -> b should return a with predecessorIsEmptyGroup set to true
            if (group.getItems().isEmpty() && !Objects.isNull(group.getNext())) {
                initialTasks = collect(group.getNext(), true);

            } else {
                var tasks = new LinkedList<Task>();

                for (Entry item : group.getItems()) {
                    tasks.addAll(collectFrom(item).getTasks());
                }

                initialTasks = InitialTasks.of(tasks, predecessorIsEmptyGroup, maxParallelism);
            }
        }

        if (Objects.isNull(initialTasks)) {
            throw new InvalidGraphException("Expected a task or a group");
        }

        return initialTasks;
    }

    private int getMinMaxParallelism(int currentMaxParallelism, Group group) {
        var groupEntry = graph.getGroupEntry(group.getId());

        if (groupEntry.maxParallelism == 0) {
            return currentMaxParallelism;
        }

        if (currentMaxParallelism == 0) {
            return groupEntry.maxParallelism;
        }

        return Math.min(groupEntry.maxParallelism, currentMaxParallelism);
    }
}
