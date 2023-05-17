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

import org.jetbrains.annotations.NotNull;

import java.util.LinkedList;
import java.util.Objects;

public class NextStepGenerator {
    private final PipelineGraph graph;

    public static NextStepGenerator of(PipelineGraph graph) {
        return new NextStepGenerator(graph);
    }

    private NextStepGenerator(PipelineGraph graph) {

        this.graph = graph;
    }

    public @NotNull NextStep getNextStep(Entry from, boolean isTaskException)
            throws InvalidGraphException {

        var next = graph.getNextEntry(from);
        var skippedTasks = new LinkedList<Task>();

        while (!Objects.isNull(next)) {
            if (next instanceof Task) {
                var task = (Task) next;

                if (task.isExceptionally() == isTaskException) {
                    break;
                }

                skippedTasks.add(task);
                graph.markComplete(task);
                next = graph.getNextEntry(task);

            } else if (next instanceof Group && isTaskException) {
                var tasks = graph.getTasks(next);

                for (var task : tasks) {
                    if (task == next.getNext()) {
                        break;  // stop once we reach the task immediately after this group
                    }
                    graph.markComplete(task);
                    skippedTasks.add(task);
                }

                next = next.getNext();

            } else {
                throw new InvalidGraphException("Expected a task or a group");
            }
        }

        return NextStep.of(next, skippedTasks);
    }
}
