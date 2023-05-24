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

import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.pipeline.GroupTaskResolver;

import java.util.ArrayList;
import java.util.List;

public class InitialTasksCollector {

    private final GroupTaskResolver groupTaskResolver;

    public static InitialTasksCollector of(GroupTaskResolver groupTaskResolver) {
        return new InitialTasksCollector(groupTaskResolver);
    }

    private InitialTasksCollector(GroupTaskResolver groupTaskResolver) {
        this.groupTaskResolver = groupTaskResolver;
    }

    public List<Task> collectFrom(Entry entry, PipelineFunctionState state)
            throws InvalidGraphException {
        var taskList = new ArrayList<Task>();
        collect(entry, taskList, state);
        return taskList;
    }

    private void collect(Entry entry, ArrayList<Task> taskList, PipelineFunctionState state)
            throws InvalidGraphException {
        if (entry instanceof Task) {
            taskList.add((Task) entry);

        } else if (entry instanceof Group) {
            var group = (Group) entry;
            var groupInitialTasks = groupTaskResolver.resolveInitialTasks(group, state);
            for (var groupEntry: groupInitialTasks) {
                if (groupEntry instanceof Task) {
                    taskList.add((Task)groupEntry);
                } else if (groupEntry instanceof Group) {
                    collect(groupEntry, taskList, state);
                } else {
                    throw new InvalidGraphException("Expected a Task or Group");
                }
            }
        } else {
            throw new InvalidGraphException("Expected a task or a group");
        }
    }
}
