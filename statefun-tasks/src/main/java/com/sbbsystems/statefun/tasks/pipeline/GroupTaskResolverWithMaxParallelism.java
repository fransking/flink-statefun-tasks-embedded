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

package com.sbbsystems.statefun.tasks.pipeline;

import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.graph.Entry;
import com.sbbsystems.statefun.tasks.graph.Group;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class GroupTaskResolverWithMaxParallelism implements GroupTaskResolver {

    private GroupTaskResolverWithMaxParallelism() {
    }

    public static GroupTaskResolverWithMaxParallelism newInstance() {
        return new GroupTaskResolverWithMaxParallelism();
    }

    @Override
    public List<Entry> resolveInitialTasks(Group group, PipelineFunctionState state) {
        var groupId = group.getId();
        var maxParallelism = state.getGroupEntries().get(groupId).maxParallelism;
        var entries = group.getItems();

        List<Entry> nextEntries;
        var numEntries = entries.size();
        if (maxParallelism > 0 && numEntries > maxParallelism) {
            nextEntries = entries.subList(0, maxParallelism);
            var deferredTasks = entries.subList(maxParallelism, numEntries)
                    .stream()
                    .map(Entry::getId)
                    .collect(Collectors.toCollection(LinkedList::new));
            state.cacheDeferredTasks(groupId, GroupDeferredTasksState.of(deferredTasks));
        } else {
            nextEntries = entries;
        }

        return nextEntries;
    }

    @Override
    public List<Entry> resolveNextTasks(String groupId, PipelineFunctionState state) {
        var groupDeferredTasks = state.getDeferredTasks(groupId);
        var deferredTaskIds = groupDeferredTasks.getDeferredTaskIds();
        if (deferredTaskIds.size() == 0) {
            return Collections.emptyList();
        }
        var nextTaskId = deferredTaskIds.remove(0);
        var nextTask = state.getEntries().getItems().get(nextTaskId);
        state.cacheDeferredTasks(groupId, groupDeferredTasks);
        return List.of(nextTask);
    }
}
