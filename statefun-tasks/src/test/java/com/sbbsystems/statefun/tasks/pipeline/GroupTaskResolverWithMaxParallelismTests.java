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
import com.sbbsystems.statefun.tasks.graph.MapOfEntries;
import com.sbbsystems.statefun.tasks.graph.Task;
import com.sbbsystems.statefun.tasks.types.GroupEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public final class GroupTaskResolverWithMaxParallelismTests {
    private Group groupOfTenTasks;

    @BeforeEach
    public void setup() {
        groupOfTenTasks = Group.of("group-id");
        groupOfTenTasks.setItems(
                IntStream.range(0, 10)
                        .boxed()
                        .map(i -> Task.of("task-" + i, false))
                        .collect(Collectors.toList()));
    }

    @Test
    public void returns_full_list_when_max_parallelism_is_zero() {
        var state = PipelineFunctionState.newInstance();
        var submitter = GroupTaskResolverWithMaxParallelism.newInstance();

        var groupEntry = new GroupEntry();
        groupEntry.maxParallelism = 0;
        state.getGroupEntries().set("group-id", groupEntry);

        var resolvedTasks = submitter.resolveInitialTasks(groupOfTenTasks, state);

        assertThat(resolvedTasks.size()).isEqualTo(10);
        assertThat(state.getDeferredTasks("group-id").getDeferredTaskIds()).isEmpty();
    }

    @Test
    public void returns_full_list_when_max_parallelism_is_three() {
        var state = PipelineFunctionState.newInstance();
        var submitter = GroupTaskResolverWithMaxParallelism.newInstance();

        var groupEntry = new GroupEntry();
        groupEntry.maxParallelism = 3;
        state.getGroupEntries().set("group-id", groupEntry);

        var resolvedTasks = submitter.resolveInitialTasks(groupOfTenTasks, state);

        assertThat(resolvedTasks.size()).isEqualTo(3);
        assertThat(resolvedTasks.stream().map(Entry::getId).collect(Collectors.toList()))
                .containsExactlyInAnyOrder("task-0", "task-1", "task-2");
        assertThat(state.getDeferredTasks("group-id").getDeferredTaskIds().size()).isEqualTo(7);
    }

    @Test
    public void returns_single_next_task_when_deferred_task_exists() {
        var resultHandler = GroupTaskResolverWithMaxParallelism.newInstance();
        var state = PipelineFunctionState.newInstance();
        var deferredTaskList = new LinkedList<String>();
        deferredTaskList.add("task-1");
        deferredTaskList.add("task-2");
        state.cacheDeferredTasks("group-id", GroupDeferredTasksState.of(deferredTaskList));
        state.setEntries(MapOfEntries.from(Map.of(
                "task-1", Task.of("task-1", false),
                "task-2", Task.of("task-2", false))));

        var nextGroupTasks = resultHandler.resolveNextTasks("group-id", state);

        assertThat(nextGroupTasks.size()).isEqualTo(1);
        assertThat(nextGroupTasks.get(0).getId()).isEqualTo("task-1");
        var remainingDeferredTasks = state.getDeferredTasks("group-id").getDeferredTaskIds();
        assertThat(remainingDeferredTasks.size()).isEqualTo(1);
        assertThat(remainingDeferredTasks.get(0)).isEqualTo("task-2");
    }

    @Test
    public void returns_empty_list_for_next_tasks_when_no_deferred_task_exists() {
        var resultHandler = GroupTaskResolverWithMaxParallelism.newInstance();
        var state = PipelineFunctionState.newInstance();

        var nextGroupTasks = resultHandler.resolveNextTasks("group-id", state);

        assertThat(nextGroupTasks.size()).isEqualTo(0);
    }
}