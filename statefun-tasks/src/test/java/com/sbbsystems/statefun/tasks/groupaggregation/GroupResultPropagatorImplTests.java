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

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.sbbsystems.statefun.tasks.graph.GraphTestUtils.fromTemplate;
import static com.sbbsystems.statefun.tasks.graph.GraphTestUtils.getGroupContainingTask;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public final class GroupResultPropagatorImplTests {
    private GroupResultPropagatorImpl propagator;


    @BeforeEach
    public void setup() {
        propagator = GroupResultPropagatorImpl.newInstance(GroupResultAggregatorImpl.newInstance());
    }

    @Test
    public void will_return_array_of_arrays_for_nested_group_results() {
        var innerGrp = List.of(
                List.of("a")
        );
        var outerGrp = List.of(
                List.of(innerGrp)
        );
        var template = List.of(outerGrp);
        var graph = fromTemplate(template);
        var groupEntry = getGroupContainingTask("a", graph);
        var group = graph.getGroup(groupEntry.groupId);
        graph.markComplete(group);

        var state = PipelineFunctionState.newInstance();
        var taskResult = TaskResult.newBuilder().setResult(Any.pack(Int32Value.of(1))).build();
        state.getIntermediateGroupResults().set("a", TaskResultOrException.newBuilder().setTaskResult(taskResult).build());

        var result = propagator.propagateGroupResult(group, graph, state, "invoc-id");

        assertThat(result.hasTaskResult()).isTrue();
        assertThat(result.getTaskResult().getResult()).isEqualTo(
                Any.pack(
                        TupleOfAny.newBuilder()
                                .addItems(
                                        Any.pack(
                                                TupleOfAny.newBuilder()
                                                        .addItems(
                                                                Any.pack(Int32Value.of(1))
                                                        ).build()
                                        ))
                                .build())
        );
    }

    @Test
    public void will_return_array_of_arrays_for_nested_group_containing_multiple_groups() {
        var innerGrp1 = List.of(
                List.of("a")
        );
        var innerGrp2 = List.of(
                List.of("b")
        );

        var outerGrp = List.of(
                List.of(innerGrp1),
                List.of(innerGrp2)
        );
        var template = List.of(outerGrp);
        var graph = fromTemplate(template);
        var groupEntry = getGroupContainingTask("a", graph);
        var group1 = graph.getGroup(groupEntry.groupId);
        graph.markComplete(graph.getTask("a"));
        graph.markComplete(graph.getTask("b"));

        var state = PipelineFunctionState.newInstance();
        state.getIntermediateGroupResults().set("a", TaskResultOrException.newBuilder().setTaskResult(
                TaskResult.newBuilder().setResult(Any.pack(Int32Value.of(1))).build()
        ).build());
        state.getIntermediateGroupResults().set("b", TaskResultOrException.newBuilder().setTaskResult(
                TaskResult.newBuilder().setResult(Any.pack(Int32Value.of(2))).build()
        ).build());

        var result = propagator.propagateGroupResult(group1, graph, state, "invoc-id");

        assertThat(result.hasTaskResult()).isTrue();
        assertThat(result.getTaskResult().getResult()).isEqualTo(
                Any.pack(
                        TupleOfAny.newBuilder()
                                .addItems(
                                        Any.pack(
                                                TupleOfAny.newBuilder()
                                                        .addItems(
                                                                Any.pack(Int32Value.of(1))
                                                        )
                                                        .build()
                                        ))
                                .addItems(
                                        Any.pack(
                                                TupleOfAny.newBuilder()
                                                        .addItems(
                                                                Any.pack(Int32Value.of(2))
                                                        )
                                                        .build()
                                        )
                                )
                                .build())
        );
    }

    @Test
    public void will_return_array_of_arrays_for_nested_group_containing_mix_of_tasks_and_groups() {
        var innerGrp = List.of(
                List.of("a")
        );

        var outerGrp = List.of(
                List.of(innerGrp),
                List.of("b")
        );
        var template = List.of(outerGrp);
        var graph = fromTemplate(template);
        var groupEntry = getGroupContainingTask("a", graph);
        var group = graph.getGroup(groupEntry.groupId);
        graph.markComplete(graph.getTask("a"));
        graph.markComplete(graph.getTask("b"));

        var state = PipelineFunctionState.newInstance();
        state.getIntermediateGroupResults().set("a", TaskResultOrException.newBuilder().setTaskResult(
                TaskResult.newBuilder().setResult(Any.pack(Int32Value.of(1))).build()
        ).build());
        state.getIntermediateGroupResults().set("b", TaskResultOrException.newBuilder().setTaskResult(
                TaskResult.newBuilder().setResult(Any.pack(Int32Value.of(2))).build()
        ).build());

        var result = propagator.propagateGroupResult(group, graph, state, "invoc-id");

        assertThat(result.hasTaskResult()).isTrue();
        assertThat(result.getTaskResult().getResult()).isEqualTo(
                Any.pack(
                        TupleOfAny.newBuilder()
                                .addItems(
                                        Any.pack(
                                                TupleOfAny.newBuilder()
                                                        .addItems(
                                                                Any.pack(Int32Value.of(1))
                                                        )
                                                        .build()
                                        ))
                                .addItems(
                                        Any.pack(
                                                Int32Value.of(2)
                                        )
                                )
                                .build())
        );
    }

    @Test
    public void will_find_results_when_group_contains_chains() {
        var grp = List.of(
                List.of("a", "b", "c")
        );
        var template = List.of(grp);
        var graph = fromTemplate(template);
        var groupEntry = getGroupContainingTask("a", graph);
        var group = graph.getGroup(groupEntry.groupId);
        graph.markComplete(graph.getTask("a"));
        graph.markComplete(graph.getTask("b"));
        graph.markComplete(graph.getTask("c"));

        var state = PipelineFunctionState.newInstance();
        state.getIntermediateGroupResults().set("a", TaskResultOrException.newBuilder().setTaskResult(
                TaskResult.newBuilder().setResult(Any.pack(Int32Value.of(1))).build()
        ).build());

        var result = propagator.propagateGroupResult(group, graph, state, "invoc-id");

        assertThat(result.hasTaskResult()).isTrue();
        assertThat(result.getTaskResult().getResult()).isEqualTo(
                Any.pack(
                        TupleOfAny.newBuilder()
                                .addItems(Any.pack(Int32Value.of(1)))
                                .build())
        );
    }
}