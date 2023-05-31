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

package com.sbbsystems.statefun.tasks.resultaggregation;

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.groupaggregation.GroupResultAggregatorImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class GroupResultAggregatorImplTests {
    private GroupResultAggregatorImpl resultAggregator;

    private TaskResultOrException wrapResult(Message result) {
        return wrapResult(result, Any.pack(NoneValue.getDefaultInstance()));
    }

    private TaskResultOrException wrapResult(Message result, Any state) {
        var taskResult = TaskResult.newBuilder()
                .setResult(Any.pack(result))
                .setState(state)
                .build();
        return TaskResultOrException.newBuilder().setTaskResult(taskResult).build();
    }

    private TaskResultOrException wrapException(String id, String type, String message) {
        var taskException = TaskException.newBuilder()
                .setId(id)
                .setExceptionType(type)
                .setExceptionMessage(message)
                .build();
        return TaskResultOrException.newBuilder().setTaskException(taskException).build();
    }

    private Any mapToProto(Map<String, Message> map) {
        var mapOfStringToAny = new HashMap<String, Any>();
        map.forEach((key, value) -> mapOfStringToAny.put(key, Any.pack(value)));
        return Any.pack(
                MapOfStringToAny.newBuilder()
                        .putAllItems(mapOfStringToAny)
                        .build()
        );
    }

    @BeforeEach
    public void setup() {
        this.resultAggregator = GroupResultAggregatorImpl.newInstance();
    }

    @Test
    public void returns_tuple_of_any_in_correct_order_when_results_are_not_errors() throws InvalidProtocolBufferException {
        var groupResults = List.of(
                wrapResult(Int32Value.of(1)),
                wrapResult(Int32Value.of(2)),
                wrapResult(Int32Value.of(3))
        );

        var aggregatedResult = resultAggregator.aggregateResult("group-id", "invocation-id", groupResults, false);

        assertThat(aggregatedResult.hasTaskException()).isFalse();
        var resultAny = aggregatedResult.getTaskResult().getResult();
        assertThat(resultAny.is(TupleOfAny.class)).isTrue();
        assertThat(resultAny.unpack(TupleOfAny.class).getItemsList()).containsExactly(
                Any.pack(Int32Value.of(1)),
                Any.pack(Int32Value.of(2)),
                Any.pack(Int32Value.of(3))
        );
    }

    @Test
    public void returns_exception_when_results_list_contains_an_exception_with_return_exceptions_false() {
        var groupResults = List.of(
                wrapResult(Int32Value.of(1)),
                wrapException("exc-task-id", "SomeExceptionType", "Something went wrong"),
                wrapResult(Int32Value.of(3))
        );

        var aggregatedResult = resultAggregator.aggregateResult("group-id", "invocation-id", groupResults, false);

        assertThat(aggregatedResult.hasTaskException()).isTrue();
        assertThat(aggregatedResult.getTaskException().getExceptionMessage())
                .isEqualTo("exc-task-id, SomeExceptionType, Something went wrong");
    }

    @Test
    public void returns_list_including_exception_when_results_list_contains_an_exception_with_return_exceptions_true() throws InvalidProtocolBufferException {
        var wrappedException = wrapException("exc-task-id", "SomeExceptionType", "Something went wrong");
        var groupResults = List.of(
                wrapResult(Int32Value.of(1)),
                wrappedException,
                wrapResult(Int32Value.of(3))
        );

        var aggregatedResult = resultAggregator.aggregateResult("group-id", "invocation-id", groupResults, true);

        assertThat(aggregatedResult.hasTaskException()).isFalse();
        var resultTupleOfAny = aggregatedResult.getTaskResult().getResult().unpack(TupleOfAny.class);
        assertThat(resultTupleOfAny.getItemsList()).containsExactly(
                Any.pack(Int32Value.of(1)),
                Any.pack(wrappedException.getTaskException()),
                Any.pack(Int32Value.of(3))
        );
    }

    @Test
    public void merges_state_when_each_task_returns_state_as_map_of_string_to_any() throws InvalidProtocolBufferException {
        var groupResults = List.of(
                wrapResult(NoneValue.getDefaultInstance(), mapToProto(Map.of("A", Int32Value.of(1)))),
                wrapResult(NoneValue.getDefaultInstance(), mapToProto(Map.of("B", Int32Value.of(2))))
        );

        var aggregatedResult = resultAggregator.aggregateResult("group-id", "invocation-id", groupResults, true);

        var aggregatedState = aggregatedResult.getTaskResult().getState();
        assertThat(aggregatedState.is(MapOfStringToAny.class)).isTrue();
        var stateMap = aggregatedState.unpack(MapOfStringToAny.class).getItemsMap();
        assertThat(stateMap).containsExactlyInAnyOrderEntriesOf(
                Map.of(
                        "A", Any.pack(Int32Value.of(1)),
                        "B", Any.pack(Int32Value.of(2))
                )
        );
    }

    @Test
    public void uses_first_results_state_when_all_states_are_not_maps_of_string_to_any() {
        var groupResults = List.of(
                wrapResult(NoneValue.getDefaultInstance(), Any.pack(Int32Value.of(1))),
                wrapResult(NoneValue.getDefaultInstance(), mapToProto(Map.of("A", Int32Value.of(1))))
        );

        var aggregatedResult = resultAggregator.aggregateResult("group-id", "invocation-id", groupResults, true);

        var aggregatedState = aggregatedResult.getTaskResult().getState();
        assertThat(aggregatedState).isEqualTo(Any.pack(Int32Value.of(1)));
    }
}
