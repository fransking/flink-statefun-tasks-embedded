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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.generated.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class GroupResultAggregatorTests {
    private GroupResultAggregator resultAggregator;

    // ---- legacy helpers ------------------------------------------------------------------------

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

    // ---- value mode helpers --------------------------------------------------------------------

    private TaskResultOrException wrapValueResult(Value value) {
        return wrapValueResult(value, Any.pack(NoneValue.getDefaultInstance()));
    }

    private TaskResultOrException wrapValueResult(Value value, Any state) {
        var taskResult = TaskResult.newBuilder()
                .setResult(Any.pack(value))
                .setState(state)
                .build();
        return TaskResultOrException.newBuilder().setTaskResult(taskResult).build();
    }

    private Any mapToValueProto(Map<String, Value> map) {
        return Any.pack(MapOfStringToValue.newBuilder().putAllItems(map).build());
    }

    // ---- setup ---------------------------------------------------------------------------------

    @BeforeEach
    public void setup() {
        this.resultAggregator = GroupResultAggregator.newInstance();
    }

    // ---- returns tuple / array of results in order ---------------------------------------------

    @Test
    public void returns_tuple_of_any_in_correct_order_when_results_are_not_errors() throws InvalidProtocolBufferException {
        var groupResults = Stream.of(
                wrapResult(Int32Value.of(1)),
                wrapResult(Int32Value.of(2)),
                wrapResult(Int32Value.of(3))
        );

        var aggregatedResult = resultAggregator.aggregateResults("group-id", "invocation-id", groupResults, false, false, true);

        assertThat(aggregatedResult.hasTaskException()).isFalse();
        var resultAny = aggregatedResult.getTaskResult().getResult();
        assertThat(resultAny.is(ArrayOfAny.class)).isTrue();
        assertThat(resultAny.unpack(ArrayOfAny.class).getItemsList()).containsExactly(
                Any.pack(Int32Value.of(1)),
                Any.pack(Int32Value.of(2)),
                Any.pack(Int32Value.of(3))
        );
    }

    @Test
    public void returns_array_of_value_in_correct_order_when_results_are_not_errors_value_mode() throws InvalidProtocolBufferException {
        var v1 = Value.newBuilder().setIntValue(1).build();
        var v2 = Value.newBuilder().setIntValue(2).build();
        var v3 = Value.newBuilder().setIntValue(3).build();
        var groupResults = Stream.of(
                wrapValueResult(v1),
                wrapValueResult(v2),
                wrapValueResult(v3)
        );

        var aggregatedResult = resultAggregator.aggregateResults("group-id", "invocation-id", groupResults, false, false, false);

        assertThat(aggregatedResult.hasTaskException()).isFalse();
        var resultAny = aggregatedResult.getTaskResult().getResult();
        assertThat(resultAny.is(ArrayOfValue.class)).isTrue();
        var arrayValue = resultAny.unpack(ArrayOfValue.class);
        assertThat(arrayValue.getItemsList()).containsExactly(v1, v2, v3);
    }

    // ---- exception with returnExceptions=false -------------------------------------------------

    @Test
    public void returns_exception_when_results_list_contains_an_exception_with_return_exceptions_false() {
        var groupResults = Stream.of(
                wrapResult(Int32Value.of(1)),
                wrapException("exc-task-id", "SomeExceptionType", "Something went wrong"),
                wrapResult(Int32Value.of(3))
        );

        var aggregatedResult = resultAggregator.aggregateResults("group-id", "invocation-id", groupResults, true, false, true);

        assertThat(aggregatedResult.hasTaskException()).isTrue();
        assertThat(aggregatedResult.getTaskException().getExceptionMessage())
                .isEqualTo("exc-task-id, SomeExceptionType, Something went wrong");
    }

    @Test
    public void returns_exception_when_results_list_contains_an_exception_with_return_exceptions_false_value_mode() {
        var groupResults = Stream.of(
                wrapValueResult(Value.newBuilder().setIntValue(1).build()),
                wrapException("exc-task-id", "SomeExceptionType", "Something went wrong"),
                wrapValueResult(Value.newBuilder().setIntValue(3).build())
        );

        var aggregatedResult = resultAggregator.aggregateResults("group-id", "invocation-id", groupResults, true, false, false);

        assertThat(aggregatedResult.hasTaskException()).isTrue();
        assertThat(aggregatedResult.getTaskException().getExceptionMessage())
                .isEqualTo("exc-task-id, SomeExceptionType, Something went wrong");
    }

    // ---- exception with returnExceptions=true --------------------------------------------------

    @Test
    public void returns_list_including_exception_when_results_list_contains_an_exception_with_return_exceptions_true() throws InvalidProtocolBufferException {
        var wrappedException = wrapException("exc-task-id", "SomeExceptionType", "Something went wrong");
        var groupResults = Stream.of(
                wrapResult(Int32Value.of(1)),
                wrappedException,
                wrapResult(Int32Value.of(3))
        );

        var aggregatedResult = resultAggregator.aggregateResults("group-id", "invocation-id", groupResults, true, true, true);

        assertThat(aggregatedResult.hasTaskException()).isFalse();
        var resultTupleOfAny = aggregatedResult.getTaskResult().getResult().unpack(ArrayOfAny.class);
        assertThat(resultTupleOfAny.getItemsList()).containsExactly(
                Any.pack(Int32Value.of(1)),
                Any.pack(wrappedException.getTaskException()),
                Any.pack(Int32Value.of(3))
        );
    }

    @Test
    public void returns_list_including_exception_when_results_list_contains_an_exception_with_return_exceptions_true_value_mode() throws InvalidProtocolBufferException {
        var v1 = Value.newBuilder().setIntValue(1).build();
        var v3 = Value.newBuilder().setIntValue(3).build();
        var wrappedException = wrapException("exc-task-id", "SomeExceptionType", "Something went wrong");
        var groupResults = Stream.of(
                wrapValueResult(v1),
                wrappedException,
                wrapValueResult(v3)
        );

        var aggregatedResult = resultAggregator.aggregateResults("group-id", "invocation-id", groupResults, true, true, false);

        assertThat(aggregatedResult.hasTaskException()).isFalse();
        var arrayValue = aggregatedResult.getTaskResult().getResult().unpack(ArrayOfValue.class);
        assertThat(arrayValue.getItemsCount()).isEqualTo(3);
        assertThat(arrayValue.getItems(0)).isEqualTo(v1);
        assertThat(arrayValue.getItems(1).getAnyValue().is(TaskException.class)).isTrue();
        assertThat(arrayValue.getItems(1).getAnyValue().unpack(TaskException.class))
                .isEqualTo(wrappedException.getTaskException());
        assertThat(arrayValue.getItems(2)).isEqualTo(v3);
    }

    // ---- state merging -------------------------------------------------------------------------

    @Test
    public void merges_state_when_each_task_returns_state_as_map_of_string_to_any() throws InvalidProtocolBufferException {
        var groupResults = Stream.of(
                wrapResult(NoneValue.getDefaultInstance(), mapToProto(Map.of("A", Int32Value.of(1)))),
                wrapResult(NoneValue.getDefaultInstance(), mapToProto(Map.of("B", Int32Value.of(2)))),
                wrapResult(NoneValue.getDefaultInstance(), mapToProto(Map.of("B", Int32Value.of(3))))
        );

        var aggregatedResult = resultAggregator.aggregateResults("group-id", "invocation-id", groupResults, false, false, true);

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
    public void merges_state_when_each_task_returns_state_as_map_of_string_to_value_value_mode() throws InvalidProtocolBufferException {
        var vA = Value.newBuilder().setIntValue(1).build();
        var vB1 = Value.newBuilder().setIntValue(2).build();
        var vB2 = Value.newBuilder().setIntValue(3).build();
        var none = Value.newBuilder().setAnyValue(Any.pack(NoneValue.getDefaultInstance())).build();
        var groupResults = Stream.of(
                wrapValueResult(none, mapToValueProto(Map.of("A", vA))),
                wrapValueResult(none, mapToValueProto(Map.of("B", vB1))),
                wrapValueResult(none, mapToValueProto(Map.of("B", vB2)))
        );

        var aggregatedResult = resultAggregator.aggregateResults("group-id", "invocation-id", groupResults, false, false, false);

        var aggregatedState = aggregatedResult.getTaskResult().getState();
        assertThat(aggregatedState.is(MapOfStringToValue.class)).isTrue();
        var stateMap = aggregatedState.unpack(MapOfStringToValue.class).getItemsMap();
        assertThat(stateMap).containsExactlyInAnyOrderEntriesOf(Map.of("A", vA, "B", vB1));
    }

    // ---- non-map state falls through unaggregated ----------------------------------------------

    @Test
    public void uses_first_results_state_when_all_states_are_not_maps_of_string_to_any() {
        var groupResults = Stream.of(
                wrapResult(NoneValue.getDefaultInstance(), Any.pack(Int32Value.of(1))),
                wrapResult(NoneValue.getDefaultInstance(), mapToProto(Map.of("A", Int32Value.of(1))))
        );

        var aggregatedResult = resultAggregator.aggregateResults("group-id", "invocation-id", groupResults, true, true, true);

        var aggregatedState = aggregatedResult.getTaskResult().getState();
        assertThat(aggregatedState).isEqualTo(Any.pack(Int32Value.of(1)));
    }

    @Test
    public void uses_first_results_state_when_all_states_are_not_maps_of_string_to_value_value_mode() {
        var none = Value.newBuilder().setAnyValue(Any.pack(NoneValue.getDefaultInstance())).build();
        var vA = Value.newBuilder().setIntValue(1).build();
        var groupResults = Stream.of(
                wrapValueResult(none, Any.pack(Int32Value.of(1))),
                wrapValueResult(none, mapToValueProto(Map.of("A", vA)))
        );

        var aggregatedResult = resultAggregator.aggregateResults("group-id", "invocation-id", groupResults, true, true, false);

        var aggregatedState = aggregatedResult.getTaskResult().getState();
        assertThat(aggregatedState).isEqualTo(Any.pack(Int32Value.of(1)));
    }
}
