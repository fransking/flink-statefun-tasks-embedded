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
package com.sbbsystems.statefun.tasks.types;

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.*;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class MessageTypesTests {
    @Test
    public void isType_returns_correct_type_for_task_request() {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskRequest").build();
        assertTrue(MessageTypes.isType(typedValue, TaskRequest.class));
    }

    @Test
    public void isType_returns_correct_type_for_task_result() {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskResult").build();
        assertTrue(MessageTypes.isType(typedValue, TaskResult.class));
    }

    @Test
    public void isType_returns_correct_type_for_task_exception() {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskException").build();
        assertTrue(MessageTypes.isType(typedValue, TaskException.class));
    }

    @Test
    public void isType_returns_correct_type_for_callback_signal() {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.CallbackSignal").build();
        assertTrue(MessageTypes.isType(typedValue, CallbackSignal.class));
    }

    @Test
    public void isType_returns_correct_type_for_results_batch() {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.ResultsBatch").build();
        assertTrue(MessageTypes.isType(typedValue, ResultsBatch.class));
    }

    @Test
    public void asType_returns_correct_type_for_task_request() throws InvalidMessageTypeException {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskRequest").build();
        assertInstanceOf(TaskRequest.class, MessageTypes.asType(typedValue, TaskRequest::parseFrom));
    }

    @Test
    public void asType_returns_correct_type_for_task_result() throws InvalidMessageTypeException {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskResult").build();
        assertInstanceOf(TaskResult.class, MessageTypes.asType(typedValue, TaskResult::parseFrom));
    }

    @Test
    public void asType_returns_correct_type_for_task_exception() throws InvalidMessageTypeException {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskException").build();
        assertInstanceOf(TaskException.class, MessageTypes.asType(typedValue, TaskException::parseFrom));
    }

    @Test
    public void asType_returns_correct_type_for_callback_signal() throws InvalidMessageTypeException {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.CallbackSignal").build();
        assertInstanceOf(CallbackSignal.class, MessageTypes.asType(typedValue, CallbackSignal::parseFrom));
    }

    @Test
    public void asType_returns_correct_type_for_results_batch() throws InvalidMessageTypeException {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.ResultsBatch").build();
        assertInstanceOf(ResultsBatch.class, MessageTypes.asType(typedValue, ResultsBatch::parseFrom));
    }

    @Test
    public void wrap_returns_typed_value_containing_argument() throws InvalidProtocolBufferException {
        var innerVal = TaskResult.newBuilder().setId("id").build();

        var typedVal = MessageTypes.wrap(innerVal);

        assertTrue(typedVal.getHasValue());
        assertEquals("io.statefun_tasks.types/statefun_tasks.TaskResult", typedVal.getTypename());
        assertEquals("id", TaskResult.parseFrom(typedVal.getValue()).getId());
    }

    @Test
    public void pack_any_packs_any_proto_message() {
        var any = MessageTypes.packAny(TaskRequest.getDefaultInstance());
        assertTrue(any.is(TaskRequest.class));
    }

    @Test
    public void pack_any_does_not_double_pack() {
        var any = MessageTypes.packAny(TaskRequest.getDefaultInstance());
        any = MessageTypes.packAny(any);
        assertTrue(any.is(TaskRequest.class));
    }

    // unpackAnyToValue tests

    @Test
    public void unpack_any_to_value_returns_value_when_any_contains_value() throws InvalidProtocolBufferException {
        var value = Value.newBuilder().setIntValue(42).build();
        var any = Any.pack(value);

        var result = MessageTypes.unpackAnyToValue(any);

        assertEquals(Value.KindCase.INT_VALUE, result.getKindCase());
        assertEquals(42, result.getIntValue());
    }

    @Test
    public void unpack_any_to_value_returns_tuple_value_when_any_contains_tuple_of_value() throws InvalidProtocolBufferException {
        var tuple = TupleOfValue.newBuilder()
                .addItems(Value.newBuilder().setIntValue(1).build())
                .addItems(Value.newBuilder().setIntValue(2).build())
                .build();
        var any = Any.pack(tuple);

        var result = MessageTypes.unpackAnyToValue(any);

        assertEquals(Value.KindCase.TUPLE_VALUE, result.getKindCase());
        assertEquals(tuple, result.getTupleValue());
    }

    @Test
    public void unpack_any_to_value_returns_array_value_when_any_contains_array_of_value() throws InvalidProtocolBufferException {
        var array = ArrayOfValue.newBuilder()
                .addItems(Value.newBuilder().setIntValue(1).build())
                .build();
        var any = Any.pack(array);

        var result = MessageTypes.unpackAnyToValue(any);

        assertEquals(Value.KindCase.ARRAY_VALUE, result.getKindCase());
        assertEquals(array, result.getArrayValue());
    }

    @Test
    public void unpack_any_to_value_returns_map_value_when_any_contains_map_of_string_to_value() throws InvalidProtocolBufferException {
        var map = MapOfStringToValue.newBuilder()
                .putItems("k", Value.newBuilder().setIntValue(99).build())
                .build();
        var any = Any.pack(map);

        var result = MessageTypes.unpackAnyToValue(any);

        assertEquals(Value.KindCase.MAP_VALUE, result.getKindCase());
        assertEquals(map, result.getMapValue());
    }

    @Test
    public void unpack_any_to_value_wraps_unknown_type_in_any_value() throws InvalidProtocolBufferException {
        var any = Any.pack(Int32Value.of(7));

        var result = MessageTypes.unpackAnyToValue(any);

        assertEquals(Value.KindCase.ANY_VALUE, result.getKindCase());
        assertTrue(result.getAnyValue().is(Int32Value.class));
    }

    // packValue tests

    @Test
    public void pack_value_returns_value_unchanged() throws InvalidProtocolBufferException {
        var value = Value.newBuilder().setStringValue("hello").build();

        var result = MessageTypes.packValue(value);

        assertEquals(value, result);
    }

    @Test
    public void pack_value_wraps_tuple_of_value() throws InvalidProtocolBufferException {
        var tuple = TupleOfValue.newBuilder()
                .addItems(Value.newBuilder().setIntValue(1).build())
                .build();

        var result = MessageTypes.packValue(tuple);

        assertEquals(Value.KindCase.TUPLE_VALUE, result.getKindCase());
        assertEquals(tuple, result.getTupleValue());
    }

    @Test
    public void pack_value_wraps_array_of_value() throws InvalidProtocolBufferException {
        var array = ArrayOfValue.newBuilder()
                .addItems(Value.newBuilder().setIntValue(1).build())
                .build();

        var result = MessageTypes.packValue(array);

        assertEquals(Value.KindCase.ARRAY_VALUE, result.getKindCase());
        assertEquals(array, result.getArrayValue());
    }

    @Test
    public void pack_value_wraps_map_of_string_to_value() throws InvalidProtocolBufferException {
        var map = MapOfStringToValue.newBuilder()
                .putItems("k", Value.newBuilder().setIntValue(5).build())
                .build();

        var result = MessageTypes.packValue(map);

        assertEquals(Value.KindCase.MAP_VALUE, result.getKindCase());
        assertEquals(map, result.getMapValue());
    }

    @Test
    public void pack_value_deconstructs_any_containing_value() throws InvalidProtocolBufferException {
        var inner = Value.newBuilder().setIntValue(42).build();
        var any = Any.pack(inner);

        var result = MessageTypes.packValue(any);

        assertEquals(Value.KindCase.INT_VALUE, result.getKindCase());
        assertEquals(42, result.getIntValue());
    }

    @Test
    public void pack_value_deconstructs_any_containing_array_of_value() throws InvalidProtocolBufferException {
        var array = ArrayOfValue.newBuilder()
                .addItems(Value.newBuilder().setIntValue(3).build())
                .build();
        var any = Any.pack(array);

        var result = MessageTypes.packValue(any);

        assertEquals(Value.KindCase.ARRAY_VALUE, result.getKindCase());
        assertEquals(array, result.getArrayValue());
    }

    @Test
    public void pack_value_wraps_any_containing_unknown_type_in_any_value() throws InvalidProtocolBufferException {
        var any = Any.pack(Int32Value.of(7));

        var result = MessageTypes.packValue(any);

        assertEquals(Value.KindCase.ANY_VALUE, result.getKindCase());
        assertTrue(result.getAnyValue().is(Int32Value.class));
    }

    @Test
    public void pack_value_wraps_non_value_message_in_any_value() throws InvalidProtocolBufferException {
        var message = Int32Value.of(100);

        var result = MessageTypes.packValue(message);

        assertEquals(Value.KindCase.ANY_VALUE, result.getKindCase());
        assertTrue(result.getAnyValue().is(Int32Value.class));
    }
}
