/*
 * Copyright [2026] [Frans King]
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
package com.sbbsystems.statefun.tasks.serialization;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import org.apache.flink.statefun.sdk.reqreply.generated.Address;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaskEntrySerializerTests {

    // ---- existing Any-based tests --------------------------------------------------------------

    @Test
    public void test_merge_with_returns_just_args_when_nothing_to_merge_with()
            throws StatefunTasksException {

        var taskEntry = new TaskEntry();
        var args = Address.getDefaultInstance();
        var packed = TaskEntrySerializer.of(taskEntry).mergeWith(args, MapOfStringToAny.getDefaultInstance());
        assertTrue(packed.is(Address.class));
    }

    @Test
    public void test_merge_with_single_proto_merges_into_args_taking_precedence()
            throws StatefunTasksException, InvalidProtocolBufferException {

        var taskEntry = new TaskEntry();

        taskEntry.request = MessageTypes.packAny(ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(TaskRequest.getDefaultInstance())))
                .build()).toByteArray();

        var args = Address.getDefaultInstance();
        var packed = TaskEntrySerializer.of(taskEntry).mergeWith(args, MapOfStringToAny.getDefaultInstance());
        assertTrue(packed.is(ArgsAndKwargs.class));

        var argsAndKwargs = packed.unpack(ArgsAndKwargs.class);
        assertThat(argsAndKwargs.getArgs().getItemsCount()).isEqualTo(2);
        assertTrue(argsAndKwargs.getArgs().getItems(0).is(Address.class));
        assertTrue(argsAndKwargs.getArgs().getItems(1).is(TaskRequest.class));
    }

    @Test
    public void test_merge_with_tuple_of_any_merges_into_args_taking_precedence()
            throws StatefunTasksException, InvalidProtocolBufferException {

        var taskEntry = new TaskEntry();

        taskEntry.request = MessageTypes.packAny(ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(TaskRequest.getDefaultInstance())))
                .build()).toByteArray();

        var args = TupleOfAny.newBuilder()
                .addItems(Any.pack(Address.getDefaultInstance()))
                .addItems(Any.pack(Address.getDefaultInstance()))
                .build();

        var packed = TaskEntrySerializer.of(taskEntry).mergeWith(args, MapOfStringToAny.getDefaultInstance());
        assertTrue(packed.is(ArgsAndKwargs.class));

        var argsAndKwargs = packed.unpack(ArgsAndKwargs.class);
        assertThat(argsAndKwargs.getArgs().getItemsCount()).isEqualTo(3);
        assertTrue(argsAndKwargs.getArgs().getItems(0).is(Address.class));
        assertTrue(argsAndKwargs.getArgs().getItems(1).is(Address.class));
        assertTrue(argsAndKwargs.getArgs().getItems(2).is(TaskRequest.class));
    }

    @Test
    public void test_merge_with_array_of_any_merges_into_args_taking_precedence()
            throws StatefunTasksException, InvalidProtocolBufferException {

        var taskEntry = new TaskEntry();

        taskEntry.request = MessageTypes.packAny(ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(TaskRequest.getDefaultInstance())))
                .build()).toByteArray();

        var args = ArrayOfAny.newBuilder()
                .addItems(Any.pack(Address.getDefaultInstance()))
                .addItems(Any.pack(Address.getDefaultInstance()))
                .build();

        var packed = TaskEntrySerializer.of(taskEntry).mergeWith(args, MapOfStringToAny.getDefaultInstance());
        assertTrue(packed.is(ArgsAndKwargs.class));

        var argsAndKwargs = packed.unpack(ArgsAndKwargs.class);

        // the array is not flattened into the args but added as a single item
        assertThat(argsAndKwargs.getArgs().getItemsCount()).isEqualTo(2);
        assertTrue(argsAndKwargs.getArgs().getItems(0).is(ArrayOfAny.class));
        assertTrue(argsAndKwargs.getArgs().getItems(1).is(TaskRequest.class));
    }

    @Test
    public void test_merge_with_merges_kwargs_taking_precedence()
            throws StatefunTasksException, InvalidProtocolBufferException {

        var taskEntry = new TaskEntry();

        taskEntry.request = MessageTypes.packAny(ArgsAndKwargs.newBuilder()
                .setKwargs(MapOfStringToAny.newBuilder()
                        .putItems("item2", Any.pack(Address.getDefaultInstance())))
                .build()).toByteArray();

        var kwargsToMerge = MapOfStringToAny.newBuilder()
                .putItems("item1", Any.pack(Address.getDefaultInstance()))
                .putItems("item2", Any.pack(TaskRequest.getDefaultInstance()))
                .build();

        var packed = TaskEntrySerializer.of(taskEntry).mergeWith(TupleOfAny.getDefaultInstance(), kwargsToMerge);
        var argsAndKwargs = packed.unpack(ArgsAndKwargs.class);

        assertThat(argsAndKwargs.getKwargs().getItemsCount()).isEqualTo(2);
        assertTrue(argsAndKwargs.getKwargs().getItemsOrThrow("item1").is(Address.class));
        assertTrue(argsAndKwargs.getKwargs().getItemsOrThrow("item2").is(TaskRequest.class));
    }

    // ---- Value-based tests (expected to fail until TaskEntrySerializer supports ValueArgsAndKwargs) ---

    @Test
    public void test_merge_with_value_returns_just_value_args_when_nothing_to_merge_with()
            throws StatefunTasksException {

        var taskEntry = new TaskEntry();
        var args = Value.newBuilder().setStringValue("hello").build();

        var packed = TaskEntrySerializer.of(taskEntry).mergeValueWith(args);

        assertTrue(packed.is(Value.class));
    }

    @Test
    public void test_merge_with_single_value_merges_into_args_taking_precedence()
            throws StatefunTasksException, InvalidProtocolBufferException {

        var taskEntry = new TaskEntry();
        var taskArg = Value.newBuilder().setIntValue(42).build();

        taskEntry.request = MessageTypes.packAny(ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(taskArg))
                .build()).toByteArray();

        var incomingArg = Value.newBuilder().setStringValue("incoming").build();

        var packed = TaskEntrySerializer.of(taskEntry).mergeValueWith(incomingArg);
        assertTrue(packed.is(ValueArgsAndKwargs.class));

        var valueArgsAndKwargs = packed.unpack(ValueArgsAndKwargs.class);
        assertThat(valueArgsAndKwargs.getArgs().getItemsCount()).isEqualTo(2);
        assertThat(valueArgsAndKwargs.getArgs().getItems(0)).isEqualTo(incomingArg);
        assertThat(valueArgsAndKwargs.getArgs().getItems(1)).isEqualTo(taskArg);
    }

    @Test
    public void test_merge_with_single_proto_merges_into_args_taking_precedence_using_value_types()
            throws StatefunTasksException, InvalidProtocolBufferException {

        var taskEntry = new TaskEntry();

        taskEntry.request = MessageTypes.packAny(ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setAnyValue(Any.pack(TaskRequest.getDefaultInstance()))))
                .build()).toByteArray();

        var args = Address.getDefaultInstance();
        var packed = TaskEntrySerializer.of(taskEntry).mergeValueWith(args, MapOfStringToValue.getDefaultInstance());
        assertTrue(packed.is(ValueArgsAndKwargs.class));

        var valueArgsAndKwargs = packed.unpack(ValueArgsAndKwargs.class);
        assertThat(valueArgsAndKwargs.getArgs().getItemsCount()).isEqualTo(2);
        assertTrue(valueArgsAndKwargs.getArgs().getItems(0).getAnyValue().is(Address.class));
        assertTrue(valueArgsAndKwargs.getArgs().getItems(1).getAnyValue().is(TaskRequest.class));
    }

    @Test
    public void test_merge_with_tuple_of_value_merges_into_args_taking_precedence()
            throws StatefunTasksException, InvalidProtocolBufferException {

        var taskEntry = new TaskEntry();
        var taskArg = Value.newBuilder().setIntValue(42).build();

        taskEntry.request = MessageTypes.packAny(ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(taskArg))
                .build()).toByteArray();

        var args = TupleOfValue.newBuilder()
                .addItems(Value.newBuilder().setStringValue("first").build())
                .addItems(Value.newBuilder().setStringValue("second").build())
                .build();

        var packed = TaskEntrySerializer.of(taskEntry).mergeValueWith(args);
        assertTrue(packed.is(ValueArgsAndKwargs.class));

        var valueArgsAndKwargs = packed.unpack(ValueArgsAndKwargs.class);
        assertThat(valueArgsAndKwargs.getArgs().getItemsCount()).isEqualTo(3);
        assertThat(valueArgsAndKwargs.getArgs().getItems(0)).isEqualTo(args.getItems(0));
        assertThat(valueArgsAndKwargs.getArgs().getItems(1)).isEqualTo(args.getItems(1));
        assertThat(valueArgsAndKwargs.getArgs().getItems(2)).isEqualTo(taskArg);
    }

    @Test
    public void test_merge_with_array_of_value_merges_into_args_taking_precedence()
            throws StatefunTasksException, InvalidProtocolBufferException {

        var taskEntry = new TaskEntry();
        var taskArg = Value.newBuilder().setIntValue(42).build();

        taskEntry.request = MessageTypes.packAny(ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(taskArg))
                .build()).toByteArray();

        var args = ArrayOfValue.newBuilder()
                .addItems(Value.newBuilder().setStringValue("first").build())
                .addItems(Value.newBuilder().setStringValue("second").build())
                .build();

        var packed = TaskEntrySerializer.of(taskEntry).mergeValueWith(args);
        assertTrue(packed.is(ValueArgsAndKwargs.class));

        var valueArgsAndKwargs = packed.unpack(ValueArgsAndKwargs.class);

        // the array is not flattened into the args but added as a single item
        assertThat(valueArgsAndKwargs.getArgs().getItemsCount()).isEqualTo(2);
        assertThat(valueArgsAndKwargs.getArgs().getItems(0).getArrayValue()).isEqualTo(args);
        assertThat(valueArgsAndKwargs.getArgs().getItems(1)).isEqualTo(taskArg);
    }

    @Test
    public void test_merge_with_map_of_string_to_value_merges_into_args_taking_precedence()
            throws StatefunTasksException, InvalidProtocolBufferException {

        var taskEntry = new TaskEntry();
        var taskArg = Value.newBuilder().setIntValue(42).build();

        taskEntry.request = MessageTypes.packAny(ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(taskArg))
                .build()).toByteArray();

        var args = MapOfStringToValue.newBuilder()
                .putItems("first", Value.newBuilder().setStringValue("first").build())
                .putItems("second", Value.newBuilder().setStringValue("second").build())
                .build();

        var packed = TaskEntrySerializer.of(taskEntry).mergeValueWith(args);
        assertTrue(packed.is(ValueArgsAndKwargs.class));

        var valueArgsAndKwargs = packed.unpack(ValueArgsAndKwargs.class);

        // the array is not flattened into the args but added as a single item
        assertThat(valueArgsAndKwargs.getArgs().getItemsCount()).isEqualTo(2);
        assertThat(valueArgsAndKwargs.getArgs().getItems(0).getMapValue()).isEqualTo(args);
        assertThat(valueArgsAndKwargs.getArgs().getItems(1)).isEqualTo(taskArg);
    }

    @Test
    public void test_merge_with_value_kwargs_merges_kwargs_incoming_takes_precedence()
            throws StatefunTasksException, InvalidProtocolBufferException {

        var taskEntry = new TaskEntry();

        taskEntry.request = MessageTypes.packAny(ValueArgsAndKwargs.newBuilder()
                .setKwargs(MapOfStringToValue.newBuilder()
                        .putItems("item2", Value.newBuilder().setIntValue(1).build()))
                .build()).toByteArray();

        var kwargsToMerge = MapOfStringToValue.newBuilder()
                .putItems("item1", Value.newBuilder().setStringValue("from_incoming").build())
                .putItems("item2", Value.newBuilder().setBoolValue(true).build())
                .build();

        var packed = TaskEntrySerializer.of(taskEntry).mergeValueWith(
                Value.newBuilder().setTupleValue(TupleOfValue.getDefaultInstance()).build(),
                kwargsToMerge);

        assertTrue(packed.is(ValueArgsAndKwargs.class));
        var valueArgsAndKwargs = packed.unpack(ValueArgsAndKwargs.class);

        assertThat(valueArgsAndKwargs.getKwargs().getItemsCount()).isEqualTo(2);
        assertThat(valueArgsAndKwargs.getKwargs().getItemsOrThrow("item1").getStringValue()).isEqualTo("from_incoming");
        assertThat(valueArgsAndKwargs.getKwargs().getItemsOrThrow("item2").getBoolValue()).isTrue();
    }
}
