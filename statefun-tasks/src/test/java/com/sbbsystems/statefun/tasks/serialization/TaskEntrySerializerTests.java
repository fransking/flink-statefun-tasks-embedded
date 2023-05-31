package com.sbbsystems.statefun.tasks.serialization;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.MapOfStringToAny;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import org.apache.flink.statefun.sdk.reqreply.generated.Address;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaskEntrySerializerTests {

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
}
