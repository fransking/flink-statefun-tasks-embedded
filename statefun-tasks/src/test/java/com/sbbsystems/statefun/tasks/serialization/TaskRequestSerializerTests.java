/*
 * Copyright [2023] [Frans King, Luke Ashworth]
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
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.*;
import org.junit.jupiter.api.Test;

import java.util.Objects;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TaskRequestSerializerTests {

    // ---- Any-based tests -----------------------------------------------------------------------

    @Test
    public void get_args_and_kwargs_from_task_request_containing_single_proto()
            throws StatefunTasksException {

        var taskRequest = TaskRequest
                .newBuilder()
                .setRequest(Any.pack(Pipeline.getDefaultInstance()))
                .build();

        var argsAndKwargs = TaskRequestSerializer.of(taskRequest).getArgsAndKwargsSerializer();

        assertThat(Objects.requireNonNull(argsAndKwargs)).isNotNull();
        assertThat(argsAndKwargs.slice(1)).isInstanceOf(ArgsAndKwargs.class);
        assertThat(argsAndKwargs.getArg(0).is(Pipeline.class)).isTrue();
    }

    @Test
    public void get_args_and_kwargs_from_task_request_containing_multiple_args()
            throws StatefunTasksException {

        var args = TupleOfAny
                .newBuilder()
                .addItems(Any.pack(Pipeline.getDefaultInstance()))
                .addItems(Any.pack(StringValue.of("Test")));

        var taskRequest = TaskRequest
                .newBuilder()
                .setRequest(Any.pack(ArgsAndKwargs
                        .newBuilder()
                        .setArgs(args)
                        .build()))
                .build();

        var argsAndKwargs = TaskRequestSerializer.of(taskRequest).getArgsAndKwargsSerializer();

        assertThat(Objects.requireNonNull(argsAndKwargs)).isNotNull();
        assertThat(argsAndKwargs.getArg(0).is(Pipeline.class)).isTrue();
        assertThat(argsAndKwargs.slice(1)).isInstanceOf(ArgsAndKwargs.class);
        assertThat(argsAndKwargs.slice(1).getArgs().getItems(0).is(StringValue.class)).isTrue();
    }

    // ---- Value-based tests ---------------------------------------------------------------------

    @Test
    public void get_value_args_and_kwargs_from_task_request_containing_single_proto()
            throws StatefunTasksException {

        var taskRequest = TaskRequest
                .newBuilder()
                .setRequest(Any.pack(Pipeline.getDefaultInstance()))
                .build();

        var argsAndKwargs = TaskRequestSerializer.of(taskRequest).getValueArgsAndKwargsSerializer();

        assertThat(Objects.requireNonNull(argsAndKwargs)).isNotNull();
        assertThat(argsAndKwargs.slice(1)).isInstanceOf(ValueArgsAndKwargs.class);
        assertThat(argsAndKwargs.getArg(0).getAnyValue().is(Pipeline.class)).isTrue();
    }

    @Test
    public void get_value_args_and_kwargs_from_task_request_containing_multiple_args()
            throws StatefunTasksException {

        var firstArg = Value.newBuilder().setAnyValue(Any.pack(Pipeline.getDefaultInstance())).build();
        var secondArg = Value.newBuilder().setStringValue("Test").build();

        var args = TupleOfValue
                .newBuilder()
                .addItems(firstArg)
                .addItems(secondArg);

        var taskRequest = TaskRequest
                .newBuilder()
                .setRequest(Any.pack(ValueArgsAndKwargs
                        .newBuilder()
                        .setArgs(args)
                        .build()))
                .build();

        var argsAndKwargs = TaskRequestSerializer.of(taskRequest).getValueArgsAndKwargsSerializer();

        assertThat(Objects.requireNonNull(argsAndKwargs)).isNotNull();
        assertThat(argsAndKwargs.getArg(0).getAnyValue().is(Pipeline.class)).isTrue();
        assertThat(argsAndKwargs.slice(1)).isInstanceOf(ValueArgsAndKwargs.class);
        assertThat(argsAndKwargs.getArg(0).getAnyValue().is(Pipeline.class)).isTrue();
    }
}
