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
package com.sbbsystems.statefun.tasks.e2e;

import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.sbbsystems.statefun.tasks.e2e.MoreStrings.asString;
import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;
import static org.assertj.core.api.Assertions.assertThat;


public class SimplePipelineTests {
    private NamespacedTestHarness legacyHarness;
    private NamespacedTestHarness valueHarness;

    @BeforeEach
    void setup() {
        legacyHarness = NamespacedTestHarness.newInstance(true);
        valueHarness = NamespacedTestHarness.newInstance(false);
    }

    @Test
    void test_pipeline_returns_result_using_legacy_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(1))
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("1");
    }

    @Test
    void test_pipeline_returns_result_using_value_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Value.newBuilder().setIntValue(1).build())
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("1");
    }

    @Test
    void test_args_are_sent_to_tasks_using_legacy_types() throws InvalidProtocolBufferException {
        var arguments = TupleOfAny
                .newBuilder()
                .addItems(packAny(Int32Value.of(1)))
                .addItems(packAny(Int32Value.of(2)))
                .build();

        var pipeline = PipelineBuilder
                .beginWith("echo", arguments)
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2)");
    }

    @Test
    void test_args_are_sent_to_tasks_using_value_types() throws InvalidProtocolBufferException {
        var arguments = TupleOfValue
                .newBuilder()
                .addItems(Value.newBuilder().setIntValue(1).build())
                .addItems(Value.newBuilder().setIntValue(2).build())
                .build();

        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", arguments)
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2)");
    }

    @Test
    void test_args_and_kwargs_are_sent_to_tasks_using_legacy_types() throws InvalidProtocolBufferException {
        var args = TupleOfAny
                .newBuilder()
                .addItems(packAny(Int32Value.of(1)))
                .addItems(packAny(Int32Value.of(2)))
                .build();

        var kwargs = MapOfStringToAny
                .newBuilder()
                .putItems("k1", packAny(Int32Value.of(1)))
                .putItems("k2", packAny(Int32Value.of(2)))
                .build();

        var argsAndKwargs = ArgsAndKwargs
                .newBuilder()
                .setArgs(args)
                .setKwargs(kwargs)
                .build();

        var pipeline = PipelineBuilder
                .beginWith("echo", argsAndKwargs)
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2, {k1: 1, k2: 2})");
    }

    @Test
    void test_args_and_kwargs_are_sent_to_tasks_using_value_types() throws InvalidProtocolBufferException {
        var args = TupleOfValue
                .newBuilder()
                .addItems(Value.newBuilder().setIntValue(1).build())
                .addItems(Value.newBuilder().setIntValue(2).build())
                .build();

        var kwargs = MapOfStringToValue
                .newBuilder()
                .putItems("k1", Value.newBuilder().setIntValue(1).build())
                .putItems("k2", Value.newBuilder().setIntValue(2).build())
                .build();

        var argsAndKwargs = ValueArgsAndKwargs
                .newBuilder()
                .setArgs(args)
                .setKwargs(kwargs)
                .build();

        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", argsAndKwargs)
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2, {k1: 1, k2: 2})");
    }

    @Test
    void test_pipeline_returns_result_and_pipeline_state_when_inline_using_legacy_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("updateAndGetState", Int32Value.of(1))
                .withInitialState(Int32Value.of(2))
                .inline()
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline, packAny(Int32Value.of(100)));
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("3");
        assertThat(state).isEqualTo("3");
    }

    @Test
    void test_pipeline_returns_result_and_pipeline_state_when_inline_using_value_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("updateAndGetState", Value.newBuilder().setIntValue(1).build())
                .withInitialState(Value.newBuilder().setIntValue(2).build())
                .inline()
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline, packAny(Value.newBuilder().setIntValue(100).build()));
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("3");
        assertThat(state).isEqualTo("3");
    }

    @Test
    void test_pipeline_returns_result_and_task_state_when_not_inline_using_legacy_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("updateAndGetState", Int32Value.of(1))
                .withInitialState(Int32Value.of(2))
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline, packAny(Int32Value.of(100)));
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("3");
        assertThat(state).isEqualTo("100");
    }

    @Test
    void test_pipeline_returns_result_and_task_state_when_not_inline_using_value_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("updateAndGetState", Value.newBuilder().setIntValue(1).build())
                .withInitialState(Value.newBuilder().setIntValue(2).build())
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline, packAny(Value.newBuilder().setIntValue(100).build()));
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("3");
        assertThat(state).isEqualTo("100");
    }

    @Test
    void test_initial_args_are_sent_to_initial_tasks_using_legacy_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(2))
                .withInitialArgs(TupleOfAny.newBuilder().addItems(packAny(Int32Value.of(1))).build())
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2)");
    }

    @Test
    void test_initial_args_are_sent_to_initial_tasks_using_value_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Value.newBuilder().setIntValue(2).build())
                .withInitialArgs(TupleOfValue.newBuilder().addItems(Value.newBuilder().setIntValue(1).build()).build())
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2)");
    }

    @Test
    void test_initial_kwargs_are_sent_to_initial_tasks_using_legacy_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(2))
                .withInitialKwargs(MapOfStringToAny.newBuilder().putItems("k1", packAny(Int32Value.of(3))).build())
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(2, {k1: 3})");
    }

    @Test
    void test_initial_kwargs_are_sent_to_initial_tasks_using_value_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Value.newBuilder().setIntValue(2).build())
                .withInitialKwargs(MapOfStringToValue.newBuilder().putItems("k1", Value.newBuilder().setIntValue(3).build()).build())
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(2, {k1: 3})");
    }

    @Test
    void test_task_results_are_sent_to_continuations_using_legacy_types() throws InvalidProtocolBufferException {
        var arguments = TupleOfAny
                .newBuilder()
                .addItems(packAny(Int32Value.of(1)))
                .addItems(packAny(Int32Value.of(2)))
                .build();

        var pipeline = PipelineBuilder
                .beginWith("echo", arguments)
                .continueWith("echo")
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2)");
    }

    @Test
    void test_task_results_are_sent_to_continuations_using_value_types() throws InvalidProtocolBufferException {
        var arguments = TupleOfValue
                .newBuilder()
                .addItems(Value.newBuilder().setIntValue(1).build())
                .addItems(Value.newBuilder().setIntValue(2).build())
                .build();

        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", arguments)
                .continueWith("echo")
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2)");
    }

    @Test
    void test_pipeline_terminates_when_an_exception_is_thrown_using_legacy_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("setState", Int32Value.of(1))
                .continueWith("error")
                .continueWith("setState", Int32Value.of(2))
                .inline()
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskException = response.unpack(TaskException.class);
        var state = asString(taskException.getState());

        assertThat(taskException.getExceptionMessage()).isEqualTo("com.sbbsystems.statefun.tasks.core.StatefunTasksException: ");
        assertThat(state).isEqualTo("1");  // 2 did not run
    }

    @Test
    void test_pipeline_terminates_when_an_exception_is_thrown_using_value_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("setState", Value.newBuilder().setIntValue(1).build())
                .continueWith("error")
                .continueWith("setState", Value.newBuilder().setIntValue(2).build())
                .inline()
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskException = response.unpack(TaskException.class);
        var state = asString(taskException.getState());

        assertThat(taskException.getExceptionMessage()).isEqualTo("com.sbbsystems.statefun.tasks.core.StatefunTasksException: ");
        assertThat(state).isEqualTo("1");  // 2 did not run
    }
}