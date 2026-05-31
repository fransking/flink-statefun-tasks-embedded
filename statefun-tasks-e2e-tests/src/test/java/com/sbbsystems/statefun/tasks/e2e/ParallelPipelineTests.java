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
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.sbbsystems.statefun.tasks.e2e.MoreStrings.asString;
import static com.sbbsystems.statefun.tasks.e2e.PipelineBuilder.inParallel;
import static com.sbbsystems.statefun.tasks.e2e.TestMessageTypes.toArgsAndKwargs;
import static com.sbbsystems.statefun.tasks.e2e.TestMessageTypes.toValueArgsAndKwargs;
import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;
import static org.assertj.core.api.Assertions.assertThat;


public class ParallelPipelineTests {
    private NamespacedTestHarness legacyHarness;
    private NamespacedTestHarness valueHarness;

    @BeforeEach
    void setup() {
        legacyHarness = NamespacedTestHarness.newInstance();
        valueHarness = NamespacedTestHarness.newInstance(false);
    }

    @Test
    void test_parallel_pipeline_returns_aggregated_results_using_legacy_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder
                .beginWith("echo", StringValue.of("a"))
                .build();

        var p2 = PipelineBuilder
                .beginWith("echo", StringValue.of("b"))
                .build();

        var pipeline = inParallel(List.of(p1, p2)).build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[a, b]");
    }

    @Test
    void test_parallel_pipeline_returns_aggregated_results_using_value_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Value.newBuilder().setStringValue("a").build())
                .build();

        var p2 = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Value.newBuilder().setStringValue("b").build())
                .build();

        var pipeline = inParallel(false, List.of(p1, p2)).build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[a, b]");
    }

    @Test
    void test_parallel_pipeline_returns_single_aggregated_state_when_all_tasks_return_mixed_state_types_using_legacy_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder
                .beginWith("setState", Int32Value.of(123))
                .build();

        var p2 = PipelineBuilder
                .beginWith("setState", Int32Value.of(456))
                .build();

        var pipeline = inParallel(List.of(p1, p2)).inline().build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("[123, 456]");
        assertThat(state).isEqualTo("123");
    }

    @Test
    void test_parallel_pipeline_returns_single_aggregated_state_when_all_tasks_return_mixed_state_types_using_value_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder.forE2eWorker(false)
                .beginWith("setState", Value.newBuilder().setIntValue(123).build())
                .build();

        var p2 = PipelineBuilder.forE2eWorker(false)
                .beginWith("setState", Value.newBuilder().setIntValue(456).build())
                .build();

        var pipeline = inParallel(false, List.of(p1, p2)).inline().build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("[123, 456]");
        assertThat(state).isEqualTo("123");
    }

    @Test
    void test_parallel_pipeline_returns_single_aggregated_state_when_all_tasks_return_map_state_using_legacy_types() throws InvalidProtocolBufferException {

        var p1Args = MapOfStringToAny.newBuilder().putItems("p1", packAny(Int32Value.of(123))).build();
        var p1 = PipelineBuilder
                .beginWith("setState", p1Args)
                .build();

        var p2Args = MapOfStringToAny.newBuilder().putItems("p2", packAny(Int32Value.of(456))).build();
        var p2 = PipelineBuilder
                .beginWith("setState", p2Args)
                .build();

        var pipeline = inParallel(List.of(p1, p2)).inline().build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var state = asString(taskResult.getState());

        assertThat(state).isEqualTo("{p1: 123, p2: 456}");
    }

    @Test
    void test_parallel_pipeline_returns_single_aggregated_state_when_all_tasks_return_map_state_using_value_types() throws InvalidProtocolBufferException {

        var p1Args = MapOfStringToValue.newBuilder().putItems("p1", Value.newBuilder().setIntValue(123).build()).build();
        var p1 = PipelineBuilder.forE2eWorker(false)
                .beginWith("setState", p1Args)
                .build();

        var p2Args = MapOfStringToValue.newBuilder().putItems("p2", Value.newBuilder().setIntValue(456).build()).build();
        var p2 = PipelineBuilder.forE2eWorker(false)
                .beginWith("setState", p2Args)
                .build();

        var pipeline = inParallel(false, List.of(p1, p2)).inline().build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);

        if (response.is(TaskException.class)) {
            var taskException = response.unpack(TaskException.class);
            System.out.println("TaskException: " + taskException);
        }

        var taskResult = response.unpack(TaskResult.class);
        var state = asString(taskResult.getState());

        assertThat(state).isEqualTo("{p1: 123, p2: 456}");
    }

    @Test
    void test_empty_parallel_pipeline_returns_empty_array_using_legacy_types() throws InvalidProtocolBufferException {
        var pipeline = inParallel(List.of()).build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[]");
    }

    @Test
    void test_empty_parallel_pipeline_returns_empty_array_using_value_types() throws InvalidProtocolBufferException {
        var pipeline = inParallel(false, List.of()).build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[]");
    }

    @Test
    void test_empty_parallel_pipeline_continues_with_empty_array_using_legacy_types() throws InvalidProtocolBufferException {
        var pipeline = inParallel(List.of()).continueWith("setState").inline().build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("[]");
        assertThat(state).isEqualTo("[]");
    }

    @Test
    void test_empty_parallel_pipeline_continues_with_empty_array_using_value_types() throws InvalidProtocolBufferException {
        var pipeline = inParallel(false, List.of()).continueWith("setState").inline().build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("[]");
        assertThat(state).isEqualTo("[]");
    }

    @Test
    void test_nested_empty_parallel_pipeline_returns_empty_array_using_legacy_types() throws InvalidProtocolBufferException {
        var nestedPipeline = inParallel(List.of()).inline().build();
        var pipeline = inParallel(List.of(nestedPipeline)).inline().build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[]");
    }

    @Test
    void test_nested_empty_parallel_pipeline_returns_empty_array_using_value_types() throws InvalidProtocolBufferException {
        var nestedPipeline = inParallel(false, List.of()).inline().build();
        var pipeline = inParallel(false, List.of(nestedPipeline)).inline().build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[]");
    }

    @Test
    void test_nested_empty_parallel_pipeline_continues_with_empty_array_using_legacy_types() throws InvalidProtocolBufferException {
        var nestedPipeline = inParallel(List.of()).inline().build();
        var pipeline = inParallel(List.of(nestedPipeline)).continueWith("setState").inline().build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("[]");
        assertThat(state).isEqualTo("[]");
    }

    @Test
    void test_nested_empty_parallel_pipeline_continues_with_empty_array_using_value_types() throws InvalidProtocolBufferException {
        var nestedPipeline = inParallel(false, List.of()).inline().build();
        var pipeline = inParallel(false, List.of(nestedPipeline)).continueWith("setState").inline().build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("[]");
        assertThat(state).isEqualTo("[]");
    }

    @Test
    void test_nested_partially_empty_parallel_pipeline_continues_with_partially_empty_array_correctly_indexed_using_legacy_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder.beginWith("echo", StringValue.of("a")).build();
        var nestedPipeline = inParallel(List.of()).inline().build();
        var nestedPipeline2 = inParallel(List.of(p1)).inline().build();
        var pipeline = inParallel(List.of(nestedPipeline, nestedPipeline2)).continueWith("echo").inline().build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[[], [a]]");
    }

    @Test
    void test_nested_partially_empty_parallel_pipeline_continues_with_partially_empty_array_correctly_indexed_using_value_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder.forE2eWorker(false).beginWith("echo", Value.newBuilder().setStringValue("a").build()).build();
        var nestedPipeline = inParallel(false, List.of()).inline().build();
        var nestedPipeline2 = inParallel(false, List.of(p1)).inline().build();
        var pipeline = inParallel(false, List.of(nestedPipeline, nestedPipeline2)).continueWith("echo").inline().build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[[], [a]]");
    }

    @Test
    void test_parallel_pipeline_that_throws_errors_returns_errors_for_whole_group_using_legacy_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder
                .beginWith("setState", Int32Value.of(123))
                .build();

        var p2 = PipelineBuilder
                .beginWith("error", toArgsAndKwargs(Map.of("message", StringValue.of("error p2"))))
                .build();

        var p3 = PipelineBuilder
                .beginWith("error", toArgsAndKwargs(Map.of("message", StringValue.of("error p3"))))
                .build();

        var pipeline = inParallel(List.of(p1, p2, p3)).inline().build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskException = response.unpack(TaskException.class);
        var state = asString(taskException.getState());

        assertThat(taskException.getExceptionMessage()).contains("error p2").contains("error p3");
        assertThat(state).isEqualTo("123");
    }

    @Test
    void test_parallel_pipeline_that_throws_errors_returns_errors_for_whole_group_using_value_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder.forE2eWorker(false)
                .beginWith("setState", Value.newBuilder().setIntValue(123).build())
                .build();

        var p2 = PipelineBuilder.forE2eWorker(false)
                .beginWith("error", toValueArgsAndKwargs(Map.of("message", Value.newBuilder().setStringValue("error p2").build())))
                .build();

        var p3 = PipelineBuilder.forE2eWorker(false)
                .beginWith("error", toValueArgsAndKwargs(Map.of("message", Value.newBuilder().setStringValue("error p3").build())))
                .build();

        var pipeline = inParallel(false, List.of(p1, p2, p3)).inline().build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskException = response.unpack(TaskException.class);
        var state = asString(taskException.getState());

        assertThat(taskException.getExceptionMessage()).contains("error p2").contains("error p3");
        assertThat(state).isEqualTo("123");
    }

    @Test
    void test_parallel_pipeline_that_throws_errors_returns_results_when_return_exceptions_is_true_using_legacy_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder
                .beginWith("echo", Int32Value.of(123))
                .build();

        var p2 = PipelineBuilder
                .beginWith("error", toArgsAndKwargs(Map.of("message", StringValue.of("error p2"))))
                .build();

        var p3 = PipelineBuilder
                .beginWith("error", toArgsAndKwargs(Map.of("message", StringValue.of("error p3"))))
                .build();

        var pipeline = inParallel(List.of(p1, p2, p3), true).build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[123, com.sbbsystems.statefun.tasks.core.StatefunTasksException: error p2, com.sbbsystems.statefun.tasks.core.StatefunTasksException: error p3]");
    }

    @Test
    void test_parallel_pipeline_that_throws_errors_returns_results_when_return_exceptions_is_true_using_value_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Value.newBuilder().setIntValue(123).build())
                .build();

        var p2 = PipelineBuilder.forE2eWorker(false)
                .beginWith("error", toValueArgsAndKwargs(Map.of("message", Value.newBuilder().setStringValue("error p2").build())))
                .build();

        var p3 = PipelineBuilder.forE2eWorker(false)
                .beginWith("error", toValueArgsAndKwargs(Map.of("message", Value.newBuilder().setStringValue("error p3").build())))
                .build();

        var pipeline = inParallel(false, List.of(p1, p2, p3), true).build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[123, com.sbbsystems.statefun.tasks.core.StatefunTasksException: error p2, com.sbbsystems.statefun.tasks.core.StatefunTasksException: error p3]");
    }

    @Test
    void test_continuations_into_parallel_pipelines_send_correct_parameters_using_legacy_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder
                .beginWith("echo", StringValue.of("a"))
                .build();

        var p2 = PipelineBuilder
                .beginWith("echo", StringValue.of("b"))
                .build();

        var pipeline = PipelineBuilder
                .beginWith("echo", StringValue.of("1"))
                .continueWith(inParallel(List.of(p1, p2)).build())
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[(1, a), (1, b)]");
    }

    @Test
    void test_continuations_into_parallel_pipelines_send_correct_parameters_using_value_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Value.newBuilder().setStringValue("a").build())
                .build();

        var p2 = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Value.newBuilder().setStringValue("b").build())
                .build();

        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Value.newBuilder().setStringValue("1").build())
                .continueWith(inParallel(false, List.of(p1, p2)).build())
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[(1, a), (1, b)]");
    }

    @Test
    void test_continuations_into_empty_parallel_pipelines_send_correct_parameters_using_legacy_types() throws InvalidProtocolBufferException {
        var empty = inParallel(List.of()).build();

        var pipeline = PipelineBuilder
                .beginWith("echo", StringValue.of("1"))
                .continueWith(empty)
                .continueWith("echo", StringValue.of("2"))
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("([], 2)");
    }

    @Test
    void test_continuations_into_empty_parallel_pipelines_send_correct_parameters_using_value_types() throws InvalidProtocolBufferException {
        var empty = inParallel(false, List.of()).build();

        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Value.newBuilder().setStringValue("1").build())
                .continueWith(empty)
                .continueWith("echo", Value.newBuilder().setStringValue("2").build())
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("([], 2)");
    }

//    @Test
//    @Execution(ExecutionMode.SAME_THREAD)
//    void test_large_parallel_pipeline_returns_aggregated_results_using_legacy_types() throws InvalidProtocolBufferException {
//        var group = new LinkedList<Pipeline>();
//
//        for (var i = 1; i <= 200000; i++) {
//            var p = PipelineBuilder
//                    .beginWith("echo", StringValue.of("" + i))
//                    .build();
//
//            group.add(p);
//        }
//
//        var pipeline = inParallel(group).build();
//
//        var response = legacyHarness.runPipelineAndGetResponse(pipeline);
//        var taskResult = response.unpack(TaskResult.class);
//        var result = asString(taskResult.getResult());
//
//        assertThat(result.contains("200000")).isTrue();
//    }

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    void test_large_parallel_pipeline_returns_aggregated_results_using_value_types() throws InvalidProtocolBufferException {
        var group = new LinkedList<Pipeline>();

        for (var i = 1; i <= 200000; i++) {
            var p = PipelineBuilder.forE2eWorker(false)
                    .beginWith("echo", Value.newBuilder().setStringValue("" + i).build())
                    .build();

            group.add(p);
        }

        var pipeline = inParallel(false, group).build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result.contains("200000")).isTrue();
    }
}