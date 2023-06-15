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
package com.sbbsystems.statefun.tasks.e2e;

import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.generated.MapOfStringToAny;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.sbbsystems.statefun.tasks.e2e.MoreStrings.asString;
import static com.sbbsystems.statefun.tasks.e2e.PipelineBuilder.inParallel;
import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;
import static org.assertj.core.api.Assertions.assertThat;


public class ParallelPipelineTests {
    private NamespacedTestHarness harness;

    @BeforeEach
    void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
    void test_parallel_pipeline_returns_aggregated_results() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder
                .beginWith("echo", StringValue.of("a"))
                .build();

        var p2 = PipelineBuilder
                .beginWith("echo", StringValue.of("b"))
                .build();

        var pipeline = inParallel(List.of(p1, p2)).build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[a, b]");
    }

    @Test
    void test_parallel_pipeline_returns_single_aggregated_state_when_all_tasks_return_mixed_state_types() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder
                .beginWith("setState", Int32Value.of(123))
                .build();

        var p2 = PipelineBuilder
                .beginWith("setState", Int32Value.of(456))
                .build();

        var pipeline = inParallel(List.of(p1, p2)).inline().build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("[123, 456]");
        assertThat(state).isEqualTo("123");
    }

    @Test
    void test_parallel_pipeline_returns_single_aggregated_state_when_all_tasks_return_map_state() throws InvalidProtocolBufferException {

        var p1Args = MapOfStringToAny.newBuilder().putItems("p1", packAny(Int32Value.of(123))).build();
        var p1 = PipelineBuilder
                .beginWith("setState", p1Args)
                .build();

        var p2Args = MapOfStringToAny.newBuilder().putItems("p2", packAny(Int32Value.of(456))).build();
        var p2 = PipelineBuilder
                .beginWith("setState", p2Args)
                .build();

        var pipeline = inParallel(List.of(p1, p2)).inline().build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var state = asString(taskResult.getState());

        assertThat(state).isEqualTo("{p1: 123, p2: 456}");
    }

    @Test
    void test_empty_parallel_pipeline_returns_empty_array() throws InvalidProtocolBufferException {
        var pipeline = inParallel(List.of()).build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[]");
    }

    @Test
    void test_empty_parallel_pipeline_continues_with_empty_array() throws InvalidProtocolBufferException {
        var pipeline = inParallel(List.of()).continueWith("setState").inline().build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("[]");
        assertThat(state).isEqualTo("[]");
    }

    @Test
    void test_nested_empty_parallel_pipeline_returns_empty_array() throws InvalidProtocolBufferException {
        var nestedPipeline = inParallel(List.of()).inline().build();
        var pipeline = inParallel(List.of(nestedPipeline)).inline().build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[]");
    }

    @Test
    void test_nested_empty_parallel_pipeline_continues_with_empty_array() throws InvalidProtocolBufferException {
        var nestedPipeline = inParallel(List.of()).inline().build();
        var pipeline = inParallel(List.of(nestedPipeline)).continueWith("setState").inline().build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("[]");
        assertThat(state).isEqualTo("[]");
    }

    @Test
    void test_nested_partially_empty_parallel_pipeline_continues_with_partially_empty_array_correctly_indexed() throws InvalidProtocolBufferException {
        var p1 = PipelineBuilder.beginWith("echo", StringValue.of("a")).build();
        var nestedPipeline = inParallel(List.of()).inline().build();
        var nestedPipeline2 = inParallel(List.of(p1)).inline().build();
        var pipeline = inParallel(List.of(nestedPipeline, nestedPipeline2)).continueWith("echo").inline().build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("[[], [a]]");
    }
}