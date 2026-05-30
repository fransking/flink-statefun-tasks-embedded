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
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.sbbsystems.statefun.tasks.e2e.MoreStrings.asString;
import static org.assertj.core.api.Assertions.assertThat;


public class NestedPipelineTests {
    private NamespacedTestHarness legacyHarness;
    private NamespacedTestHarness valueHarness;

    @BeforeEach
    void setup() {
        legacyHarness = NamespacedTestHarness.newInstance();
        valueHarness = NamespacedTestHarness.newInstance(false);
    }

    @Test
    void test_nested_pipelines_using_legacy_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("newPipeline", Int32Value.of(1))
                .continueWith("echo")
                .build();

        var response = legacyHarness.runPipelineAndGetResponse(pipeline);

        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("1");

        var events = legacyHarness.getEvents(taskResult.getId());
        assertThat(events.stream().filter(Event::hasPipelineCreated)).hasSize(2);
    }

    @Test
    void test_nested_pipelines_using_value_types() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("newPipeline", Value.newBuilder().setIntValue(1).build())
                .continueWith("echo")
                .build();

        var response = valueHarness.runPipelineAndGetResponse(pipeline);

        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("1");

        var events = valueHarness.getEvents(taskResult.getId());
        assertThat(events.stream().filter(Event::hasPipelineCreated)).hasSize(2);
    }
}