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
import com.sbbsystems.statefun.tasks.generated.Event;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.sbbsystems.statefun.tasks.e2e.MoreStrings.asString;
import static org.assertj.core.api.Assertions.assertThat;


public class NestedPipelineTests {
    private NamespacedTestHarness harness;

    @BeforeEach
    void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
    void test_nested_pipelines() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("newPipeline", Int32Value.of(1))
                .continueWith("echo")
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);

        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("1");

        var events = harness.getEvents(taskResult.getId());
        assertThat(events.stream().filter(Event::hasPipelineCreated)).hasSize(2);
    }
}