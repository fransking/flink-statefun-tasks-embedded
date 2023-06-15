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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.sbbsystems.statefun.tasks.e2e.MoreStrings.asString;
import static com.sbbsystems.statefun.tasks.e2e.TestMessageTypes.toArgsAndKwargs;
import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;
import static org.assertj.core.api.Assertions.assertThat;


public class ExceptionallyTests {
    private NamespacedTestHarness harness;

    @BeforeEach
    void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
    void test_exceptionally_is_skipped_when_there_is_no_error() throws InvalidProtocolBufferException {
        var arguments = TupleOfAny
                .newBuilder()
                .addItems(packAny(StringValue.of("task 1")))
                .build();

        var pipeline = PipelineBuilder
                .beginWith("echo", arguments)
                .exceptionally("echo", StringValue.of("exceptionally 1"))
                .exceptionally("echo", StringValue.of("exceptionally 2"))
                .continueWith("echo", StringValue.of("task 2"))
                .exceptionally("echo", StringValue.of("exceptionally 3"))
                .continueWith("echo", StringValue.of("task 3"))
                .exceptionally("echo", StringValue.of("exceptionally 4"))
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(task 1, task 2, task 3)");
    }

    @Test
    void test_continuation_is_skipped_when_is_an_error() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("error", toArgsAndKwargs(Map.of("message", StringValue.of("error 1"))))
                .continueWith("echo", StringValue.of("task 1"))
                .exceptionally("echo", StringValue.of("exceptionally 1"))
                .continueWith("echo", StringValue.of("task 3"))
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(com.sbbsystems.statefun.tasks.core.StatefunTasksException: error 1, exceptionally 1, task 3)");
    }
}
