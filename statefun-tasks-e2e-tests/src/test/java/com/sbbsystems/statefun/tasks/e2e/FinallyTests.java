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

import com.google.protobuf.BoolValue;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.generated.TaskException;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.sbbsystems.statefun.tasks.e2e.MoreStrings.asString;
import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;
import static org.assertj.core.api.Assertions.assertThat;


public class FinallyTests {
    private NamespacedTestHarness harness;

    @BeforeEach
    void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
    void test_finally_runs_when_there_is_no_error() throws InvalidProtocolBufferException {
        var arguments = TupleOfAny
                .newBuilder()
                .addItems(packAny(StringValue.of("task 1")))
                .build();

        var pipeline = PipelineBuilder
                .beginWith("echo", arguments)
                .finally_do("cleanup")
                .withInitialState(BoolValue.of(false))  // do not throw error
                .inline()
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("task 1");
        assertThat(state).isEqualTo("false");
    }

    @Test
    void test_when_finally_throws_an_error_it_is_returned() throws InvalidProtocolBufferException {
        var arguments = TupleOfAny
                .newBuilder()
                .addItems(packAny(StringValue.of("task 1")))
                .build();

        var pipeline = PipelineBuilder
                .beginWith("echo", arguments)
                .finally_do("cleanup")
                .withInitialState(BoolValue.of(true))  // throw error
                .inline()
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskException = response.unpack(TaskException.class);
        var state = asString(taskException.getState());

        assertThat(taskException.getExceptionMessage()).isEqualTo("com.sbbsystems.statefun.tasks.core.StatefunTasksException: error in finally");
        assertThat(state).isEqualTo("true");
    }
}
