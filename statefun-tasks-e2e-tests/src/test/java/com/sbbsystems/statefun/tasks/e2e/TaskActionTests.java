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
import com.sbbsystems.statefun.tasks.generated.TaskAction;
import com.sbbsystems.statefun.tasks.generated.TaskActionException;
import com.sbbsystems.statefun.tasks.generated.TaskActionResult;
import com.sbbsystems.statefun.tasks.util.Id;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.sbbsystems.statefun.tasks.e2e.MoreStrings.asString;
import static org.assertj.core.api.Assertions.assertThat;

public class TaskActionTests {
    private NamespacedTestHarness harness;

    @BeforeEach
    public void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
    public void test_getting_the_status_of_a_pipeline() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(0))
                .build();

        harness.runPipeline(pipeline, null, uid);

        var actionResult = harness.sendActionAndGetResponse(TaskAction.GET_STATUS, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var status = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(status).isEqualTo("COMPLETED");
    }

    @Test
    public void test_getting_the_status_of_a_pipeline_that_has_not_yet_started() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var actionResult = harness.sendActionAndGetResponse(TaskAction.GET_STATUS, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var status = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(status).isEqualTo("PENDING");
    }

    @Test
    public void test_getting_the_pipeline_request() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(123))
                .build();

        harness.runPipeline(pipeline, null, uid);

        var actionResult = harness.sendActionAndGetResponse(TaskAction.GET_REQUEST, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var result = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(result).startsWith("entries").contains("echo");
    }

    @Test
    public void test_getting_the_result_of_a_pipeline() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(123))
                .build();

        harness.runPipeline(pipeline, null, uid);

        var actionResult = harness.sendActionAndGetResponse(TaskAction.GET_RESULT, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var result = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(result).isEqualTo("123");
    }

    @Test
    public void test_getting_the_result_of_a_pipeline_that_has_not_finished() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var actionResult = harness.sendActionAndGetResponse(TaskAction.GET_RESULT, uid);
        assertThat(actionResult.is(TaskActionException.class)).isTrue();
    }
}
