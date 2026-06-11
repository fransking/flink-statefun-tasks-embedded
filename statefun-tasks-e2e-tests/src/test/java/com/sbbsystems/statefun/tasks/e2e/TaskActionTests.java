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
    private NamespacedTestHarness legacyHarness;
    private NamespacedTestHarness valueHarness;

    @BeforeEach
    public void setup() {
        legacyHarness = NamespacedTestHarness.newInstance();
        valueHarness = NamespacedTestHarness.newInstance(false);
    }

    @Test
    public void test_getting_the_status_of_a_pipeline_using_legacy_types() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(0))
                .build();

        legacyHarness.runPipeline(pipeline, null, uid);

        var actionResult = legacyHarness.sendActionAndGetResponse(TaskAction.GET_STATUS, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var status = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(status).isEqualTo("COMPLETED");
    }

    @Test
    public void test_getting_the_status_of_a_pipeline_using_value_types() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Int32Value.of(0))
                .build();

        valueHarness.runPipeline(pipeline, null, uid);

        var actionResult = valueHarness.sendActionAndGetResponse(TaskAction.GET_STATUS, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var status = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(status).isEqualTo("COMPLETED");
    }

    @Test
    public void test_getting_the_status_of_a_pipeline_that_has_not_yet_started_using_legacy_types() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var actionResult = legacyHarness.sendActionAndGetResponse(TaskAction.GET_STATUS, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var status = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(status).isEqualTo("PENDING");
    }

    @Test
    public void test_getting_the_status_of_a_pipeline_that_has_not_yet_started_using_value_types() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var actionResult = valueHarness.sendActionAndGetResponse(TaskAction.GET_STATUS, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var status = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(status).isEqualTo("PENDING");
    }

    @Test
    public void test_getting_the_pipeline_request_using_legacy_types() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(123))
                .build();

        legacyHarness.runPipeline(pipeline, null, uid);

        var actionResult = legacyHarness.sendActionAndGetResponse(TaskAction.GET_REQUEST, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var result = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(result).startsWith("entries").contains("echo");
    }

    @Test
    public void test_getting_the_pipeline_request_using_value_types() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Int32Value.of(123))
                .build();

        valueHarness.runPipeline(pipeline, null, uid);

        var actionResult = valueHarness.sendActionAndGetResponse(TaskAction.GET_REQUEST, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var result = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(result).startsWith("entries").contains("echo");
    }

    @Test
    public void test_getting_the_result_of_a_pipeline_using_legacy_types() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(123))
                .build();

        legacyHarness.runPipeline(pipeline, null, uid);

        var actionResult = legacyHarness.sendActionAndGetResponse(TaskAction.GET_RESULT, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var result = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(result).isEqualTo("123");
    }

    @Test
    public void test_getting_the_result_of_a_pipeline_using_value_types() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var pipeline = PipelineBuilder.forE2eWorker(false)
                .beginWith("echo", Int32Value.of(123))
                .build();

        valueHarness.runPipeline(pipeline, null, uid);

        var actionResult = valueHarness.sendActionAndGetResponse(TaskAction.GET_RESULT, uid);
        assertThat(actionResult.is(TaskActionResult.class)).isTrue();

        var result = asString(actionResult.unpack(TaskActionResult.class).getResult());
        assertThat(result).isEqualTo("123");
    }

    @Test
    public void test_getting_the_result_of_a_pipeline_that_has_not_finished_using_legacy_types() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var actionResult = legacyHarness.sendActionAndGetResponse(TaskAction.GET_RESULT, uid);
        assertThat(actionResult.is(TaskActionException.class)).isTrue();
    }

    @Test
    public void test_getting_the_result_of_a_pipeline_that_has_not_finished_using_value_types() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var actionResult = valueHarness.sendActionAndGetResponse(TaskAction.GET_RESULT, uid);
        assertThat(actionResult.is(TaskActionException.class)).isTrue();
    }
}
