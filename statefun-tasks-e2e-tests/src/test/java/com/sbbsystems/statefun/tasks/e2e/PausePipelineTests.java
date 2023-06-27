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

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class PausePipelineTests {
    private NamespacedTestHarness harness;

    @BeforeEach
    public void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
    public void test_pausing_and_resuming_pipeline() throws InvalidProtocolBufferException, InterruptedException {
        var taskWaitTime = 1_000;
        var pipeline = PipelineBuilder
                .beginWith("sleep", Int32Value.of(taskWaitTime))
                .build();

        var uid = UUID.randomUUID().toString();
        harness.startPipeline(pipeline, null, uid);
        List<Event> initialEvents = null;
        for (int i = 0; i < 200; i++) {
            Thread.sleep(50);
            initialEvents = harness.getEvents(uid);
            if (initialEvents.size() > 0) {
                break;
            }
        }

        assertThat(initialEvents).isNotEmpty();
        assertThat(initialEvents.get(0).hasPipelineCreated()).isTrue();

        var pauseResult = harness.sendActionAndGetResponse(TaskAction.PAUSE_PIPELINE, uid);
        // sleep for longer than task takes to run before resuming
        Thread.sleep(taskWaitTime + 100);
        var resumeResult = harness.sendActionAndGetResponse(TaskAction.UNPAUSE_PIPELINE, uid);

        var pipelineResult = KafkaProducerRecord.parseFrom(harness.getMessage(uid).getValue());

        assertThat(Any.parseFrom(pipelineResult.getValueBytes()).is(TaskResult.class)).isTrue();
        if (!pauseResult.is(TaskActionResult.class)) {
            fail("Pause task failed: " + pauseResult.unpack(TaskActionException.class).getExceptionMessage());
        }
        if (!resumeResult.is(TaskActionResult.class)) {
            fail("Resume task failed: " + resumeResult.unpack(TaskActionException.class).getExceptionMessage());
        }

        var pipelineStatuses = Stream.concat(initialEvents.stream(), harness.getEvents(uid).stream())
                .filter(Event::hasPipelineStatusChanged)
                .map(event -> event.getPipelineStatusChanged().getStatus().getValue())
                .collect(Collectors.toList());
        assertThat(pipelineStatuses).containsExactly(
                TaskStatus.Status.RUNNING,
                TaskStatus.Status.PAUSED,
                TaskStatus.Status.RUNNING,
                TaskStatus.Status.COMPLETED);
    }

    @Test
    public void test_pausing_completed_pipeline() throws InvalidProtocolBufferException {
        var uid = UUID.randomUUID().toString();
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(0))
                .build();

        harness.runPipeline(pipeline, null, uid);

        var pauseResult = harness.sendActionAndGetResponse(TaskAction.PAUSE_PIPELINE, uid);

        assertThat(pauseResult.is(TaskActionException.class)).isTrue();
    }
}
