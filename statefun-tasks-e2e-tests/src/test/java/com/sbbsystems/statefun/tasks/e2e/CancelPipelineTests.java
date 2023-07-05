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
import com.sbbsystems.statefun.tasks.util.Id;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class CancelPipelineTests {
    private final long POLL_WAIT_MILLIS = 10_000;
    private NamespacedTestHarness harness;

    @BeforeEach
    public void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
        public void test_cancelling_a_pipeline()
            throws InvalidProtocolBufferException, InterruptedException {

        var pipeline = PipelineBuilder
                .beginWith("sleep")
                .continueWith("echo")
                .build();

        var uid = Id.generate();
        WaitHandles.create(uid);
        harness.startPipeline(pipeline, null, uid);

        var event = harness.pollForEvent(uid, POLL_WAIT_MILLIS);  // created pipeline
        assertThat(event.hasPipelineCreated()).isTrue();

        var cancelResult = harness.sendActionAndGetResponse(TaskAction.CANCEL_PIPELINE, uid);
        assertThat(cancelResult.is(TaskActionResult.class)).isTrue();

        WaitHandles.set(uid);

        var result = harness.getMessage(uid);  // task exception (cancelled)
        assertThat(result.toString()).contains("PipelineCancelledException");

        var events = harness.getEvents(uid);

        var pipelineStatuses = events.stream()
                .filter(Event::hasPipelineStatusChanged)
                .map(e -> e.getPipelineStatusChanged().getStatus().getValue())
                .collect(Collectors.toList());
        assertThat(pipelineStatuses).containsExactly(
                TaskStatus.Status.RUNNING,
                TaskStatus.Status.CANCELLING,
                TaskStatus.Status.CANCELLED);

        var tasksCompleted = events.stream()
                .filter(Event::hasPipelineTaskFinished)
                .map(e -> e.getPipelineTaskFinished().getUid())
                .collect(Collectors.toUnmodifiableList());

        var echoUid = pipeline.getEntries(1).getTaskEntry().getUid();
        assertThat(tasksCompleted).doesNotContain(echoUid);
    }

    @Test
    public void test_cancelling_a_completed_pipeline() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(0))
                .build();

        harness.runPipeline(pipeline, null, uid);

        var pauseResult = harness.sendActionAndGetResponse(TaskAction.CANCEL_PIPELINE, uid);

        assertThat(pauseResult.is(TaskActionException.class)).isTrue();
    }

    @Test
    public void test_cancelling_a_pipeline_with_a_finally()
            throws InvalidProtocolBufferException, InterruptedException {

        var pipeline = PipelineBuilder
                .beginWith("sleep")
                .continueWith("echo")
                .finally_do("cleanup")
                .build();

        var uid = Id.generate();
        WaitHandles.create(uid);

        harness.startPipeline(pipeline, null, uid);

        var event = harness.pollForEvent(uid, POLL_WAIT_MILLIS);  // created pipeline
        assertThat(event.hasPipelineCreated()).isTrue();

        var cancelResult = harness.sendActionAndGetResponse(TaskAction.CANCEL_PIPELINE, uid);
        assertThat(cancelResult.is(TaskActionResult.class)).isTrue();

        WaitHandles.set(uid);

        var result = harness.getMessage(uid);  // task exception (cancelled)
        assertThat(result.toString()).contains("PipelineCancelledException");

        var events = harness.getEvents(uid);

        var pipelineStatuses = events.stream()
                .filter(Event::hasPipelineStatusChanged)
                .map(e -> e.getPipelineStatusChanged().getStatus().getValue())
                .collect(Collectors.toList());
        assertThat(pipelineStatuses).containsExactly(
                TaskStatus.Status.RUNNING,
                TaskStatus.Status.CANCELLING,
                TaskStatus.Status.CANCELLED);

        var tasksCompleted = events.stream()
                .filter(Event::hasPipelineTaskFinished)
                .map(e -> e.getPipelineTaskFinished().getUid())
                .collect(Collectors.toUnmodifiableList());

        var finallyUid = pipeline.getEntries(2).getTaskEntry().getUid();
        assertThat(tasksCompleted).contains(finallyUid);
    }
}
