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

import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class PausePipelineTests {
    private final Int32Value SLEEP_TIME_MILLIS = Int32Value.of(5_000);
    private final long POLL_WAIT_MILLIS = 10_000;
    private NamespacedTestHarness harness;

    @BeforeEach
    public void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
        public void test_pausing_and_resuming_a_pipeline()
            throws InvalidProtocolBufferException, InterruptedException {

        var pipeline = PipelineBuilder
                .beginWith("sleep", SLEEP_TIME_MILLIS)
                .continueWith("echo")
                .build();

        var uid = Id.generate();
        harness.startPipeline(pipeline, null, uid);

        var event = harness.pollForEvent(uid, POLL_WAIT_MILLIS);  // created pipeline
        assertThat(event.hasPipelineCreated()).isTrue();

        var pauseResult = harness.sendActionAndGetResponse(TaskAction.PAUSE_PIPELINE, uid);
        assertThat(pauseResult.is(TaskActionResult.class)).isTrue();

        // wait for sleep task to complete
        var events = new LinkedList<Event>();
        do {
            event = harness.pollForEvent(uid, POLL_WAIT_MILLIS);
            events.add(event);
        } while (!event.hasPipelineTaskFinished());

        // now un-pause
        var resumeResult = harness.sendActionAndGetResponse(TaskAction.UNPAUSE_PIPELINE, uid);
        assertThat(resumeResult.is(TaskActionResult.class)).isTrue();

        harness.getMessage(uid);  // task result
        events.addAll(harness.getEvents(uid));

        var pipelineStatuses = Stream.concat(events.stream(), harness.getEvents(uid).stream())
                .filter(Event::hasPipelineStatusChanged)
                .map(e -> e.getPipelineStatusChanged().getStatus().getValue())
                .collect(Collectors.toList());
        assertThat(pipelineStatuses).containsExactly(
                TaskStatus.Status.RUNNING,
                TaskStatus.Status.PAUSED,
                TaskStatus.Status.RUNNING,
                TaskStatus.Status.COMPLETED);
    }

    @Test
    public void test_pausing_completed_pipeline() throws InvalidProtocolBufferException {
        var uid = Id.generate();
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(0))
                .build();

        harness.runPipeline(pipeline, null, uid);

        var pauseResult = harness.sendActionAndGetResponse(TaskAction.PAUSE_PIPELINE, uid);

        assertThat(pauseResult.is(TaskActionException.class)).isTrue();
    }
}
