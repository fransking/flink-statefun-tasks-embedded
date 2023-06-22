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
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;


public class EventsTests {
    private NamespacedTestHarness harness;

    @BeforeEach
    void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
    void test_pipeline_created_event_is_sent() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(1))
                .continueWith("echo")
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);

        var events = harness.getEvents(taskResult.getId());
        assertThat(events.stream().filter(Event::hasPipelineCreated)).hasSize(1);
    }

    @Test
    void test_pipeline_status_changed_events_are_sent() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(1))
                .continueWith("echo")
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);

        var events = harness.getEvents(taskResult.getId());
        var statusEvents = events.stream().filter(Event::hasPipelineStatusChanged).collect(Collectors.toUnmodifiableList());
        assertThat(statusEvents).hasSize(2);
        assertThat(statusEvents.get(0).getPipelineStatusChanged().getStatus().getValue()).isEqualTo(TaskStatus.Status.RUNNING);
        assertThat(statusEvents.get(1).getPipelineStatusChanged().getStatus().getValue()).isEqualTo(TaskStatus.Status.COMPLETED);
    }
}