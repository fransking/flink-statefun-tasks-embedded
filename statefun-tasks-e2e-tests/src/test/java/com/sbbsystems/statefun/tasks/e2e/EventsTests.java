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
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.generated.Event;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.sbbsystems.statefun.tasks.e2e.PipelineBuilder.inParallel;
import static org.assertj.core.api.Assertions.assertThat;


public class EventsTests {
    private NamespacedTestHarness harness;

    @BeforeEach
    void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
    void test_pipeline_created_event_is_sent() throws InvalidProtocolBufferException {
//        var pipeline = PipelineBuilder
//                .beginWith("echo", Int32Value.of(1))
//                .continueWith("echo")
//                .build();

        var p1 = PipelineBuilder
                .beginWith("echo", StringValue.of("a"))
                .continueWith("echo")
                .build();

        var p2 = PipelineBuilder
                .beginWith("echo", StringValue.of("b"))
                .build();

        var pipeline = inParallel(List.of(p1, p2)).build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);

        var events = harness.getEvents(taskResult.getId());
        var pipelineCreatedEvents = events.stream().filter(Event::hasPipelineCreated).collect(Collectors.toUnmodifiableList());
        assertThat(pipelineCreatedEvents).hasSize(1);

        var event = pipelineCreatedEvents.get(0).getPipelineCreated();
        assertThat(event.getPipeline().getEntriesCount()).isEqualTo(1);
        assertThat(event.getPipeline().getEntries(0).hasGroupEntry()).isTrue();
        assertThat(event.getPipeline().getEntries(0).getGroupEntry().getGroupCount()).isEqualTo(2);
        assertThat(event.getPipeline().getEntries(0).getGroupEntry().getGroup(0).getEntriesCount()).isEqualTo(2);
        assertThat(event.getPipeline().getEntries(0).getGroupEntry().getGroup(1).getEntriesCount()).isEqualTo(1);
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