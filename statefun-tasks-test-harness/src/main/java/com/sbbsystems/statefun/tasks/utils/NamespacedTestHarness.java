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

package com.sbbsystems.statefun.tasks.utils;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.Event;
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.testmodule.IoIdentifiers;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.sbbsystems.statefun.tasks.util.Unchecked.unchecked;

public class NamespacedTestHarness {
    private final String namespace;
    private static final AtomicInteger counter = new AtomicInteger();
    private static final Logger LOG = LoggerFactory.getLogger(NamespacedTestHarness.class);

    private NamespacedTestHarness(String namespace) {
        this.namespace = namespace;
    }

    public static NamespacedTestHarness newInstance() {
        HarnessUtils.ensureHarnessThreadIsRunning();
        var counterVal = counter.getAndIncrement();
        var namespace = String.format("test-%s-", counterVal);
        LOG.info("Created {} instance with namespace {}", NamespacedTestHarness.class.getSimpleName(), namespace);
        return new NamespacedTestHarness(namespace);
    }

    public void addIngressTaskRequest(TaskRequest taskRequest) {
        var replyTopic = taskRequest.getReplyTopic();
        var prefixedReplyTopic = namespace + replyTopic;
        TestEgress.initialiseTopic(prefixedReplyTopic);

        var pipelineId = taskRequest.getId();
        var prefixedEventsTopic = pipelineId + "_" + IoIdentifiers.EVENTS_TOPIC;
        TestEgress.initialiseTopic(prefixedEventsTopic);

        var updatedRequest = taskRequest.toBuilder()
                .setReplyTopic(prefixedReplyTopic)
                .build();
        TestIngress.addMessage(MessageTypes.wrap(updatedRequest));
    }

    public TypedValue getMessage(String topic) {
        var topicWithPrefix = namespace + topic;
        return TestEgress.getMessage(topicWithPrefix);
    }

    public List<Event> getEvents(String pipelineId) {
        var topicWithPrefix = pipelineId + "_" + IoIdentifiers.EVENTS_TOPIC;

        return TestEgress.getMessages(topicWithPrefix).stream().map(unchecked(message -> {
            var kafkaProducerRecord = KafkaProducerRecord.parseFrom(message.getValue());
            return Any.parseFrom(kafkaProducerRecord.getValueBytes()).unpack(Event.class);
        })).collect(Collectors.toUnmodifiableList());
    }

    public TypedValue runPipeline(Pipeline pipeline) {
        return runPipeline(pipeline, null);
    }

    public TypedValue runPipeline(Pipeline pipeline, Any state) {
        var uid = UUID.randomUUID().toString();
        var taskRequest = TaskRequest.newBuilder()
                .setId(uid)
                .setUid(uid)
                .setReplyTopic(uid)
                .setRequest(Any.pack(pipeline));

        if (!Objects.isNull(state)) {
            taskRequest.setState(state);
        }

        addIngressTaskRequest(taskRequest.build());
        return getMessage(uid);
    }

    public Any runPipelineAndGetResponse(Pipeline pipeline)
            throws InvalidProtocolBufferException
    {
        return runPipelineAndGetResponse(pipeline, null);
    }

    public Any runPipelineAndGetResponse(Pipeline pipeline, Any state)
            throws InvalidProtocolBufferException
    {
        var kafkaProducerRecord = KafkaProducerRecord.parseFrom(runPipeline(pipeline, state).getValue());
        return Any.parseFrom(kafkaProducerRecord.getValueBytes());
    }
}
