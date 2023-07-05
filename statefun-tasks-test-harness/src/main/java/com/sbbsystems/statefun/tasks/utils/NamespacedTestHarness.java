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
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.testmodule.IoIdentifiers;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.sbbsystems.statefun.tasks.util.Unchecked.unchecked;
import static java.util.Objects.isNull;

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

    public void addIngressTaskActionRequest(TaskActionRequest action) {
        var replyTopic = action.getReplyTopic();
        var prefixedReplyTopic = namespace + "-action-" + replyTopic;
        TestEgress.initialiseTopic(prefixedReplyTopic);

        var updatedRequest = action.toBuilder()
                .setReplyTopic(prefixedReplyTopic)
                .build();
        TestIngress.addMessage(MessageTypes.wrap(updatedRequest));
    }

    public TypedValue getMessage(String topic) {
        var topicWithPrefix = namespace + topic;
        return TestEgress.getMessage(topicWithPrefix);
    }

    public TypedValue getActionMessage(String topic) {
        var topicWithPrefix = namespace + "-action-" + topic;
        return TestEgress.getMessage(topicWithPrefix);
    }

    public List<Event> getEvents(String pipelineId) {
        var topicWithPrefix = pipelineId + "_" + IoIdentifiers.EVENTS_TOPIC;

        return TestEgress.getMessages(topicWithPrefix).stream().map(unchecked(message -> {
            var kafkaProducerRecord = KafkaProducerRecord.parseFrom(message.getValue());
            return Any.parseFrom(kafkaProducerRecord.getValueBytes()).unpack(Event.class);
        })).collect(Collectors.toUnmodifiableList());
    }

    public Event pollForEvent(String pipelineId, long timeoutMillis)
            throws InterruptedException, InvalidProtocolBufferException {

        var topicWithPrefix = pipelineId + "_" + IoIdentifiers.EVENTS_TOPIC;

        var typedValue = TestEgress.pollMessage(topicWithPrefix, timeoutMillis);

        if (isNull(typedValue)) {
            throw new NoSuchElementException("Timed out polling for event");
        }

        var kafkaProducerRecord = KafkaProducerRecord.parseFrom(typedValue.getValue());
        return Any.parseFrom(kafkaProducerRecord.getValueBytes()).unpack(Event.class);
    }

    public TypedValue runPipeline(Pipeline pipeline) {
        return runPipeline(pipeline, null);
    }

    public TypedValue runPipeline(Pipeline pipeline, Any state) {
        var uid = UUID.randomUUID().toString();
        return runPipeline(pipeline, state, uid);
    }

    public TypedValue runPipeline(Pipeline pipeline, Any state, String uid) {
        startPipeline(pipeline, state, uid);
        return getMessage(uid);
    }

    public void startPipeline(Pipeline pipeline, Any state, String uid) {
        var taskRequest = TaskRequest.newBuilder()
                .setId(uid)
                .setUid(uid)
                .setReplyTopic(uid)
                .setRequest(Any.pack(pipeline));

        if (!isNull(state)) {
            taskRequest.setState(state);
        }

        addIngressTaskRequest(taskRequest.build());
    }

    public TypedValue sendAction(TaskAction action, String uid) {
        var taskActionRequest = TaskActionRequest.newBuilder()
                .setId(uid)
                .setUid(uid)
                .setAction(action)
                .setReplyTopic(uid);

        addIngressTaskActionRequest(taskActionRequest.build());
        return getActionMessage(uid);
    }

    public Any runPipelineAndGetResponse(Pipeline pipeline)
            throws InvalidProtocolBufferException {
        return runPipelineAndGetResponse(pipeline, null, UUID.randomUUID().toString());
    }

    public Any runPipelineAndGetResponse(Pipeline pipeline, Any state)
            throws InvalidProtocolBufferException {
        return runPipelineAndGetResponse(pipeline, state, UUID.randomUUID().toString());
    }

    public Any runPipelineAndGetResponse(Pipeline pipeline, Any state, String uid)
            throws InvalidProtocolBufferException {
        var kafkaProducerRecord = KafkaProducerRecord.parseFrom(runPipeline(pipeline, state, uid).getValue());
        return Any.parseFrom(kafkaProducerRecord.getValueBytes());
    }


    public Any sendActionAndGetResponse(TaskAction action, String uid)
            throws InvalidProtocolBufferException {
        var actionResult = sendAction(action, uid);
        var kafkaProducerRecord = KafkaProducerRecord.parseFrom(actionResult.getValue());
        return Any.parseFrom(kafkaProducerRecord.getValueBytes());
    }
}
