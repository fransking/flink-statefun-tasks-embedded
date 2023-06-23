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
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestEgress {
    private static final Map<String, BlockingQueue<TypedValue>> egressMap = new HashMap<>();
    private static Thread harnessThread;
    private static final int threadPollFrequencyMs = 100;

    public static void initialise(Thread harnessThread) {
        TestEgress.harnessThread = harnessThread;
    }

    public static void initialiseTopic(String topic) {
        egressMap.put(topic, new LinkedBlockingQueue<>());
    }

    public static void addMessage(TypedValue message) {
        try {
            var kafkaRecord = MessageTypes.asType(message, KafkaProducerRecord::parseFrom);
            var topic = kafkaRecord.getTopic();

            var topicQueue = egressMap.get(topic);
            if (topicQueue == null) {
                throw new RuntimeException(MessageFormat.format("Topic {0} has not been initialised", topic));
            }
            topicQueue.put(message);
        } catch (InvalidMessageTypeException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void addEventMessage(TypedValue message) {
        try {
            var kafkaRecord = MessageTypes.asType(message, KafkaProducerRecord::parseFrom);
            var event = Any.parseFrom(kafkaRecord.getValueBytes()).unpack(Event.class);
            var topic = event.getRootPipelineId() + "_" + kafkaRecord.getTopic();

            var topicQueue = egressMap.get(topic);
            if (topicQueue == null) {
                throw new RuntimeException(MessageFormat.format("Topic {0} has not been initialised", topic));
            }
            topicQueue.put(message);
        } catch (InvalidMessageTypeException | InterruptedException | InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public static TypedValue getMessage(String topic) {
        var topicQueue = getTopicQueue(topic);

        try {
            TypedValue result = null;
            while (result == null) {
                if (!harnessThread.isAlive()) {
                    throw new RuntimeException("Harness has stopped" + harnessThread);
                }
                result = topicQueue.poll(threadPollFrequencyMs, TimeUnit.MILLISECONDS);
            }
            return result;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<TypedValue> getMessages(String topic) {
        var events = new LinkedList<TypedValue>();
        var topicQueue = getTopicQueue(topic);
        topicQueue.drainTo(events);
        return events;
    }

    private static BlockingQueue<TypedValue> getTopicQueue(String topic) {
        if (harnessThread == null) {
            throw new RuntimeException("TestEgress had not been initialised");
        }

        var topicQueue = egressMap.get(topic);

        if (topicQueue == null) {
            throw new RuntimeException(MessageFormat.format("Topic {0} has not been initialised", topic));
        }

        return topicQueue;
    }
}
