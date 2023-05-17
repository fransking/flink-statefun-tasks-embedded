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
package com.sbbsystems.statefun.tasks.messagehandlers;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TaskRequestHandler extends MessageHandler<TaskRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(TaskRequestHandler.class);

    @Override
    public boolean canHandle(Context context, Object input) {
        return MessageTypes.isType(input, TaskRequest.class);
    }

    @Override
    public CheckedFunction<ByteString, TaskRequest, InvalidProtocolBufferException> getMessageBuilder() {
        return TaskRequest::parseFrom;
    }

    @Override
    public void handleMessage(Context context, TaskRequest taskRequest)
            throws StatefunTasksException {

        LOG.info("GOT A TASK_REQUEST");

        LOG.info("TASK_REQUEST_ID is {}", taskRequest.getId());

        var egress = new EgressIdentifier<>("example", "kafka-generic-egress", TypedValue.class);
        var taskResult = TaskResult.newBuilder()
                .setId(taskRequest.getId())
                .setUid(taskRequest.getUid())
                .build();

        var egressRecord = KafkaProducerRecord.newBuilder()
                .setTopic(taskRequest.getReplyTopic())
                .setValueBytes(Any.pack(taskResult).toByteString())
                .build();

        var typedValue = TypedValue.newBuilder()
                .setValue(egressRecord.toByteString())
                .setHasValue(true)
                .setTypename("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord")
                .build();

        context.send(egress, typedValue);
    }
}
