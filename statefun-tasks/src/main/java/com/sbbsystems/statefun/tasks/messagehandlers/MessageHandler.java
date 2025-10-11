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

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.Address;
import com.sbbsystems.statefun.tasks.generated.TaskActionRequest;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class MessageHandler<TMessage, TState> {
    private static final Logger LOG = LoggerFactory.getLogger(MessageHandler.class);

    protected final PipelineConfiguration configuration;

    public MessageHandler(PipelineConfiguration configuration) {
        this.configuration = configuration;
    }

    public abstract boolean canHandle(Context context, Object input, TState state);

    public abstract CheckedFunction<ByteString, TMessage, InvalidProtocolBufferException> getMessageBuilder();

    public abstract void handleMessage(Context context, TMessage message, TState state)
            throws StatefunTasksException;


    public final void handleInput(Context context, Object input, TState state) {
        try {
            var message = MessageTypes.asType(input, getMessageBuilder());
            handleMessage(context, message, state);
        } catch (StatefunTasksException e) {
            LOG.error("Unable to handle input message", e);
        }
    }

    public final void respond(@NotNull Context context, TaskRequest taskRequest, Message message) {
        respond(context, taskRequest.getReplyTopic(), taskRequest.getReplyAddress(), message);
    }

    public final void respond(@NotNull Context context, TaskActionRequest taskActionRequest, Message message) {
        respond(context, taskActionRequest.getReplyTopic(), taskActionRequest.getReplyAddress(), message);
    }

    private void respond(@NotNull Context context, String replyTopic, Address replyAddress, Message message) {
        if (!Strings.isNullOrEmpty(replyTopic)) {
            // send a message to egress if reply_topic was specified
            context.send(MessageTypes.getEgress(configuration), MessageTypes.toEgress(message, replyTopic));
        } else if (replyAddress != null) {
            // else call back to a particular flink function if reply_address was specified
            if (!Strings.isNullOrEmpty(replyAddress.getNamespace())
                && !Strings.isNullOrEmpty(replyAddress.getType())
                && !Strings.isNullOrEmpty(replyAddress.getId())) {

                context.send(MessageTypes.toSdkAddress(replyAddress), MessageTypes.wrap(message));
            }
        } else if (!Objects.isNull(context.caller())) {
            // else call back to the caller of this function (if there is one)
            context.send(context.caller(), MessageTypes.wrap(message));
        }
    }
}
