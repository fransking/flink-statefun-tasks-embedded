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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MessageHandler<T> {
    private static final Logger LOG = LoggerFactory.getLogger(MessageHandler.class);

    public abstract boolean canHandle(Context context, Object input);

    public abstract CheckedFunction<ByteString, T, InvalidProtocolBufferException> getMessageBuilder();

    public abstract void handleMessage(Context context, T message)
            throws StatefunTasksException;

    public final void handleInput(Context context, Object input) {
        try {
            var message = MessageTypes.asType(input, getMessageBuilder());
            handleMessage(context, message);
        } catch (StatefunTasksException e) {
            LOG.error("Unable to handle input message", e);
        }
    }
}
