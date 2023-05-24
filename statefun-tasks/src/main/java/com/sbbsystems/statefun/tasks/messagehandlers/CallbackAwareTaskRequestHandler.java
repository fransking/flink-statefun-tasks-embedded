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
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.CallbackSignal;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class CallbackAwareTaskRequestHandler extends MessageHandler<TaskRequest, PipelineFunctionState> {
    private final FunctionType callbackFunctionType;
    private final MessageHandler<TaskRequest, PipelineFunctionState> innerHandler;

    private static final TypedValue START_SIGNAL = MessageTypes.wrap(CallbackSignal.newBuilder()
            .setValue(CallbackSignal.Signal.PIPELINE_STARTING)
            .build());

    public static CallbackAwareTaskRequestHandler withRequestHandler(FunctionType callbackFunctionType,
                                                                     MessageHandler<TaskRequest, PipelineFunctionState> requestHandler) {
        return new CallbackAwareTaskRequestHandler(callbackFunctionType, requestHandler);
    }

    private CallbackAwareTaskRequestHandler(FunctionType callbackFunctionType, MessageHandler<TaskRequest, PipelineFunctionState> innerHandler) {
        super(innerHandler.configuration);
        this.callbackFunctionType = callbackFunctionType;
        this.innerHandler = innerHandler;
    }

    @Override
    public boolean canHandle(Context context, Object input, PipelineFunctionState state) {
        return MessageTypes.isType(input, TaskRequest.class) && innerHandler.canHandle(context, input, state);
    }

    @Override
    public CheckedFunction<ByteString, TaskRequest, InvalidProtocolBufferException> getMessageBuilder() {
        return TaskRequest::parseFrom;
    }

    @Override
    public void handleMessage(Context context, TaskRequest message, PipelineFunctionState state)
            throws StatefunTasksException {

        String id = context.self().id();
        context.send(this.callbackFunctionType, id, START_SIGNAL);
        innerHandler.handleMessage(context, message, state);
    }
}
