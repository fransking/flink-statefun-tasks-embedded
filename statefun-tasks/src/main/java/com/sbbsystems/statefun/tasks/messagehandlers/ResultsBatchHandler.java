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
import com.sbbsystems.statefun.tasks.generated.ResultsBatch;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;

public final class ResultsBatchHandler extends MessageHandler<ResultsBatch, PipelineFunctionState> {
    private final FunctionType callbackFunctionType;
    private final MessageHandler<TaskResultOrException, PipelineFunctionState> resultHandler;
    private static final CallbackSignal BATCH_PROCESSED_SIGNAL = CallbackSignal.newBuilder()
            .setValue(CallbackSignal.Signal.BATCH_PROCESSED)
            .build();

    private ResultsBatchHandler(FunctionType callbackFunctionType, MessageHandler<TaskResultOrException, PipelineFunctionState> resultHandler) {
        this.callbackFunctionType = callbackFunctionType;
        this.resultHandler = resultHandler;
    }

    public static ResultsBatchHandler withResultHandler(FunctionType callbackFunctionType,
                                                        MessageHandler<TaskResultOrException, PipelineFunctionState> resultHandler) {
        return new ResultsBatchHandler(callbackFunctionType, resultHandler);
    }

    @Override
    public boolean canHandle(Context context, Object input, PipelineFunctionState state) {
        return MessageTypes.isType(input, ResultsBatch.class);
    }

    @Override
    public CheckedFunction<ByteString, ResultsBatch, InvalidProtocolBufferException> getMessageBuilder() {
        return ResultsBatch::parseFrom;
    }

    @Override
    public void handleMessage(Context context, ResultsBatch message, PipelineFunctionState state) throws StatefunTasksException {
        for (var resultOrException : message.getResultsList()) {
            this.resultHandler.handleMessage(context, resultOrException, state);
        }
        context.send(this.callbackFunctionType, context.self().id(), MessageTypes.wrap(BATCH_PROCESSED_SIGNAL));
    }
}
