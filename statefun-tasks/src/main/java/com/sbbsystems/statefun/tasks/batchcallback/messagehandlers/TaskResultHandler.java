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

package com.sbbsystems.statefun.tasks.batchcallback.messagehandlers;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.batchcallback.CallbackFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.messagehandlers.MessageHandler;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;

public class TaskResultHandler extends MessageHandler<TaskResult, CallbackFunctionState> {
    private final BatchSubmitter batchSubmitter;

    private TaskResultHandler(PipelineConfiguration configuration, BatchSubmitter batchSubmitter) {
        super(configuration);
        this.batchSubmitter = batchSubmitter;
    }

    public static TaskResultHandler of(PipelineConfiguration configuration, BatchSubmitter batchSubmitter) {
        return new TaskResultHandler(configuration, batchSubmitter);
    }

    @Override
    public boolean canHandle(Context context, Object input, CallbackFunctionState callbackFunctionState) {
        return MessageTypes.isType(input, TaskResult.class);
    }

    @Override
    public CheckedFunction<ByteString, TaskResult, InvalidProtocolBufferException> getMessageBuilder() {
        return TaskResult::parseFrom;
    }

    @Override
    public void handleMessage(Context context, TaskResult taskResult, CallbackFunctionState state) {
        state.getBatch().append(TaskResultOrException.newBuilder().setTaskResult(taskResult).build());
        this.batchSubmitter.trySubmitBatch(context, state);
    }
}
