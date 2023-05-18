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
import com.sbbsystems.statefun.tasks.generated.TaskException;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.messagehandlers.MessageHandler;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;

public class TaskExceptionHandler extends MessageHandler<TaskException, CallbackFunctionState> {
    private final BatchSubmitter batchSubmitter;

    private TaskExceptionHandler(BatchSubmitter batchSubmitter) {
        this.batchSubmitter = batchSubmitter;
    }

    public static TaskExceptionHandler newInstance(BatchSubmitter batchSubmitter) {
        return new TaskExceptionHandler(batchSubmitter);
    }

    @Override
    public boolean canHandle(Context context, Object input, CallbackFunctionState callbackFunctionState) {
        return MessageTypes.isType(input, TaskException.class);
    }

    @Override
    public CheckedFunction<ByteString, TaskException, InvalidProtocolBufferException> getMessageBuilder() {
        return TaskException::parseFrom;
    }

    @Override
    public void handleMessage(Context context, TaskException taskException, CallbackFunctionState state) {
        state.getBatch().append(TaskResultOrException.newBuilder().setTaskException(taskException).build());
        this.batchSubmitter.trySubmitBatch(context, state);
    }
}
