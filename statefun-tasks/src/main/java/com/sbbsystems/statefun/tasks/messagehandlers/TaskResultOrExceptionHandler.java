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
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TaskResultOrExceptionHandler extends MessageHandler<TaskResultOrException, PipelineFunctionState> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskResultOrExceptionHandler.class);

    private final PipelineConfiguration configuration;

    public static TaskResultOrExceptionHandler from(PipelineConfiguration configuration) {
        return new TaskResultOrExceptionHandler(configuration);
    }

    private TaskResultOrExceptionHandler(PipelineConfiguration configuration) {
        this.configuration = configuration;
    }


    @Override
    public boolean canHandle(Context context, Object input, PipelineFunctionState state) {
        return MessageTypes.isType(input, TaskResultOrException.class);
    }

    @Override
    public CheckedFunction<ByteString, TaskResultOrException, InvalidProtocolBufferException> getMessageBuilder() {
        return TaskResultOrException::parseFrom;
    }

    @Override
    public void handleMessage(Context context, TaskResultOrException message, PipelineFunctionState state) {
        if (message.hasTaskResult()) {
            LOG.info("Received a TaskResult with ID {}", message.getTaskResult().getId());
        } else {
            LOG.info("Received a TaskException with ID {}", message.getTaskException().getId());
        }
    }
}
