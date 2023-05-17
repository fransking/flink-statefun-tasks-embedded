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
package com.sbbsystems.statefun.tasks;

import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CallbackFunction implements StatefulFunction {
    private static final Logger LOG = LoggerFactory.getLogger(CallbackFunction.class);
    @Persisted
    private final PersistedAppendingBuffer<TaskResultOrException> batch = PersistedAppendingBuffer.of("batch", TaskResultOrException.class);
    @Persisted
    private final PersistedValue<Boolean> pipelineRequestInProgress = PersistedValue.of("pipelineRequestInProgress", Boolean.class);
    private final FunctionType pipelineFunctionType;

    public CallbackFunction(FunctionType pipelineFunctionType) {
        this.pipelineFunctionType = pipelineFunctionType;
    }

    @Override
    public void invoke(Context context, Object input) {
        try {
            if (MessageTypes.isType(input, CallbackSignal.class)) {
                // signal from PipelineFunction
                var signal = MessageTypes.asType(input, CallbackSignal::parseFrom).getValue();
                handleSignal(context, signal);
            } else if (MessageTypes.isType(input, TaskResult.class)) {
                // result from remote worker
                var taskResult = MessageTypes.asType(input, TaskResult::parseFrom);
                batch.append(TaskResultOrException.newBuilder().setTaskResult(taskResult).build());
            } else if (MessageTypes.isType(input, TaskException.class)) {
                // exception from remote worker
                var taskException = MessageTypes.asType(input, TaskException::parseFrom);
                batch.append(TaskResultOrException.newBuilder().setTaskException(taskException).build());
            } else {
                throw new InvalidMessageTypeException(MessageFormat.format("Unexpected message type: {0}", input.getClass().getCanonicalName()));
            }

            sendNextBatch(context);

        } catch (InvalidMessageTypeException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleSignal(Context context, CallbackSignal.Signal signal) throws InvalidMessageTypeException {
        // Signal from PipelineFunction
        switch (signal) {
            case PIPELINE_STARTING:
                LOG.info("{} - starting pipeline", context.self().id());
                resetState();
                break;
            case BATCH_PROCESSED:
                LOG.debug("{} - batch processed", context.self().id());
                pipelineRequestInProgress.set(false);
                break;
            default:
                throw new InvalidMessageTypeException(
                        MessageFormat.format("Unexpected CallbackSignal {0}", signal.getValueDescriptor().getName()));
        }
    }

    private void resetState() {
        batch.clear();
        pipelineRequestInProgress.set(false);
    }

    private void sendNextBatch(Context context) {
        if (!this.pipelineRequestInProgress.getOrDefault(false)) {
            var batchList = StreamSupport.stream(this.batch.view().spliterator(), false)
                    .collect(Collectors.toList());

            if (batchList.size() > 0) {
                var functionId = context.self().id();
                LOG.info("{} - Sending batch of {} results", functionId, batchList.size());
                var batchMessage = ResultsBatch.newBuilder()
                        .addAllResults(batchList)
                        .build();
                var message = MessageTypes.wrap(batchMessage);
                context.send(this.pipelineFunctionType, functionId, message);
                this.pipelineRequestInProgress.set(true);
                this.batch.clear();
            }
        }
    }
}
