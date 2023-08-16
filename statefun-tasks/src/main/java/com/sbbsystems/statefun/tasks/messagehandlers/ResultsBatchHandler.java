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
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.events.PipelineEvents;
import com.sbbsystems.statefun.tasks.generated.CallbackSignal;
import com.sbbsystems.statefun.tasks.generated.ResultsBatch;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.graph.PipelineGraphBuilder;
import com.sbbsystems.statefun.tasks.pipeline.CancelPipelineHandler;
import com.sbbsystems.statefun.tasks.pipeline.ContinuePipelineHandler;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import com.sbbsystems.statefun.tasks.util.TimedBlock;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public final class ResultsBatchHandler extends MessageHandler<ResultsBatch, PipelineFunctionState> {
    private static final Logger LOG = LoggerFactory.getLogger(ResultsBatchHandler.class);

    private final FunctionType callbackFunctionType;

    public static ResultsBatchHandler forCallback(FunctionType callbackFunctionType, PipelineConfiguration configuration) {
        return new ResultsBatchHandler(callbackFunctionType, configuration);
    }

    private static final CallbackSignal BATCH_PROCESSED_SIGNAL = CallbackSignal.newBuilder()
            .setValue(CallbackSignal.Signal.BATCH_PROCESSED)
            .build();

    private ResultsBatchHandler(FunctionType callbackFunctionType, PipelineConfiguration configuration) {
        super(configuration);
        this.callbackFunctionType = callbackFunctionType;
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
    public void handleMessage(Context context, ResultsBatch message, PipelineFunctionState state) {
        try (var ignored = TimedBlock.of(LOG::info, "Processing results batch of {0} messages for pipeline {1}", message.getResultsCount(), context.self())) {
            try {
                // create the graph
                var graph = PipelineGraphBuilder.from(state).build();

                // create events
                var events = PipelineEvents.from(configuration, state);

                for (var resultOrException : message.getResultsList()) {
                    handleEachMessage(context, resultOrException, state, graph, events);
                }

                // save updated graph state
                graph.saveUpdatedState();

            } catch (StatefunTasksException e) {
                failWithError(context, state, e);
            } finally {
                // todo we are calling the callback function too often for too small batches
                // need to dynamically compute an optimal delay based on number of tasks in flight
                // & time between callbacks to increase the batch size without adding latency

                //context.send(this.callbackFunctionType, context.self().id(), MessageTypes.wrap(BATCH_PROCESSED_SIGNAL));
                context.sendAfter(Duration.ofSeconds(1), this.callbackFunctionType, context.self().id(), MessageTypes.wrap(BATCH_PROCESSED_SIGNAL));
            }
        }
    }

    private void handleEachMessage(Context context,
                                   TaskResultOrException message,
                                   PipelineFunctionState state,
                                   PipelineGraph graph,
                                   PipelineEvents events) {

        try {
            // handle message
            switch (state.getStatus().getNumber()) {
                case TaskStatus.Status.RUNNING_VALUE:
                case TaskStatus.Status.PAUSED_VALUE:
                    // continue pipeline onto continuations
                    ContinuePipelineHandler.from(configuration, state, graph, events).continuePipeline(context, message);
                    break;

                case TaskStatus.Status.CANCELLING_VALUE:
                case TaskStatus.Status.CANCELLED_VALUE:
                    // cancel pipeline dealing after any finally task is complete
                    CancelPipelineHandler.from(configuration, state, graph, events).cancelPipeline(context, message);
                    break;
            }
        } catch (Exception e) {
            failWithError(context, state, e);
        }
    }

    private void failWithError(Context context, PipelineFunctionState state, Exception e) {
        var taskRequest = state.getTaskRequest();
        LOG.error(e.getMessage(), e);
        state.setStatus(TaskStatus.Status.FAILED);
        respond(context, taskRequest, MessageTypes.toTaskException(taskRequest, e));
    }
}
