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

import com.sbbsystems.statefun.tasks.generated.CallbackSignal;
import com.sbbsystems.statefun.tasks.generated.ResultsBatch;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.messagehandlers.MessageHandler;
import com.sbbsystems.statefun.tasks.messagehandlers.TaskRequestHandler;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.TimedBlock;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PipelineFunction implements StatefulFunction {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineFunction.class);
    private final FunctionType callbackFunctionType;

    public PipelineFunction(FunctionType callbackFunctionType) {
        this.callbackFunctionType = callbackFunctionType;
    }

    @Persisted
    PipelineFunctionState state = PipelineFunctionState.getInstance();

    private static final List<MessageHandler<?>> messageHandlers = List.of(
            TaskRequestHandler.getInstance()
    );

    @Override
    public void invoke(Context context, Object input) {
        try (var ignored = TimedBlock.of(LOG::info, "Invoking function {0}", context.self())) {
            if (MessageTypes.isType(input, TaskRequest.class)) {
                var startSignal = CallbackSignal.newBuilder()
                        .setValue(CallbackSignal.Signal.PIPELINE_STARTING)
                        .build();

                context.send(this.callbackFunctionType, context.self().id(), MessageTypes.wrap(startSignal));
            }

            for (var handler : messageHandlers) {
                if (handler.canHandle(context, input, state)) {
                    handler.handleInput(context, input, state);
                    break;
                }
            }

            if (MessageTypes.isType(input, ResultsBatch.class)) {
                var signal = CallbackSignal.newBuilder()
                        .setValue(CallbackSignal.Signal.BATCH_PROCESSED)
                        .build();
                context.send(this.callbackFunctionType, context.self().id(), MessageTypes.wrap(signal));
            }
        }
    }
}
