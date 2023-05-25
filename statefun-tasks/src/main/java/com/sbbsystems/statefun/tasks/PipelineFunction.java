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

import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.messagehandlers.*;
import com.sbbsystems.statefun.tasks.util.TimedBlock;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PipelineFunction implements StatefulFunction {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineFunction.class);

    private final List<MessageHandler<?, PipelineFunctionState>> messageHandlers;
    @Persisted
    private final PipelineFunctionState state;


    public static PipelineFunction of(PipelineConfiguration configuration, FunctionType callbackFunctionType) {
        return new PipelineFunction(configuration,
                List.of(
                        CallbackAwareTaskRequestHandler.withRequestHandler(
                                callbackFunctionType, TaskRequestHandler.from(configuration)
                        ),
                        ResultsBatchHandler.withResultHandler(
                                callbackFunctionType, TaskResultOrExceptionHandler.from(configuration)
                        )
                )
        );
    }

    private PipelineFunction(PipelineConfiguration configuration, List<MessageHandler<?, PipelineFunctionState>> messageHandlers) {
        this.messageHandlers = messageHandlers;
        this.state = PipelineFunctionState.withExpiration(configuration.getStateExpiration());
    }

    @Override
    public void invoke(Context context, Object input) {
        try (var ignored = TimedBlock.of(LOG::info, "Invoking function {0}", context.self())) {
            for (var handler : messageHandlers) {
                if (handler.canHandle(context, input, state)) {
                    handler.handleInput(context, input, state);
                    return;
                }
            }
            LOG.warn("No handlers for message {}", input instanceof TypedValue ? ((TypedValue) input).getTypename() : input.getClass().getName());
        }
    }
}
