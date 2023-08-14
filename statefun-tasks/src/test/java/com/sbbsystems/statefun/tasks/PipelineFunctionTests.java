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
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class PipelineFunctionTests {
    private static final FunctionType CALLBACK_FUNCTION_TYPE = new FunctionType("test", "callback");
    private PipelineFunction pipelineFunction;
    private Context context;

    private static CallbackSignal.Signal parseSignalMessage(Object arg) {
        try {
            return MessageTypes.asType(arg, CallbackSignal::parseFrom).getValue();
        } catch (InvalidMessageTypeException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    public void setup() {
        var configuration = PipelineConfiguration.of("example/embedded_pipeline", "example/kafka-generic-egress");
        pipelineFunction = PipelineFunction.of(configuration, CALLBACK_FUNCTION_TYPE);
        context = mock(Context.class);
        when(context.self()).thenReturn(new Address(CALLBACK_FUNCTION_TYPE, "pipeline-id"));
    }

    @Test
    public void invoking_with_task_request_sends_pipeline_started_signal() {
        var taskRequest = MessageTypes.wrap(TaskRequest.newBuilder().setId("pipeline-id").build());

        pipelineFunction.invoke(context, taskRequest);

        verify(context).send(
                eq(CALLBACK_FUNCTION_TYPE),
                eq("di-enilepip"),
                argThat(arg -> parseSignalMessage(arg) == CallbackSignal.Signal.PIPELINE_STARTING));
    }

    @Test
    public void invoking_with_results_batch_sends_batch_processed_signal() {
        var batchRequest = ResultsBatch.newBuilder()
                .addResults(TaskResultOrException.newBuilder().setTaskResult(TaskResult.newBuilder().build()))
                .build();

        pipelineFunction.invoke(context, MessageTypes.wrap(batchRequest));

        verify(context).send(
                eq(CALLBACK_FUNCTION_TYPE),
                eq("di-enilepip"),
                argThat(arg -> parseSignalMessage(arg) == CallbackSignal.Signal.BATCH_PROCESSED));
    }
}
