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
package com.sbbsystems.statefun.tasks.batchcallback;

import com.sbbsystems.statefun.tasks.batchcallback.messagehandlers.SimpleBatchSubmitter;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.generated.CallbackSignal;
import com.sbbsystems.statefun.tasks.generated.ResultsBatch;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.flink.statefun.sdk.Context;

import java.util.List;
import java.util.stream.IntStream;

import static com.sbbsystems.statefun.tasks.generated.CallbackSignal.Signal.*;
import static org.mockito.Mockito.*;

public class CallbackFunctionTests {

    private static final FunctionType PIPELINE_FUNCTION_TYPE = new FunctionType("test", "pipeline");
    private static final FunctionType CALLBACK_FUNCTION_TYPE = new FunctionType("test", "callback");
    private CallbackFunction callbackFunction;
    private Context context;

    private static List<TaskResultOrException> extractResultsList(Object arg) {
        try {
            return MessageTypes.asType(arg, ResultsBatch::parseFrom).getResultsList();
        } catch (InvalidMessageTypeException e) {
            throw new RuntimeException(e);
        }
    }

    private static TypedValue buildSignalMessage(CallbackSignal.Signal signal) {
        return MessageTypes.wrap(CallbackSignal.newBuilder().setValue(signal).build());
    }

    @BeforeEach
    public void setup() {
        this.callbackFunction = CallbackFunction.of(PipelineConfiguration.of("example/embedded_pipeline","example/kafka-generic-egress"), SimpleBatchSubmitter.of(PIPELINE_FUNCTION_TYPE));
        this.context = mock(Context.class);
        when(context.self()).thenReturn(new Address(CALLBACK_FUNCTION_TYPE, "pipeline-id"));
    }

    @Test
    public void invoking_with_task_result_sends_result_in_batch() {
        var taskResult = TaskResult.newBuilder().setId("result-id").build();
        var wrappedTaskResult = MessageTypes.wrap(taskResult);

        this.callbackFunction.invoke(context, wrappedTaskResult);

        verify(context).send(
                eq(PIPELINE_FUNCTION_TYPE),
                eq("pipeline-id"),
                argThat(
                        arg -> {
                            List<TaskResultOrException> resultsList = extractResultsList(arg);
                            return resultsList.size() == 1 && resultsList.get(0).getTaskResult().getId().equals("result-id");
                        }));
    }

    @Test
    public void invoking_with_multiple_task_results_without_signalling_sends_only_first_result() {
        var taskResults = IntStream.range(0, 5)
                .boxed()
                .map(i -> TaskResult.newBuilder().setId("result-" + i).build())
                .map(MessageTypes::wrap);

        taskResults.forEach(result -> this.callbackFunction.invoke(context, result));

        verify(context, times(1)).send(eq(PIPELINE_FUNCTION_TYPE), eq("pipeline-id"), any());
        verify(context).send(any(), anyString(), argThat(arg -> extractResultsList(arg).size() == 1));
    }

    @Test
    public void invoking_with_multiple_task_results_then_signalling_sends_two_batches() {
        var taskResults = IntStream.range(0, 5)
                .boxed()
                .map(i -> TaskResult.newBuilder().setId("result-" + i).build())
                .map(MessageTypes::wrap);

        this.callbackFunction.invoke(context, buildSignalMessage(PIPELINE_STARTING));
        taskResults.forEach(result -> this.callbackFunction.invoke(context, result));
        TypedValue batchProcessedMessage = MessageTypes.wrap(
                CallbackSignal.newBuilder().setValue(CallbackSignal.Signal.BATCH_PROCESSED).build());
        this.callbackFunction.invoke(context, batchProcessedMessage);

        verify(context, times(2)).send(eq(PIPELINE_FUNCTION_TYPE), eq("pipeline-id"), any());
        verify(context).send(any(), anyString(), argThat(arg -> extractResultsList(arg).size() == 1));
        verify(context).send(any(), anyString(), argThat(arg -> extractResultsList(arg).size() == 4));
    }

    @Test
    public void pausing_prevents_batch_from_being_sent() {
        var taskResult = TaskResult.newBuilder().setId("result").build();

        this.callbackFunction.invoke(context, buildSignalMessage(PIPELINE_STARTING));
        this.callbackFunction.invoke(context, buildSignalMessage(PAUSE_PIPELINE));
        this.callbackFunction.invoke(context, MessageTypes.wrap(taskResult));

        verify(context, times(0)).send(any(), anyString(), any());
    }

    @Test
    public void pausing_then_resuming_causes_batch_to_be_sent() {
        var taskResult = TaskResult.newBuilder().setId("result").build();

        this.callbackFunction.invoke(context, buildSignalMessage(PIPELINE_STARTING));
        this.callbackFunction.invoke(context, buildSignalMessage(PAUSE_PIPELINE));
        this.callbackFunction.invoke(context, MessageTypes.wrap(taskResult));
        this.callbackFunction.invoke(context, buildSignalMessage(RESUME_PIPELINE));

        verify(context, times(1)).send(eq(PIPELINE_FUNCTION_TYPE), eq("pipeline-id"), any());
    }

    @Test
    public void pausing_then_resuming_then_sending_task_causes_batch_to_be_sent() {
        var taskResult = TaskResult.newBuilder().setId("result").build();

        this.callbackFunction.invoke(context, buildSignalMessage(PIPELINE_STARTING));
        this.callbackFunction.invoke(context, buildSignalMessage(PAUSE_PIPELINE));
        this.callbackFunction.invoke(context, buildSignalMessage(RESUME_PIPELINE));
        this.callbackFunction.invoke(context, MessageTypes.wrap(taskResult));

        verify(context, times(1)).send(eq(PIPELINE_FUNCTION_TYPE), eq("pipeline-id"), any());
    }
}