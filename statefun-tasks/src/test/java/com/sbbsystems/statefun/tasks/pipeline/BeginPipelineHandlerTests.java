/*
 * Copyright [2026] [Frans King]
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
package com.sbbsystems.statefun.tasks.pipeline;

import com.google.protobuf.Any;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.events.PipelineEvents;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.graph.InvalidGraphException;
import com.sbbsystems.statefun.tasks.graph.v2.PipelineGraph;
import com.sbbsystems.statefun.tasks.graph.v2.PipelineGraphBuilder;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static com.sbbsystems.statefun.tasks.graph.v2.GraphTestUtils.buildPipelineFromTemplate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class BeginPipelineHandlerTests {

    private static final FunctionType PIPELINE_FUNCTION_TYPE = new FunctionType("example", "embedded_pipeline");
    private static final String PIPELINE_ID = "pipeline-id";

    private PipelineConfiguration configuration;
    private PipelineFunctionState state;
    private Context context;

    @BeforeEach
    public void setup() {
        configuration = PipelineConfiguration.of("example/embedded_pipeline", "example/kafka-generic-egress", true);
        state = PipelineFunctionState.newInstance();
        context = mock(Context.class);
        when(context.self()).thenReturn(new Address(PIPELINE_FUNCTION_TYPE, PIPELINE_ID));
    }

    private PipelineGraph buildGraph(List<?> template) throws InvalidGraphException {
        var pipeline = buildPipelineFromTemplate(template);
        return PipelineGraphBuilder.from(state)
                .withDefaultNamespace("example")
                .withDefaultWorkerName("worker")
                .fromProto(pipeline)
                .build();
    }

    private BeginPipelineHandler handlerFor(PipelineGraph graph) {
        return BeginPipelineHandler.from(configuration, state, graph, PipelineEvents.from(configuration, state));
    }

    private static TaskRequest parseSentTaskRequest(TypedValue typedValue) throws InvalidMessageTypeException {
        return MessageTypes.asType(typedValue, TaskRequest::parseFrom);
    }

    // ---- single task pipeline ------------------------------------------------------------------

    @Test
    public void sends_one_task_request_for_single_task_pipeline() throws Exception {

        state.setInitialArgsAndKwargs(ArgsAndKwargs.getDefaultInstance());
        var graph = buildGraph(List.of("task-1"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        var captor = ArgumentCaptor.forClass(TypedValue.class);
        verify(context, times(1)).send(any(Address.class), captor.capture());

        var sent = parseSentTaskRequest(captor.getValue());
        assertThat(sent.getId()).isEqualTo("task-1");
        assertThat(sent.getUid()).isEqualTo("task-1");
    }

    @Test
    public void sends_one_task_request_for_single_task_pipeline_value_mode() throws Exception {

        configuration = PipelineConfiguration.of("example/embedded_pipeline", "example/kafka-generic-egress", false);
        state.setInitialValueArgsAndKwargs(ValueArgsAndKwargs.getDefaultInstance());
        var graph = buildGraph(List.of("task-1"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        var captor = ArgumentCaptor.forClass(TypedValue.class);
        verify(context, times(1)).send(any(Address.class), captor.capture());

        var sent = parseSentTaskRequest(captor.getValue());
        assertThat(sent.getId()).isEqualTo("task-1");
        assertThat(sent.getUid()).isEqualTo("task-1");
    }

    // ---- reply address -------------------------------------------------------------------------

    @Test
    public void reply_address_on_outgoing_request_points_to_callback_function() throws Exception {

        state.setInitialArgsAndKwargs(ArgsAndKwargs.getDefaultInstance());
        var graph = buildGraph(List.of("task-1"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        var captor = ArgumentCaptor.forClass(TypedValue.class);
        verify(context).send(any(Address.class), captor.capture());

        var replyAddress = parseSentTaskRequest(captor.getValue()).getReplyAddress();
        assertThat(replyAddress.getNamespace()).isEqualTo("example");
        assertThat(replyAddress.getType()).isEqualTo("embedded_pipeline_callback");
        assertThat(replyAddress.getId()).isEqualTo(PIPELINE_ID);
    }

    @Test
    public void reply_address_on_outgoing_request_points_to_callback_function_value_mode() throws Exception {

        configuration = PipelineConfiguration.of("example/embedded_pipeline", "example/kafka-generic-egress", false);
        state.setInitialValueArgsAndKwargs(ValueArgsAndKwargs.getDefaultInstance());
        var graph = buildGraph(List.of("task-1"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        var captor = ArgumentCaptor.forClass(TypedValue.class);
        verify(context).send(any(Address.class), captor.capture());

        var replyAddress = parseSentTaskRequest(captor.getValue()).getReplyAddress();
        assertThat(replyAddress.getNamespace()).isEqualTo("example");
        assertThat(replyAddress.getType()).isEqualTo("embedded_pipeline_callback");
        assertThat(replyAddress.getId()).isEqualTo(PIPELINE_ID);
    }

    // ---- initial args forwarding ---------------------------------------------------------------

    @Test
    public void initial_args_are_forwarded_to_first_task() throws Exception {

        var initialTuple = TupleOfAny.newBuilder()
                .addItems(Any.pack(Pipeline.getDefaultInstance()))
                .build();
        state.setInitialArgsAndKwargs(ArgsAndKwargs.newBuilder().setArgs(initialTuple).build());
        var graph = buildGraph(List.of("task-1"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        var captor = ArgumentCaptor.forClass(TypedValue.class);
        verify(context).send(any(Address.class), captor.capture());

        // task entry has no pre-set args so the initial tuple is passed through directly
        var request = parseSentTaskRequest(captor.getValue()).getRequest();
        assertThat(request.is(TupleOfAny.class)).isTrue();
        assertThat(request.unpack(TupleOfAny.class).getItemsCount()).isEqualTo(1);
        assertThat(request.unpack(TupleOfAny.class).getItems(0).is(Pipeline.class)).isTrue();
    }

    @Test
    public void initial_args_are_forwarded_to_first_task_value_mode() throws Exception {

        configuration = PipelineConfiguration.of("example/embedded_pipeline", "example/kafka-generic-egress", false);
        var initialTuple = TupleOfValue.newBuilder()
                .addItems(Value.newBuilder().setAnyValue(Any.pack(Pipeline.getDefaultInstance())).build())
                .build();
        state.setInitialValueArgsAndKwargs(ValueArgsAndKwargs.newBuilder().setArgs(initialTuple).build());
        var graph = buildGraph(List.of("task-1"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        var captor = ArgumentCaptor.forClass(TypedValue.class);
        verify(context).send(any(Address.class), captor.capture());

        // task entry has no pre-set args so the initial value tuple is passed through directly as a Value
        var request = parseSentTaskRequest(captor.getValue()).getRequest();
        assertThat(request.is(TupleOfValue.class)).isTrue();
        var values = request.unpack(TupleOfValue.class);
        assertThat(values.getItemsCount()).isEqualTo(1);
        assertThat(values.getItems(0).getAnyValue().is(Pipeline.class)).isTrue();
    }

    // ---- pipeline status -----------------------------------------------------------------------

    @Test
    public void state_is_running_after_beginning_a_non_empty_pipeline() throws Exception {

        state.setInitialArgsAndKwargs(ArgsAndKwargs.getDefaultInstance());
        var graph = buildGraph(List.of("task-1"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        assertThat(state.getStatus()).isEqualTo(TaskStatus.Status.RUNNING);
    }

    @Test
    public void state_is_running_after_beginning_a_non_empty_pipeline_value_mode() throws Exception {

        configuration = PipelineConfiguration.of("example/embedded_pipeline", "example/kafka-generic-egress", false);
        state.setInitialValueArgsAndKwargs(ValueArgsAndKwargs.getDefaultInstance());
        var graph = buildGraph(List.of("task-1"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        assertThat(state.getStatus()).isEqualTo(TaskStatus.Status.RUNNING);
    }

    // ---- parallel group ------------------------------------------------------------------------

    @Test
    public void sends_one_task_request_per_chain_in_a_parallel_group() throws Exception {

        state.setInitialArgsAndKwargs(ArgsAndKwargs.getDefaultInstance());
        var group = List.of(
                List.of("task-a"),
                List.of("task-b"),
                List.of("task-c")
        );
        var graph = buildGraph(List.of(group));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        verify(context, times(3)).send(any(Address.class), any(TypedValue.class));
    }

    @Test
    public void sends_one_task_request_per_chain_in_a_parallel_group_value_mode() throws Exception {

        configuration = PipelineConfiguration.of("example/embedded_pipeline", "example/kafka-generic-egress", false);
        state.setInitialValueArgsAndKwargs(ValueArgsAndKwargs.getDefaultInstance());
        var group = List.of(
                List.of("task-a"),
                List.of("task-b"),
                List.of("task-c")
        );
        var graph = buildGraph(List.of(group));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        verify(context, times(3)).send(any(Address.class), any(TypedValue.class));
    }

    // ---- task preceded by empty group ----------------------------------------------------------

    @Test
    public void task_preceded_by_empty_group_receives_empty_array_not_initial_args() throws Exception {

        // set non-trivial initial args to confirm they are NOT forwarded
        var initialTuple = TupleOfAny.newBuilder()
                .addItems(Any.pack(Pipeline.getDefaultInstance()))
                .build();
        state.setInitialArgsAndKwargs(ArgsAndKwargs.newBuilder().setArgs(initialTuple).build());

        var emptyGroup = List.of();  // a List item in the template becomes a GroupEntry with no chains
        var graph = buildGraph(List.of(emptyGroup, "task-1"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        var captor = ArgumentCaptor.forClass(TypedValue.class);
        verify(context).send(any(Address.class), captor.capture());

        // expect MessageTypes.argsOfEmptyArray(): TupleOfAny containing one pack(ArrayOfAny)
        var request = parseSentTaskRequest(captor.getValue()).getRequest();
        assertThat(request.is(TupleOfAny.class)).isTrue();
        var tuple = request.unpack(TupleOfAny.class);
        assertThat(tuple.getItemsCount()).isEqualTo(1);
        assertThat(tuple.getItems(0).is(ArrayOfAny.class)).isTrue();
    }

    @Test
    public void task_preceded_by_empty_group_receives_empty_array_not_initial_args_value_mode() throws Exception {

        configuration = PipelineConfiguration.of("example/embedded_pipeline", "example/kafka-generic-egress", false);
        var initialTuple = TupleOfValue.newBuilder()
                .addItems(Value.newBuilder().setStringValue("should-not-appear").build())
                .build();
        state.setInitialValueArgsAndKwargs(ValueArgsAndKwargs.newBuilder().setArgs(initialTuple).build());

        var emptyGroup = List.of();
        var graph = buildGraph(List.of(emptyGroup, "task-1"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        var captor = ArgumentCaptor.forClass(TypedValue.class);
        verify(context).send(any(Address.class), captor.capture());

        // empty group path uses valueArgsOfEmptyArray(): Value with a single-item TupleOfValue containing an ArrayOfValue
        var request = parseSentTaskRequest(captor.getValue()).getRequest();
        assertThat(request.is(TupleOfValue.class)).isTrue();
        var values = request.unpack(TupleOfValue.class);
        assertThat(values.getItemsCount()).isEqualTo(1);
        assertThat(values.getItems(0).hasArrayValue()).isTrue();
    }

    // ---- all-empty-group pipeline --------------------------------------------------------------

    @Test
    public void all_empty_group_pipeline_responds_with_empty_array_result() throws Exception {

        // a single empty group: PipelineStep iterates past it, finds no tasks, hasNoTasksToCall() = true
        var graph = buildGraph(List.of(List.of()));
        when(context.caller()).thenReturn(new Address(new FunctionType("caller", "func"), "caller-id"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        var captor = ArgumentCaptor.forClass(TypedValue.class);
        verify(context, times(1)).send(any(Address.class), captor.capture());

        var result = MessageTypes.asType(captor.getValue(), TaskResult::parseFrom);
        assertThat(result.getResult().is(ArrayOfAny.class)).isTrue();
    }

    @Test
    public void all_empty_group_pipeline_responds_with_empty_array_result_value_mode() throws Exception {

        configuration = PipelineConfiguration.of("example/embedded_pipeline", "example/kafka-generic-egress", false);
        var graph = buildGraph(List.of(List.of()));
        when(context.caller()).thenReturn(new Address(new FunctionType("caller", "func"), "caller-id"));

        handlerFor(graph).beginPipeline(context, TaskRequest.newBuilder().setId(PIPELINE_ID).build());

        var captor = ArgumentCaptor.forClass(TypedValue.class);
        verify(context, times(1)).send(any(Address.class), captor.capture());

        var result = MessageTypes.asType(captor.getValue(), TaskResult::parseFrom);
        assertThat(result.getResult().is(ArrayOfValue.class)).isTrue();
    }
}
