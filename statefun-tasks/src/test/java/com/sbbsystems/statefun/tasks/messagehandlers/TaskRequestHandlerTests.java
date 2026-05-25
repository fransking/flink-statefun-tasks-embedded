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

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.generated.ArrayOfAny;
import com.sbbsystems.statefun.tasks.generated.ArrayOfValue;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.ResultsBatch;
import com.sbbsystems.statefun.tasks.generated.TaskException;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import com.sbbsystems.statefun.tasks.generated.TupleOfValue;
import com.sbbsystems.statefun.tasks.generated.Value;
import com.sbbsystems.statefun.tasks.generated.ValueArgsAndKwargs;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class TaskRequestHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Proto Address used as the replyAddress on task requests that test the replyAddress path
    private static final com.sbbsystems.statefun.tasks.generated.Address REPLY_ADDRESS =
            com.sbbsystems.statefun.tasks.generated.Address.newBuilder()
                    .setNamespace("test")
                    .setType("caller")
                    .setId("test-id")
                    .build();

    // SDK Address returned by context.caller() for the caller-fallthrough tests
    private static final FunctionType CALLER_FUNCTION_TYPE = new FunctionType("test", "caller");
    private static final Address CALLER_ADDRESS = new Address(CALLER_FUNCTION_TYPE, "caller-id");

    /** Non-legacy handler (useLegacyTypes = false, the default). */
    private TaskRequestHandler handler;
    /** Legacy handler (useLegacyTypes = true). */
    private TaskRequestHandler legacyHandler;
    private Context context;
    private PipelineFunctionState state;

    /** Build a TaskRequest.Builder pre-populated with the standard reply address. */
    private static TaskRequest.Builder taskRequest() {
        return TaskRequest.newBuilder().setReplyAddress(REPLY_ADDRESS);
    }

    private static TaskResult parseTaskResult(Object arg) {
        try {
            return MessageTypes.asType(arg, TaskResult::parseFrom);
        } catch (InvalidMessageTypeException e) {
            throw new RuntimeException(e);
        }
    }

    private static TaskException parseTaskException(Object arg) {
        try {
            return MessageTypes.asType(arg, TaskException::parseFrom);
        } catch (InvalidMessageTypeException e) {
            throw new RuntimeException(e);
        }
    }

    private ObjectNode legacyConfigNode() {
        var node = MAPPER.createObjectNode();
        node.put("id", "example/embedded_pipeline");
        node.put("egress", "example/kafka-generic-egress");
        node.put("useLegacyTypes", true);
        return node;
    }

    @BeforeEach
    public void setup() {
        handler = TaskRequestHandler.with(
                PipelineConfiguration.of("example/embedded_pipeline", "example/kafka-generic-egress"));
        legacyHandler = TaskRequestHandler.with(
                PipelineConfiguration.fromNode(legacyConfigNode()));
        context = mock(Context.class);
        when(context.caller()).thenReturn(CALLER_ADDRESS);
        state = PipelineFunctionState.newInstance();
    }

    // ---- canHandle ---------------------------------------------------------------------------------

    @Test
    public void canHandle_returns_true_for_task_request() {
        var wrapped = MessageTypes.wrap(TaskRequest.newBuilder().build());
        assertThat(handler.canHandle(context, wrapped, state)).isTrue();
    }

    @Test
    public void canHandle_returns_false_for_results_batch() {
        var wrapped = MessageTypes.wrap(ResultsBatch.newBuilder().build());
        assertThat(handler.canHandle(context, wrapped, state)).isFalse();
    }

    // ---- respond: caller fallthrough ---------------------------------------------------------------

    @Test
    public void respond_falls_through_to_caller_when_no_reply_address_or_topic_is_set() {
        // No replyAddress / replyTopic — oneof is unset, getReplyAddress() returns a default
        // proto instance with empty fields.  The respond() method must treat that as "not set"
        // and fall through to context.caller().
        var request = TaskRequest.newBuilder()
                .setId("test-id")
                .setType(MessageTypes.ECHO_TASK_TYPE)
                .build();

        handler.handleMessage(context, request, state);

        // Verify the message arrived at context.caller() (same object reference returned by the stub)
        verify(context).send(eq(CALLER_ADDRESS), argThat(arg -> {
            try {
                return MessageTypes.isType(arg, TaskResult.class)
                        && parseTaskResult(arg).getId().equals("test-id");
            } catch (Exception e) {
                return false;
            }
        }));
    }

    // ---- echo --------------------------------------------------------------------------------------

    @Test
    public void echo_responds_with_value_args_from_request() {
        var valueArgsAndKwargs = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setStringValue("hello").build())
                        .addItems(Value.newBuilder().setIntValue(42).build())
                        .build())
                .build();

        var request = taskRequest()
                .setId("test-id")
                .setType(MessageTypes.ECHO_TASK_TYPE)
                .setRequest(Any.pack(valueArgsAndKwargs))
                .build();

        handler.handleMessage(context, request, state);

        verify(context).send(any(org.apache.flink.statefun.sdk.Address.class), argThat(arg -> {
            try {
                assertThat(MessageTypes.isType(arg, TaskResult.class)).isTrue();
                var result = parseTaskResult(arg);
                var resultArgs = result.getResult().unpack(TupleOfValue.class);
                return resultArgs.getItemsCount() == 2
                        && resultArgs.getItems(0).getStringValue().equals("hello")
                        && resultArgs.getItems(1).getIntValue() == 42;
            } catch (Exception e) {
                return false;
            }
        }));
    }

    @Test
    public void echo_with_legacy_types_responds_with_args() {
        var argsAndKwargs = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(StringValue.of("hello")))
                        .addItems(Any.pack(StringValue.of("world")))
                        .build())
                .build();

        var request = taskRequest()
                .setId("test-id")
                .setType(MessageTypes.ECHO_TASK_TYPE)
                .setRequest(Any.pack(argsAndKwargs))
                .build();

        legacyHandler.handleMessage(context, request, state);

        verify(context).send(any(org.apache.flink.statefun.sdk.Address.class), argThat(arg -> {
            try {
                assertThat(MessageTypes.isType(arg, TaskResult.class)).isTrue();
                var result = parseTaskResult(arg);
                var resultArgs = result.getResult().unpack(TupleOfAny.class);
                return resultArgs.getItemsCount() == 2
                        && resultArgs.getItems(0).unpack(StringValue.class).getValue().equals("hello")
                        && resultArgs.getItems(1).unpack(StringValue.class).getValue().equals("world");
            } catch (Exception e) {
                return false;
            }
        }));
    }

    @Test
    public void echo_result_has_correct_id_uid_and_type() {
        var request = taskRequest()
                .setId("pipeline-123")
                .setUid("uid-456")
                .setType(MessageTypes.ECHO_TASK_TYPE)
                .build();

        handler.handleMessage(context, request, state);

        verify(context).send(any(org.apache.flink.statefun.sdk.Address.class), argThat(arg -> {
            try {
                var result = parseTaskResult(arg);
                return result.getId().equals("pipeline-123")
                        && result.getUid().equals("uid-456")
                        && result.getType().equals(MessageTypes.ECHO_TASK_TYPE + ".result");
            } catch (Exception e) {
                return false;
            }
        }));
    }

    @Test
    public void echo_preserves_task_state() {
        var taskState = Any.pack(StringValue.of("my-state"));

        var request = taskRequest()
                .setId("test-id")
                .setType(MessageTypes.ECHO_TASK_TYPE)
                .setState(taskState)
                .build();

        handler.handleMessage(context, request, state);

        verify(context).send(any(org.apache.flink.statefun.sdk.Address.class), argThat(arg -> {
            try {
                var result = parseTaskResult(arg);
                return result.getState().equals(taskState);
            } catch (Exception e) {
                return false;
            }
        }));
    }

    // ---- flattenResults ----------------------------------------------------------------------------

    @Test
    public void flattenResults_responds_with_flattened_value_array() {
        var innerArray1 = ArrayOfValue.newBuilder()
                .addItems(Value.newBuilder().setStringValue("a").build())
                .addItems(Value.newBuilder().setStringValue("b").build())
                .build();

        var innerArray2 = ArrayOfValue.newBuilder()
                .addItems(Value.newBuilder().setStringValue("c").build())
                .build();

        var outerArray = ArrayOfValue.newBuilder()
                .addItems(Value.newBuilder().setArrayValue(innerArray1).build())
                .addItems(Value.newBuilder().setArrayValue(innerArray2).build())
                .build();

        var valueArgsAndKwargs = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setArrayValue(outerArray).build())
                        .build())
                .build();

        var request = taskRequest()
                .setId("test-id")
                .setType(MessageTypes.FLATTEN_RESULTS_TASK_TYPE)
                .setRequest(Any.pack(valueArgsAndKwargs))
                .build();

        handler.handleMessage(context, request, state);

        verify(context).send(any(org.apache.flink.statefun.sdk.Address.class), argThat(arg -> {
            try {
                assertThat(MessageTypes.isType(arg, TaskResult.class)).isTrue();
                var result = parseTaskResult(arg);
                var flattened = result.getResult().unpack(ArrayOfValue.class);
                return flattened.getItemsCount() == 3
                        && flattened.getItems(0).getStringValue().equals("a")
                        && flattened.getItems(1).getStringValue().equals("b")
                        && flattened.getItems(2).getStringValue().equals("c");
            } catch (Exception e) {
                return false;
            }
        }));
    }

    @Test
    public void flattenResults_with_legacy_types_responds_with_flattened_array() {
        var innerArray1 = ArrayOfAny.newBuilder()
                .addItems(Any.pack(StringValue.of("a")))
                .addItems(Any.pack(StringValue.of("b")))
                .build();

        var innerArray2 = ArrayOfAny.newBuilder()
                .addItems(Any.pack(StringValue.of("c")))
                .build();

        var outerArray = ArrayOfAny.newBuilder()
                .addItems(Any.pack(innerArray1))
                .addItems(Any.pack(innerArray2))
                .build();

        var argsAndKwargs = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(outerArray))
                        .build())
                .build();

        var request = taskRequest()
                .setId("test-id")
                .setType(MessageTypes.FLATTEN_RESULTS_TASK_TYPE)
                .setRequest(Any.pack(argsAndKwargs))
                .build();

        legacyHandler.handleMessage(context, request, state);

        verify(context).send(any(org.apache.flink.statefun.sdk.Address.class), argThat(arg -> {
            try {
                assertThat(MessageTypes.isType(arg, TaskResult.class)).isTrue();
                var result = parseTaskResult(arg);
                var flattened = result.getResult().unpack(ArrayOfAny.class);
                return flattened.getItemsCount() == 3
                        && flattened.getItems(0).unpack(StringValue.class).getValue().equals("a")
                        && flattened.getItems(1).unpack(StringValue.class).getValue().equals("b")
                        && flattened.getItems(2).unpack(StringValue.class).getValue().equals("c");
            } catch (Exception e) {
                return false;
            }
        }));
    }

    @Test
    public void flattenResults_with_invalid_request_sets_failed_status_and_responds_with_exception() {
        // A request with no args — getArg(0) throws IndexOutOfBoundsException
        var request = taskRequest()
                .setId("test-id")
                .setType(MessageTypes.FLATTEN_RESULTS_TASK_TYPE)
                .build();

        handler.handleMessage(context, request, state);

        assertThat(state.getStatus()).isEqualTo(TaskStatus.Status.FAILED);
        verify(context).send(any(org.apache.flink.statefun.sdk.Address.class), argThat(arg -> {
            try {
                assertThat(MessageTypes.isType(arg, TaskException.class)).isTrue();
                var exception = parseTaskException(arg);
                return exception.getId().equals("test-id");
            } catch (Exception e) {
                return false;
            }
        }));
    }

    // ---- unknown task type -------------------------------------------------------------------------

    @Test
    public void unknown_task_type_sets_failed_status_and_responds_with_exception() {
        var request = taskRequest()
                .setId("test-id")
                .setType("__builtins.unknown_type")
                .build();

        handler.handleMessage(context, request, state);

        assertThat(state.getStatus()).isEqualTo(TaskStatus.Status.FAILED);
        verify(context).send(any(org.apache.flink.statefun.sdk.Address.class), argThat(arg -> {
            try {
                assertThat(MessageTypes.isType(arg, TaskException.class)).isTrue();
                var exception = parseTaskException(arg);
                return exception.getId().equals("test-id");
            } catch (Exception e) {
                return false;
            }
        }));
    }

    // ---- runPipeline guard check -------------------------------------------------------------------

    @Test
    public void run_pipeline_in_running_state_sets_failed_status_and_responds_with_exception() {
        state.setStatus(TaskStatus.Status.RUNNING);

        var request = taskRequest()
                .setId("test-id")
                .setType(MessageTypes.RUN_PIPELINE_TASK_TYPE)
                .build();

        handler.handleMessage(context, request, state);

        assertThat(state.getStatus()).isEqualTo(TaskStatus.Status.FAILED);
        verify(context).send(any(org.apache.flink.statefun.sdk.Address.class), argThat(arg -> {
            try {
                assertThat(MessageTypes.isType(arg, TaskException.class)).isTrue();
                var exception = parseTaskException(arg);
                return exception.getExceptionMessage().contains("Pipelines must have finished");
            } catch (Exception e) {
                return false;
            }
        }));
    }
}

