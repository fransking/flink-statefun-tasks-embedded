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
package com.sbbsystems.statefun.tasks.e2e;

import com.google.protobuf.*;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.generated.Value;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static com.sbbsystems.statefun.tasks.types.MessageTypes.RUN_PIPELINE_TASK_TYPE;
import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;
import static com.sbbsystems.statefun.tasks.types.MessageTypes.packValue;
import static java.util.Objects.isNull;

public class EndToEndRemoteFunction implements StatefulFunction {

    public static final FunctionType FUNCTION_TYPE = new FunctionType("e2e", "RemoteFunctionUsingLegacyTypes");

    public static final FunctionType VALUE_FUNCTION_TYPE = new FunctionType("e2e", "RemoteFunctionUsingValueTypes");

    private final boolean useLegacyTypes;

    public EndToEndRemoteFunction(boolean useLegacyTypes) {
        this.useLegacyTypes = useLegacyTypes;
    }

    @Persisted
    private final PersistedValue<TaskRequest> originalTaskRequest = PersistedValue.of("originalTaskRequest", TaskRequest.class);

    @Override
    public void invoke(Context context, Object input) {

        try {
            if (MessageTypes.isType(input, TaskRequest.class)) {
                var taskRequest = MessageTypes.asType(input, TaskRequest::parseFrom);
                taskRequest = originalTaskRequest.getOrDefault(taskRequest);
                originalTaskRequest.set(taskRequest);

                Message output = null;

                try {
                    switch (taskRequest.getType()) {
                        case "echo":
                            output = getOutput(taskRequest, echoTask(taskRequest));
                            break;

                        case "updateAndGetState":
                            output = getOutput(taskRequest, updateAndGetStateTask(taskRequest));
                            break;

                        case "error":
                            output = getOutput(taskRequest, errorTask(taskRequest));
                            break;

                        case "setState":
                            output = getOutput(taskRequest, setStateTask(taskRequest));
                            break;

                        case "cleanup":
                            output = getOutput(taskRequest, cleanupTask(taskRequest));
                            break;

                        case "newPipeline":
                            newPipelineTask(context, taskRequest);
                            break;

                        case "delay":
                            output = getOutput(taskRequest, sleepTask(context, taskRequest));
                            break;

                        default:
                            var error = MessageFormat.format("Unknown task type {0}", taskRequest.getType());
                            output = MessageTypes.toTaskException(taskRequest, new StatefunTasksException(error));
                    }
                } catch (StatefunTasksException e) {
                    output = MessageTypes.toTaskException(taskRequest, e);
                }

                if (!isNull(output)) {
                    var wrappedResult = MessageTypes.wrap(output);
                    var replyAddress = taskRequest.getReplyAddress();
                    context.send(new FunctionType(replyAddress.getNamespace(), replyAddress.getType()), replyAddress.getId(), wrappedResult);
                }
            }

        } catch (InvalidMessageTypeException | InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private Message getOutput(TaskRequest taskRequest, TaskResult.Builder taskResult) {
        if (isNull(taskResult)) {
            return null;
        }

        return taskResult
                .setId(taskRequest.getId())
                .setUid(taskRequest.getUid())
                .setInvocationId(taskRequest.getInvocationId())
                .setType(taskRequest.getType() + ".result")
                .build();
    }

    private ArgsAndKwargs getArgsAndKwargs(TaskRequest taskRequest)
            throws InvalidProtocolBufferException {

        var request = taskRequest.getRequest();

        if (request.is(ArgsAndKwargs.class)) {
            return request.unpack(ArgsAndKwargs.class);
        }

        var args = request.is(TupleOfAny.class)
                ? request.unpack(TupleOfAny.class)
                : TupleOfAny.newBuilder().addItems(request).build();

        return ArgsAndKwargs
                .newBuilder()
                .setArgs(args)
                .build();
    }

    private ValueArgsAndKwargs getValueArgsAndKwargs(TaskRequest taskRequest)
            throws InvalidProtocolBufferException {

        var request = taskRequest.getRequest();

        if (request.is(ValueArgsAndKwargs.class)) {
            return request.unpack(ValueArgsAndKwargs.class);
        }

        var args = request.is(TupleOfValue.class)
                ? request.unpack(TupleOfValue.class)
                : TupleOfValue.newBuilder().addItems(packValue(request)).build();

        return ValueArgsAndKwargs
                .newBuilder()
                .setArgs(args)
                .build();
    }

    private Any toResult(TupleOfAny result) {
        // if a single element tuple remains then unpack back to single value so (8,) becomes 8 but (8,9) remains a tuple
        // consistent with Python API
        return result.getItemsCount() == 1
                ? packAny(result.getItems(0))
                : packAny(result);
    }

    private Any toResult(TupleOfValue result) {
        // if a single element tuple remains then unpack back to single value so (8,) becomes 8 but (8,9) remains a tuple
        // consistent with Python API
        return result.getItemsCount() == 1
                ? packAny(result.getItems(0))
                : packAny(result);
    }

    private TaskResult.Builder echoTask(TaskRequest taskRequest)
            throws InvalidProtocolBufferException {

        if (this.useLegacyTypes) {
            var request = getArgsAndKwargs(taskRequest);
            var args = request.getArgs().toBuilder();

            if (request.getKwargs().getItemsCount() > 0) {
                args.addItems(packAny(request.getKwargs()));
            }

            return TaskResult
                    .newBuilder()
                    .setResult(toResult(args.build()))
                    .setState(taskRequest.getState());
        } else {
            var request = getValueArgsAndKwargs(taskRequest);
            var args = request.getArgs().toBuilder();

            if (request.getKwargs().getItemsCount() > 0) {
                args.addItems(packValue(request.getKwargs()));
            }

            return TaskResult
                    .newBuilder()
                    .setResult(toResult(args.build()))
                    .setState(taskRequest.getState());
        }
    }

    private TaskResult.Builder errorTask(TaskRequest taskRequest)
            throws StatefunTasksException, InvalidProtocolBufferException {

        if (useLegacyTypes) {
            var request = getArgsAndKwargs(taskRequest);
            var kwargs = request.getKwargs();
            var message = kwargs.getItemsOrDefault("message", packAny(StringValue.of("")));
            throw new StatefunTasksException(message.unpack(StringValue.class).getValue());
        } else {
            var request = getValueArgsAndKwargs(taskRequest);
            var kwargs = request.getKwargs();
            var message = kwargs.getItemsOrDefault("message", Value.newBuilder().setStringValue("").build());
            throw new StatefunTasksException(message.getStringValue());
        }
    }

    private TaskResult.Builder updateAndGetStateTask(TaskRequest taskRequest)
            throws InvalidProtocolBufferException {

        if (useLegacyTypes) {
            var request = getArgsAndKwargs(taskRequest);
            var currentValue = taskRequest.getState().unpack(Int32Value.class);
            var updateValue = request.getArgs().getItems(0).unpack(Int32Value.class);
            var updatedValue = Int32Value.of(currentValue.getValue() + updateValue.getValue());

            var result = TupleOfAny.newBuilder().addItems(packAny(updatedValue)).build();

            return TaskResult
                    .newBuilder()
                    .setResult(toResult(result))
                    .setState(packAny(updatedValue));
        } else {
            var request = getValueArgsAndKwargs(taskRequest);
            var currentValue = taskRequest.getState().unpack(Value.class).getIntValue();
            var updateValue = request.getArgs().getItems(0).getIntValue();
            var updatedValue = Value.newBuilder().setIntValue(currentValue + updateValue).build();

            var result = TupleOfValue.newBuilder().addItems(updatedValue).build();

            return TaskResult
                    .newBuilder()
                    .setResult(toResult(result))
                    .setState(packAny(updatedValue));
        }
    }

    private TaskResult.Builder setStateTask(TaskRequest taskRequest)
            throws InvalidProtocolBufferException {

        if (useLegacyTypes) {
            var request = getArgsAndKwargs(taskRequest);

            return TaskResult
                    .newBuilder()
                    .setResult(toResult(request.getArgs()))
                    .setState(toResult(request.getArgs()));
            // returns Any(TupleOfAny(0) = MapOfStringAny)) = Any(MapOfStringAny)
        } else {
            var request = getValueArgsAndKwargs(taskRequest);

            return TaskResult
                    .newBuilder()
                    .setResult(toResult(request.getArgs()))
                    .setState(toResult(request.getArgs()));
            // return Any(TupleOfValue(0) = Value(MapOfStringValue)) = Any(Value[MapOfStringValue])
        }
    }

    private TaskResult.Builder cleanupTask(TaskRequest taskRequest)
            throws InvalidProtocolBufferException, StatefunTasksException {

        var state = taskRequest.getState();

        if (useLegacyTypes) {
            if (state.is(BoolValue.class) && state.unpack(BoolValue.class).getValue()) {
                throw new StatefunTasksException("error in finally");
            }
        } else {
            if (state.is(Value.class) && state.unpack(Value.class).getBoolValue()) {
                throw new StatefunTasksException("error in finally");
            }
        }

        return TaskResult.newBuilder();
    }

    private TaskResult.Builder sleepTask(Context context, TaskRequest taskRequest)
            throws InvalidProtocolBufferException, StatefunTasksException {

        var pipelineId = taskRequest.getMetaMap().get("root_pipeline_id");

        var startTime = LocalDateTime.now();

        if (!WaitHandles.isReady(pipelineId)) {
            context.sendAfter(Duration.of(1000, ChronoUnit.MILLIS), context.self(), MessageTypes.wrap(originalTaskRequest.get()));
            return null;
        }

        var endTime = LocalDateTime.now();
        var stringResult = MessageFormat.format("{0}|{1}", startTime, endTime);

        if (useLegacyTypes) {
            return TaskResult.newBuilder().setResult(Any.pack(StringValue.of(stringResult)));
        } else {
            return TaskResult.newBuilder().setResult(Any.pack(Value.newBuilder().setStringValue(stringResult).build()));
        }
    }

    private void newPipelineTask(Context context, TaskRequest taskRequest)
            throws InvalidProtocolBufferException {

        var request = (useLegacyTypes) ? getArgsAndKwargs(taskRequest) : getValueArgsAndKwargs(taskRequest);
        var builder = PipelineBuilder.forE2eWorker(useLegacyTypes).beginWith("echo", request);

        var output = TaskRequest.newBuilder()
                .setUid(taskRequest.getUid())
                .setId(taskRequest.getId())
                .setType(RUN_PIPELINE_TASK_TYPE)
                .setInvocationId(taskRequest.getInvocationId())  // this is important to match invocation id of the calling pipeline
                .setRequest(packAny(builder.build()))
                .setState(taskRequest.getState())
                .putAllMeta(taskRequest.getMetaMap())
                .setReplyAddress(taskRequest.getReplyAddress())
                .build();

        context.send(context.caller().type(), output.getUid(), MessageTypes.wrap(output));
    }
}
