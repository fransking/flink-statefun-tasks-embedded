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
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;

import java.text.MessageFormat;

import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;

public class EndToEndRemoteFunction implements StatefulFunction {

    public static final FunctionType FUNCTION_TYPE = new FunctionType("e2e", "RemoteFunction");

    @Override
    public void invoke(Context context, Object input) {

        try {
            if (MessageTypes.isType(input, TaskRequest.class)) {
                var taskRequest = MessageTypes.asType(input, TaskRequest::parseFrom);
                Message output;

                try {
                    switch (taskRequest.getType()) {
                        case "echo":
                            output = getOutput(taskRequest, echo(taskRequest));
                            break;

                        case "updateAndGetState":
                            output = getOutput(taskRequest, updateAndGetStateTask(taskRequest));
                            break;

                        case "error":
                            output = getOutput(taskRequest, errorTask());
                            break;

                        case "setState":
                            output = getOutput(taskRequest, setStateTask(taskRequest));
                            break;

                        default:
                            var error = MessageFormat.format("Unknown task type {0}", taskRequest.getType());
                            output = MessageTypes.toTaskException(taskRequest, new StatefunTasksException(error));
                    }
                }
                catch (StatefunTasksException e) {
                    output = MessageTypes.toTaskException(taskRequest, e);
                }

                var wrappedResult = MessageTypes.wrap(output);
                var replyAddress = taskRequest.getReplyAddress();
                context.send(new FunctionType(replyAddress.getNamespace(), replyAddress.getType()), replyAddress.getId(), wrappedResult);
            }


        } catch (InvalidMessageTypeException | InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private Message getOutput(TaskRequest taskRequest, TaskResult.Builder taskResult) {
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

    private Any toResult(TupleOfAny result) {
        // if a single element tuple remains then unpack back to single value so (8,) becomes 8 but (8,9) remains a tuple
        // consistent with Python API
        return result.getItemsCount() == 1
                ? packAny(result.getItems(0))
                : packAny(result);
    }

    private TaskResult.Builder echo(TaskRequest taskRequest)
            throws InvalidProtocolBufferException {

        var request = getArgsAndKwargs(taskRequest);
        var args = request.getArgs().toBuilder();

        if (request.getKwargs().getItemsCount() > 0) {
            args.addItems(packAny(request.getKwargs()));
        }

        return TaskResult
                .newBuilder()
                .setResult(toResult(args.build()))
                .setState(taskRequest.getState());
    }

    private TaskResult.Builder errorTask()
            throws StatefunTasksException {

        throw new StatefunTasksException("An error occurred");
    }

    private TaskResult.Builder updateAndGetStateTask(TaskRequest taskRequest)
            throws InvalidProtocolBufferException {

        var request = getArgsAndKwargs(taskRequest);
        var currentValue = taskRequest.getState().unpack(Int32Value.class);
        var updateValue = request.getArgs().getItems(0).unpack(Int32Value.class);
        var updatedValue = Int32Value.of(currentValue.getValue() + updateValue.getValue());

        var result = TupleOfAny.newBuilder().addItems(packAny(updatedValue)).build();

        return TaskResult
                .newBuilder()
                .setResult(toResult(result))
                .setState(packAny(updatedValue));
    }

    private TaskResult.Builder setStateTask(TaskRequest taskRequest)
            throws InvalidProtocolBufferException {

        var request = getArgsAndKwargs(taskRequest);

        return TaskResult
                .newBuilder()
                .setResult(toResult(request.getArgs()))
                .setState(toResult(request.getArgs()));
    }
}