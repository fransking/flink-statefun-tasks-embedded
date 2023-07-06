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
package com.sbbsystems.statefun.tasks.types;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNullElse;

public final class MessageTypes {
    public static final TypeName TASK_REQUEST_TYPE = TypeName.parseFrom("io.statefun_tasks.types/statefun_tasks.TaskRequest");
    public static final TypeName TASK_RESULT_TYPE = TypeName.parseFrom("io.statefun_tasks.types/statefun_tasks.TaskResult");
    public static final TypeName TASK_EXCEPTION_TYPE = TypeName.parseFrom("io.statefun_tasks.types/statefun_tasks.TaskException");
    public static final TypeName CALLBACK_SIGNAL_TYPE = TypeName.parseFrom("io.statefun_tasks.types/statefun_tasks.CallbackSignal");
    public static final TypeName RESULTS_BATCH_TYPE = TypeName.parseFrom("io.statefun_tasks.types/statefun_tasks.ResultsBatch");
    public static final TypeName CHILD_PIPELINE = TypeName.parseFrom("io.statefun_tasks.types/statefun_tasks.ChildPipeline");
    public static final TypeName TASK_ACTION_REQUEST_TYPE = TypeName.parseFrom("io.statefun_tasks.types/statefun_tasks.TaskActionRequest");
    public static final TypeName TASK_ACTION_RESULT_TYPE = TypeName.parseFrom("io.statefun_tasks.types/statefun_tasks.TaskActionRequest");
    public static final Map<Class<? extends Message>, TypeName> TYPES = Map.of(
            TaskRequest.class, TASK_REQUEST_TYPE,
            TaskResult.class, TASK_RESULT_TYPE,
            TaskException.class, TASK_EXCEPTION_TYPE,
            CallbackSignal.class, CALLBACK_SIGNAL_TYPE,
            ResultsBatch.class, RESULTS_BATCH_TYPE,
            ChildPipeline.class, CHILD_PIPELINE,
            TaskActionRequest.class, TASK_ACTION_REQUEST_TYPE,
            TaskActionResult.class, TASK_ACTION_RESULT_TYPE
    );

    public static final String RUN_PIPELINE_TASK_TYPE = "__builtins.run_pipeline";
    public static final String FLATTEN_RESULTS_TASK_TYPE = "__builtins.flatten_results";

    public static <T extends Message> boolean isType(Object input, Class<T> type) {
        if (input instanceof TypedValue) {
            var typedValue = (TypedValue) input;

            var typeName = Optional.ofNullable(MessageTypes.TYPES.get(type))
                    .map(TypeName::canonicalTypenameString);

            return typeName.equals(Optional.of(typedValue.getTypename()));
        }

        return false;
    }

    public static <T> T asType(Object input, CheckedFunction<ByteString, T, InvalidProtocolBufferException> builder) throws InvalidMessageTypeException {
        if (input instanceof TypedValue) {
            var typedValue = (TypedValue) input;

            try {
                return builder.apply(typedValue.getValue());
            } catch (InvalidProtocolBufferException e) {
                throw new InvalidMessageTypeException("Protobuf parsing error", e);
            }
        }

        throw new InvalidMessageTypeException("Input must be an instance of TypedValue");
    }

    public static <T extends Message> TypedValue wrap(T innerValue) {
        return TypedValue.newBuilder()
                .setTypename(TYPES.get(innerValue.getClass()).canonicalTypenameString())
                .setHasValue(true)
                .setValue(innerValue.toByteString())
                .build();
    }

    public static ArgsAndKwargs argsOfEmptyArray() {
        return ArgsAndKwargs.newBuilder()
                .setArgs(tupleOfEmptyArray())
                .build();
    }

    public static TupleOfAny tupleOfEmptyArray() {
        return TupleOfAny.newBuilder()
                .addItems(Any.pack(ArrayOfAny.getDefaultInstance()))
                .build();
    }

    public static TaskResultOrException emptyGroupResult() {
        return TaskResultOrException.newBuilder()
                .setTaskResult(TaskResult.newBuilder()
                        .setResult(Any.pack(ArrayOfAny.getDefaultInstance())))
                .build();
    }

    public static TypedValue toEgress(Message message, String topic) {
        var egressRecord = KafkaProducerRecord.newBuilder()
                .setTopic(topic)
                .setValueBytes(Any.pack(message).toByteString())
                .build();

        return TypedValue.newBuilder()
                .setValue(egressRecord.toByteString())
                .setHasValue(true)
                .setTypename("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord")
                .build();
    }

    public static TaskResult toOutgoingTaskResult(TaskRequest incomingTaskRequest, Message result) {
        return MessageTypes.toOutgoingTaskResult(incomingTaskRequest, result, incomingTaskRequest.getState());
    }

    public static TaskResult toOutgoingTaskResult(TaskRequest incomingTaskRequest, Message result, Any state) {
        return TaskResult.newBuilder()
                .setId(incomingTaskRequest.getId())
                .setUid(incomingTaskRequest.getUid())
                .setInvocationId(incomingTaskRequest.getInvocationId())
                .setType(incomingTaskRequest.getType() + ".result")
                .setResult(MessageTypes.packAny(result))
                .setState(state)
                .build();
    }

    public static TaskResult toTaskResult(TaskException taskException) {
        return TaskResult.newBuilder()
                .setId(taskException.getId())
                .setUid(taskException.getUid())
                .setInvocationId(taskException.getInvocationId())
                .setType(taskException.getType() + ".result")
                .setResult(Any.pack(taskException))
                .setState(taskException.getState())
                .build();
    }

    public static TaskException toTaskException(TaskRequest incomingTaskRequest, Exception e) {
        return MessageTypes.toTaskException(incomingTaskRequest, e, incomingTaskRequest.getState());
    }

    public static TaskException toTaskException(TaskRequest incomingTaskRequest, Exception e, Any state) {
        return TaskException.newBuilder()
                .setId(incomingTaskRequest.getId())
                .setUid(incomingTaskRequest.getUid())
                .setInvocationId(incomingTaskRequest.getInvocationId())
                .setType(incomingTaskRequest.getType() + ".error")
                .setExceptionType(e.getClass().getTypeName())
                .setExceptionMessage(String.valueOf(e))
                .setStacktrace(Arrays.toString(e.getStackTrace()))
                .setState(requireNonNullElse(state, Any.getDefaultInstance()))
                .build();
    }

    public static TaskActionException toTaskActionException(TaskActionRequest incomingTaskActionRequest, Exception e) {
        return TaskActionException
                .newBuilder()
                .setId(incomingTaskActionRequest.getId())
                .setUid(incomingTaskActionRequest.getUid())
                .setAction(incomingTaskActionRequest.getAction())
                .setExceptionMessage(String.valueOf(e))
                .setStacktrace(Arrays.toString(e.getStackTrace()))
                .build();
    }

    public static TaskException toOutgoingTaskException(TaskRequest incomingTaskRequest, TaskException e) {
        return MessageTypes.toOutgoingTaskException(incomingTaskRequest, e, incomingTaskRequest.getState());
    }

    public static TaskException toOutgoingTaskException(TaskRequest incomingTaskRequest, TaskException e, Any state) {
        return TaskException.newBuilder()
                .setId(incomingTaskRequest.getId())
                .setUid(incomingTaskRequest.getUid())
                .setInvocationId(incomingTaskRequest.getInvocationId())
                .setType(incomingTaskRequest.getType() + ".error")
                .setExceptionType(e.getExceptionType())
                .setExceptionMessage(e.getExceptionMessage())
                .setStacktrace(e.getStacktrace())
                .setState(state)
                .build();
    }

    public static EgressIdentifier<TypedValue> getEgress(PipelineConfiguration configuration) {
        return new EgressIdentifier<>(configuration.getEgressNamespace(), configuration.getEgressType(), TypedValue.class);
    }

    public static EgressIdentifier<TypedValue> getEventsEgress(PipelineConfiguration configuration) {
        return new EgressIdentifier<>(configuration.getEventsEgressNamespace(), configuration.getEventsEgressType(), TypedValue.class);
    }

    public static org.apache.flink.statefun.sdk.Address toSdkAddress(Address address) {
        var functionType = new FunctionType(address.getNamespace(), address.getType());
        return new org.apache.flink.statefun.sdk.Address(functionType, address.getId());
    }

    public static org.apache.flink.statefun.sdk.Address getSdkAddress(TaskEntry taskEntry) {
        var functionType = new FunctionType(taskEntry.namespace, taskEntry.workerName);
        return new org.apache.flink.statefun.sdk.Address(functionType, taskEntry.taskId);
    }

    public static Address toAddress(org.apache.flink.statefun.sdk.Address sdkAddress) {
        return Address.newBuilder()
                .setNamespace(sdkAddress.type().namespace())
                .setType(sdkAddress.type().name())
                .setId(sdkAddress.id())
                .build();
    }

    public static Address toAddress(String namespaceAndType, String id) {
        var split = namespaceAndType.split("/");
        return Address.newBuilder()
                .setNamespace(split[0])
                .setType(split[1])
                .setId(id)
                .build();
    }

    public static FunctionType toFunctionType(String namespaceAndType) {
        var split = namespaceAndType.split("/");
        return new FunctionType(split[0], split[1]);
    }

    public static Address getCallbackFunctionAddress(PipelineConfiguration configuration, String id) {
        return Address.newBuilder()
            .setNamespace(configuration.getNamespace())
            .setType(configuration.getCallbackType())
            .setId(id)
            .build();
    }

    public static String toTypeName(org.apache.flink.statefun.sdk.Address address) {
        return address.type().namespace() + "/" + address.type().name();
    }

    public static String toTypeName(Address address) {
        return address.getNamespace() + "/" + address.getType();
    }

    public static String asString(org.apache.flink.statefun.sdk.Address address) {
        return isNull(address)
                ? ""
                : MessageFormat.format("{0}/{1}/{2}", address.type().namespace(), address.type().name(), address.id());
    }

    public static String asString(Address address) {
        return isNull(address)
                ? ""
                : MessageFormat.format("{0}/{1}/{2}", address.getNamespace(), address.getType(), address.getId());
    }

    public static Any packAny(Message.Builder builder) {
        return packAny(builder.build());
    }

    public static Any packAny(Message message) {
        if (message instanceof Any) {
            return (Any) message;
        }

        return Any.pack(message);
    }

    public static boolean isEmpty(Any any) {
        return any.getTypeUrl().isEmpty();
    }

    public static TaskInfo toTaskInfo(TaskEntry taskEntry) {
        return TaskInfo.newBuilder()
                .setTaskId(taskEntry.taskId)
                .setTaskUid(taskEntry.uid)
                .setTaskType(taskEntry.taskType)
                .setNamespace(taskEntry.namespace)
                .setWorkerName(taskEntry.workerName)
                .setDisplayName(taskEntry.displayName)
                .build();
    }

    public static Event.Builder buildEventFor(PipelineFunctionState state) {
        return Event.newBuilder()
                .setPipelineId(state.getPipelineAddress().getId())
                .setPipelineAddress(MessageTypes.toTypeName(state.getPipelineAddress()))
                .setRootPipelineId(state.getRootPipelineAddress().getId())
                .setRootPipelineAddress(MessageTypes.toTypeName(state.getRootPipelineAddress()));
    }
}
