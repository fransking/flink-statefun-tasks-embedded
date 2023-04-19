package com.sbbsystems.flink.tasks.embedded.types;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sbbsystems.flink.tasks.embedded.proto.TaskException;
import com.sbbsystems.flink.tasks.embedded.util.CheckedFunction;
import com.sbbsystems.flink.tasks.embedded.proto.TaskRequest;
import com.sbbsystems.flink.tasks.embedded.proto.TaskResult;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

import java.util.Map;
import java.util.Optional;

public final class MessageTypes {
    public static final TypeName TASK_REQUEST_TYPE = TypeName.parseFrom("io.statefun_tasks.types/statefun_tasks.TaskRequest");
    public static final TypeName TASK_RESULT_TYPE = TypeName.parseFrom("io.statefun_tasks.types/statefun_tasks.TaskResult");
    public static final TypeName TASK_EXCEPTION_TYPE = TypeName.parseFrom("io.statefun_tasks.types/statefun_tasks.TaskException");

    public static final Map<Class<? extends Message>, TypeName> TYPES = Map.of(
            TaskRequest.class, TASK_REQUEST_TYPE,
            TaskResult.class, TASK_RESULT_TYPE,
            TaskException.class, TASK_EXCEPTION_TYPE
    );

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
}
