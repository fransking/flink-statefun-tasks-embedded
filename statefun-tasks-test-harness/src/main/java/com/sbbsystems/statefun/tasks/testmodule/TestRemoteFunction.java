package com.sbbsystems.statefun.tasks.testmodule;

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

import java.util.Map;
import java.util.function.BiFunction;

import static com.sbbsystems.statefun.tasks.testmodule.IoIdentifiers.EMBEDDED_FUNCTION_TYPE;
import static org.apache.flink.util.function.BiFunctionWithException.unchecked;

public class TestRemoteFunction implements StatefulFunction {
    private final Map<String, BiFunction<Context, TaskRequest, TaskResult>> registeredFunctions;

    private static TaskResult add(Context context, TaskRequest taskRequest) throws InvalidProtocolBufferException {
        var args = taskRequest.getRequest().unpack(ArgsAndKwargs.class).getArgs().getItemsList();
        var a = args.get(0).unpack(Int32Value.class).getValue();
        var b = args.get(1).unpack(Int32Value.class).getValue();
        var result = a + b;
        return TaskResult.newBuilder()
                .setId(taskRequest.getId())
                .setUid(taskRequest.getUid())
                .setType(taskRequest.getType() + ".result")
                .setResult(Any.pack(Int32Value.of(result)))
                .build();
    }

    public TestRemoteFunction() {
        this.registeredFunctions = Map.of(
                "add", unchecked(TestRemoteFunction::add)
        );
    }

    @Override
    public void invoke(Context context, Object o) {
        var taskRequest = (TaskRequest) o;
        var type = taskRequest.getType();
        var handler = this.registeredFunctions.getOrDefault(type, null);
        if (handler == null) {
            throw new RuntimeException(String.format("No handler for %s", type));
        }
        var taskResult = handler.apply(context, taskRequest);
        context.send(EMBEDDED_FUNCTION_TYPE, taskResult.getId(), taskResult);
    }
}
