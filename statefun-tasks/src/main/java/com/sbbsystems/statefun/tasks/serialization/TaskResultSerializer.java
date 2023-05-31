package com.sbbsystems.statefun.tasks.serialization;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;

public class TaskResultSerializer {
    private final TaskResult taskResult;

    public static TaskResultSerializer of(TaskResultOrException taskResultOrException) {
        var taskResult = taskResultOrException.hasTaskException()
                ? MessageTypes.toTaskResult(taskResultOrException.getTaskException())
                : taskResultOrException.getTaskResult();

        return new TaskResultSerializer(taskResult);
    }

    private TaskResultSerializer(TaskResult taskResult) {
        this.taskResult = taskResult;
    }

    public Message getResult() {
        return taskResult.getResult();
    }

    public Any getState() {
        return taskResult.getState();
    }
}
