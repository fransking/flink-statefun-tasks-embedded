package com.sbbsystems.statefun.tasks.serialization;

import com.google.protobuf.Any;
import com.sbbsystems.statefun.tasks.generated.ArrayOfAny;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;

public class TaskResultOrExceptionSerializer {
    private final TaskResultOrException taskResultOrException;

    public static TaskResultOrExceptionSerializer of(TaskResultOrException taskResultOrException) {
        return new TaskResultOrExceptionSerializer(taskResultOrException);
    }

    private TaskResultOrExceptionSerializer(TaskResultOrException taskResultOrException) {
        this.taskResultOrException = taskResultOrException;
    }

    public TaskResultOrException toEmptyGroupResult() {
        if (!taskResultOrException.hasTaskResult()) {
            return taskResultOrException;
        }

        var taskResult = taskResultOrException.getTaskResult().toBuilder();
        taskResult.setResult(Any.pack(ArrayOfAny.getDefaultInstance()));
        return taskResultOrException.toBuilder().setTaskResult(taskResult).build();
    }
}
