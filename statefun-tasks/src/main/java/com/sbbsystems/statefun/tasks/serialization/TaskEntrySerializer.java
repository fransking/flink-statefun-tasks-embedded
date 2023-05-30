package com.sbbsystems.statefun.tasks.serialization;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.MapOfStringToAny;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.types.TaskEntry;

public class TaskEntrySerializer {
    private final TaskEntry taskEntry;

    public static TaskEntrySerializer of(TaskEntry taskEntry) {
        return new TaskEntrySerializer(taskEntry);
    }

    private TaskEntrySerializer(TaskEntry taskEntry) {
        this.taskEntry = taskEntry;
    }

    public Any mergeWith(ArgsAndKwargs argsAndKwargs)
            throws StatefunTasksException {

        return mergeWith(argsAndKwargs.getArgs(), argsAndKwargs.getKwargs());
    }

    public Any mergeWith(Message args, MapOfStringToAny kwargs)
            throws StatefunTasksException {

        var argsAndKwargs = ArgsAndKwargsSerializer.of(taskEntry.request).getArgsAndKwargs();
        var mergedKwargs = argsAndKwargs.getKwargs().toBuilder();

        // merge kwargs
        mergedKwargs.putAllItems(argsAndKwargs.getKwargs().getItemsMap());

        // if task kwargs and merged kwargs are empty then just return args - nothing in task entry to pass to task
        if (argsAndKwargs.getArgs().getItemsCount() == 0 && mergedKwargs.getItemsCount() == 0) {
            return MessageTypes.packAny(args);
        }

        // merge args
        var mergedArgs = TupleOfAny.newBuilder();

        if (args instanceof TupleOfAny) {
            mergedArgs.addAllItems(((TupleOfAny) args).getItemsList());
        } else {
            mergedArgs.addItems(MessageTypes.packAny(args));
        }

        return MessageTypes.packAny(ArgsAndKwargs.newBuilder()
                .setArgs(mergedArgs)
                .setKwargs(mergedKwargs)
                .build());
    }
}
