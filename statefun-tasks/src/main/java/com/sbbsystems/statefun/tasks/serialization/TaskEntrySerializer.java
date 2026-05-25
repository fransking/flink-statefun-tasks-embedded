/*
 * Copyright [2023] [Frans King, Luke Ashworth]
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
package com.sbbsystems.statefun.tasks.serialization;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.types.TaskEntry;

import java.util.Objects;

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

    public Any mergeWith(Message args)
            throws StatefunTasksException {

        return mergeWith(args, null);
    }

    public Any mergeWith(Message args, MapOfStringToAny kwargs)
            throws StatefunTasksException {

        try {

            var argsAndKwargs = Objects.isNull(taskEntry.request)
                    ? ArgsAndKwargs.getDefaultInstance()
                    : ArgsAndKwargsSerializer.of(taskEntry.request).getArgsAndKwargs();

            var mergedKwargs = argsAndKwargs.getKwargs().toBuilder();

            // merge kwargs
            if (!Objects.isNull(kwargs)) {
                mergedKwargs.putAllItems(kwargs.getItemsMap());
            }

            // if task args and merged kwargs are empty then just return args - nothing in task entry to pass to task
            if (argsAndKwargs.getArgs().getItemsCount() == 0 && mergedKwargs.getItemsCount() == 0) {
                return MessageTypes.packAny(args);
            }

            // merge args
            var mergedArgs = TupleOfAny.newBuilder();

            if (args instanceof Any && ((Any) args).is(TupleOfAny.class)) {
                var tupleArgs = ((Any) args).unpack(TupleOfAny.class);
                mergedArgs.addAllItems(tupleArgs.getItemsList());
            }
            else if (args instanceof TupleOfAny) {
                var tupleArgs = (TupleOfAny) args;
                mergedArgs.addAllItems(tupleArgs.getItemsList());
            } else {
                mergedArgs.addItems(MessageTypes.packAny(args));
            }

            mergedArgs.addAllItems(argsAndKwargs.getArgs().getItemsList());

            return MessageTypes.packAny(ArgsAndKwargs.newBuilder()
                    .setArgs(mergedArgs)
                    .setKwargs(mergedKwargs)
                    .build());

        } catch (InvalidProtocolBufferException e) {
            throw new InvalidMessageTypeException("Protobuf parsing error", e);
        }
    }

    public Any mergeValueWith(ValueArgsAndKwargs valueArgsAndKwargs)
            throws StatefunTasksException {

        return mergeValueWith(valueArgsAndKwargs.getArgs(), valueArgsAndKwargs.getKwargs());
    }

    public Any mergeValueWith(Message args)
            throws StatefunTasksException {

        return mergeValueWith(args, null);
    }

    public Any mergeValueWith(Message args, MapOfStringToValue kwargs)
            throws StatefunTasksException {

        try {

            var taskValueArgsAndKwargs = Objects.isNull(taskEntry.request)
                    ? ValueArgsAndKwargs.getDefaultInstance()
                    : ValueArgsAndKwargsSerializer.of(taskEntry.request).getArgsAndKwargs();

            var mergedKwargs = taskValueArgsAndKwargs.getKwargs().toBuilder();

            // merge kwargs — incoming takes precedence
            if (!Objects.isNull(kwargs)) {
                mergedKwargs.putAllItems(kwargs.getItemsMap());
            }

            // if task args and merged kwargs are empty then just return args — nothing in task entry to pass to task
            if (taskValueArgsAndKwargs.getArgs().getItemsCount() == 0 && mergedKwargs.getItemsCount() == 0) {
                return MessageTypes.packAny(args);
            }

            // merge args — incoming precedes task entry args
            var mergedArgs = TupleOfValue.newBuilder();

            if (args instanceof Any && ((Any) args).is(TupleOfValue.class)) {
                var tupleArgs = ((Any) args).unpack(TupleOfValue.class);
                mergedArgs.addAllItems(tupleArgs.getItemsList());
            }
            else if (args instanceof Any && ((Any) args).is(Value.class)) {
                var valueArg = ((Any) args).unpack(Value.class);
                mergedArgs.addItems(valueArg);
            }
            else if (args instanceof TupleOfValue) {
                var tupleArgs = (TupleOfValue) args;
                mergedArgs.addAllItems(tupleArgs.getItemsList());
            } else {
                mergedArgs.addItems(MessageTypes.packValue(args));
            }

            mergedArgs.addAllItems(taskValueArgsAndKwargs.getArgs().getItemsList());

            return MessageTypes.packAny(ValueArgsAndKwargs.newBuilder()
                    .setArgs(mergedArgs)
                    .setKwargs(mergedKwargs)
                    .build());

        } catch (InvalidProtocolBufferException e) {
            throw new InvalidMessageTypeException("Protobuf parsing error", e);
        }
    }
}
