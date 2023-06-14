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
package com.sbbsystems.statefun.tasks.serialization;

import com.google.common.collect.Iterables;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.MapOfStringToAny;
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.api.java.tuple.Tuple;

public final class ArgsAndKwargsSerializer {
    private final ArgsAndKwargs argsAndKwargs;

    public static ArgsAndKwargsSerializer of(ArgsAndKwargs argsAndKwargs) {
        return new ArgsAndKwargsSerializer(argsAndKwargs);
    }

    public static ArgsAndKwargsSerializer of(byte[] bytes)
            throws InvalidMessageTypeException {

        try {
            return ArgsAndKwargsSerializer.of(Any.parseFrom(bytes));
        } catch (InvalidProtocolBufferException e) {
            throw new InvalidMessageTypeException("Protobuf parsing error", e);
        }
    }

    public static ArgsAndKwargsSerializer of(Any any)
            throws InvalidMessageTypeException {

        try {
            if (any.is(ArgsAndKwargs.class)) {
                return ArgsAndKwargsSerializer.of(any.unpack(ArgsAndKwargs.class));
            }
            else {

                TupleOfAny args;

                if (any.is(TupleOfAny.class)) {
                    args = any.unpack(TupleOfAny.class);
                }
                else if (MessageTypes.isEmpty(any)) {
                    args = TupleOfAny.getDefaultInstance();
                }
                else {
                    args = TupleOfAny.newBuilder().addItems(any).build();
                }

                return ArgsAndKwargsSerializer.of(ArgsAndKwargs
                        .newBuilder()
                        .setArgs(args)
                        .build());
            }
        } catch (InvalidProtocolBufferException e) {
            throw new InvalidMessageTypeException("Protobuf parsing error", e);
        }
    }

    private ArgsAndKwargsSerializer(ArgsAndKwargs argsAndKwargs) {
        this.argsAndKwargs = argsAndKwargs;
    }

    public ArgsAndKwargs slice(int start) {
        var builder = ArgsAndKwargs.newBuilder();
        var args = Iterables.skip(argsAndKwargs.getArgs().getItemsList(), start);
        builder.setArgs(TupleOfAny.newBuilder().addAllItems(args).build());
        builder.setKwargs(argsAndKwargs.getKwargs());
        return builder.build();
    }

    public ArgsAndKwargs getArgsAndKwargs() {
        return argsAndKwargs;
    }

    public Any getArg(int index) {
        return this.argsAndKwargs.getArgs().getItems(index);
    }

    public ArgsAndKwargs getInitialArgsAndKwargs(Pipeline pipelineProto, int slice)
            throws InvalidProtocolBufferException {

        var taskArgs = slice(slice);

        if (taskArgs.getArgs().getItemsCount() > 0) {
            return taskArgs;
        }

        var argsAndKwargs = ArgsAndKwargs.newBuilder();

        if (pipelineProto.hasInitialArgs()) {
            var args = pipelineProto.getInitialArgs();

            argsAndKwargs.setArgs(args.is(TupleOfAny.class)
                    ? args.unpack(TupleOfAny.class)
                    : TupleOfAny.newBuilder().addItems(args).build());
        }

        return argsAndKwargs.setKwargs(pipelineProto.getInitialKwargs()).build();
    }
}
