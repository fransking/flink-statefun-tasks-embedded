/*
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

import com.google.common.collect.Iterables;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;

import static com.sbbsystems.statefun.tasks.types.MessageTypes.packValue;

public final class ValueArgsAndKwargsSerializer {
    private final ValueArgsAndKwargs argsAndKwargs;

    public static ValueArgsAndKwargsSerializer of(ValueArgsAndKwargs argsAndKwargs) {
        return new ValueArgsAndKwargsSerializer(argsAndKwargs);
    }

    public static ValueArgsAndKwargsSerializer of(byte[] bytes)
            throws InvalidMessageTypeException {

        try {
            return ValueArgsAndKwargsSerializer.of(Any.parseFrom(bytes));
        } catch (InvalidProtocolBufferException e) {
            throw new InvalidMessageTypeException("Protobuf parsing error", e);
        }
    }

    public static ValueArgsAndKwargsSerializer of(Any any)
            throws InvalidMessageTypeException {

        try {
            if (any.is(ValueArgsAndKwargs.class)) {
                return ValueArgsAndKwargsSerializer.of(any.unpack(ValueArgsAndKwargs.class));
            }
            else {
                TupleOfValue args;

                if (any.is(TupleOfValue.class)) {
                    args = any.unpack(TupleOfValue.class);
                }
                else if (any.is(Value.class)) {
                    args = TupleOfValue.newBuilder()
                            .addItems(any.unpack(Value.class))
                            .build();
                }
                else if (MessageTypes.isEmpty(any)) {
                    args = TupleOfValue.getDefaultInstance();
                }
                else {
                    args = TupleOfValue.newBuilder()
                            .addItems(packValue(any))
                            .build();
                }

                return ValueArgsAndKwargsSerializer.of(ValueArgsAndKwargs
                        .newBuilder()
                        .setArgs(args)
                        .build());
            }
        } catch (InvalidProtocolBufferException e) {
            throw new InvalidMessageTypeException("Protobuf parsing error", e);
        }
    }

    public static ValueArgsAndKwargsSerializer of(Value value)
            throws InvalidMessageTypeException {

        if (value.hasTupleValue()) {
            return ValueArgsAndKwargsSerializer.of(ValueArgsAndKwargs
                    .newBuilder()
                    .setArgs(value.getTupleValue())
                    .build());
        }
        else if (value.hasAnyValue()) {
            var wrapped = Value.newBuilder().setAnyValue(value.getAnyValue()).build();
            return ValueArgsAndKwargsSerializer.of(ValueArgsAndKwargs
                    .newBuilder()
                    .setArgs(TupleOfValue.newBuilder().addItems(wrapped).build())
                    .build());
        }
        else if (value.getKindCase() == Value.KindCase.KIND_NOT_SET) {
            return ValueArgsAndKwargsSerializer.of(ValueArgsAndKwargs
                    .newBuilder()
                    .setArgs(TupleOfValue.getDefaultInstance())
                    .build());
        }
        else {
            return ValueArgsAndKwargsSerializer.of(ValueArgsAndKwargs
                    .newBuilder()
                    .setArgs(TupleOfValue.newBuilder().addItems(value).build())
                    .build());
        }
    }

    private ValueArgsAndKwargsSerializer(ValueArgsAndKwargs argsAndKwargs) {
        this.argsAndKwargs = argsAndKwargs;
    }

    public ValueArgsAndKwargs slice(int start) {
        var builder = ValueArgsAndKwargs.newBuilder();
        var args = Iterables.skip(argsAndKwargs.getArgs().getItemsList(), start);
        builder.setArgs(TupleOfValue.newBuilder().addAllItems(args).build());
        builder.setKwargs(argsAndKwargs.getKwargs());
        return builder.build();
    }

    public ValueArgsAndKwargs getArgsAndKwargs() {
        return argsAndKwargs;
    }

    public Value getArg(int index) {
        return this.argsAndKwargs.getArgs().getItems(index);
    }

    public ValueArgsAndKwargs getInitialArgsAndKwargs(Pipeline pipelineProto, int slice)
            throws InvalidProtocolBufferException {

        var taskArgs = slice(slice);

        if (taskArgs.getArgs().getItemsCount() > 0) {
            return taskArgs;
        }

        var valueArgsAndKwargs = ValueArgsAndKwargs.newBuilder();

        if (pipelineProto.hasInitialArgs()) {
            var args = pipelineProto.getInitialArgs();

            valueArgsAndKwargs.setArgs(args.is(TupleOfValue.class)
                    ? args.unpack(TupleOfValue.class)
                    : TupleOfValue.newBuilder().addItems(packValue(args)).build());
        }

        return valueArgsAndKwargs.setKwargs(pipelineProto.getInitialValueKwargs()).build();
    }
}
