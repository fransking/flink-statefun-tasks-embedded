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
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;

public final class ArgsAndKwargsSerializer {
    private final ArgsAndKwargs argsAndKwargs;

    public static ArgsAndKwargsSerializer from(ArgsAndKwargs argsAndKwargs) {
        return new ArgsAndKwargsSerializer(argsAndKwargs);
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

    public Any getArg(int index) {
        return this.argsAndKwargs.getArgs().getItems(index);
    }
}
