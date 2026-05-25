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
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class ArgsAndKwargsSerializerTests {

    // ---- of(ArgsAndKwargs) ---------------------------------------------------------------------

    @Test
    public void of_direct_returns_serializer_wrapping_supplied_proto()
            throws InvalidMessageTypeException {

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(StringValue.of("hello"))))
                .build();

        var serializer = ArgsAndKwargsSerializer.of(proto);

        assertThat(serializer.getArgsAndKwargs()).isEqualTo(proto);
    }

    // ---- of(byte[]) ----------------------------------------------------------------------------

    @Test
    public void of_bytes_round_trips_correctly()
            throws InvalidMessageTypeException {

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(StringValue.of("round_trip"))))
                .build();

        // bytes must be a packed Any containing ArgsAndKwargs
        var serializer = ArgsAndKwargsSerializer.of(Any.pack(proto).toByteArray());

        assertThat(serializer.getArgsAndKwargs()).isEqualTo(proto);
    }

    @Test
    public void of_bytes_throws_on_invalid_bytes() {
        assertThatThrownBy(() -> ArgsAndKwargsSerializer.of(new byte[]{-1, -2, -3}))
                .isInstanceOf(InvalidMessageTypeException.class);
    }

    // ---- of(Any) - ArgsAndKwargs ---------------------------------------------------------------

    @Test
    public void of_any_containing_args_and_kwargs_unpacks_directly()
            throws InvalidMessageTypeException {

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(StringValue.of("direct"))))
                .build();

        var serializer = ArgsAndKwargsSerializer.of(Any.pack(proto));

        assertThat(serializer.getArgsAndKwargs()).isEqualTo(proto);
    }

    // ---- of(Any) - TupleOfAny ------------------------------------------------------------------

    @Test
    public void of_any_containing_tuple_uses_tuple_as_args()
            throws InvalidMessageTypeException {

        var item = Any.pack(StringValue.of("world"));
        var tuple = TupleOfAny.newBuilder().addItems(item).build();

        var serializer = ArgsAndKwargsSerializer.of(Any.pack(tuple));

        assertThat(serializer.getArgsAndKwargs().getArgs()).isEqualTo(tuple);
        assertThat(serializer.getArg(0)).isEqualTo(item);
    }

    // ---- of(Any) - empty Any -------------------------------------------------------------------

    @Test
    public void of_any_with_empty_any_produces_empty_args()
            throws InvalidMessageTypeException {

        var serializer = ArgsAndKwargsSerializer.of(Any.getDefaultInstance());

        assertThat(serializer.getArgsAndKwargs().getArgs().getItemsCount()).isEqualTo(0);
    }

    // ---- of(Any) - single non-tuple Any --------------------------------------------------------

    @Test
    public void of_any_with_single_proto_wraps_as_single_arg()
            throws InvalidMessageTypeException {

        var packed = Any.pack(Pipeline.getDefaultInstance());

        var serializer = ArgsAndKwargsSerializer.of(packed);

        assertThat(serializer.getArgsAndKwargs().getArgs().getItemsCount()).isEqualTo(1);
        assertThat(serializer.getArg(0)).isEqualTo(packed);
    }

    // ---- slice ---------------------------------------------------------------------------------

    @Test
    public void slice_removes_leading_args()
            throws InvalidMessageTypeException {

        var a = Any.pack(StringValue.of("a"));
        var b = Any.pack(StringValue.of("b"));
        var c = Any.pack(StringValue.of("c"));

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder().addItems(a).addItems(b).addItems(c))
                .build();

        var sliced = ArgsAndKwargsSerializer.of(proto).slice(1);

        assertThat(sliced.getArgs().getItemsCount()).isEqualTo(2);
        assertThat(sliced.getArgs().getItems(0)).isEqualTo(b);
        assertThat(sliced.getArgs().getItems(1)).isEqualTo(c);
    }

    @Test
    public void slice_preserves_kwargs()
            throws InvalidMessageTypeException {

        var kwargs = MapOfStringToAny.newBuilder()
                .putItems("key", Any.pack(StringValue.of("val")))
                .build();

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(StringValue.of("first")))
                        .addItems(Any.pack(StringValue.of("second"))))
                .setKwargs(kwargs)
                .build();

        var sliced = ArgsAndKwargsSerializer.of(proto).slice(1);

        assertThat(sliced.getKwargs()).isEqualTo(kwargs);
    }

    @Test
    public void slice_beyond_bounds_returns_empty_args()
            throws InvalidMessageTypeException {

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(StringValue.of("only"))))
                .build();

        var sliced = ArgsAndKwargsSerializer.of(proto).slice(5);

        assertThat(sliced.getArgs().getItemsCount()).isEqualTo(0);
    }

    // ---- getArg --------------------------------------------------------------------------------

    @Test
    public void getArg_returns_correct_item_by_index()
            throws InvalidMessageTypeException {

        var first = Any.pack(StringValue.of("first"));
        var second = Any.pack(StringValue.of("second"));

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder().addItems(first).addItems(second))
                .build();

        var serializer = ArgsAndKwargsSerializer.of(proto);

        assertThat(serializer.getArg(0)).isEqualTo(first);
        assertThat(serializer.getArg(1)).isEqualTo(second);
    }

    // ---- getInitialArgsAndKwargs ---------------------------------------------------------------

    @Test
    public void getInitialArgsAndKwargs_returns_sliced_args_when_non_empty()
            throws InvalidMessageTypeException, InvalidProtocolBufferException {

        var first = Any.pack(StringValue.of("keep"));
        var second = Any.pack(StringValue.of("sliced"));

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder().addItems(first).addItems(second))
                .build();

        // slice(1) leaves [second], so initial args from pipeline should be ignored
        var pipeline = Pipeline.getDefaultInstance();
        var result = ArgsAndKwargsSerializer.of(proto).getInitialArgsAndKwargs(pipeline, 1);

        assertThat(result.getArgs().getItemsCount()).isEqualTo(1);
        assertThat(result.getArgs().getItems(0)).isEqualTo(second);
    }

    @Test
    public void getInitialArgsAndKwargs_falls_back_to_pipeline_initial_args_when_sliced_is_empty()
            throws InvalidMessageTypeException, InvalidProtocolBufferException {

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(StringValue.of("consumed"))))
                .build();

        var initialArg = Any.pack(StringValue.of("from_pipeline"));
        var packedTuple = Any.pack(TupleOfAny.newBuilder().addItems(initialArg).build());

        var pipeline = Pipeline.newBuilder()
                .setInitialArgs(packedTuple)
                .build();

        // slice(1) leaves nothing, so falls back to pipeline initial_args
        var result = ArgsAndKwargsSerializer.of(proto).getInitialArgsAndKwargs(pipeline, 1);

        assertThat(result.getArgs().getItemsCount()).isEqualTo(1);
        assertThat(result.getArgs().getItems(0)).isEqualTo(initialArg);
    }

    @Test
    public void getInitialArgsAndKwargs_wraps_non_tuple_initial_args_as_single_item()
            throws InvalidMessageTypeException, InvalidProtocolBufferException {

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(StringValue.of("consumed"))))
                .build();

        // initial_args is a packed Pipeline (not a TupleOfAny)
        var packedPipeline = Any.pack(Pipeline.getDefaultInstance());
        var pipeline = Pipeline.newBuilder()
                .setInitialArgs(packedPipeline)
                .build();

        var result = ArgsAndKwargsSerializer.of(proto).getInitialArgsAndKwargs(pipeline, 1);

        assertThat(result.getArgs().getItemsCount()).isEqualTo(1);
        assertThat(result.getArgs().getItems(0)).isEqualTo(packedPipeline);
    }

    @Test
    public void getInitialArgsAndKwargs_uses_initial_kwargs_from_pipeline()
            throws InvalidMessageTypeException, InvalidProtocolBufferException {

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(StringValue.of("consumed"))))
                .build();

        var kwargs = MapOfStringToAny.newBuilder()
                .putItems("k", Any.pack(StringValue.of("v")))
                .build();

        var pipeline = Pipeline.newBuilder()
                .setInitialKwargs(kwargs)
                .build();

        var result = ArgsAndKwargsSerializer.of(proto).getInitialArgsAndKwargs(pipeline, 1);

        assertThat(result.getKwargs()).isEqualTo(kwargs);
    }

    @Test
    public void getInitialArgsAndKwargs_returns_empty_args_when_no_initial_args_on_pipeline()
            throws InvalidMessageTypeException, InvalidProtocolBufferException {

        var proto = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(StringValue.of("consumed"))))
                .build();

        var result = ArgsAndKwargsSerializer.of(proto)
                .getInitialArgsAndKwargs(Pipeline.getDefaultInstance(), 1);

        assertThat(result.getArgs().getItemsCount()).isEqualTo(0);
    }
}

