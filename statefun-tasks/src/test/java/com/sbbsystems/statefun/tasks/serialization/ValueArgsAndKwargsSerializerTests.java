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

import com.google.protobuf.Any;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class ValueArgsAndKwargsSerializerTests {

    // ---- of(ValueArgsAndKwargs) ----------------------------------------------------------------

    @Test
    public void of_direct_returns_serializer_wrapping_supplied_proto()
            throws InvalidMessageTypeException {

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setStringValue("hello").build()))
                .build();

        var serializer = ValueArgsAndKwargsSerializer.of(proto);

        assertThat(serializer.getArgsAndKwargs()).isEqualTo(proto);
    }

    // ---- of(byte[]) ----------------------------------------------------------------------------

    @Test
    public void of_bytes_round_trips_correctly()
            throws InvalidMessageTypeException {

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setIntValue(42).build()))
                .build();

        // bytes are a packed Any containing ValueArgsAndKwargs, same as how TaskEntry stores requests
        var serializer = ValueArgsAndKwargsSerializer.of(Any.pack(proto).toByteArray());

        assertThat(serializer.getArgsAndKwargs()).isEqualTo(proto);
    }

    @Test
    public void of_bytes_throws_on_invalid_bytes() {
        assertThatThrownBy(() -> ValueArgsAndKwargsSerializer.of(new byte[]{-1, -2, -3}))
                .isInstanceOf(InvalidMessageTypeException.class);
    }

    // ---- of(Any) - ValueArgsAndKwargs ----------------------------------------------------------

    @Test
    public void of_any_containing_value_args_and_kwargs_unpacks_directly()
            throws InvalidMessageTypeException {

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setStringValue("direct").build()))
                .build();

        var serializer = ValueArgsAndKwargsSerializer.of(Any.pack(proto));

        assertThat(serializer.getArgsAndKwargs()).isEqualTo(proto);
    }

    // ---- of(Any) - TupleOfValue ----------------------------------------------------------------

    @Test
    public void of_any_containing_tuple_of_value_uses_it_as_args()
            throws InvalidMessageTypeException {

        var item = Value.newBuilder().setStringValue("world").build();
        var tuple = TupleOfValue.newBuilder().addItems(item).build();

        var serializer = ValueArgsAndKwargsSerializer.of(Any.pack(tuple));

        assertThat(serializer.getArgsAndKwargs().getArgs()).isEqualTo(tuple);
        assertThat(serializer.getArg(0)).isEqualTo(item);
    }

    // ---- of(Any) - empty Any -------------------------------------------------------------------

    @Test
    public void of_any_with_empty_any_produces_empty_args()
            throws InvalidMessageTypeException {

        var serializer = ValueArgsAndKwargsSerializer.of(Any.getDefaultInstance());

        assertThat(serializer.getArgsAndKwargs().getArgs().getItemsCount()).isEqualTo(0);
    }

    // ---- of(Any) - single non-tuple Any --------------------------------------------------------

    @Test
    public void of_any_with_single_proto_wraps_as_any_value_arg()
            throws InvalidMessageTypeException {

        var packed = Any.pack(Pipeline.getDefaultInstance());

        var serializer = ValueArgsAndKwargsSerializer.of(packed);

        assertThat(serializer.getArgsAndKwargs().getArgs().getItemsCount()).isEqualTo(1);
        assertThat(serializer.getArg(0).getAnyValue()).isEqualTo(packed);
    }

    // ---- of(Value) - tuple value ---------------------------------------------------------------

    @Test
    public void of_value_with_tuple_uses_tuple_as_args()
            throws InvalidMessageTypeException {

        var item = Value.newBuilder().setStringValue("world").build();
        var tuple = TupleOfValue.newBuilder().addItems(item).build();
        var value = Value.newBuilder().setTupleValue(tuple).build();

        var serializer = ValueArgsAndKwargsSerializer.of(value);

        assertThat(serializer.getArgsAndKwargs().getArgs()).isEqualTo(tuple);
        assertThat(serializer.getArg(0)).isEqualTo(item);
    }

    // ---- of(Value) - any value -----------------------------------------------------------------

    @Test
    public void of_value_with_any_value_wraps_as_single_arg()
            throws InvalidMessageTypeException {

        var inner = Any.pack(Pipeline.getDefaultInstance());
        var value = Value.newBuilder().setAnyValue(inner).build();

        var serializer = ValueArgsAndKwargsSerializer.of(value);

        assertThat(serializer.getArgsAndKwargs().getArgs().getItemsCount()).isEqualTo(1);
        assertThat(serializer.getArg(0).getAnyValue()).isEqualTo(inner);
    }

    // ---- of(Value) - empty (KIND_NOT_SET) -------------------------------------------------------

    @Test
    public void of_value_with_no_kind_set_produces_empty_args()
            throws InvalidMessageTypeException {

        var value = Value.getDefaultInstance();

        var serializer = ValueArgsAndKwargsSerializer.of(value);

        assertThat(serializer.getArgsAndKwargs().getArgs().getItemsCount()).isEqualTo(0);
    }

    // ---- of(Value) - scalar value --------------------------------------------------------------

    @Test
    public void of_value_with_scalar_wraps_as_single_arg()
            throws InvalidMessageTypeException {

        var value = Value.newBuilder().setDoubleValue(3.14).build();

        var serializer = ValueArgsAndKwargsSerializer.of(value);

        assertThat(serializer.getArgsAndKwargs().getArgs().getItemsCount()).isEqualTo(1);
        assertThat(serializer.getArg(0).getDoubleValue()).isEqualTo(3.14);
    }

    @Test
    public void of_value_with_bool_wraps_as_single_arg()
            throws InvalidMessageTypeException {

        var value = Value.newBuilder().setBoolValue(true).build();

        var serializer = ValueArgsAndKwargsSerializer.of(value);

        assertThat(serializer.getArgsAndKwargs().getArgs().getItemsCount()).isEqualTo(1);
        assertThat(serializer.getArg(0).getBoolValue()).isTrue();
    }

    // ---- slice ---------------------------------------------------------------------------------

    @Test
    public void slice_removes_leading_args()
            throws InvalidMessageTypeException {

        var a = Value.newBuilder().setStringValue("a").build();
        var b = Value.newBuilder().setStringValue("b").build();
        var c = Value.newBuilder().setStringValue("c").build();

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder().addItems(a).addItems(b).addItems(c))
                .build();

        var sliced = ValueArgsAndKwargsSerializer.of(proto).slice(1);

        assertThat(sliced.getArgs().getItemsCount()).isEqualTo(2);
        assertThat(sliced.getArgs().getItems(0)).isEqualTo(b);
        assertThat(sliced.getArgs().getItems(1)).isEqualTo(c);
    }

    @Test
    public void slice_preserves_kwargs()
            throws InvalidMessageTypeException {

        var kwargs = MapOfStringToValue.newBuilder()
                .putItems("key", Value.newBuilder().setStringValue("val").build())
                .build();

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setStringValue("first"))
                        .addItems(Value.newBuilder().setStringValue("second")))
                .setKwargs(kwargs)
                .build();

        var sliced = ValueArgsAndKwargsSerializer.of(proto).slice(1);

        assertThat(sliced.getKwargs()).isEqualTo(kwargs);
    }

    @Test
    public void slice_beyond_bounds_returns_empty_args()
            throws InvalidMessageTypeException {

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setStringValue("only")))
                .build();

        var sliced = ValueArgsAndKwargsSerializer.of(proto).slice(5);

        assertThat(sliced.getArgs().getItemsCount()).isEqualTo(0);
    }

    // ---- getArg --------------------------------------------------------------------------------

    @Test
    public void getArg_returns_correct_item_by_index()
            throws InvalidMessageTypeException {

        var first = Value.newBuilder().setIntValue(1).build();
        var second = Value.newBuilder().setIntValue(2).build();

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder().addItems(first).addItems(second))
                .build();

        var serializer = ValueArgsAndKwargsSerializer.of(proto);

        assertThat(serializer.getArg(0)).isEqualTo(first);
        assertThat(serializer.getArg(1)).isEqualTo(second);
    }

    // ---- getInitialArgsAndKwargs ---------------------------------------------------------------

    @Test
    public void getInitialArgsAndKwargs_returns_sliced_args_when_non_empty()
            throws InvalidMessageTypeException, InvalidProtocolBufferException {

        var first = Value.newBuilder().setStringValue("keep").build();
        var second = Value.newBuilder().setStringValue("sliced").build();

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder().addItems(first).addItems(second))
                .build();

        // slice(1) leaves [second], so initial args from pipeline should be ignored
        var pipeline = Pipeline.getDefaultInstance();
        var result = ValueArgsAndKwargsSerializer.of(proto).getInitialArgsAndKwargs(pipeline, 1);

        assertThat(result.getArgs().getItemsCount()).isEqualTo(1);
        assertThat(result.getArgs().getItems(0)).isEqualTo(second);
    }

    @Test
    public void getInitialArgsAndKwargs_falls_back_to_pipeline_initial_args_when_sliced_is_empty()
            throws InvalidMessageTypeException, InvalidProtocolBufferException {

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setStringValue("consumed")))
                .build();

        var initialArg = Value.newBuilder().setStringValue("from_pipeline").build();
        var packedTuple = Any.pack(TupleOfValue.newBuilder().addItems(initialArg).build());

        var pipeline = Pipeline.newBuilder()
                .setInitialArgs(packedTuple)
                .build();

        // slice(1) leaves nothing, so falls back to pipeline initial_args
        var result = ValueArgsAndKwargsSerializer.of(proto).getInitialArgsAndKwargs(pipeline, 1);

        assertThat(result.getArgs().getItemsCount()).isEqualTo(1);
        assertThat(result.getArgs().getItems(0)).isEqualTo(initialArg);
    }

    @Test
    public void getInitialArgsAndKwargs_wraps_non_tuple_initial_args_as_any_value()
            throws InvalidMessageTypeException, InvalidProtocolBufferException {

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setStringValue("consumed")))
                .build();

        // initial_args is a packed Pipeline (not a TupleOfValue)
        var packedPipeline = Any.pack(Pipeline.getDefaultInstance());
        var pipeline = Pipeline.newBuilder()
                .setInitialArgs(packedPipeline)
                .build();

        var result = ValueArgsAndKwargsSerializer.of(proto).getInitialArgsAndKwargs(pipeline, 1);

        assertThat(result.getArgs().getItemsCount()).isEqualTo(1);
        assertThat(result.getArgs().getItems(0).getAnyValue()).isEqualTo(packedPipeline);
    }

    @Test
    public void getInitialArgsAndKwargs_uses_initial_value_kwargs_from_pipeline()
            throws InvalidMessageTypeException, InvalidProtocolBufferException {

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setStringValue("consumed")))
                .build();

        var kwargs = MapOfStringToValue.newBuilder()
                .putItems("k", Value.newBuilder().setIntValue(99).build())
                .build();

        var pipeline = Pipeline.newBuilder()
                .setInitialValueKwargs(kwargs)
                .build();

        var result = ValueArgsAndKwargsSerializer.of(proto).getInitialArgsAndKwargs(pipeline, 1);

        assertThat(result.getKwargs()).isEqualTo(kwargs);
    }

    @Test
    public void getInitialArgsAndKwargs_returns_empty_args_when_no_initial_args_on_pipeline()
            throws InvalidMessageTypeException, InvalidProtocolBufferException {

        var proto = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setStringValue("consumed")))
                .build();

        var result = ValueArgsAndKwargsSerializer.of(proto)
                .getInitialArgsAndKwargs(Pipeline.getDefaultInstance(), 1);

        assertThat(result.getArgs().getItemsCount()).isEqualTo(0);
    }
}

