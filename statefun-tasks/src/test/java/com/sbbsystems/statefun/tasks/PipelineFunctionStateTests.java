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
package com.sbbsystems.statefun.tasks;

import com.google.protobuf.Any;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PipelineFunctionStateTests {

    private PipelineFunctionState state;

    @BeforeEach
    public void setup() {
        state = PipelineFunctionState.newInstance();
    }

    // ---- initialArgsAndKwargs ------------------------------------------------------------------

    @Test
    public void initial_args_and_kwargs_is_null_by_default() {
        assertThat(state.getInitialArgsAndKwargs()).isNull();
    }

    @Test
    public void set_and_get_initial_args_and_kwargs() {
        var argsAndKwargs = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(TaskRequest.getDefaultInstance())))
                .build();

        state.setInitialArgsAndKwargs(argsAndKwargs);

        assertThat(state.getInitialArgsAndKwargs()).isEqualTo(argsAndKwargs);
    }

    // ---- initialValueArgsAndKwargs -------------------------------------------------------------

    @Test
    public void initial_value_args_and_kwargs_is_null_by_default() {
        assertThat(state.getInitialValueArgsAndKwargs()).isNull();
    }

    @Test
    public void set_and_get_initial_value_args_and_kwargs() {
        var valueArgsAndKwargs = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setStringValue("hello").build()))
                .build();

        state.setInitialValueArgsAndKwargs(valueArgsAndKwargs);

        assertThat(state.getInitialValueArgsAndKwargs()).isEqualTo(valueArgsAndKwargs);
    }

    @Test
    public void set_initial_value_args_and_kwargs_does_not_affect_any_args_and_kwargs() {
        var argsAndKwargs = ArgsAndKwargs.newBuilder()
                .setArgs(TupleOfAny.newBuilder()
                        .addItems(Any.pack(TaskRequest.getDefaultInstance())))
                .build();

        var valueArgsAndKwargs = ValueArgsAndKwargs.newBuilder()
                .setArgs(TupleOfValue.newBuilder()
                        .addItems(Value.newBuilder().setIntValue(42).build()))
                .build();

        state.setInitialArgsAndKwargs(argsAndKwargs);
        state.setInitialValueArgsAndKwargs(valueArgsAndKwargs);

        assertThat(state.getInitialArgsAndKwargs()).isEqualTo(argsAndKwargs);
        assertThat(state.getInitialValueArgsAndKwargs()).isEqualTo(valueArgsAndKwargs);
    }

    // ---- reset ---------------------------------------------------------------------------------

    @Test
    public void reset_clears_initial_args_and_kwargs() throws StatefunTasksException {
        state.setInitialArgsAndKwargs(ArgsAndKwargs.getDefaultInstance());
        state.reset();
        assertThat(state.getInitialArgsAndKwargs()).isNull();
    }

    @Test
    public void reset_clears_initial_value_args_and_kwargs() throws StatefunTasksException {
        state.setInitialValueArgsAndKwargs(ValueArgsAndKwargs.getDefaultInstance());
        state.reset();
        assertThat(state.getInitialValueArgsAndKwargs()).isNull();
    }

    // ---- other existing state ------------------------------------------------------------------

    @Test
    public void is_inline_defaults_to_false() {
        assertThat(state.getIsInline()).isFalse();
    }

    @Test
    public void set_and_get_is_inline() {
        state.setIsInline(true);
        assertThat(state.getIsInline()).isTrue();
    }

    @Test
    public void is_fruitful_defaults_to_true() {
        assertThat(state.getIsFruitful()).isTrue();
    }

    @Test
    public void set_and_get_status() {
        state.setStatus(TaskStatus.Status.RUNNING);
        assertThat(state.getStatus()).isEqualTo(TaskStatus.Status.RUNNING);
    }

    @Test
    public void reset_clears_status() throws StatefunTasksException {
        state.setStatus(TaskStatus.Status.RUNNING);
        state.reset();
        assertThat(state.getStatus()).isEqualTo(TaskStatus.Status.PENDING);
    }
}

