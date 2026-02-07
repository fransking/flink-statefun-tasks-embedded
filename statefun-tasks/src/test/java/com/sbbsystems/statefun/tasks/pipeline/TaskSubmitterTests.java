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

package com.sbbsystems.statefun.tasks.pipeline;

import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.graph.DeferredTaskIds;
import com.sbbsystems.statefun.tasks.graph.v2.GraphEntry;
import com.sbbsystems.statefun.tasks.graph.v2.MapOfGraphEntries;
import com.sbbsystems.statefun.tasks.types.DeferredTask;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;


public class TaskSubmitterTests {
    private PipelineFunctionState state;
    private Context context;
    private Address address;

    @BeforeEach
    public void setup() {
        this.state = PipelineFunctionState.newInstance();
        this.context = mock(Context.class);
        this.address = new Address(new FunctionType("namespace", "func"), "id");
    }

    @Test
    public void submits_two_tasks_when_max_parallelism_is_zero() throws StatefunTasksException {
        var group = GraphEntry.forGroup("group-id", 0, false);
        var task1 = GraphEntry.forTask("task-1", false, false, false);
        var task2 = GraphEntry.forTask("task-2", false, false, false);

        task1.setParentGroupId(group.getId());
        task2.setParentGroupId(group.getId());

        state.setGraphEntries(MapOfGraphEntries.from(new HashMap<>() {
            {
                put(group.getId(), group);
                put(task1.getId(), task1);
                put(task2.getId(), task2);
            }
        }));

        var message1 = TypedValue.newBuilder().setTypename("request-type-1").build();
        var message2 = TypedValue.newBuilder().setTypename("request-type-2").build();

        try (var submitter = TaskSubmitter.of(state, context)) {
            submitter.submitOrDefer(task1, address, message1);
            submitter.submitOrDefer(task2, address, message2);
        }

        verify(context).send(eq(address), eq(message1));
        verify(context).send(eq(address), eq(message2));

        // No deferrals should be written to the state
        assertThat(state.getDeferredTaskIds().get("group-id")).isNull();
        assertThat(state.getDeferredTasks().get("task-1")).isNull();
        assertThat(state.getDeferredTasks().get("task-2")).isNull();

        verifyNoMoreInteractions(context);
    }

    @Test
    public void delays_second_task_when_max_parallelism_one()
            throws StatefunTasksException, InvalidProtocolBufferException {
        var group = GraphEntry.forGroup("group-id", 1, false);

        var task1 = GraphEntry.forTask("task-1", false, false, false);
        var task2 = GraphEntry.forTask("task-2", false, false, false);

        task1.setParentGroupId(group.getId());
        task2.setParentGroupId(group.getId());

        state.setGraphEntries(MapOfGraphEntries.from(new HashMap<>() {
            {
                put(group.getId(), group);
                put(task1.getId(), task1);
                put(task2.getId(), task2);
            }
        }));

        var message1 = TypedValue.newBuilder().setTypename("request-type-1").build();
        var message2 = TypedValue.newBuilder().setTypename("request-type-2").build();

        try (var submitter = TaskSubmitter.of(state, context)) {
            submitter.submitOrDefer(task1, address, message1);
            submitter.submitOrDefer(task2, address, message2);
        }

        // First message should be sent
        verify(context).send(eq(address), eq(message1));

        // Second message should be deferred
        assertThat(state.getDeferredTaskIds().get("group-id").getTaskIds()).containsExactly("task-2");
        assertThat(state.getDeferredTasks().get("task-1")).isNull();
        assertThat(state.getDeferredTasks().get("task-2")).isNotNull();
        assertThat(state.getDeferredTasks().get("task-2").addressNamespace).isEqualTo(address.type().namespace());
        assertThat(state.getDeferredTasks().get("task-2").addressType).isEqualTo(address.type().name());
        assertThat(state.getDeferredTasks().get("task-2").addressId).isEqualTo(address.id());
        assertThat(TypedValue.parseFrom(state.getDeferredTasks().get("task-2").messageBytes)).isEqualTo(message2);

        verifyNoMoreInteractions(context);
    }

    @Test
    public void submits_tasks_across_multiple_groups() throws StatefunTasksException {
        var group1 = GraphEntry.forGroup("group-id-1", 1, false);
        var task1 = GraphEntry.forTask("task-1", false, false, false);
        var task2 = GraphEntry.forTask("task-2", false, false, false);

        task1.setParentGroupId(group1.getId());
        task2.setParentGroupId(group1.getId());

        var group2 = GraphEntry.forGroup("group-id-2", 1, false);
        var task3 = GraphEntry.forTask("task-3", false, false, false);
        var task4 = GraphEntry.forTask("task-4", false, false, false);

        task3.setParentGroupId(group2.getId());
        task4.setParentGroupId(group2.getId());

        state.setGraphEntries(MapOfGraphEntries.from(new HashMap<>() {
            {
                put(group1.getId(), group1);
                put(task1.getId(), task1);
                put(task2.getId(), task2);

                put(group2.getId(), group2);
                put(task3.getId(), task3);
                put(task4.getId(), task4);
            }
        }));

        var message1 = TypedValue.newBuilder().setTypename("request-type-1").build();
        var message2 = TypedValue.newBuilder().setTypename("request-type-2").build();
        var message3 = TypedValue.newBuilder().setTypename("request-type-3").build();
        var message4 = TypedValue.newBuilder().setTypename("request-type-4").build();

        try (var submitter = TaskSubmitter.of(state, context)) {
            submitter.submitOrDefer(task1, address, message1);
            submitter.submitOrDefer(task2, address, message2);
            submitter.submitOrDefer(task3, address, message3);
            submitter.submitOrDefer(task4, address, message4);
        }

        // First message from each group should be sent
        verify(context).send(eq(address), eq(message1));
        verify(context).send(eq(address), eq(message3));

        // Second messages should be deferred
        assertThat(state.getDeferredTaskIds().get("group-id-1").getTaskIds()).containsExactly("task-2");
        assertThat(state.getDeferredTaskIds().get("group-id-2").getTaskIds()).containsExactly("task-4");

        verifyNoMoreInteractions(context);
    }

    @Test
    public void submits_next_deferred_task() throws StatefunTasksException {
        var group = GraphEntry.forGroup("group-id", 1, false);
        var message = TypedValue.newBuilder().setTypename("request-type").build();
        var deferredTask = DeferredTask.of("namespace", "func", "id", message);
        state.getDeferredTasks().set("task-id", deferredTask);
        var taskIds = new LinkedList<String>();
        taskIds.add("task-id");
        state.getDeferredTaskIds().set("group-id", DeferredTaskIds.of(taskIds));

        TaskSubmitter.submitNextDeferredTask(state, context, group);

        verify(context).send(address, message);
        assertThat(state.getDeferredTaskIds().get("group-id")).isNull();
        assertThat(state.getDeferredTasks().get("task-id")).isNull();
    }

    @Test
    public void does_nothing_when_submitting_next_deferred_task_with_no_tasks_in_state() throws StatefunTasksException {
        var group = GraphEntry.forGroup("group-id", 1, false);

        TaskSubmitter.submitNextDeferredTask(state, context, group);

        verifyNoInteractions(context);
    }
}
