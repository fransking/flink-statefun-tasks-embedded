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
package com.sbbsystems.statefun.tasks.graph;

import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.PipelineEntry;
import com.sbbsystems.statefun.tasks.types.GroupEntry;
import com.sbbsystems.statefun.tasks.types.GroupEntryBuilder;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import com.sbbsystems.statefun.tasks.types.TaskEntryBuilder;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.isNull;

public final class PipelineGraphBuilder {

    private final List<TaskEntry> taskEntries = new LinkedList<>();
    private final List<GroupEntry> groupEntries = new LinkedList<>();

    private final PipelineFunctionState state;
    private final MapOfEntries entries;
    private Pipeline pipelineProto;
    private String tail;

    private PipelineGraphBuilder(PipelineFunctionState state) {
        this.state = state;
        this.entries = state.getEntries();
    }

    public static PipelineGraphBuilder newInstance() {
        return new PipelineGraphBuilder(PipelineFunctionState.newInstance());
    }

    public static PipelineGraphBuilder from(PipelineFunctionState state) {
        return new PipelineGraphBuilder(state);
    }

    public PipelineGraphBuilder fromProto(@NotNull Pipeline pipelineProto) {
        this.pipelineProto = Objects.requireNonNull(pipelineProto);
        return this;
    }

    public PipelineGraph build()
            throws InvalidGraphException {

        if (!Objects.isNull(pipelineProto)) {
            //build graph from protobuf
            var headEntry = buildGraph(pipelineProto);

            state.setHead(Objects.isNull(headEntry) ? null : headEntry.getId());
            state.setTail(tail);
            state.setEntries(entries);
            taskEntries.forEach(task -> state.getTaskEntries().set(task.uid, task));
            groupEntries.forEach(group -> state.getGroupEntries().set(group.groupId, group));
        }

        return PipelineGraph.from(state);
    }

    private Entry buildGraph(Pipeline pipelineProto)
            throws InvalidGraphException {
        return buildGraph(pipelineProto, null, null, null, new MutableInt(0), false);
    }

    private Entry buildGraph(
            Pipeline pipelineProto,
            Group parentGroup,
            GroupEntry parentGroupEntry,
            Task finallyTask,
            MutableInt chainTaskSize,
            boolean parentIsPrecededByAnEmptyGroup)
            throws InvalidGraphException {

        Entry head = null;
        Entry current = null;
        boolean isPrecededByAnEmptyGroup = parentIsPrecededByAnEmptyGroup;

        for (PipelineEntry entry : pipelineProto.getEntriesList()) {
            if (!Objects.isNull(finallyTask)) {
                throw new InvalidGraphException("Only one finally task is allowed per pipeline and it must be the last task");
            }

            Entry next = null;

            if (entry.hasTaskEntry()) {
                var taskEntry = entry.getTaskEntry();
                next = Task.of(taskEntry.getUid(), taskEntry.getIsExceptionally(), taskEntry.getIsFinally(), taskEntry.getIsWait());
                next.setPrecededByAnEmptyGroup(isPrecededByAnEmptyGroup);

                if (entries.getItems().containsKey(next.getId())) {
                    throw new InvalidGraphException(MessageFormat.format("Duplicate task uid {0}", next.getId()));
                }

                var task = (Task) next;
                if (task.isFinally()) {
                    finallyTask = task;
                }

                entries.getItems().put(next.getId(), next);
                taskEntries.add(TaskEntryBuilder.fromProto(taskEntry));
                chainTaskSize.increment();
                isPrecededByAnEmptyGroup = false;

            } else if (entry.hasGroupEntry()) {
                var groupEntryProto = entry.getGroupEntry();
                var groupEntry = GroupEntryBuilder.fromProto(groupEntryProto);
                next = Group.of(groupEntryProto.getGroupId(), groupEntryProto.getMaxParallelism(), groupEntryProto.getIsWait());
                next.setPrecededByAnEmptyGroup(isPrecededByAnEmptyGroup);

                if (entries.getItems().containsKey(next.getId())) {
                    throw new InvalidGraphException(MessageFormat.format("Duplicate group id {0}", next.getId()));
                }

                entries.getItems().put(next.getId(), next);
                groupEntries.add(groupEntry);

                var group = (Group) next;

                for (Pipeline pipelineInGroupProto : groupEntryProto.getGroupList()) {
                    var thisChainTaskSize = new MutableInt(0);
                    group.addEntry(this.buildGraph(pipelineInGroupProto, group, groupEntry, finallyTask, thisChainTaskSize, isPrecededByAnEmptyGroup));
                    chainTaskSize.add(thisChainTaskSize);
                }

                isPrecededByAnEmptyGroup = isEmpty(group);
            }

            if (!Objects.isNull(next)) {

                next.setParentGroup(parentGroup);

                if (head == null) {
                    head = next;
                } else {
                    current.setNext(next);
                    next.setPrevious(current);

                    if (current instanceof Task && ((Task) current).isExceptionally()) {
                        // if we have [] -> ex -> t2 then ex will be skipped and t2 needs to receive an empty array
                        // propagate the empty group flag to allow this to happen
                        next.setPrecededByAnEmptyGroup(current.isPrecededByAnEmptyGroup());
                    }
                }

                next.setChainHead(head);
                current = next;

                if (Objects.isNull(parentGroup)) {
                    // keep track of tail node in the main chain
                    tail = current.getId();
                }
            }
        }

        // check for valid graph structure
        if (head instanceof Task) {
            var task = (Task) head;

            if (task.isExceptionally()) {
                throw new InvalidGraphException("Chains cannot begin with an exceptionally task");
            }

            if (task.isFinally()) {
                throw new InvalidGraphException("Chains cannot begin with a finally task");
            }
        }

        // decrement remaining count for any groups containing empty nested groups
        if (!isNull(parentGroupEntry) && chainTaskSize.getValue() == 0) {
            parentGroupEntry.remaining = Math.max(0, parentGroupEntry.remaining - 1);
        }

        return head;
    }

    private boolean isEmpty(Group group) {
        for (var item: group.getItems()) {
            while (!Objects.isNull(item)) {
                boolean isEmpty = true;

                if (item instanceof Task) {
                    // exceptionally tasks count as empty because [[] -> ex] is effectively an empty group
                    isEmpty = ((Task) item).isExceptionally();
                }
                else if (item instanceof Group) {
                    isEmpty = isEmpty((Group) item);
                }

                if (!isEmpty) {
                    return false;
                }

                item = item.getNext();
            }
        }
        return true;
    }
}
