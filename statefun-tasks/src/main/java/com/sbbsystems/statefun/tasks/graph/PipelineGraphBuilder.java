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
import com.sbbsystems.statefun.tasks.types.GroupEntryBuilder;
import com.sbbsystems.statefun.tasks.types.TaskEntryBuilder;
import org.jetbrains.annotations.NotNull;

import java.text.MessageFormat;
import java.util.Objects;

public final class PipelineGraphBuilder {

    private final PipelineFunctionState state;
    private Pipeline pipelineProto;

    private PipelineGraphBuilder(PipelineFunctionState state) {
        this.state = state;
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
        }

        return PipelineGraph.from(state);
    }

    private Entry buildGraph(Pipeline pipelineProto)
            throws InvalidGraphException {
        return buildGraph(pipelineProto, null);
    }

    private Entry buildGraph(Pipeline pipelineProto, Group parentGroup)
            throws InvalidGraphException {

        Entry head = null;
        Entry current = null;

        for (PipelineEntry entry: pipelineProto.getEntriesList()) {
            Entry next = null;

            if (entry.hasTaskEntry()) {
                var taskEntry = entry.getTaskEntry();
                next = Task.of(taskEntry.getUid(), taskEntry.getIsExceptionally());

                if (state.getEntries().getItems().containsKey(next.getId())) {
                    throw new InvalidGraphException(MessageFormat.format("Duplicate task uid {0}", next.getId()));
                }

                state.getEntries().getItems().put(next.getId(), next);
                state.getTaskEntries().set(next.getId(), TaskEntryBuilder.fromProto(taskEntry));

            } else if (entry.hasGroupEntry()) {
                var groupEntry = entry.getGroupEntry();
                next = Group.of(groupEntry.getGroupId());

                if (state.getEntries().getItems().containsKey(next.getId())) {
                    throw new InvalidGraphException(MessageFormat.format("Duplicate group id {0}", next.getId()));
                }

                state.getEntries().getItems().put(next.getId(), next);
                state.getGroupEntries().set(next.getId(), GroupEntryBuilder.fromProto(groupEntry));

                var group = (Group) next;
                for (Pipeline pipelineInGroupProto : groupEntry.getGroupList()) {
                    group.addEntry(this.buildGraph(pipelineInGroupProto, group));
                }
            }

            if (!Objects.isNull(next)) {
                next.setParentGroup(parentGroup);

                if (head == null) {
                    head = next;
                } else {
                    current.setNext(next);
                    next.setPrevious(current);
                }

                current = next;

                if (Objects.isNull(parentGroup)) {
                    // keep track of tail node in the main chain
                    state.setTail(current.getId());
                }
            }
        }

        return head;
    }
}
