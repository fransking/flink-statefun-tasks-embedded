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

import com.sbbsystems.statefun.tasks.generated.GroupEntry;
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.PipelineEntry;
import com.sbbsystems.statefun.tasks.generated.TaskEntry;
import com.sbbsystems.statefun.tasks.util.Id;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public final class PipelineGraphBuilderTests {

    @NotNull
    public static Pipeline buildSingleChainPipeline(int length) {
        var pipeline = Pipeline.newBuilder();

        for (int i = 1; i <= length; i++) {
            var taskEntry = TaskEntry.newBuilder()
                    .setTaskId(String.valueOf(i))
                    .setUid(String.valueOf(i));

            var pipelineEntry = PipelineEntry.newBuilder()
                    .setTaskEntry(taskEntry);

            pipeline.addEntries(pipelineEntry);
        }

        return pipeline.build();
    }

    public static Pipeline buildPipelineFromTemplate(List<?> template) {
        var pipeline = Pipeline.newBuilder();

        template.forEach(item -> {
            if (item instanceof List<?>) {
                var groupEntry = GroupEntry.newBuilder()
                        .setGroupId(String.valueOf(UUID.randomUUID()));

                ((List<?>) item).forEach(groupItem -> {
                    if (groupItem instanceof List<?>) {
                        var group = PipelineGraphBuilderTests.buildPipelineFromTemplate((List<?>) groupItem);
                        groupEntry.addGroup(group);
                    }
                });

                pipeline.addEntries(PipelineEntry.newBuilder().setGroupEntry(groupEntry));
            }
            else {
                var taskEntry = TaskEntry.newBuilder()
                        .setTaskId(String.valueOf(item))
                        .setUid(String.valueOf(item))
                        .build();

                pipeline.addEntries(PipelineEntry.newBuilder().setTaskEntry(taskEntry));
            }
        });

        return pipeline.build();
    }

    @Test
    public void graph_contains_all_task_entries_given_a_pipeline()
            throws InvalidGraphException {
        var pipeline = PipelineGraphBuilderTests.buildSingleChainPipeline(10);
        var builder = PipelineGraphBuilder.newInstance().fromProto(pipeline);
        PipelineGraph graph = builder.build();

        assertThat(graph.getTasks()).hasSize(10);

        for (Entry entry : graph.getTasks()) {
            assertThat(graph.getTaskEntry(entry.getId())).isNotNull();
        }
    }

    @Test
    public void graph_contains_all_task_entries_given_a_pipeline_with_groups()
            throws InvalidGraphException {
        var nestedGroup = List.of(
                List.of("a", "b", "c"),
                List.of("d", "e", "f")
        );

        var group = List.of(
                List.of("x", nestedGroup, "y")
        );

        var template = List.of("1", group, "2");

        var pipeline = PipelineGraphBuilderTests.buildPipelineFromTemplate(template);
        var builder = PipelineGraphBuilder.newInstance().fromProto(pipeline);
        PipelineGraph graph = builder.build();

        assertThat(graph.getTasks()).hasSize(10);

        var taskIds = List.of("1", "x", "a", "b", "c", "d", "e", "f", "y", "2");
        var entryTaskIds = StreamSupport.stream(graph.getTasks().spliterator(), false).map(Entry::getId);
        assertThat(entryTaskIds.collect(Collectors.toList())).isEqualTo(taskIds);

        taskIds.forEach(taskId -> assertThat(graph.getTaskEntry(taskId)).isNotNull());

        for (Entry entry : graph.getTasks()) {
            assertThat(graph.getTaskEntry(entry.getId())).isNotNull();
        }
    }

    @Test
    public void graph_can_be_recreated_from_state()
            throws InvalidGraphException {
        var group = List.of(
                List.of("x", "y", "z"),
                List.of("a", "b", "c")
        );

        var template = List.of("1", group, "2");
        var pipeline = PipelineGraphBuilderTests.buildPipelineFromTemplate(template);

        // empty initial state
        var tasks = new HashMap<String, Task>();
        var taskEntries = PersistedTable.of(Id.generate(), String.class, com.sbbsystems.statefun.tasks.types.TaskEntry.class);
        var groupEntries = PersistedTable.of(Id.generate(), String.class, com.sbbsystems.statefun.tasks.types.GroupEntry.class);
        Entry head = null;
        Entry tail = null;

        // initial graph from protobuf
        var builder = PipelineGraphBuilder.newInstance()
                .withHead(head)
                .withTail(tail)
                .withTasks(tasks)
                .withTaskEntries(taskEntries)
                .withGroupEntries(groupEntries)
                .fromProto(pipeline);

        PipelineGraph graph = builder.build();

        assertThat(graph.getTasks()).hasSize(8);

        // updated state
        head = graph.getHead();
        tail = graph.getTail();

        assertThat(head).isNotNull();
        assertThat(head.getId()).isEqualTo("1");
        assertThat(tail).isNotNull();
        assertThat(tail.getId()).isEqualTo("2");
        assertThat(tasks).hasSize(8);
        assertThat(taskEntries.entries()).hasSize(8);
        assertThat(groupEntries.entries()).hasSize(1);

        var task = graph.getTask("a");
        assertThat(task.getParentGroup()).isNotNull();

        var groupEntry = groupEntries.get(task.getParentGroup().getId());
        assertThat(groupEntry.size).isEqualTo(2);
        assertThat(groupEntry.remaining).isEqualTo(2);

        // new graph from previous state
        var newBuilder = PipelineGraphBuilder.newInstance()
                .withHead(head)
                .withTail(tail)
                .withTasks(tasks)
                .withTaskEntries(taskEntries)
                .withGroupEntries(groupEntries);

        PipelineGraph newGraph = newBuilder.build();

        assertThat(newGraph.getTasks()).hasSize(8);
    }

    @Test
    public void builder_throws_exceptions_when_it_has_duplicate_tasks() {
        var template = List.of("1", "2", "2");
        var pipeline = PipelineGraphBuilderTests.buildPipelineFromTemplate(template);
        var builder = PipelineGraphBuilder.newInstance().fromProto(pipeline);

        assertThrows(InvalidGraphException.class, builder::build);
    }

    @Test
    public void builder_throws_exceptions_when_it_has_duplicate_groups() {
        var entryOne = PipelineEntry.newBuilder().setGroupEntry(GroupEntry.newBuilder().setGroupId("1"));
        var entryTwp = PipelineEntry.newBuilder().setGroupEntry(GroupEntry.newBuilder().setGroupId("1"));
        var pipeline = Pipeline.newBuilder().addEntries(entryOne).addEntries(entryTwp);
        var builder = PipelineGraphBuilder.newInstance().fromProto(pipeline.build());

        assertThrows(InvalidGraphException.class, builder::build);
    }
}
