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
import com.sbbsystems.statefun.tasks.generated.GroupEntry;
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.PipelineEntry;
import com.sbbsystems.statefun.tasks.generated.TaskEntry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.sbbsystems.statefun.tasks.graph.GraphTestUtils.buildPipelineFromTemplate;
import static java.util.Objects.requireNonNull;
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

        var pipeline = buildPipelineFromTemplate(template);
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
        var pipeline = buildPipelineFromTemplate(template);

        // empty initial state
        var state = PipelineFunctionState.newInstance();

        // initial graph from protobuf
        var builder = PipelineGraphBuilder
                .from(state)
                .fromProto(pipeline);

        PipelineGraph graph = builder.build();

        assertThat(graph.getTasks()).hasSize(8);

        // update state
        graph.saveState();

        var head = graph.getHead();
        var tail = graph.getTail();

        assertThat(head).isNotNull();
        assertThat(head.getId()).isEqualTo("1");
        assertThat(tail).isNotNull();
        assertThat(tail.getId()).isEqualTo("2");
        assertThat(state.getEntries().getItems()).hasSize(9);
        assertThat(state.getTaskEntries().entries()).hasSize(8);
        assertThat(state.getGroupEntries().entries()).hasSize(1);

        var task = graph.getTask("a");
        assertThat(task.getParentGroup()).isNotNull();

        var groupEntry = state.getGroupEntries().get(task.getParentGroup().getId());
        assertThat(groupEntry.size).isEqualTo(2);
        assertThat(groupEntry.remaining).isEqualTo(2);

        // new graph from previous state
        var newBuilder = PipelineGraphBuilder.from(state);

        PipelineGraph newGraph = newBuilder.build();

        assertThat(newGraph.getTasks()).hasSize(8);
    }

    @Test
    public void builder_throws_exceptions_when_it_has_duplicate_tasks() {
        var template = List.of("1", "2", "2");
        var pipeline = buildPipelineFromTemplate(template);
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

    @Test
    public void builder_throws_exceptions_if_graph_starts_with_exceptionally() {
        var entry = PipelineEntry.newBuilder().setTaskEntry(TaskEntry.newBuilder().setTaskId("1").setIsExceptionally(true));
        var pipeline = Pipeline.newBuilder().addEntries(entry);
        var builder = PipelineGraphBuilder.newInstance().fromProto(pipeline.build());

        assertThrows(InvalidGraphException.class, builder::build);
    }

    @Test
    public void builder_throws_exceptions_if_graph_has_more_than_one_finally() {
        var entryOne = PipelineEntry.newBuilder().setTaskEntry(TaskEntry.newBuilder().setTaskId("1"));
        var finallyOne = PipelineEntry.newBuilder().setTaskEntry(TaskEntry.newBuilder().setTaskId("2").setIsFinally(true));
        var finallyTwo = PipelineEntry.newBuilder().setTaskEntry(TaskEntry.newBuilder().setTaskId("3").setIsFinally(true));
        var pipeline = Pipeline.newBuilder().addEntries(entryOne).addEntries(finallyOne).addEntries(finallyTwo);
        var builder = PipelineGraphBuilder.newInstance().fromProto(pipeline.build());

        assertThrows(InvalidGraphException.class, builder::build);
    }

    @Test
    public void sets_chain_head_for_each_element_in_chain()
            throws InvalidGraphException {

        var group = List.of(
                List.of("a", "b", "c")
        );
        var template = List.of("x", group, "y");
        var pipeline = buildPipelineFromTemplate(template);

        var graph = PipelineGraphBuilder
                .from(PipelineFunctionState.newInstance())
                .fromProto(pipeline)
                .build();

        for (var entry : List.of(graph.getTask("a"), graph.getTask("b"), graph.getTask("c"))) {
            assertThat(entry.getChainHead()).isEqualTo(graph.getTask("a"));
        }
        for (var entry : List.of(graph.getTask("x"), requireNonNull(graph.getTask("a").getParentGroup()), graph.getTask("y"))) {
            assertThat(entry.getChainHead()).isEqualTo(graph.getTask("x"));
        }
    }

    @Test
    public void calculates_correct_remaining_count_for_a_group()
            throws InvalidGraphException {

        var group = List.of(
                List.of("a"),
                List.of("b"),
                List.of("c")
        );

        var template = List.of(group);

        var pipeline = buildPipelineFromTemplate(template);

        var graph = PipelineGraphBuilder
                .from(PipelineFunctionState.newInstance())
                .fromProto(pipeline)
                .build();

        var grp = (Group) graph.getHead();
        assertThat(grp).isNotNull();
        assertThat(graph.getGroupEntry(grp.getId()).remaining).isEqualTo(3);
    }

    @Test
    public void calculates_correct_remaining_count_for_groups_that_are_empty_empty()
            throws InvalidGraphException {

        var emptyGroup = List.of();

        var group = List.of(
                List.of(emptyGroup),  // empty chain
                List.of(emptyGroup, emptyGroup),  // empty chain
                List.of("a"),   // not empty
                List.of(emptyGroup, "b", emptyGroup), // not empty
                List.of(emptyGroup, "c") // not empty for total of 3 items
        );

        var template = List.of(group);

        var pipeline = buildPipelineFromTemplate(template);

        var graph = PipelineGraphBuilder
                .from(PipelineFunctionState.newInstance())
                .fromProto(pipeline)
                .build();

        var grp = (Group) graph.getHead();
        assertThat(grp).isNotNull();
        assertThat(graph.getGroupEntry(grp.getId()).remaining).isEqualTo(3);
    }

    @Test
    public void calculates_correct_remaining_count_for_groups_that_are_partially_empty()
            throws InvalidGraphException {

        var emptyGroup = List.of();

        var nonEmptyGroup = List.of(
                List.of("a")
        );

        var group = List.of(
                List.of(nonEmptyGroup),
                List.of(emptyGroup, emptyGroup, "b")
        );

        var template = List.of(group);
        var pipeline = buildPipelineFromTemplate(template);

        var graph = PipelineGraphBuilder
                .from(PipelineFunctionState.newInstance())
                .fromProto(pipeline)
                .build();

        var grp = (Group) graph.getHead();
        assertThat(grp).isNotNull();
        assertThat(graph.getGroupEntry(grp.getId()).remaining).isEqualTo(2);
    }

    @Test
    public void marks_tasks_that_are_preceded_by_empty_groups()
            throws InvalidGraphException {

        var emptyGroup = List.of();
        var nestedEmptyGroup = List.of(List.of(emptyGroup));

        var group = List.of(
                List.of("a", emptyGroup, "b", "c", emptyGroup, "d", nestedEmptyGroup, "e"),
                List.of("x", "y"),
                List.of("1", emptyGroup, "ex2", "ex3", "4", "5")
        );

        var template = List.of(emptyGroup, group);
        var pipeline = buildPipelineFromTemplate(template);

        var graph = PipelineGraphBuilder
                .from(PipelineFunctionState.newInstance())
                .fromProto(pipeline)
                .build();

        var groupOfa = graph.getTask("a").getParentGroup();

        assertThat(groupOfa).isNotNull();
        assertThat(graph.getGroup(groupOfa.getId()).isPrecededByAnEmptyGroup()).isTrue();

        assertThat(graph.getTask("a").isPrecededByAnEmptyGroup()).isTrue();
        assertThat(graph.getTask("b").isPrecededByAnEmptyGroup()).isTrue();
        assertThat(graph.getTask("c").isPrecededByAnEmptyGroup()).isFalse();
        assertThat(graph.getTask("d").isPrecededByAnEmptyGroup()).isTrue();
        assertThat(graph.getTask("e").isPrecededByAnEmptyGroup()).isTrue();

        assertThat(graph.getTask("x").isPrecededByAnEmptyGroup()).isTrue();
        assertThat(graph.getTask("y").isPrecededByAnEmptyGroup()).isFalse();

        assertThat(graph.getTask("1").isPrecededByAnEmptyGroup()).isTrue();
        assertThat(graph.getTask("ex2").isPrecededByAnEmptyGroup()).isTrue();
        assertThat(graph.getTask("ex3").isPrecededByAnEmptyGroup()).isTrue();
        assertThat(graph.getTask("4").isPrecededByAnEmptyGroup()).isTrue();
        assertThat(graph.getTask("5").isPrecededByAnEmptyGroup()).isFalse();
    }

    @Test
    public void marks_tasks_that_are_preceded_by_groups_which_have_exceptionally_in_them_but_are_effectively_empty()
            throws InvalidGraphException {

        var emptyGroup = List.of();

        var group = List.of(
                List.of(emptyGroup, "ex2"), // empty followed by exceptionally and nothing else is equivalent to empty
                List.of(emptyGroup, "ex3") // empty followed by exceptionally and nothing else is equivalent to empty
        );

        var template = List.of(group, "a");
        var pipeline = buildPipelineFromTemplate(template);

        var graph = PipelineGraphBuilder
                .from(PipelineFunctionState.newInstance())
                .fromProto(pipeline)
                .build();

        assertThat(graph.getTask("ex2").isPrecededByAnEmptyGroup()).isTrue();
        assertThat(graph.getTask("a").isPrecededByAnEmptyGroup()).isTrue();
    }

    @Test
    public void marks_tasks_that_are_preceded_by_nested_empty_groups_correctly()
            throws InvalidGraphException {

        var emptyGroup = List.of();

        var group = List.of(
                List.of(emptyGroup, "a")
        );

        var template = List.of(group, "b");
        var pipeline = buildPipelineFromTemplate(template);

        var graph = PipelineGraphBuilder
                .from(PipelineFunctionState.newInstance())
                .fromProto(pipeline)
                .build();

        assertThat(graph.getTask("a").isPrecededByAnEmptyGroup()).isTrue();
        assertThat(graph.getTask("b").isPrecededByAnEmptyGroup()).isFalse();
    }
}
