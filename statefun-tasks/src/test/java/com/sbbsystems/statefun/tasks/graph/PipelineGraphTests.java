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

import com.sbbsystems.statefun.tasks.types.GroupEntry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

public final class PipelineGraphTests {

    private PipelineGraph fromTemplate(List<?> template) {
        var pipeline = PipelineGraphBuilderTests.buildPipelineFromTemplate(template);
        var builder = PipelineGraphBuilder.newInstance().fromProto(pipeline);

        try {
            return builder.build();
        } catch (InvalidGraphException e) {
            throw new RuntimeException(e);
        }
    }

    private GroupEntry getGroupContainingTask(String taskId, PipelineGraph graph) {
        var taskA = graph.getTask(taskId);
        var groupId = Objects.requireNonNull(taskA.getParentGroup()).getId();
        return graph.getGroupEntry(groupId);
    }

    @Test
    public void can_fetch_tasks_given_id() {
        var template = List.of("a", "b", "c");
        var graph = fromTemplate(template);

        var a = graph.getTask("a");
        var b = graph.getTask("b");
        var c = graph.getTask("c");

        assertThat(a).isNotNull();
        assertThat(b).isEqualTo(a.getNext());
        assertThat(c).isEqualTo(b.getNext());
    }

    @Test
    public void can_fetch_group_given_id() {
        var grp = List.of(
                List.of("a", "b", "c")
        );

        var template = List.of("one", grp, "two");
        var graph = fromTemplate(template);

        var group = graph.getTask("a").getParentGroup();
        assertThat(group).isNotNull();

        var groupEntry = graph.getGroupEntry(group.getId());
        assertThat(groupEntry).isNotNull();
    }

    @Test
    public void basic_graph_structure_is_correct() {
        var nestedGroup = List.of(
                List.of("x", "y", "z")
        );

        var group = List.of(
                List.of("a", nestedGroup, "b")
        );

        var template = List.of("one", group, "two");
        var graph = fromTemplate(template);

        var one = graph.getTask("one");
        var a = graph.getTask("a");
        var b = graph.getTask("b");
        var x = graph.getTask("x");
        var y = graph.getTask("y");
        var z = graph.getTask("z");
        var two = graph.getTask("two");

        //parent group hierarchy
        assertThat(one.getParentGroup()).isNull();
        assertThat(two.getParentGroup()).isNull();
        assertThat(a.getParentGroup()).isNotNull();
        assertThat(b.getParentGroup()).isEqualTo(a.getParentGroup());
        assertThat(b.getParentGroup()).isNotNull();
        assertThat(b.getParentGroup().getParentGroup()).isNull();
        assertThat(x.getParentGroup()).isNotNull();
        assertThat(y.getParentGroup()).isEqualTo(x.getParentGroup());
        assertThat(z.getParentGroup()).isEqualTo(x.getParentGroup());
        assertThat(x.getParentGroup().getParentGroup()).isEqualTo(a.getParentGroup());

        //task chain
        assertThat(one.getNext()).isEqualTo(a.getParentGroup());
        assertThat(a.getNext()).isEqualTo(x.getParentGroup());
        assertThat(a.getParentGroup().getNext()).isEqualTo(two);
        assertThat(x.getParentGroup().getNext()).isEqualTo(b);
        assertThat(x.getNext()).isEqualTo(y);
        assertThat(b.getNext()).isNull();
        assertThat(z.getNext()).isNull();
        assertThat(two.getNext()).isNull();
    }

    @Test
    public void returns_initial_task_from_a_chain()
            throws InvalidGraphException {

        var template = List.of("a", "b", "c");
        var graph = fromTemplate(template);
        var initialTasks = graph.getInitialTasks();

        assertThat(initialTasks).hasSize(1);
        assertThat(initialTasks.getTasks().get(0).getId()).isEqualTo("a");
        assertThat(initialTasks.getMaxParallelism()).isEqualTo(0);  // unset
        assertThat(initialTasks.isPredecessorIsEmptyGroup()).isEqualTo(false);
    }

    @Test
    public void returns_initial_tasks_from_a_group()
            throws InvalidGraphException {

        var group = List.of(
                List.of("a", "b", "c"),
                List.of("d", "e", "f")
        );

        var template = List.of(group);
        var graph = fromTemplate(template);
        getGroupContainingTask("a", graph).maxParallelism = 1;  // set max parallelism on group

        var initialTasks = graph.getInitialTasks();

        assertThat(initialTasks).hasSize(2);
        assertThat(initialTasks.getTasks().get(0).getId()).isEqualTo("a");
        assertThat(initialTasks.getTasks().get(1).getId()).isEqualTo("d");
        assertThat(initialTasks.getMaxParallelism()).isEqualTo(1);
        assertThat(initialTasks.isPredecessorIsEmptyGroup()).isEqualTo(false);
    }

    @Test
    public void returns_initial_tasks_from_a_group_of_groups()
            throws InvalidGraphException {

        var groupOne = List.of(
                List.of("a", "b", "c")
        );

        var groupTwo = List.of(
                List.of("d", "e", "f")
        );

        var group = List.of(
                List.of(groupOne),
                List.of(groupTwo)
        );

        var template = List.of(group);
        var graph = fromTemplate(template);
        getGroupContainingTask("a", graph).maxParallelism = 2;  // set max parallelism on group
        getGroupContainingTask("d", graph).maxParallelism = 3;  // set max parallelism on group

        var initialTasks = graph.getInitialTasks();

        assertThat(initialTasks).hasSize(2);
        assertThat(initialTasks.getTasks().get(0).getId()).isEqualTo("a");
        assertThat(initialTasks.getTasks().get(1).getId()).isEqualTo("d");
        assertThat(initialTasks.getMaxParallelism()).isEqualTo(2);
        assertThat(initialTasks.isPredecessorIsEmptyGroup()).isEqualTo(false);
    }

    @Test
    public void returns_initial_tasks_from_a_nested_group()
            throws InvalidGraphException {

        var nested = List.of(
                List.of("a", "b", "c"),
                List.of("d", "e", "f")
        );

        var group = List.of(
                List.of(nested, "g")
        );

        var template = List.of(group);

        var graph = fromTemplate(template);
        var initialTasks = graph.getInitialTasks();

        assertThat(initialTasks).hasSize(2);
        assertThat(initialTasks.getTasks().get(0).getId()).isEqualTo("a");
        assertThat(initialTasks.getTasks().get(1).getId()).isEqualTo("d");
    }

    @Test
    public void returns_initial_tasks_after_an_empty_group()
            throws InvalidGraphException {

        var emptyGroup = List.of();
        var template = List.of(emptyGroup, emptyGroup, "a", "b", "c");

        var graph = fromTemplate(template);
        var initialTasks = graph.getInitialTasks();

        assertThat(initialTasks).hasSize(1);
        assertThat(initialTasks.getTasks().get(0).getId()).isEqualTo("a");
        assertThat(initialTasks.isPredecessorIsEmptyGroup()).isEqualTo(true);
    }

    @Test
    public void can_return_all_tasks() {
        var template = List.of("a", "b", "c");
        var graph = fromTemplate(template);

        var taskIds = StreamSupport.stream(graph.getTasks().spliterator(), false).map(Entry::getId);
        assertThat(taskIds.collect(Collectors.toList())).isEqualTo(List.of("a", "b", "c"));
    }

    @Test
    public void can_return_tasks_from_a_given_point_in_a_chain() {
        var template = List.of("a", "b", "c");
        var graph = fromTemplate(template);

        var b = graph.getTask("b");
        var taskIds = StreamSupport.stream(graph.getTasks(b).spliterator(), false).map(Entry::getId);
        assertThat(taskIds.collect(Collectors.toList())).isEqualTo(List.of("b", "c"));
    }

    @Test
    public void can_return_tasks_from_a_given_point_in_a_sub_chain() {
        var nested = List.of(
                List.of("1", "2", "3")
        );

        var group = List.of(
                List.of("a", "b", "c", nested),
                List.of("d", "e", "f")
        );

        var template = List.of(group);
        var graph = fromTemplate(template);

        var b = graph.getTask("b");
        var taskIds = StreamSupport.stream(graph.getTasks(b).spliterator(), false).map(Entry::getId);
        assertThat(taskIds.collect(Collectors.toList())).isEqualTo(List.of("b", "c", "1", "2", "3"));
    }

    @Test
    public void steps_through_chain_of_tasks_correctly() {
        var template = List.of("a", "b", "c");
        var graph = fromTemplate(template);

        var head = graph.getTask("a");

        graph.markComplete(head);
        var b = graph.getNextEntry(head);
        assertThat(b).isEqualTo(graph.getTask("b"));

        graph.markComplete(b);
        var c = graph.getNextEntry(b);
        assertThat(c).isEqualTo(graph.getTask("c"));

        graph.markComplete(c);
        assertThat(graph.getNextEntry(c)).isNull();
    }

    @Test
    public void steps_through_chain_of_tasks_including_groups_correctly()
        throws InvalidGraphException {

        var grp = List.of(
                List.of("x", "y", "z")
        );

        var template = List.of("a", grp, "c");

        var graph = fromTemplate(template);

        // start at a
        var head = graph.getTask("a");
        var group = Objects.requireNonNull(graph.getTask("x").getParentGroup());
        var groupEntry = graph.getGroupEntry(group.getId());

        // group is next
        graph.markComplete(head);
        var g = graph.getNextEntry(head);
        assertThat(g).isEqualTo(group);

        // get initial tasks of group
        var initial = graph.getInitialTasks(g);

        // next is x
        var x = initial.getTasks().get(0);
        assertThat(x).isEqualTo(graph.getTask("x"));

        // next is y
        graph.markComplete(x);
        var y = graph.getNextEntry(graph.getTask("x"));
        assertThat(y).isEqualTo(graph.getTask("y"));

        // next is z
        graph.markComplete(y);
        var z = graph.getNextEntry(graph.getTask("y"));
        assertThat(z).isEqualTo(graph.getTask("z"));

        // next is c
        graph.markComplete(z);
        groupEntry.remaining--;  // when z completes the group is done
        var c = graph.getNextEntry(z);
        assertThat(c).isEqualTo(graph.getTask("c"));

        // next is null
        graph.markComplete(c);
        assertThat(graph.getNextEntry(c)).isNull();
    }

    @Test
    public void steps_through_chain_of_tasks_including_nested_groups_correctly()
            throws InvalidGraphException {

        var nested = List.of(
                List.of("x", "y", "z")
        );

        var grp = List.of(
                List.of(nested)
        );

        var template = List.of("a", grp, "c");

        var graph = fromTemplate(template);

        // start at a
        var head = graph.getTask("a");
        var nestedGroup = Objects.requireNonNull(graph.getTask("x").getParentGroup());
        var group = Objects.requireNonNull(nestedGroup.getParentGroup());

        // group is next
        graph.markComplete(head);
        var g = graph.getNextEntry(head);
        assertThat(g).isEqualTo(group);

        // get initial tasks of group - jumps to the nested group since grp contains just nested
        var initial = graph.getInitialTasks(g);

        // next is x
        var x = initial.getTasks().get(0);
        assertThat(x).isEqualTo(graph.getTask("x"));

        // next is y
        graph.markComplete(x);
        var y = graph.getNextEntry(graph.getTask("x"));
        assertThat(y).isEqualTo(graph.getTask("y"));

        // next is z
        graph.markComplete(y);
        var z = graph.getNextEntry(graph.getTask("y"));
        assertThat(z).isEqualTo(graph.getTask("z"));

        // next is c
        graph.markComplete(z);
        var c = graph.getNextEntry(z);
        assertThat(c).isEqualTo(graph.getTask("c"));

        // next is null
        graph.markComplete(c);
        assertThat(graph.getNextEntry(c)).isNull();
    }

    @Test
    public void steps_through_chain_of_tasks_skipping_exceptionally_tasks_correctly()
            throws InvalidGraphException {

        var template = List.of("a", "ex1", "ex2", "b");
        var graph = fromTemplate(template);

        // setup exceptionally tasks
        var ex1 = graph.getTask("ex1");
        var ex2 = graph.getTask("ex2");
        ex1.setExceptionally(true);
        ex2.setExceptionally(true);

        // start at a
        var head = graph.getTask("a");

        // a returns TaskResult
        graph.markComplete(head);
        var next = graph.getNextStep(head, false);

        // ex1 & ex2 should be skipped
        assertThat(next.getEntry()).isEqualTo(graph.getTask("b"));
        assertThat(next.getSkippedTasks()).isEqualTo(List.of(graph.getTask("ex1"), graph.getTask("ex2")));
    }

    @Test
    public void steps_through_chain_of_tasks_jumping_to_exceptionally_tasks_correctly()
            throws InvalidGraphException {

        var group = List.of(
                List.of("x", "y", "z")
        );

        var template = List.of("a", group, "c", "ex1", "d");
        var graph = fromTemplate(template);

        // setup exceptionally task
        var ex1 = graph.getTask("ex1");
        ex1.setExceptionally(true);

        // start at a
        var head = graph.getTask("a");

        // a returns TaskException
        graph.markComplete(head);
        var next = graph.getNextStep(head, true);

        // x, y, z & c should be skipped
        assertThat(next.getEntry()).isEqualTo(graph.getTask("ex1"));
        assertThat(next.getSkippedTasks()).isEqualTo(List.of(
                graph.getTask("x"),
                graph.getTask("y"),
                graph.getTask("z"),
                graph.getTask("c")
        ));
    }

    @Test
    public void steps_through_chain_of_tasks_jumping_to_exceptionally_tasks_only_when_group_is_complete()
            throws InvalidGraphException {

        var group = List.of(
                List.of("x", "y", "z"),
                List.of("p")
        );

        var template = List.of(group, "c", "ex1", "d");
        var graph = fromTemplate(template);

        // setup exceptionally task
        var ex1 = graph.getTask("ex1");
        ex1.setExceptionally(true);

        // start at x & p in parallel
        var x = graph.getTask("x");
        var p = graph.getTask("p");
        var g = p.getParentGroup();
        assertThat(g).isNotNull();
        var groupEntry = graph.getGroupEntry(g.getId());

        // x returns TaskException
        graph.markComplete(x);
        var next = graph.getNextStep(x, true);

        // p must complete before we get a next step
        assertThat(groupEntry.remaining).isEqualTo(1);
        assertThat(next.hasEntry()).isFalse();
        assertThat(next.getSkippedTasks()).isEqualTo(List.of(
                graph.getTask("y"),
                graph.getTask("z")
        ));

        // p completes, group aggregates to exception overall
        graph.markComplete(p);
        assertThat(groupEntry.remaining).isEqualTo(0);
        next = graph.getNextStep(g, true);

        // skips to c to ex1
        assertThat(next.getSkippedTasks()).isEqualTo(List.of(
                graph.getTask("c")
        ));

        assertThat(next.hasEntry()).isTrue();
        assertThat(next.getEntry()).isEqualTo(graph.getTask("ex1"));
    }

    @Test
    public void steps_through_chain_of_tasks_jumping_to_exceptionally_tasks_when_group_is_complete()
            throws InvalidGraphException {

        var group = List.of(
                List.of("p"),
                List.of("x", "y", "z")
        );

        var template = List.of(group, "c", "ex1", "d");
        var graph = fromTemplate(template);

        // setup exceptionally task
        var ex1 = graph.getTask("ex1");
        ex1.setExceptionally(true);

        // start at x & p in parallel
        var x = graph.getTask("x");
        var p = graph.getTask("p");
        var g = p.getParentGroup();
        assertThat(g).isNotNull();
        var groupEntry = graph.getGroupEntry(g.getId());

        // p completes
        graph.markComplete(p);

        // x returns TaskException
        graph.markComplete(x);
        var next = graph.getNextStep(x, true);
        assertThat(next.hasEntry()).isTrue();

        assertThat(groupEntry.remaining).isEqualTo(0);
        // skips to x, y, z, & c to ex1
        assertThat(next.getSkippedTasks()).isEqualTo(List.of(
                graph.getTask("y"),
                graph.getTask("z"),
                graph.getTask("c")
        ));

        assertThat(next.hasEntry()).isTrue();
        assertThat(next.getEntry()).isEqualTo(graph.getTask("ex1"));
    }
}
