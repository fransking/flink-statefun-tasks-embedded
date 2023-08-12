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

import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.sbbsystems.statefun.tasks.graph.GraphTestUtils.fromTemplate;
import static org.assertj.core.api.Assertions.assertThat;

public final class PipelineGraphTests {

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
    public void returns_initial_task_from_a_chain() {

        var template = List.of("a", "b", "c");
        var graph = fromTemplate(template);

        var skippedTasks = new LinkedList<Task>();
        var initialTasks = graph.getInitialTasks(graph.getHead(), skippedTasks).collect(Collectors.toList());

        assertThat(skippedTasks).isEmpty();
        assertThat(initialTasks).hasSize(1);
        assertThat(initialTasks.get(0).getId()).isEqualTo("a");
    }

    @Test
    public void returns_initial_tasks_from_a_group() {

        var group = List.of(
                List.of("a", "b", "c"),
                List.of("d", "e", "f")
        );

        var template = List.of(group);
        var graph = fromTemplate(template);

        var skippedTasks = new LinkedList<Task>();
        var initialTasks = graph.getInitialTasks(graph.getHead(), skippedTasks).collect(Collectors.toList());

        assertThat(skippedTasks).isEmpty();
        assertThat(initialTasks).hasSize(2);
        assertThat(initialTasks.get(0).getId()).isEqualTo("a");
        assertThat(initialTasks.get(1).getId()).isEqualTo("d");
    }

    @Test
    public void returns_initial_tasks_from_a_large_group() {

        var group = new LinkedList<List<String>>();
        for (var i = 0; i < 500000; i++) {
            group.add(List.of("" + i));
        }

        var template = List.of(group);
        var graph = fromTemplate(template);

        var skippedTasks = new LinkedList<Task>();
        var initialTasks = graph.getInitialTasks(graph.getHead(), skippedTasks).collect(Collectors.toList());

        assertThat(skippedTasks).isEmpty();
        assertThat(initialTasks).hasSize(500000);
    }

    @Test
    public void returns_initial_tasks_from_a_group_of_groups() {

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

        var skippedTasks = new LinkedList<Task>();
        var initialTasks = graph.getInitialTasks(graph.getHead(), skippedTasks).collect(Collectors.toList());

        assertThat(skippedTasks).isEmpty();
        assertThat(initialTasks).hasSize(2);

        var group1InitialTasks = graph.getInitialTasks(initialTasks.get(0), skippedTasks).collect(Collectors.toList());
        var group2InitialTasks = graph.getInitialTasks(initialTasks.get(1), skippedTasks).collect(Collectors.toList());

        assertThat(skippedTasks).isEmpty();
        assertThat(group1InitialTasks.get(0).getId()).isEqualTo("a");
        assertThat(group2InitialTasks.get(0).getId()).isEqualTo("d");
    }

    @Test
    public void returns_initial_tasks_from_a_nested_group() {

        var nested = List.of(
                List.of("a", "b", "c"),
                List.of("d", "e", "f")
        );

        var group = List.of(
                List.of(nested, "g")
        );

        var template = List.of(group);

        var graph = fromTemplate(template);
        var skippedTasks = new LinkedList<Task>();
        var initialTasks = graph.getInitialTasks(graph.getHead(), skippedTasks).collect(Collectors.toList());

        assertThat(skippedTasks).isEmpty();
        assertThat(initialTasks).hasSize(2); // nested group
        assertThat(initialTasks.get(0).getId()).isEqualTo("a");
        assertThat(initialTasks.get(1).getId()).isEqualTo("d");
    }

    @Test
    public void returns_initial_tasks_after_an_empty_group() {

        var emptyGroup = List.of();
        var template = List.of(emptyGroup, emptyGroup, "a", "b", "c");

        var graph = fromTemplate(template);
        var skippedTasks = new LinkedList<Task>();
        var initialTasks = graph.getInitialTasks(graph.getHead(), skippedTasks).collect(Collectors.toList());

        assertThat(skippedTasks).isEmpty();
        assertThat(initialTasks).hasSize(1);
        assertThat(initialTasks.get(0).getId()).isEqualTo("a");
    }

    @Test
    public void can_return_all_tasks() {
        var template = List.of("a", "b", "c");
        var graph = fromTemplate(template);

        var taskIds = StreamSupport.stream(graph.getTasks().spliterator(), false).map(Entry::getId);
        assertThat(taskIds.collect(Collectors.toList())).isEqualTo(List.of("a", "b", "c"));
    }

    @Test
    public void can_return_all_tasks_including_from_groups() {
        var group = List.of(
                List.of("1", "2", "3")
        );

        var template = List.of("a", "b", group, "c");
        var graph = fromTemplate(template);

        var taskIds = StreamSupport.stream(graph.getTasks().spliterator(), false).map(Entry::getId);
        assertThat(taskIds.collect(Collectors.toList())).isEqualTo(List.of("a", "b", "1", "2", "3", "c"));
    }

    @Test
    public void can_return_all_tasks_including_from_nested_groups() {
        var nested = List.of(
                List.of("x", "y", "z")
        );

        var group2 = List.of(
                List.of("q")
        );

        var group = List.of(
                List.of("1", "2", nested, "3")
        );

        var template = List.of("a", "b", group, "c", group2);
        var graph = fromTemplate(template);

        var taskIds = StreamSupport.stream(graph.getTasks().spliterator(), false).map(Entry::getId);
        assertThat(taskIds.collect(Collectors.toList())).isEqualTo(List.of("a", "b", "1", "2", "x", "y", "z", "3", "c", "q"));
    }

    @Test
    public void can_return_all_tasks_including_from_empty_groups() {
        var emptyGroup = List.of(
        );

        var group = List.of(
                List.of("1", "2", "3")
        );

        var template = List.of("a", "b", emptyGroup, emptyGroup, group, "c");
        var graph = fromTemplate(template);

        var taskIds = StreamSupport.stream(graph.getTasks().spliterator(), false).map(Entry::getId);
        assertThat(taskIds.collect(Collectors.toList())).isEqualTo(List.of("a", "b", "1", "2", "3", "c"));
    }

    @Test
    public void can_return_all_tasks_including_from_nested_empty_groups() {
        var emptyGroup = List.of(
        );

        var group = List.of(
                List.of(emptyGroup)
        );

        var template = List.of("a", "b", group, "c");
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
    public void steps_through_chain_of_tasks_including_groups_correctly() {

        var grp = List.of(
                List.of("x", "y", "z")
        );

        var template = List.of("a", grp, "c");

        var graph = fromTemplate(template);

        // start at a
        var head = graph.getTask("a");
        var group = Objects.requireNonNull(graph.getTask("x").getParentGroup());

        // group is next
        graph.markComplete(head);
        var g = graph.getNextEntry(head);
        assertThat(g).isEqualTo(group);
        assertThat(graph.getGroupEntry(g.getId()).remaining).isEqualTo(1);

        // get initial tasks of group
        var skippedTasks = new LinkedList<Task>();
        var initial = graph.getInitialTasks(g, skippedTasks).collect(Collectors.toList());

        // next is x
        var x = initial.get(0);
        assertThat(x).isEqualTo(graph.getTask("x"));

        // next is y
        graph.markComplete(x);
        var y = graph.getNextEntry(graph.getTask("x"));
        assertThat(y).isEqualTo(graph.getTask("y"));

        // next is z
        graph.markComplete(y);
        var z = graph.getNextEntry(graph.getTask("y"));
        assertThat(z).isEqualTo(graph.getTask("z"));

        // next is grp which is now complete
        graph.markComplete(z);
        g = graph.getNextEntry(z);
        assertThat(g).isEqualTo(group);
        assertThat(graph.getGroupEntry(g.getId()).remaining).isEqualTo(0);

        // next is c
        var c = graph.getNextEntry(g);
        assertThat(c).isEqualTo(graph.getTask("c"));

        // next is null
        graph.markComplete(c);
        assertThat(graph.getNextEntry(c)).isNull();
    }

    @Test
    public void steps_through_chain_of_tasks_including_nested_groups_correctly() {

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

        // get initial tasks of nested group
        var skippedTasks = new LinkedList<Task>();
        var initial = graph.getInitialTasks(g, skippedTasks).collect(Collectors.toList());

        // next is x
        var x = initial.get(0);
        assertThat(x).isEqualTo(graph.getTask("x"));

        // next is y
        graph.markComplete(x);
        var y = graph.getNextEntry(graph.getTask("x"));
        assertThat(y).isEqualTo(graph.getTask("y"));

        // next is z
        graph.markComplete(y);
        var z = graph.getNextEntry(graph.getTask("y"));
        assertThat(z).isEqualTo(graph.getTask("z"));

        // next is nestedGroup which is now complete
        graph.markComplete(z);
        g = graph.getNextEntry(z);
        assertThat(graph.getGroupEntry(g.getId()).remaining).isEqualTo(0);
        assertThat(g).isEqualTo(nestedGroup);

        // next is group which is now complete
        g = graph.getNextEntry(g);
        assertThat(g).isEqualTo(group);

        // next is c
        var c = graph.getNextEntry(g);
        assertThat(c).isEqualTo(graph.getTask("c"));

        // next is null
        graph.markComplete(c);
        assertThat(graph.getNextEntry(c)).isNull();
    }

    @Test
    public void steps_through_nested_empty_groups_correctly() {
        var emptyGroup = List.of();

        var group = List.of(
                List.of(emptyGroup, "a")
        );

        var template = List.of(group, "b");
        var graph = fromTemplate(template);
        assertThat(graph.getHead()).isNotNull();

        var skippedTasks = new LinkedList<Task>();
        var initialTasks = graph.getInitialTasks(graph.getHead(), skippedTasks).collect(Collectors.toUnmodifiableList());
        assertThat(initialTasks).containsExactly(graph.getTask("a"));

        var previousTask = graph.getTask("a").getPrevious();
        assertThat(previousTask).isNotNull();
        assertThat(previousTask.isEmpty()).isTrue();
    }

    @Test
    public void steps_through_skipped_tasks_correctly() {
        var template = List.of("a", "ex1", "ex2", "b");
        var graph = fromTemplate(template);
        assertThat(graph.getHead()).isNotNull();

        graph.markComplete(graph.getHead());

        var skippedTasks = new LinkedList<Task>();
        var initialTasks = graph.getInitialTasks(graph.getHead().getNext(), skippedTasks).collect(Collectors.toUnmodifiableList());
        assertThat(skippedTasks).contains(graph.getTask("ex1"));
        assertThat(skippedTasks).contains(graph.getTask("ex2"));
        assertThat(initialTasks).containsExactly(graph.getTask("b"));
    }

    @Test
    public void steps_through_skipped_tasks_correctly_when_exceptionally() {
        var group = List.of(
                List.of("x", "y", "z"),
                List.of("1", "2", "3")
        );

        var template = List.of("a", group, "ex1");
        var graph = fromTemplate(template);
        assertThat(graph.getHead()).isNotNull();

        var skippedTasks = new LinkedList<Task>();
        var initialTasks = graph.getInitialTasks(graph.getHead(), skippedTasks, true).collect(Collectors.toUnmodifiableList());
        assertThat(skippedTasks).contains(graph.getTask("a"));
        assertThat(skippedTasks).contains(graph.getTask("x"));
        assertThat(skippedTasks).contains(graph.getTask("y"));
        assertThat(skippedTasks).contains(graph.getTask("z"));
        assertThat(skippedTasks).contains(graph.getTask("1"));
        assertThat(skippedTasks).contains(graph.getTask("2"));
        assertThat(skippedTasks).contains(graph.getTask("3"));
        assertThat(initialTasks).isEmpty();
    }
}
