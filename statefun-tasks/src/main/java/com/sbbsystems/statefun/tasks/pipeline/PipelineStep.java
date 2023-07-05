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

import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.graph.Entry;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.graph.Task;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

public class PipelineStep {
    private final Entry entry;
    private final List<Task> tasksToCall;
    private final List<Task> skippedTasks;

    public static PipelineStep fromHead(PipelineGraph graph)
            throws StatefunTasksException {

        var entry = graph.getHead();

        if (isNull(entry)) {
            throw new StatefunTasksException("Cannot run an empty pipeline");
        }

        var skippedTasks = new LinkedList<Task>();
        var initialTasks = graph.getInitialTasks(entry, skippedTasks).collect(Collectors.toUnmodifiableList());

        // we may have no initial tasks in the case of empty groups so continue to iterate over these
        while (initialTasks.isEmpty() && !isNull(entry)) {
            graph.markComplete(entry);
            entry = graph.getNextEntry(entry);
            initialTasks = graph.getInitialTasks(entry, skippedTasks).collect(Collectors.toUnmodifiableList());
        }

        return new PipelineStep(entry, initialTasks, skippedTasks);
    }

    public static PipelineStep next(PipelineGraph graph, Entry entry, boolean exceptionally) {
        var parentGroup = entry.getParentGroup();
        var skippedTasks = new LinkedList<Task>();
        var nextEntry = graph.getNextEntry(entry);

        var continuationTasks = nextEntry == parentGroup
                ? List.<Task>of()
                : graph.getInitialTasks(nextEntry, skippedTasks, exceptionally).collect(Collectors.toUnmodifiableList());

        // skip over empty & exceptionally tasks as required
        while (nextEntry != parentGroup && continuationTasks.isEmpty()) {
            graph.markComplete(nextEntry);
            nextEntry = graph.getNextEntry(nextEntry);
            continuationTasks = graph.getInitialTasks(nextEntry, skippedTasks, exceptionally).collect(Collectors.toUnmodifiableList());
        }

        return new PipelineStep(nextEntry, continuationTasks, skippedTasks);
    }

    private PipelineStep(Entry entry, List<Task> tasksToCall, List<Task> skippedTasks) {
        this.entry = entry;
        this.tasksToCall = tasksToCall;
        this.skippedTasks = skippedTasks;
    }

    public Entry getEntry() {
        return entry;
    }

    public List<Task> getTasksToCall() {
        return tasksToCall;
    }

    public List<Task> getSkippedTasks() {
        return skippedTasks;
    }

    public boolean hasNoTasksToCall() {
        return tasksToCall.isEmpty();
    }

    public int numTasksToCall() {
        return tasksToCall.size();
    }
}
