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

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.sbbsystems.statefun.tasks.types.Task;
import org.apache.flink.statefun.sdk.state.PersistedTable;

import java.util.Iterator;

public final class PipelineGraph implements Graph {
    private final PersistedTable<String, Task> taskLookup;

    private final Chain entries;

    public PipelineGraph(PersistedTable<String, Task> taskLookup, Chain entries) {
        this.taskLookup = taskLookup;
        this.entries = entries;
    }

    public Task getTask(String id) {
        return taskLookup.get(id);
    }

    public Task getTask(Entry entry) {
        return taskLookup.get(entry.getId());
    }

    public Chain getEntries() {
        return entries;
    }

    public Iterable<TaskId> getTasks() {
        return entries.getTasks();
    }

    public Entry getNextStep(Entry currentStep) {
        var iterator = Iterators.peekingIterator(getEntries().iterator());

        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry instanceof Grouping) {

            }
            else if (entry instanceof TaskId) {
                if (entry.equals(currentStep)) {

                }
            }
            else {
                // throw
            }
        }

        return null;
    }
}
