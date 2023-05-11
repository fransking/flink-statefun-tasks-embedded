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

import java.util.Iterator;
import java.util.Objects;

public final class TasksIterator implements Iterator<Task> {
    Iterator<Task> groupIterator = null;
    private Entry current;

    public static TasksIterator from(Entry entry) {
        return new TasksIterator(entry);
    }

    private TasksIterator(Entry entry) {
        this.current = entry;
    }

    public boolean childHasNext() {
        return !Objects.isNull(groupIterator) && groupIterator.hasNext();
    }

    @Override
    public boolean hasNext() {
        return childHasNext() || !Objects.isNull(current);
    }

    @Override
    public Task next() {
        var value = current;

        if (value instanceof Group) {
            var group = (Group) value;

            for (var head : group.getItems()) {
                if (Objects.isNull(groupIterator)) {
                    groupIterator = TasksIterator.from(head);
                } else {
                    groupIterator = Iterators.concat(groupIterator, TasksIterator.from(head));
                }
            }

            current = value.getNext();
        }

        if (childHasNext()) {
            return groupIterator.next();
        }

        if (hasNext()) {
            current = value.getNext();
        }

        assert value instanceof Task;
        return (Task) value;
    }
}
