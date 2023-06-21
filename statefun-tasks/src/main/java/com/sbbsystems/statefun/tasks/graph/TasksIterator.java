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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.util.Objects.isNull;

public final class TasksIterator implements Iterator<Task> {
    private Iterator<Task> iterators = Collections.emptyIterator();
    private Entry current;

    public static TasksIterator from(Entry entry) {
        return new TasksIterator(entry);
    }

    private TasksIterator(Entry entry) {
        this.current = entry;
    }

    @Override
    public boolean hasNext() {
        if (iterators.hasNext()) {
            return true;
        }
        else {
            return !isNull(current) && (!isNull(current.getNext()) || !current.isEmpty());
        }
    }

    @Override
    public Task next() {
        while (current instanceof Group) {
            var group = (Group) current;

            for (var head : group.getItems()) {
                iterators = Iterators.concat(iterators, TasksIterator.from(head));
            }

            current = current.getNext();
        }

        if (iterators.hasNext()) {
            return iterators.next();
        }

        if (isNull(current)) {
            throw new NoSuchElementException();
        }

        var value = current;
        current = current.getNext();
        return (Task) value;
    }
}