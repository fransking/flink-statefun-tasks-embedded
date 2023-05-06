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

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.StreamSupport;

public class Grouping implements Entry {
    private String id;
    private List<Chain> entries = new LinkedList<>();

    public Grouping() {
        this(null);
    }

    public Grouping(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Chain> getEntries() {
        return entries;
    }

    public void setEntries(List<Chain> entries) {
        this.entries = entries;
    }

    @NotNull
    @Override
    public Iterator<TaskId> iterator() {
        return entries.stream()
                .flatMap(chain -> StreamSupport.stream(chain.getTasks().spliterator(), false))
                .iterator();
    }
}