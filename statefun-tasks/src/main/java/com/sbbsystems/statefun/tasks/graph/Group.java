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

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedList;
import java.util.List;

@TypeInfo(GroupTypeInfoFactory.class)
public final class Group extends EntryBase implements Entry {
    private List<Entry> items;

    public static Group of(@NotNull String id) {
        var group = new Group();
        group.setId(id);
        return group;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public Group() {
        items = new LinkedList<>();
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public List<Entry> getItems() {
        return items;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public void setItems(@NotNull List<Entry> items) {
        this.items = items;
    }

    public void addEntry(@NotNull Entry entry) {
        items.add(entry);
    }
}
