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

import com.sbbsystems.statefun.tasks.util.Id;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import static java.util.Objects.requireNonNull;

abstract class EntryBase implements Entry {
    private String id;
    private Entry next;
    private Entry previous;
    private Group parentGroup;
    private Entry chainHead;

    @SuppressWarnings("unused")  // POJO serialisation
    public EntryBase() {
        id = Id.generate();
    }

    @SuppressWarnings("unused")  // POJO serialisation
    @Override
    public String getId() {
        return id;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public void setId(@NotNull String id) {
        this.id = requireNonNull(id);
    }

    @Nullable
    @Override
    public Entry getNext() {
        return next;
    }

    @Override
    public void setNext(@Nullable Entry next) {
        this.next = next;
    }

    @Nullable
    @Override
    public Entry getPrevious() {
        return previous;
    }

    @Override
    public void setPrevious(Entry previous) {
        this.previous = previous;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    @Nullable
    @Override
    public Group getParentGroup() {
        return parentGroup;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    @Override
    public void setParentGroup(@Nullable Group parentGroup) {
        this.parentGroup = parentGroup;
    }

    @Override
    public void setChainHead(Entry head) {
        this.chainHead = head;
    }
    @Override
    public Entry getChainHead() {
        return this.chainHead;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Entry && Objects.equals(((Entry) o).getId(), getId()));
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}
