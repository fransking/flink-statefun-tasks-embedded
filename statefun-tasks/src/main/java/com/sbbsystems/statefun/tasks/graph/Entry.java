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

import org.jetbrains.annotations.Nullable;

public interface Entry {
    String getId();

    @Nullable Entry getNext();

    @Nullable Entry getPrevious();

    void setNext(@Nullable Entry next);

    void setPrevious(@Nullable Entry previous);

    @Nullable Group getParentGroup();

    void setParentGroup(@Nullable Group parentGroup);

    void setChainHead(Entry head);

    Entry getChainHead();

    boolean isPrecededByAnEmptyGroup();

    void setPrecededByAnEmptyGroup(boolean precededByAnEmptyGroup);

    boolean isEmpty();
}
