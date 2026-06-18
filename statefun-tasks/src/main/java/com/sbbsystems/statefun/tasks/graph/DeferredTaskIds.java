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

import com.sbbsystems.statefun.tasks.graph.v2.ListTypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class DeferredTaskIds {
    @TypeInfo(ListTypeInfoFactory.class)
    private ArrayList<String> taskIds;

    @SuppressWarnings("unused")  // POJO serialisation
    public DeferredTaskIds() {
    }

    public static DeferredTaskIds newInstance() {
        return new DeferredTaskIds();
    }

    public static DeferredTaskIds of(ArrayList<String> taskIds) {
        var instance = newInstance();
        instance.setTaskIds(taskIds);
        return instance;
    }

    public boolean hasMoreEntries(int index) {
        return numberRemaining(index) > 0;
    }

    public int numberRemaining(int index) {
        return Math.max(0, taskIds.size() - index);
    }

    public ArrayList<String> getTaskIds() {
        return taskIds;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public void setTaskIds(ArrayList<String> taskIds) {
        this.taskIds = taskIds;
    }
}
