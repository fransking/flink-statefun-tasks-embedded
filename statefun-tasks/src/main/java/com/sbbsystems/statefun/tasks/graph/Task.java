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

import java.text.MessageFormat;

import static java.util.Objects.isNull;

public final class Task extends EntryBase implements Entry {
    private boolean isExceptionally;
    private boolean isFinally;

    public static Task of(@NotNull String id, boolean isExceptionally, boolean isFinally) {
        var task = new Task();
        task.setId(id);
        task.setExceptionally(isExceptionally);
        task.setFinally(isFinally);
        return task;
    }

    @Override
    public String toString() {
        return MessageFormat.format("Task {0}", getId());
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public boolean isExceptionally() {
        return isExceptionally;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public void setExceptionally(boolean exceptionally) {
        isExceptionally = exceptionally;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public boolean isFinally() {
        return isFinally;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public void setFinally(boolean isFinally) {
        this.isFinally = isFinally;
    }

//    public boolean isPreviousEntryEmpty() {
//        return (!isNull(getPrevious()) && getPrevious().isEmpty());
//    }
}
