/*
 * Copyright [2025] [Frans King]
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
package com.sbbsystems.statefun.tasks.graph.v2;

import com.sbbsystems.statefun.tasks.generated.GroupEntry;
import com.sbbsystems.statefun.tasks.graph.InvalidGraphException;
import com.sbbsystems.statefun.tasks.util.Id;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class GraphEntry {
    private String id;
    private String nextId;
    private String previousId;
    private String parentGroupId;
    private String chainHeadId;
    private boolean precededByAnEmptyGroup;
    private boolean isWait;

    // task fields
    private boolean isExceptionally;
    private boolean isFinally;

    // group fields
    private boolean isGroup;
    @TypeInfo(ListTypeInfoFactory.class)
    private List<String> idsInGroup;
    private int groupMaxParallelism;
    private boolean isGroupUnordered;

    public static GraphEntry forTask(@NotNull String id, boolean isExceptionally, boolean isFinally, boolean isWait) {
        var task = new GraphEntry();
        task.setId(id);
        task.setIsExceptionally(isExceptionally);
        task.setIsFinally(isFinally);
        task.setIsWait(isWait);
        return task;
    }

    public static GraphEntry forGroup(@NotNull GroupEntry groupEntry) {
        return GraphEntry.forGroup(
                groupEntry.getGroupId(),
                groupEntry.getMaxParallelism(),
                groupEntry.getIsWait(),
                groupEntry.getIsUnordered()
        );
    }

    public static GraphEntry forGroup(@NotNull String id, int maxParallelism, boolean isWait) {
        return GraphEntry.forGroup(id, maxParallelism, isWait, false);
    }

    public static GraphEntry forGroup(@NotNull String id, int maxParallelism, boolean isWait, boolean isUnordered) {
        var group = new GraphEntry();
        group.setIsGroup(true);
        group.setId(id);
        group.setGroupMaxParallelism(maxParallelism);
        group.setIsWait(isWait);
        group.setIsGroupUnordered(isUnordered);
        group.setIdsInGroup(new LinkedList<>());
        return group;
    }

    public static boolean isGroup(@Nullable GraphEntry entry) {
        return !Objects.isNull(entry) && entry.isGroup();
    }

    public static boolean isTask(@Nullable GraphEntry entry) {
        return !Objects.isNull(entry) && !entry.isGroup();
    }

    public static GraphEntry getNext(@Nullable GraphEntry entry, Map<String, GraphEntry> entries) {
        if (!Objects.isNull(entry)) {
            return entries.get(entry.getNextId());
        }
        return null;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public GraphEntry() {
        id = Id.generate();
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public String getId() {
        return id;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public void setId(@NotNull String id) {
        this.id = requireNonNull(id);
    }

    @Nullable
    public String getNextId() {
        return nextId;
    }

    public void setNextId(@Nullable String nextId) {
        this.nextId = nextId;
    }

    @Nullable
    public String getPreviousId() {
        return previousId;
    }

    public void setPreviousId(String previousId) {
        this.previousId = previousId;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    @Nullable
    public String getParentGroupId() {
        return parentGroupId;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public void setParentGroupId(@Nullable String parentGroupId) {
        this.parentGroupId = parentGroupId;
    }

    public void setChainHeadId(String headId) {
        this.chainHeadId = headId;
    }

    public String getChainHeadId() {
        return this.chainHeadId;
    }

    public boolean isPrecededByAnEmptyGroup() {
        return precededByAnEmptyGroup;
    }

    public void setPrecededByAnEmptyGroup(boolean precededByAnEmptyGroup) {
        this.precededByAnEmptyGroup = precededByAnEmptyGroup;
    }

    public boolean isWait() {
        return isWait;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public void setIsWait(boolean isWait) {
        this.isWait = isWait;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public boolean isExceptionally() {
        return isExceptionally;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public void setIsExceptionally(boolean exceptionally) {
        isExceptionally = exceptionally;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public boolean isFinally() {
        return isFinally;
    }

    @SuppressWarnings("unused")  // POJO serialisation
    public void setIsFinally(boolean isFinally) {
        this.isFinally = isFinally;
    }

    // group fields
    public boolean isGroup() {
        return isGroup;
    }

    public void setIsGroup(boolean isGroup) {
        this.isGroup = isGroup;
    }

    public List<String> getIdsInGroup() {
        return idsInGroup;
    }

    public void setIdsInGroup(List<String> idsInGroup) {
        this.idsInGroup = idsInGroup;
    }

    public void setGroupMaxParallelism(int groupMaxParallelism) {
        this.groupMaxParallelism = groupMaxParallelism;
    }

    public int getGroupMaxParallelism() {
        return this.groupMaxParallelism;
    }

    public void setIsGroupUnordered(boolean isGroupUnordered) {
        this.isGroupUnordered = isGroupUnordered;
    }

    public boolean isGroupUnordered() {
        return this.isGroupUnordered;
    }

    public boolean isEmpty(Map<String, GraphEntry> mapOfEntries) {
        if (isGroup) {
            for (var id : idsInGroup) {
                while (!Objects.isNull(id)) {
                    GraphEntry entry = mapOfEntries.get(id);
                    if (!entry.isEmpty(mapOfEntries)) {
                        return false;
                    }
                    id = entry.getNextId();
                }
            }
            return true;
        }
        return false;
    }

    public void addEntryToGroup(@NotNull GraphEntry entry) throws InvalidGraphException {
        if (!isGroup) {
            throw new InvalidGraphException(MessageFormat.format("GraphEntry {0} is not a group", getId()));
        }

        idsInGroup.add(entry.getId());
    }

    public String toString() {
        return MessageFormat.format("{0} {1}", isGroup ? "Group" : "Task", getId());
    }

    public boolean equals(Object o) {
        return (o instanceof GraphEntry && Objects.equals(((GraphEntry) o).getId(), getId()));
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}
