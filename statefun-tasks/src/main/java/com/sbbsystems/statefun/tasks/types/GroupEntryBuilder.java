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
package com.sbbsystems.statefun.tasks.types;

public class GroupEntryBuilder {
    public static GroupEntry fromProto(com.sbbsystems.statefun.tasks.generated.GroupEntry groupEntry) {
        var group = new GroupEntry();

        group.groupId = groupEntry.getGroupId();
        group.maxParallelism = groupEntry.getMaxParallelism();
        group.returnExceptions = groupEntry.getReturnExceptions();
        group.isWait = groupEntry.getIsWait();
        group.size = groupEntry.getGroupCount();
        group.remaining = group.size;

        return group;
    }
}
