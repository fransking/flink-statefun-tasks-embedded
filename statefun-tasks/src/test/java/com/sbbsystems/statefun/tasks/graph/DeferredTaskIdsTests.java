/*
 * Copyright [2026] [Frans King]
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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

public class DeferredTaskIdsTests {

    private static DeferredTaskIds ofSize(int size) {
        var ids = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            ids.add("task-" + i);
        }
        return DeferredTaskIds.of(ids);
    }

    // ---- numberRemaining -----------------------------------------------------------------------

    @Test
    public void numberRemaining_for_list_of_size_1_at_index_0_is_1() {
        var deferred = ofSize(1);
        assertThat(deferred.numberRemaining(0)).isEqualTo(1);
    }

    @Test
    public void numberRemaining_for_list_of_size_2_at_index_1_is_1() {
        var deferred = ofSize(2);
        assertThat(deferred.numberRemaining(1)).isEqualTo(1);
    }

    @Test
    public void numberRemaining_for_list_of_size_2_at_index_2_is_0() {
        var deferred = ofSize(2);
        assertThat(deferred.numberRemaining(2)).isEqualTo(0);
    }

    @Test
    public void numberRemaining_cannot_go_negative() {
        var deferred = ofSize(1);
        assertThat(deferred.numberRemaining(5)).isEqualTo(0);
    }

    @Test
    public void numberRemaining_for_empty_list_is_0() {
        var deferred = ofSize(0);
        assertThat(deferred.numberRemaining(0)).isEqualTo(0);
    }

    // ---- hasMoreEntries ------------------------------------------------------------------------

    @Test
    public void hasMoreEntries_returns_true_when_remaining_is_positive() {
        var deferred = ofSize(1);
        assertThat(deferred.hasMoreEntries(0)).isTrue();
    }

    @Test
    public void hasMoreEntries_returns_false_when_remaining_is_zero() {
        var deferred = ofSize(2);
        assertThat(deferred.hasMoreEntries(2)).isFalse();
    }

    @Test
    public void hasMoreEntries_returns_false_for_empty_list() {
        var deferred = ofSize(0);
        assertThat(deferred.hasMoreEntries(0)).isFalse();
    }

    // ---- consistency ---------------------------------------------------------------------------

    @Test
    public void hasMoreEntries_is_consistent_with_numberRemaining() {
        var deferred = ofSize(3);

        for (int i = 0; i <= 4; i++) {
            boolean expectedHasMore = deferred.numberRemaining(i) > 0;
            assertThat(deferred.hasMoreEntries(i))
                    .as("hasMoreEntries(%d) should match numberRemaining(%d) > 0", i, i)
                    .isEqualTo(expectedHasMore);
        }
    }
}
