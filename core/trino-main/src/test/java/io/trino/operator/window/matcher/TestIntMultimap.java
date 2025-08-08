/*
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
package io.trino.operator.window.matcher;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIntMultimap
{
    @Test
    public void testCopySharesUntilMutation()
    {
        IntMultimap map = new IntMultimap(2, 2);
        map.add(1, 10);

        // create child 2 that shares with parent 1
        map.copy(1, 2);
        ArrayView childView = map.getArrayView(2);
        assertThat(childView.length()).isEqualTo(1);
        assertThat(childView.get(0)).isEqualTo(10);

        // mutate child list
        map.add(2, 11);
        ArrayView updatedChildView = map.getArrayView(2);
        assertThat(updatedChildView.length()).isEqualTo(2);
        assertThat(updatedChildView.get(1)).isEqualTo(11);

        // parent remains intact
        ArrayView parentView = map.getArrayView(1);
        assertThat(parentView.length()).isEqualTo(1);
        assertThat(parentView.get(0)).isEqualTo(10);
    }

    @Test
    public void testClearResetsButKeepsCapacity()
    {
        IntMultimap map = new IntMultimap(2, 2);
        map.add(0, 1);
        map.add(1, 2);
        map.clear();
        assertThat(map.getArrayView(0).length()).isEqualTo(0);
        assertThat(map.getArrayView(1).length()).isEqualTo(0);
        // should still be able to add after clear
        map.add(1, 3);
        assertThat(map.getArrayView(1).length()).isEqualTo(1);
        assertThat(map.getArrayView(1).get(0)).isEqualTo(3);
    }

    @Test
    public void testCopyFromEmptyOverExistingChild()
    {
        IntMultimap map = new IntMultimap(2, 2);
        map.add(1, 7);      // child initially has a list
        map.copy(0, 1);     // parent key 0 is empty; copying should null out child
        assertThat(map.getArrayView(1).length()).isEqualTo(0);
    }

    @Test
    public void testMultipleCopiesAndWrites()
    {
        IntMultimap map = new IntMultimap(2, 2);
        map.add(0, 5);

        // create two children from the same parent
        map.copy(0, 1);
        map.copy(0, 2);

        // write to child 1
        map.add(1, 6);
        assertThat(map.getArrayView(1).length()).isEqualTo(2);
        assertThat(map.getArrayView(1).get(1)).isEqualTo(6);

        // parent remains size 1
        assertThat(map.getArrayView(0).length()).isEqualTo(1);

        // write to child 2 independently
        map.add(2, 7);
        assertThat(map.getArrayView(2).length()).isEqualTo(2);
        assertThat(map.getArrayView(2).get(1)).isEqualTo(7);
    }

    @Test
    public void testClearDoesNotAffectSharedParents()
    {
        // Prepare a hierarchy: key 0 as parent, keys 1 and 2 copied from parent
        IntMultimap map = new IntMultimap(2, 2);
        map.add(0, 1);
        map.copy(0, 1);
        map.copy(0, 2);

        // Mutate child 1 so it COWs away from parent
        map.add(1, 9);
        assertThat(map.getArrayView(1).length()).isEqualTo(2);

        // Clear the map; this should release child lists but not mutate any shared backing
        map.clear();

        // Reconstruct a child from the parent again and verify semantics are intact
        map.add(0, 1); // parent now recreated as [1]
        map.copy(0, 1);
        assertThat(map.getArrayView(1).length()).isEqualTo(1);
        assertThat(map.getArrayView(1).get(0)).isEqualTo(1);
    }
}
