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

public class TestIntList
{
    @Test
    public void testCopyOnWriteIsolationOnSet()
    {
        IntList parent = new IntList(2);
        parent.add(1);
        parent.add(2);

        IntList child = parent.copy();
        child.set(1, 99);

        assertThat(parent.size()).isEqualTo(2);
        assertThat(parent.get(1)).isEqualTo(2);
        assertThat(child.size()).isEqualTo(2);
        assertThat(child.get(1)).isEqualTo(99);

        // release references (should be no-op safety)
        parent.release();
        child.release();
    }

    @Test
    public void testCopyOnWriteIsolationOnAdd()
    {
        IntList parent = new IntList(1);
        parent.add(7);

        IntList child = parent.copy();
        child.add(8);

        assertThat(parent.size()).isEqualTo(1);
        assertThat(parent.get(0)).isEqualTo(7);
        assertThat(child.size()).isEqualTo(2);
        assertThat(child.get(1)).isEqualTo(8);

        parent.release();
        child.release();
    }

    @Test
    public void testClearDoesNotAffectSharedCopy()
    {
        IntList parent = new IntList(3);
        parent.add(4);
        parent.add(5);

        IntList child = parent.copy();
        child.clear();

        assertThat(child.size()).isEqualTo(0);
        assertThat(parent.size()).isEqualTo(2);
        assertThat(parent.get(0)).isEqualTo(4);
        assertThat(parent.get(1)).isEqualTo(5);

        parent.release();
        child.release();
    }

    @Test
    public void testToArrayViewReflectsSize()
    {
        IntList list = new IntList(2);
        list.add(1);
        list.add(2);
        ArrayView view = list.toArrayView();
        assertThat(view.length()).isEqualTo(2);
        assertThat(view.get(0)).isEqualTo(1);
        assertThat(view.get(1)).isEqualTo(2);

        list.release();
    }

    @Test
    public void testCopyChainIsolation()
    {
        IntList p = new IntList(2);
        p.add(1);
        p.add(2);

        IntList c1 = p.copy();
        IntList c2 = c1.copy();

        // mutate deepest child
        c2.add(3);
        assertThat(c2.size()).isEqualTo(3);
        assertThat(c1.size()).isEqualTo(2);
        assertThat(p.size()).isEqualTo(2);

        // mutate middle child
        c1.set(0, 9);
        assertThat(c1.get(0)).isEqualTo(9);
        assertThat(p.get(0)).isEqualTo(1);

        // mutate parent
        p.add(4);
        assertThat(p.size()).isEqualTo(3);
        assertThat(c1.size()).isEqualTo(2);
        assertThat(c2.size()).isEqualTo(3);

        p.release();
        c1.release();
        c2.release();
    }

    @Test
    public void testEmptyCopyThenAdd()
    {
        IntList p = new IntList(0);
        IntList c = p.copy();
        c.add(5);
        assertThat(p.size()).isEqualTo(0);
        assertThat(c.size()).isEqualTo(1);
        assertThat(c.get(0)).isEqualTo(5);
        p.release();
        c.release();
    }

    @Test
    public void testSetBeyondCurrentSize()
    {
        IntList p = new IntList(2);
        p.add(1);
        p.add(2);
        IntList c = p.copy();

        // sparse set far beyond current size
        c.set(1000, 42);
        assertThat(c.size()).isEqualTo(1001);
        assertThat(c.get(0)).isEqualTo(1);
        assertThat(c.get(1)).isEqualTo(2);
        assertThat(c.get(1000)).isEqualTo(42);

        // parent remains unchanged
        assertThat(p.size()).isEqualTo(2);
        p.release();
        c.release();
    }

    @Test
    public void testBothSidesAddIndependently()
    {
        IntList p = new IntList(1);
        p.add(10);
        IntList c = p.copy();

        p.add(11);
        c.add(12);

        assertThat(p.size()).isEqualTo(2);
        assertThat(p.get(1)).isEqualTo(11);
        assertThat(c.size()).isEqualTo(2);
        assertThat(c.get(1)).isEqualTo(12);

        p.release();
        c.release();
    }
}
