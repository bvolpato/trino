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

import io.airlift.slice.SizeOf;
import io.trino.annotation.NotThreadSafe;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;

/**
 * Sparse multimap from integer keys to lists of integers with copy-on-write
 * semantics for values. Designed for the pattern matcher to track per-thread
 * captures/labels efficiently.
 * <p>
 * - Values are {@link IntList} instances that share on copy and materialize on
 * first write.
 * - {@link #copy(int, int)} shares the parent list with the child. Any
 * subsequent mutation of either will cause an independent copy for the
 * mutating side.
 * - {@link #clear()} releases all lists and resets accounting without
 * reallocating the outer array.
 * <p>
 * Not thread-safe.
 */
@NotThreadSafe
class IntMultimap
{
    private static final long INSTANCE_SIZE = instanceSize(IntMultimap.class);

    private IntList[] values;
    private final int listCapacity;
    private long valuesSize;

    public IntMultimap(int capacity, int listCapacity)
    {
        this.values = new IntList[capacity];
        this.listCapacity = listCapacity;
        this.valuesSize = 0L;
    }

    /**
     * Appends {@code value} to the list at {@code key}, creating the list on
     * demand. Adjusts memory accounting for the list header and any capacity
     * growth caused by the write.
     */
    public void add(int key, int value)
    {
        boolean expanded = ensureCapacity(key);
        long listSizeBefore;
        if (expanded || values[key] == null) {
            listSizeBefore = 0L;
            values[key] = new IntList(listCapacity);
        }
        else {
            listSizeBefore = values[key].getSizeInBytes();
        }
        values[key].add(value);
        valuesSize += values[key].getSizeInBytes() - listSizeBefore;
    }

    /**
     * Releases and removes the list at {@code key}. Memory accounting is
     * updated by subtracting the list's unique size.
     */
    public void release(int key)
    {
        if (key < values.length && values[key] != null) {
            long sizeBefore = values[key].getSizeInBytes();
            values[key].release();
            valuesSize -= sizeBefore;
            values[key] = null;
        }
    }

    /**
     * Makes the list at {@code child} refer to the same list as {@code parent}
     * (copy-on-write). If the child already has a list, it is replaced and
     * accounting is adjusted accordingly.
     */
    public void copy(int parent, int child)
    {
        boolean expanded = ensureCapacity(child);
        if (expanded || values[child] == null) {
            if (values[parent] != null) {
                values[child] = values[parent].copy();
                valuesSize += values[child].getSizeInBytes();
            }
        }
        else if (values[parent] != null) {
            long listSizeBefore = values[child].getSizeInBytes();
            values[child] = values[parent].copy();
            valuesSize += values[child].getSizeInBytes() - listSizeBefore;
        }
        else {
            valuesSize -= values[child].getSizeInBytes();
            values[child] = null;
        }
    }

    /**
     * Returns an immutable view of the list at {@code key}. If missing, an
     * empty view is returned.
     */
    public ArrayView getArrayView(int key)
    {
        if (key >= values.length || values[key] == null) {
            return ArrayView.EMPTY;
        }
        return values[key].toArrayView();
    }

    /**
     * Clears all keys and releases their lists without reallocating the outer
     * array.
     */
    public void clear()
    {
        // Reuse the existing array and clear references. Release lists to keep
        // copy-on-write reference counts correct for any shared backing data.
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                values[i].release();
                values[i] = null;
            }
        }
        valuesSize = 0L;
    }

    /**
     * Ensures the outer array can address {@code key}.
     *
     * @return true if the array grew; false otherwise
     */
    private boolean ensureCapacity(int key)
    {
        if (key >= values.length) {
            values = Arrays.copyOf(values, Math.max(values.length * 2, key + 1));
            return true;
        }

        return false;
    }

    /**
     * Returns the estimated memory footprint in bytes, including the outer
     * array and unique sizes of stored lists.
     */
    public long getSizeInBytes()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(values) + valuesSize;
    }
}
