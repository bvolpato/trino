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
 * A compact, growable list of primitive {@code int} values optimized for the
 * row pattern matcher hot path.
 * <p>
 * This implementation employs copy-on-write (COW) with reference counting to
 * make logical copies of lists essentially free until one of the copies is
 * mutated. COW is critical for the matcher because forking execution threads
 * (e.g., on {@code SPLIT}) duplicates thread state frequently.
 * <p>
 * Memory accounting: to avoid double counting shared arrays, {@link #getSizeInBytes()}
 * includes the backing array only when this instance is the sole owner
 * (refCount == 1). Instance header is always counted.
 * <p>
 * Thread-safety: not thread-safe. Instances are confined to the execution of a
 * single operator task.
 */
@NotThreadSafe
public class IntList
{
    private static final int INSTANCE_SIZE = instanceSize(IntList.class);

    private static final class RefCountedIntArray
    {
        private int[] values;
        private int size;
        private int refCount;

        RefCountedIntArray(int[] values, int size)
        {
            this.values = values;
            this.size = size;
            this.refCount = 1;
        }
    }

    private RefCountedIntArray data;

    /**
     * Creates an {@code IntList} with an initial capacity.
     *
     * @param capacity initial backing array capacity; may be 0
     */
    public IntList(int capacity)
    {
        this.data = new RefCountedIntArray(new int[capacity], 0);
    }

    /**
     * Appends a value to the end of the list, performing copy-on-write if this
     * instance shares storage with another list.
     */
    public void add(int value)
    {
        ensureUnique();
        ensureCapacity(data.size);
        data.values[data.size] = value;
        data.size++;
    }

    /**
     * Returns the value at the given index.
     *
     * @throws ArrayIndexOutOfBoundsException if {@code index} is out of range
     */
    public int get(int index)
    {
        return data.values[index];
    }

    /**
     * Sets the value at {@code index}. Grows the list if necessary and performs
     * copy-on-write if storage is shared.
     */
    public void set(int index, int value)
    {
        ensureUnique();
        ensureCapacity(index);
        data.values[index] = value;
        data.size = Math.max(data.size, index + 1);
    }

    /**
     * Returns the number of elements logically stored in the list.
     */
    public int size()
    {
        return data.size;
    }

    /**
     * Removes all elements from the list (logical size becomes 0). Performs
     * copy-on-write if storage is shared.
     */
    public void clear()
    {
        ensureUnique();
        data.size = 0;
    }

    /**
     * Returns a logical copy that shares the backing storage. The first subsequent
     * write to either copy will materialize an independent array (copy-on-write).
     */
    public IntList copy()
    {
        // Shared copy: increment refcount and share backing storage.
        data.refCount++;
        IntList copy = new IntList(0);
        copy.data = this.data;
        return copy;
    }

    /**
     * Returns a read-only view of the internal array up to the current size.
     */
    public ArrayView toArrayView()
    {
        return new ArrayView(data.values, data.size);
    }

    /**
     * Ensures the backing array can address the given index. May grow the
     * array using {@code Arrays.copyOf}. This method does not change the
     * logical size. Callers should invoke {@link #ensureUnique()} first to
     * respect copy-on-write semantics before any potential growth.
     */
    private void ensureCapacity(int index)
    {
        if (index >= data.values.length) {
            int[] newValues = Arrays.copyOf(data.values, Math.max(data.values.length * 2, index + 1));
            data.values = newValues;
        }
    }

    /**
     * Materializes a unique backing array if this instance shares storage with
     * others (ref count > 1). Copies only the used portion.
     */
    private void ensureUnique()
    {
        if (data.refCount > 1) {
            data.refCount--;
            data = new RefCountedIntArray(Arrays.copyOf(data.values, data.size), data.size);
        }
    }

    /**
     * Decrements the reference count for the shared backing storage. A balanced
     * pair of {@code copy()} and {@code release()} calls helps the owning
     * structure (e.g., {@link IntMultimap}) account memory precisely.
     */
    public void release()
    {
        if (data.refCount > 0) {
            data.refCount--;
        }
    }

    /**
     * Returns the estimated memory footprint in bytes. To avoid double-counting,
     * the backing array is included only for uniquely owned instances.
     */
    public long getSizeInBytes()
    {
        // Over-counting is acceptable, but try to avoid double counting
        // shared arrays: only include backing array when uniquely owned.
        long arraySize = (data.refCount == 1) ? SizeOf.sizeOf(data.values) : 0L;
        return INSTANCE_SIZE + arraySize;
    }

    /**
     * Returns the constant instance overhead (without backing array).
     */
    public long getInstanceSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
