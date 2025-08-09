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

import io.trino.annotation.NotThreadSafe;

import static io.airlift.slice.SizeOf.instanceSize;

/**
 * Aggregates thread-local state used by the row pattern matcher.
 * <p>
 * Holds two {@link IntMultimap} instances:
 * - captures: start/end indices delimiting excluded subsequences
 * - labels: matched label IDs for each position in the input
 * Both maps leverage copy-on-write to make thread forking inexpensive.
 * <p>
 * Not thread-safe.
 */
@NotThreadSafe
class Captures
{
    private static final int INSTANCE_SIZE = instanceSize(Captures.class);

    private final IntMultimap captures;
    private final IntMultimap labels;

    public Captures(int initialCapacity, int slotCount, int labelCount)
    {
        this.captures = new IntMultimap(initialCapacity, slotCount);
        this.labels = new IntMultimap(initialCapacity, labelCount);
    }

    /**
     * Saves an exclusion/capture boundary for a given thread.
     */
    public void save(int threadId, int value)
    {
        captures.add(threadId, value);
    }

    /**
     * Saves a matched label for a given thread.
     */
    public void saveLabel(int threadId, int value)
    {
        labels.add(threadId, value);
    }

    /**
     * Copies per-thread state from {@code parent} to {@code child} (shares
     * storage; copy-on-write on mutation).
     */
    public void copy(int parent, int child)
    {
        captures.copy(parent, child);
        labels.copy(parent, child);
    }

    /**
     * Returns a view of capture boundaries for a thread.
     */
    public ArrayView getCaptures(int threadId)
    {
        return captures.getArrayView(threadId);
    }

    /**
     * Returns a view of labels matched for a thread.
     */
    public ArrayView getLabels(int threadId)
    {
        return labels.getArrayView(threadId);
    }

    /**
     * Releases per-thread state for a finished thread.
     */
    public void release(int threadId)
    {
        captures.release(threadId);
        labels.release(threadId);
    }

    /**
     * Returns the estimated memory footprint of this structure and the maps it
     * owns (excluding the program and equivalence structures).
     */
    public long getSizeInBytes()
    {
        return INSTANCE_SIZE + captures.getSizeInBytes() + labels.getSizeInBytes();
    }
}
