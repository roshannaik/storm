package org.apache.storm.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/***
 *  A fixed size array with a customizable indexing range. The index range can have -ve lower / upper bound.
 *  Intended to be a faster alternative to HashMap<Integer, .. >. Only applicable as a substitute if the
 *  largest & smallest indices are known at time of creation.
 *  This class does not inherit from :
 *   - Map<Integer,T> : For performance reasons. The get(Object key) method requires key to be cast to Integer before use.
 *   - ArrayList<T> : as this class supports negative indexes & cannot grow/shrink.
 */

public class CustomIndexArray<T>  {

    public final int baseIndex;
    private final ArrayList<T> elements;
    private final int elemCount;

    /**
     * Creates the array with (upperIndex-lowerIndex+1) elements, initialized to null.
     * @param lowerIndex Smallest (inclusive) valid index for the array. Can be +ve, -ve or 0.
     * @param upperIndex Largest  (inclusive) valid index for the array. Can be +ve, -ve or 0. Must be > lowerIndex
     */
    public CustomIndexArray(int lowerIndex, int upperIndex) {
        if (lowerIndex >= upperIndex) {
            throw new IllegalArgumentException("lowerIndex must be < upperIndex");
        }

        this.baseIndex = lowerIndex;
        this.elemCount = upperIndex - lowerIndex + 1;

        this.elements = makeNullInitializedArray(elemCount);
    }

    private static <T> ArrayList<T> makeNullInitializedArray(int elemCount) {
        ArrayList<T> result = new ArrayList<T>(elemCount);
        for (int i = 0; i < elemCount; i++) {
            result.add(null);
        }
        return result;
    }

    /**
     * Initializes the array with elements from a HashTable.
     *
     * @param src  the source map from which to initialize
     */
    public CustomIndexArray(Map<Integer, T> src) {
        Integer lowerIndex = Integer.MAX_VALUE;
        Integer upperIndex = Integer.MIN_VALUE;

        for (Map.Entry<Integer, T> entry : src.entrySet()) { // calculate smallest & largest indexes
            Integer key =  entry.getKey();
            if (key < lowerIndex) {
                lowerIndex = key;
            }
            if (key > upperIndex) {
                upperIndex = key;
            }
        }
        this.baseIndex = lowerIndex;
        this.elemCount = upperIndex - lowerIndex + 1;

        this.elements = makeNullInitializedArray(elemCount);

        for (Map.Entry<Integer, T> entry : src.entrySet()) {
            elements.set(entry.getKey() - baseIndex, entry.getValue());
        }
    }

    /**
     * Get the value at the index.
     * @throws IndexOutOfBoundsException if index is outside of bounds specified to constructor
     */
    public T get(int index) {
        return elements.get(index - baseIndex);
    }

    /**
     * Set the value at the index.
     * @throws IndexOutOfBoundsException if index is outside of bounds specified to constructor
     */
    public T set(int index, T value) {
        return elements.set(index - baseIndex, value);
    }

    /**
     * Returns the number of elements (including null elements).
     */
    public int size() {
        return elemCount;
    }

    /**
     * Always returns true as this cannot be empty.
     */
    public boolean isEmpty() {
        return false;
    }


    /**
     * Check if index is valid.
     */
    public boolean isValidIndex(int index) {
        int actualIndex = index - baseIndex;
        if (actualIndex < 0  ||  actualIndex >= elemCount) {
            return false;
        }
        return true;
    }

    /**
     * Returns the index range as a Set.
     */
    public Set<Integer> indices() {
        Set<Integer> result = new TreeSet<>(); // retain order
        for (int i = 0; i < elemCount; i++) {
            result.add(i + baseIndex);
        }
        return result;
    }

    /**
     * Inverts this array into a Map.
     */
    public Map<T, List<Integer>> getReverseMap() {
        HashMap<T, List<Integer>> result = new HashMap<>(elemCount);
        for (int i = 0; i < elements.size(); i++) {
            T item = elements.get(i);
            if (item == null) {
                continue;
            }
            List<Integer> tmp = result.get(item);
            if (tmp == null) {
                tmp = new ArrayList<>();
                result.put(item, tmp);
            }
            tmp.add(i + baseIndex);
        }
        return result;
    }

    /**
     * Returns the stored values as an ArrayList.
     */
    public ArrayList<T> getItems() {
        return elements;
    }
}
