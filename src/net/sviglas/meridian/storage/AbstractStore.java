/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */

package net.sviglas.meridian.storage;

import java.util.Iterator;

/**
 * Abstract base class for all in-memory dataset stores. These stores are
 * effectively linked lists of containers, each different store with its own
 * different kind of container.
 *
 * @param <T> the type of the records of the store.
 */

public abstract class AbstractStore<T> extends Dataset<T> {
    // default allocation
    public static final int DEFAULT_ALLOCATION = 1000;
    // the number of records to allocate in each container
    private final int allocationSize;
    // the head of the container list
    private AbstractStoreContainer<T> head;
    // the tail of the container list
    private AbstractStoreContainer<T> tail;
    // the size of the store in number of records
    private long size;

    /**
     * Constructs a dataset hosting records of the given type. The type must
     * have a parameter-less constructor.
     *
     * @param c the type of the records this dataset hosts.
     * @throws BadTypeException if the record type is not acceptable (i.e., it
     * does not comprise primitive types and does not have a parameter-less
     * constructor).
     */
    public AbstractStore(Class<T> c) throws BadTypeException {
        this(c, DEFAULT_ALLOCATION);
    }

    /**
     * Constructs a dataset hosting records of the given type and with the
     * given default allocation increments in its containers. The type must
     * have a parameter-less constructor.
     *
     * @param c the type of the records this dataset hosts.
     * @param da the default allocation increment.
     * @throws BadTypeException if the record type is not acceptable (i.e., it
     * does not comprise primitive types and does not have a parameter-less
     * constructor).
     */
    public AbstractStore(Class<T> c, int da) throws BadTypeException {
        super(c);
        allocationSize = fixAllocationSize(da);
        head = null;
        tail = null;
        size = 0;
    }

    /**
     * Called internally to potentially align the allocation size -- it depends
     * on the actual subclasses to see if that is necessary.
     *
     * @param da the original allocation size.
     * @return the fixed allocation size.
     */
    protected int fixAllocationSize(int da) { return da; }

    /**
     * Retrieves this store's allocation size in number of records.
     *
     * @return this store's allocation size in number of records.
     */
    protected int getAllocationSize() { return allocationSize; }

    /**
     * The size of this store.
     *
     * @return the size of this store.
     */
    @Override
    public long size() { return size; }

    /**
     * Adds a new record to this store.
     *
     * @param t the record to be added.
     */
    @Override
    public void add(T t) {
        if (head != null) {
            if (tail.size() == getAllocationSize()) {
                AbstractStoreContainer<T> newTail = allocateContainer();
                newTail.add(t);
                tail.setNext(newTail);
                tail = newTail;
            }
            else {
                tail.add(t);
            }
        }
        else {
            head = allocateContainer();
            head.add(t);
            tail = head;
        }
        size++;
    }

    /**
     * Internal method to allocate a container of records for this store.
     *
     * @return a container of records for this store.
     */
    protected abstract AbstractStoreContainer<T> allocateContainer();

    /**
     * Given an index, retrieves the record at that index.
     *
     * @param i the index of the record to be retrieved.
     * @return the record at the given index.
     * @throws IndexOutOfBoundsException if the requested record is not within
     * the boundaries of this store.
     */
    @Override
    public T get(long i) throws IndexOutOfBoundsException {
        long accumulated = 0;
        AbstractStoreContainer<T> current = head;
        while (current != null) {
            if (accumulated + current.size() > i) {
                return current.get((int) (i - accumulated));
            }
            else {
                accumulated += current.size();
                current = current.getNext();
            }
        }
        throw new IndexOutOfBoundsException("Out of bounds: " + i + " > "
                + size);
    }

    /**
     * Appends a new dataset to this one.
     *
     * @param d the dataset to be appended to this.
     */
    @Override
    public void append(Dataset<T> d) {
        if (d.getClass().equals(this.getClass())) {
            AbstractStore<T> ad = (AbstractStore<T>) d;
            tail.setNext(ad.head);
            tail = ad.tail;
            AbstractStoreContainer<T> current = ad.head;
            while (current != null) {
                size += current.size();
                current = current.getNext();
            }
        }
        else {
            for (T t : d) add(t);
        }
    }

    /**
     * Returns an iterator over the elements of this store.
     *
     * @return an iterator over the elements of this store.
     */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            AbstractStoreContainer<T> currentContainer = head;
            int currentCounter = 0;

            @Override
            public boolean hasNext() {
                if (currentContainer == null) return false;
                if (currentCounter < currentContainer.size()) return true;
                if (currentContainer.getNext() == null) {
                    currentContainer = null;
                    currentCounter = 0;
                    return false;
                }
                do {
                    currentContainer = currentContainer.getNext();
                }
                while (currentContainer != null
                        && currentContainer.size() == 0);
                if (currentContainer == null) return false;
                currentCounter = 0;
                return true;
            }

            @Override
            public T next() {
                if (currentContainer != null) {
                    return currentContainer.get(currentCounter++);
                }
                return null;
            }
        };
    }
}
