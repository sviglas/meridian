package net.sviglas.meridian.storage;

import java.util.Iterator;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 16/08/15.
 */
public abstract class AbstractStore<T> extends Dataset<T> {
    public static final int DEFAULT_ALLOCATION = 1000;
    private final int allocationSize;
    private AbstractStoreContainer<T> head;
    private AbstractStoreContainer<T> tail;
    private long size;

    /**
     * Constructs a dataset with the given name, hosting element of the given
     * type. The type must have a parameter-less constructor.
     *
     * @param n the name of the dataset.
     * @param c the type of the elements this dataset hosts.
     * @throws BadTypeException if the element type is not acceptable (i.e., it
     *                          does not comprise primitive types and does not
     *                          have a parameter-less constructor).
     */
    public AbstractStore(String n, Class<T> c) throws BadTypeException {
        this(n, c, DEFAULT_ALLOCATION);
    }

    public AbstractStore(String n, Class<T> c, int da) throws BadTypeException {
        super(n, c);
        allocationSize = fixAllocationSize(da);
        head = null;
        tail = null;
        size = 0;
    }

    protected int fixAllocationSize(int da) { return da; }

    protected int getAllocationSize() { return allocationSize; }

    @Override
    public long size() { return size; }

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

    protected abstract AbstractStoreContainer<T> allocateContainer();

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
