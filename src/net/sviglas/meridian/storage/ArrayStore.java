package net.sviglas.meridian.storage;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */

class ArrayStoreContainer<T> {
    public ArrayList<T> contents;
    public ArrayStoreContainer<T> next;
    public ArrayStoreContainer(ArrayList<T> a) { contents = a; next = null; }
}

public class ArrayStore<T> extends Dataset<T> {
    private static final int DEFAULT_ALLOCATION = 1000;
    private final int defaultAllocation;
    private ArrayStoreContainer<T> head;
    private ArrayStoreContainer<T> tail;
    private int size;

    public ArrayStore(String n, Class<T> c) throws BadTypeException {
        this(n, c, DEFAULT_ALLOCATION);
    }

    public ArrayStore(String n, Class<T> c, int da) throws BadTypeException {
        super(n, c);
        defaultAllocation = da;
        head = null;
        tail = null;
        size = 0;
    }

    @Override
    public long size() { return size; }

    @Override
    public void add(T t) {
        if (head != null) {
            if (tail.contents.size() == defaultAllocation) {
                System.out.println("allocating container");
                ArrayStoreContainer<T> newTail = new ArrayStoreContainer<>(
                        new ArrayList<>(defaultAllocation));
                newTail.contents.add(t);
                tail.next = newTail;
                tail = newTail;
            }
            else {
                tail.contents.add(t);
            }
        }
        else {
            head = new ArrayStoreContainer<>(
                    new ArrayList<>(defaultAllocation));
            head.contents.add(t);
            tail = head;
        }
        size++;
    }

    @Override
    public T get(long i) throws IndexOutOfBoundsException {
        long accumulated = 0;
        ArrayStoreContainer<T> current = head;
        while (current != null) {
            if (accumulated + current.contents.size() > i) {
                return current.contents.get((int) (i - accumulated));
            }
            else {
                accumulated += current.contents.size();
                current = current.next;
            }
        }
        throw new IndexOutOfBoundsException("Out of bounds: " + i + " > "
                + size);
    }

    @Override
    public void append(Dataset<T> d) {
        if (d.getClass().equals(this.getClass())) {
            ArrayStore<T> ad = (ArrayStore<T>) d;
            tail.next = ad.head;
            tail = ad.tail;
            ArrayStoreContainer<T> current = ad.head;
            while (current != null) {
                size += current.contents.size();
                current = current.next;
            }
        }
        else {
            for (T t : d) add(t);
        }
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            ArrayStoreContainer<T> currentContainer = head;
            Iterator<T> currentIterator = (head != null
                    ? currentContainer.contents.iterator() : null);

            @Override
            public boolean hasNext() {
                if (currentIterator == null || currentContainer == null)
                    return false;
                if (currentIterator.hasNext()) return true;
                if (currentContainer != null && currentContainer.next == null) {
                    currentIterator = null;
                    return false;
                }
                do {
                    currentContainer = currentContainer.next;
                }
                while (currentContainer != null
                        && currentContainer.contents.size() == 0);
                if (currentContainer == null) return false;
                currentIterator = currentContainer.contents.iterator();
                return currentIterator.hasNext();
            }

            @Override
            public T next() {
                return (currentIterator != null
                        ? currentIterator.next() : null);
            }
        };
    }

    public static void main(String [] s) {
        try {
            ArrayStore<Integer> lala = new ArrayStore<>("lala", Integer.class, 10);
            ArrayStore<Integer> koko = new ArrayStore<>("koko", Integer.class, 10);
            for (int i = 0; i < 100; i++) lala.add(i);
            for (int i = 1000; i < 1100; i++) koko.add(i);
            for (int i : lala) System.out.println("lala: " + i);
            System.out.println("lala size: " + lala.size());
            for (int i : koko) System.out.println("koko: " + i);
            System.out.println("koko size: " + koko.size());
            lala.append(koko);
            for (int i : lala) System.out.println("lala: " + i);
            System.out.println("lala size: " + lala.size());
            System.out.println("at 150: " + lala.get(150    ));
        }
        catch (Exception e) {
            System.err.println("Exception " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

}
