/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */

package net.sviglas.meridian.storage;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * The simplest type of store, uses an array list to store records in its
 * container.
 *
 * @param <T> the record type.
 */

public class ArrayStore<T> extends AbstractStore<T> {
    /**
     * Constructs a new array store given the type of its records.
     *
     * @param c the record type.
     * @throws BadTypeException if the type does not have a parameter-less
     * constructor, or if the type is not a primitive type, or if its fields are
     * not primitive-typed.
     */
    public ArrayStore(Class<T> c) throws BadTypeException {
        super(c);
    }

    /**
     * Constructs a new array store given the type of its records and its
     * allocation increment.
     *
     * @param c the record type.
     * @param da the allocation increment.
     * @throws BadTypeException if the type does not have a parameter-less
     * constructor, or if the type is not a primitive type, or if its fields are
     * not primitive-typed.
     */
    public ArrayStore(Class<T> c, int da) throws BadTypeException {
        super(c, da);
    }

    /**
     * Internal method to allocate containers of this store.
     *
     * @return a new container for this store.
     */
    @Override
    protected AbstractStoreContainer<T> allocateContainer() {
        return new ArrayStoreContainer();
    }

    /**
     * Internal class that encapsulates the containers of this store.
     */
    class ArrayStoreContainer extends AbstractStoreContainer<T> {
        // the arraylist of contents
        public ArrayList<T> contents;

        /**
         * Default constructor for an array store container.
         */
        public ArrayStoreContainer() {
            contents = new ArrayList<>();
        }

        /**
         * Returns the size of this container.
         *
         * @return the size of this container.
         */
        @Override
        public int size() { return contents.size(); }

        /**
         * Retrieves the record at the given index.
         *
         * @param i the index of the record to be retrieved.
         * @return the record at the given index.
         * @throws BadAccessException if the record cannot be retrieved.
         */
        @Override
        public T get(int i) throws BadAccessException {
            return contents.get(i);
        }

        /**
         * Adds a new record to this container.
         *
         * @param t the record to be added.
         * @throws BadAccessException whenever the record cannot be added.
         */
        @Override
        public void add(T t) throws BadAccessException { contents.add(t); }
    }

    /**
     * Debug main.
     *
     * @param s parameters.
     */
    public static void main(String [] s) {
        class TestClass {
            private int key;
            private long value;
            public TestClass() { this(0, 0); }
            public TestClass(int k, long v) { key = k; value = v; }
            public String toString() { return "<" + key + ", " + value + ">"; }
        }
        try {
            ArrayStore<TestClass> foo = new ArrayStore<>(TestClass.class, 10);
            ArrayStore<TestClass> bar = new ArrayStore<>(TestClass.class, 10);
            for (int i = 0; i < 100; i++) foo.add(new TestClass(i, i*i));
            for (int i = 1000; i < 1100; i++) bar.add(new TestClass(i, i*i));
            for (TestClass i : foo) System.out.println("foo: " + i);
            System.out.println("foo size: " + foo.size());
            for (TestClass i : bar) System.out.println("bar: " + i);
            System.out.println("bar size: " + bar.size());
            foo.append(bar);
            for (TestClass i : foo) System.out.println("foo: " + i);
            System.out.println("foo size: " + foo.size());
            System.out.println("at 150: " + foo.get(150));
        }
        catch (Exception e) {
            System.err.println("Exception " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
