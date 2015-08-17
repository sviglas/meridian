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

public class ArrayStore<T> extends AbstractStore<T> {

    public ArrayStore(Class<T> c) throws BadTypeException {
        super(c);
    }

    public ArrayStore(Class<T> c, int da) throws BadTypeException {
        super(c, da);
    }

    @Override
    protected AbstractStoreContainer<T> allocateContainer() {
        return new ArrayStoreContainer();
    }

    class ArrayStoreContainer extends AbstractStoreContainer<T> {
        public ArrayList<T> contents;
        public ArrayStoreContainer() {
            contents = new ArrayList<>();
        }
        public int size() { return contents.size(); }
        public T get(int i) throws BadAccessException {
            return contents.get(i);
        }
        public void add(T t) throws BadAccessException { contents.add(t); }
    }

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
