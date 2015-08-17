package net.sviglas.meridian.storage;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 14/08/15.
 */

public class ColumnStore<T> extends AbstractStore<T> {

    public ColumnStore(Class<T> c) throws BadTypeException {
        super(c);
    }

    public ColumnStore(Class<T> c, int da) throws BadTypeException {
        super(c, da);
    }

    @Override
    protected AbstractStoreContainer<T> allocateContainer() {
        return new ColumnStoreContainer();
    }

    class ColumnStoreContainer extends AbstractStoreContainer<T> {
        protected static final String DEFAULT_NAME = "COLUMN";
        public Map<String, Object> columns;
        public int occupied;

        public ColumnStoreContainer() {
            columns = new HashMap<>();
            if (isSupportedType(getRecordType())) {
                columns.put(DEFAULT_NAME, makeColumn(getRecordType(),
                        getAllocationSize()));
            }
            else {
                for (Field f : getFields()) {
                    columns.put(f.getName(), makeColumn(f.getType(),
                            getAllocationSize()));
                }
            }
        }

        protected Object makeColumn(Class<?> cls, int alloc) {
            if (cls.equals(Byte.class) || cls.equals(byte.class))
                return new byte [alloc];
            else if (cls.equals(Short.class) || cls.equals(short.class))
                return new short [alloc];
            else if (cls.equals(Character.class) || cls.equals(char.class))
                return new char [alloc];
            else if (cls.equals(Integer.class) || cls.equals(int.class))
                return new int [alloc];
            else if (cls.equals(Long.class) || cls.equals(long.class))
                return new long [alloc];
            else if (cls.equals(Float.class) || cls.equals(float.class))
                return new float [alloc];
            else if (cls.equals(Double.class) || cls.equals(double.class))
                return new double [alloc];
            return null;
        }

        @Override
        public int size() {
            return occupied;
        }

        @Override
        public T get(int i) throws BadAccessException {
            if (isSupportedType(getRecordType())) {
                return getRecordType().cast(getField(DEFAULT_NAME,
                        getRecordType(), i));
            }
            else {
                try {
                    T obj = getRecordType().newInstance();
                    for (Field field : getFields()) {
                        field.set(obj, getField(field.getName(),
                                field.getType(), i));
                    }
                    return obj;
                }
                catch (InstantiationException e) {
                    throw new BadAccessException("Could not instantiate type "
                            + "through default constructor: " + e.getMessage());
                } catch (IllegalAccessException e) {
                    throw new BadAccessException("Could not access field: "
                            + e.getMessage());
                }
            }
        }

        protected Object getField(String n, Class<?> cls, int i) {
            Object o = columns.get(n);
            if (cls.equals(Byte.class) || cls.equals(byte.class))
                return ((byte []) o)[i];
            else if (cls.equals(Short.class) || cls.equals(short.class))
                return ((short []) o)[i];
            else if (cls.equals(Character.class) || cls.equals(char.class))
                return ((char []) o)[i];
            else if (cls.equals(Integer.class) || cls.equals(int.class))
                return ((int []) o)[i];
            else if (cls.equals(Long.class) || cls.equals(long.class))
                return ((long []) o)[i];
            else if (cls.equals(Float.class) || cls.equals(float.class))
                return ((float []) o)[i];
            else if (cls.equals(Double.class) || cls.equals(double.class))
                return ((double []) o)[i];
            return null;
        }

        @Override
        public void add(T t) throws BadAccessException {
            if (isSupportedType(getRecordType())) {
                addField(DEFAULT_NAME, getRecordType(), t);
            }
            else {
                try {
                    for (Field field : getFields()) {
                        addField(field.getName(), field.getType(),
                                field.get(t));
                    }
                }
                catch (IllegalAccessException e) {
                    throw new BadAccessException("Could not access field: "
                            + e.getMessage(), e);
                }
            }
            occupied++;
        }

        protected void addField(String n, Class<?> cls, Object v) {
            Object o = columns.get(n);
            if (cls.equals(Byte.class) || cls.equals(byte.class))
                ((byte []) o)[occupied] = (byte) v;
            else if (cls.equals(Short.class) || cls.equals(short.class))
                ((short []) o)[occupied] = (short) v;
            else if (cls.equals(Character.class) || cls.equals(char.class))
                ((char []) o)[occupied] = (char) v;
            else if (cls.equals(Integer.class) || cls.equals(int.class))
                ((int []) o)[occupied] = (int) v;
            else if (cls.equals(Long.class) || cls.equals(long.class))
                ((long []) o)[occupied] = (long) v;
            else if (cls.equals(Float.class) || cls.equals(float.class))
                ((float []) o)[occupied] = (float) v;
            else if (cls.equals(Double.class) || cls.equals(double.class))
                ((double []) o)[occupied] = (double) v;
        }
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
            ColumnStore<TestClass> foo = new ColumnStore<>(TestClass.class, 10);
            ColumnStore<TestClass> bar = new ColumnStore<>(TestClass.class, 10);
            for (int i = 0; i < 100; i++) foo.add(new TestClass(i, i*i));
            for (int i = 1000; i < 1100; i++) bar.add(new TestClass(i, i*i));
            for (TestClass i : foo) System.out.println("foo: " + i);
            System.out.println("foo size: " + foo.size());
            for (TestClass i : bar) System.out.println("bar: " + i);
            System.out.println("barsize: " + bar.size());
            foo.append(bar);
            for (TestClass i : foo) System.out.println("foo: " + i);
            System.out.println("foosize: " + foo.size());
            System.out.println("at 150: " + foo.get(150));
        }
        catch (Exception e) {
            System.err.println("Exception " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
