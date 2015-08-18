/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 14/08/15.
 */

package net.sviglas.meridian.storage;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * A column store effectively decomposes the dataset into multiple columns, one
 * for each field of the record type.
 *
 * @param <T> the record type.
 */

public class ColumnStore<T> extends AbstractStore<T> {

    /**
     * Constructs a new column store given the type of its records.
     *
     * @param c the record type.
     * @throws BadTypeException if the type does not have a parameter-less
     * constructor, or if the type is not a primitive type, or if its fields are
     * not primitive-typed.
     */
    public ColumnStore(Class<T> c) throws BadTypeException {
        super(c);
    }

    /**
     * Constructs a new column store given the type of its records and its
     * allocation increment.
     *
     * @param c the record type.
     * @param da the allocation increment.
     * @throws BadTypeException if the type does not have a parameter-less
     * constructor, or if the type is not a primitive type, or if its fields are
     * not primitive-typed.
     */
    public ColumnStore(Class<T> c, int da) throws BadTypeException {
        super(c, da);
    }

    /**
     * Allocates a new container for this type of store.
     *
     * @return a new container for the store.
     */
    @Override
    protected AbstractStoreContainer<T> allocateContainer() {
        return new ColumnStoreContainer();
    }

    /**
     * Internal class that encapsulates the containers of this store.
     */
    class ColumnStoreContainer extends AbstractStoreContainer<T> {
        // default column name
        protected static final String DEFAULT_NAME = "COLUMN";
        // map of columns for this container
        public Map<String, Object> columns;
        // number of records occupied
        public int occupied;

        /**
         * Default constructor for a column store container.
         */
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

        /**
         * Given a primitive type, return a column for elements of that type.
         *
         * @param cls the primitive type.
         * @param alloc the number of elements to allocate.
         * @return an array of elements of the given type.
         */
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

        /**
         * Returns the size of this container.
         *
         * @return the size of this container.
         */
        @Override
        public int size() {
            return occupied;
        }

        /**
         * Retrieves the record at the given index.
         *
         * @param i the index of the record to be retrieved.
         * @return the record at the given index.
         * @throws BadAccessException if the record cannot be retrieved.
         */
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

        /**
         * Internal method to get a single field.
         *
         * @param n the name of the field.
         * @param cls the type of the field.
         * @param i the index of the field.
         * @return the value of the field.
         */
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

        /**
         * Adds a new record to this container.
         *
         * @param t the record to be added.
         * @throws BadAccessException whenever the record cannot be added.
         */
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

        /**
         * Internal method to add a single field.
         *
         * @param n the name of the field.
         * @param cls the type of the field.
         * @param v the value of the field.
         */
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
