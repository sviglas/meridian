/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */

package net.sviglas.meridian.storage;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * A row store lays out the records contiguously in a directly allocated array
 * of bytes.
 *
 * @param <T> the type of records this dataset hosts.
 */
public class RowStore<T> extends AbstractStore<T> {
    // the default chunk size for an allocation
    private static final int CHUNK_SIZE = 4096;

    /**
     * Constructs a new row store given the type of its records.
     *
     * @param c the record type.
     * @throws BadTypeException if the type does not have a parameter-less
     * constructor, or if the type is not a primitive type, or if its fields are
     * not primitive-typed.
     */
    public RowStore(Class<T> c) throws BadTypeException {
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
    public RowStore(Class<T> c, int da) throws BadTypeException {
        super(c, da);
    }

    /**
     * Aligns the allocation size to the size of an operating system page.
     *
     * @param da the original allocation size.
     * @return the aligned allocation size.
     */
    @Override
    protected int fixAllocationSize(int da) {
        return ((da * getRecordSize()) / CHUNK_SIZE + 1) * CHUNK_SIZE;
    }

    /**
     * Internal method to allocate containers of this store.
     *
     * @return a new container for this store.
     */
    @Override
    protected AbstractStoreContainer<T> allocateContainer() {
        return new RowStoreContainer();
    }

    /**
     * Internal class that encapsulates the containers of this store.
     */
    class RowStoreContainer extends AbstractStoreContainer<T> {
        // the contents of the container
        private final ByteBuffer contents;
        // the number of occupied records
        private int occupied;

        /**
         * Default constructor for a column store container.
         */
        public RowStoreContainer() {
            contents = ByteBuffer.allocateDirect(
                    getAllocationSize()* getRecordSize());
            occupied = 0;
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
            contents.position(i* getRecordSize());
            return read();
        }

        /**
         * Reads a record from the current position of the byte buffer.
         *
         * @return the record read.
         * @throws BadAccessException if the record cannot be properly read.
         */
        protected T read() throws BadAccessException {
            if (isSupportedType(getRecordType())) {
                return getRecordType().cast(readField(getRecordType()));
            }
            else {
                try {
                    T obj = getRecordType().newInstance();
                    for (Field field : getFields())
                        field.set(obj, readField(field.getType()));
                    return obj;
                }
                catch (InstantiationException e) {
                    throw new BadAccessException("Could not instantiate type "
                            + "through default constructor: " + e.getMessage());
                }
                catch (IllegalAccessException e) {
                    throw new BadAccessException("Could not access field: "
                            + e.getMessage());
                }
            }
        }

        /**
         * Reads a single field for this class.
         *
         * @param cls the primitive type of field to be read.
         * @return the field's value.
         */
        protected Object readField(Class<?> cls) {
            if (cls.equals(Byte.class) || cls.equals(byte.class))
                return contents.get();
            else if (cls.equals(Short.class) || cls.equals(short.class))
                return contents.getShort();
            else if (cls.equals(Character.class) || cls.equals(char.class))
                return contents.getChar();
            else if (cls.equals(Integer.class) || cls.equals(int.class))
                return contents.getInt();
            else if (cls.equals(Long.class) || cls.equals(long.class))
                return contents.getLong();
            else if (cls.equals(Float.class) || cls.equals(float.class))
                return contents.getLong();
            else if (cls.equals(Double.class) || cls.equals(double.class))
                return contents.getDouble();
            else return null;
        }

        /**
         * Adds a new record to this container.
         *
         * @param t the record to be added.
         * @throws BadAccessException whenever the record cannot be added.
         */
        @Override
        public void add(T t) throws BadAccessException {
            contents.position(occupied* getRecordSize());
            write(t);
            occupied++;
        }

        /**
         * Writes the given record at the current position in the container's
         * buffer.
         *
         * @param t the record to be written.
         * @throws BadAccessException whenever the record cannot be read.
         */
        protected void write(T t) throws BadAccessException {
            if (isSupportedType(t.getClass())) {
                writeField(t);
            }
            else {
                try {
                    for (Field field : getFields()) {
                        writeField(field.get(t));
                    }
                }
                catch (IllegalAccessException e) {
                    throw new BadAccessException("Could not access field: "
                            + e.getMessage(), e);
                }
            }
        }

        /**
         * Writes a single field.
         *
         * @param t the field to be written.
         */
        protected void writeField(Object t) {
            // this is probably nonsensical
            if (t.getClass().equals(Byte.class)
                    || t.getClass().equals(byte.class))
                contents.put((byte) t);
            else if (t.getClass().equals(Short.class)
                    || t.getClass().equals(short.class))
                contents.putShort((short) t);
            else if (t.getClass().equals(Character.class)
                    || t.getClass().equals(char.class))
                contents.putChar((char) t);
            else if (t.getClass().equals(Integer.class)
                    || t.getClass().equals(int.class))
                contents.putInt((int) t);
            else if (t.getClass().equals(Long.class)
                    || t.getClass().equals(long.class))
                contents.putLong((long) t);
            else if (t.getClass().equals(Float.class)
                    || t.getClass().equals(float.class))
                contents.putFloat((float) t);
            else if (t.getClass().equals(Double.class)
                    || t.getClass().equals(double.class))
                contents.putDouble((double) t);
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
            RowStore<TestClass> foo = new RowStore<>(TestClass.class, 10);
            RowStore<TestClass> bar = new RowStore<>(TestClass.class, 10);
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
