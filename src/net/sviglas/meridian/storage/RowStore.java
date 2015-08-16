package net.sviglas.meridian.storage;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */

public class RowStore<T> extends AbstractStore<T> {
    private static final int CHUNK_SIZE = 4096;

    public RowStore(String name, Class<T> type) throws BadTypeException {
        super(name, type);
    }

    public RowStore(String n, Class<T> c, int da) throws BadTypeException {
        super(n, c, da);
    }

    protected int fixAllocationSize(int da) {
        return ((da * getElementSize()) / CHUNK_SIZE + 1) * CHUNK_SIZE;
    }

    @Override
    protected AbstractStoreContainer<T> allocateContainer() {
        return new RowStoreContainer();
    }

    class RowStoreContainer extends AbstractStoreContainer<T> {
        private final ByteBuffer contents;
        private int occupied;

        public RowStoreContainer() {
            contents = ByteBuffer.allocateDirect(
                    getAllocationSize()*getElementSize());
            occupied = 0;
        }

        @Override
        public int size() {
            return occupied;
        }

        @Override
        public T get(int i) throws BadAccessException {
            contents.position(i*getElementSize());
            return read();
        }

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

        @Override
        public void add(T t) throws BadAccessException {
            contents.position(occupied*getElementSize());
            write(t);
            occupied++;
        }

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

    public static void main(String [] s) {
        class TestClass {
            private int key;
            private long value;
            public TestClass() { this(0, 0); }
            public TestClass(int k, long v) { key = k; value = v; }
            public String toString() { return "<" + key + ", " + value + ">"; }
        }
        try {
            RowStore<TestClass> foo =
                    new RowStore<>("foo", TestClass.class, 10);
            RowStore<TestClass> bar =
                    new RowStore<>("bar", TestClass.class, 10);
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
