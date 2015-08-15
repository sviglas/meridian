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

class RowStoreContainer<T> {
    public ByteBuffer contents;
    public int occupied;
    public RowStoreContainer<T> next;
    public RowStoreContainer(ByteBuffer b) {
        contents = b;
        occupied = 0;
        next = null;

    }
}

public class RowStore<T> extends Dataset<T> {
    private static final int CHUNK_SIZE = 4096;
    private static final int DEFAULT_ALLOCATION = 1000;
    private final int defaultAllocation;
    private RowStoreContainer<T> head;
    private RowStoreContainer<T> tail;
    private int size;

    public RowStore(String name, Class<T> type) throws BadTypeException {
        this(name, type, DEFAULT_ALLOCATION);
    }

    public RowStore(String n, Class<T> c, int da) throws BadTypeException {
        super(n, c);
        defaultAllocation =
                ((da * getElementSize()) / CHUNK_SIZE + 1) * CHUNK_SIZE;
        head = null;
        tail = null;
        size = 0;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public void add(T t) {
        if (head != null) {
            if (tail.occupied == defaultAllocation) {
                RowStoreContainer<T> newTail = new RowStoreContainer<>(
                        ByteBuffer.allocateDirect(
                                defaultAllocation*getElementSize()));
                newTail.contents.position(0);
                write(newTail.contents, t);
                newTail.occupied++;
                tail.next = newTail;
                tail = newTail;
            }
            else {
                tail.contents.position(tail.occupied*getElementSize());
                write(tail.contents, t);
                tail.occupied++;
            }
        }
        else {
            head = new RowStoreContainer<>(ByteBuffer.allocateDirect(
                    defaultAllocation*getElementSize()));
            head.contents.position(0);
            write(head.contents, t);
            head.occupied++;
            tail = head;
        }
        size++;
    }

    @Override
    public T get(long i) throws IndexOutOfBoundsException, BadAccessException {
        long accumulated = 0;
        RowStoreContainer<T> current = head;
        while (current != null) {
            if (accumulated + current.occupied > i) {
                current.contents.position(
                        ((int) (i - accumulated))*getElementSize());
                return read(current.contents);
            }
            else {
                accumulated += current.occupied;
                current = current.next;
            }
        }
        throw new IndexOutOfBoundsException("Out of bounds: " + i + " > "
                + size);
    }

    protected T read(ByteBuffer buffer) throws BadAccessException {
        if (isSupportedType(getRecordType())) {
            return getRecordType().cast(readField(buffer, getRecordType()));
        }
        else {
            try {
                T obj = getRecordType().newInstance();
                for (Field field : getFields())
                    field.set(obj, readField(buffer, field.getType()));
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

    protected Object readField(ByteBuffer buffer, Class<?> cls) {
        if (cls.equals(Byte.class))
            return buffer.get();
        else if (cls.equals(Short.class))
            return buffer.getShort();
        else if (cls.equals(Character.class))
            return buffer.getChar();
        else if (cls.equals(Integer.class))
            return buffer.getInt();
        else if (cls.equals(Long.class))
            return buffer.getLong();
        else if (cls.equals(Float.class))
            return buffer.getFloat();
        else if (cls.equals(Double.class))
            return buffer.getDouble();
        else return null;
    }

    @Override
    public void append(Dataset<T> d) throws BadAccessException {
        if (d.getClass().equals(this.getClass())) {
            RowStore<T> ad = (RowStore<T>) d;
            tail.next = ad.head;
            tail = ad.tail;
            RowStoreContainer<T> current = ad.head;
            while (current != null) {
                size += current.occupied;
                current = current.next;
            }
        }
        else {
            for (T t : d) add(t);
        }

    }

    protected void write(ByteBuffer buffer, T t) throws BadAccessException {
        if (isSupportedType(t.getClass())) {
            writeField(buffer, t);
        }
        else {
            try {
                for (Field field : getFields()) {
                    writeField(buffer, field.get(t));
                }
            }
            catch (IllegalAccessException e) {
                throw new BadAccessException("Could not access field: "
                        + e.getMessage(), e);
            }
        }
    }

    protected void writeField(ByteBuffer buffer, Object t) {
        // this is probably nonsensical
        if (t.getClass().equals(Byte.class))
            writeField(buffer, (Byte) t);
        else if (t.getClass().equals(Short.class))
            writeField(buffer, (Short) t);
        else if (t.getClass().equals(Character.class))
            writeField(buffer, (Character) t);
        else if (t.getClass().equals(Integer.class))
            writeField(buffer, (Integer) t);
        else if (t.getClass().equals(Long.class))
            writeField(buffer, (Long) t);
        else if (t.getClass().equals(Float.class))
            writeField(buffer, (Float) t);
        else if (t.getClass().equals(Double.class))
            writeField(buffer, (Double) t);
    }

    protected void writeField(ByteBuffer buffer, Integer t) {
        buffer.putInt(t);
    }

    protected void writeField(ByteBuffer buffer, Long t) {
        buffer.putLong(t);
    }

    protected void writeField(ByteBuffer buffer, Float t) {
        buffer.putFloat(t);
    }

    protected void writeField(ByteBuffer buffer, Double t) {
        buffer.putDouble(t);
    }

    protected void writeField(ByteBuffer buffer, Byte t) {
        buffer.put(t);
    }

    protected void writeField(ByteBuffer buffer, Short t) {
        buffer.putShort(t);
    }

    protected void writeField(ByteBuffer buffer, Character t) {
        buffer.putChar(t);
    }


    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            RowStoreContainer<T> currentContainer = head;
            int currentCounter = 0;

            @Override
            public boolean hasNext() {
                if (currentContainer == null) return false;
                if (currentCounter < currentContainer.occupied) return true;
                if (currentContainer.next == null) {
                    currentContainer = null;
                    currentCounter = 0;
                    return false;
                }
                do {
                    currentContainer = currentContainer.next;
                }
                while (currentContainer != null
                        && currentContainer.occupied == 0);
                if (currentContainer == null) return false;
                currentCounter = 0;
                return true;
            }

            @Override
            public T next() {
                if (currentContainer != null)  {
                    currentContainer.contents.position(
                            currentCounter*getElementSize());
                    currentCounter++;
                    return read(currentContainer.contents);
                }
                return null;
            }
        };
    }

    public static void main(String [] s) {
        try {
            RowStore<Integer> lala = new RowStore<>("lala", Integer.class, 10);
            RowStore<Integer> koko = new RowStore<>("koko", Integer.class, 10);
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
