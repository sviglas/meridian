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

class ColumnStoreContainer<T> {
    protected static final String DEFAULT_NAME = "COLUMN";
    protected Class<T> type;
    protected Field [] fields;
    public ColumnStoreContainer<T> next;
    public int occupied;
    public Map<String, Object> columns;
    private boolean primitiveType;

    public ColumnStoreContainer(Class<T> t, int a) {
        type = t;
        fields = null;
        columns = new HashMap<>();
        columns.put(DEFAULT_NAME, makeColumn(type, a));
        primitiveType = true;
    }

    public ColumnStoreContainer(Class<T> t, Field [] fs, int a) {
        type = t;
        fields = fs;
        columns = new HashMap<>();
        for (Field f : fields) {
            columns.put(f.getName(), makeColumn(f.getType(), a));
        }
        primitiveType = false;
    }

    protected Object makeColumn(Class<?> cls, int alloc) {
        if (cls.equals(Byte.class))
            return new byte [alloc];
        else if (cls.equals(Short.class))
            return new short [alloc];
        else if (cls.equals(Character.class))
            return new char [alloc];
        else if (cls.equals(Integer.class))
            return new int [alloc];
        else if (cls.equals(Long.class))
            return new long [alloc];
        else if (cls.equals(Float.class))
            return new float [alloc];
        else if (cls.equals(Double.class))
            return new double [alloc];
        return null;
    }

    public T get(int i) throws BadAccessException {
        if (primitiveType) {
            return type.cast(getField(DEFAULT_NAME, type, i));
        }
        else {
            try {
                T obj = type.newInstance();
                for (Field field : fields) {
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
        if (cls.equals(Byte.class))
            return ((byte []) o)[i];
        else if (cls.equals(Short.class))
            return ((short []) o)[i];
        else if (cls.equals(Character.class))
            return ((char []) o)[i];
        else if (cls.equals(Integer.class))
            return ((int []) o)[i];
        else if (cls.equals(Long.class))
            return ((long []) o)[i];
        else if (cls.equals(Float.class))
            return ((float []) o)[i];
        else if (cls.equals(Double.class))
            return ((double []) o)[i];
        return null;
    }

    protected void add(T t) {
        if (primitiveType) {
            addField(DEFAULT_NAME, type, t);
        }
        else {
            try {
                for (Field field : fields) {
                    addField(field.getName(), field.getType(), field.get(t));
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
        if (cls.equals(Byte.class))
            ((byte []) o)[occupied] = (byte) v;
        else if (cls.equals(Short.class))
            ((short []) o)[occupied] = (short) v;
        else if (cls.equals(Character.class))
            ((char []) o)[occupied] = (char) v;
        else if (cls.equals(Integer.class))
            ((int []) o)[occupied] = (int) v;
        else if (cls.equals(Long.class))
            ((long []) o)[occupied] = (long) v;
        else if (cls.equals(Float.class))
            ((float []) o)[occupied] = (float) v;
        else if (cls.equals(Double.class))
            ((double []) o)[occupied] = (double) v;
    }
}


public class ColumnStore<T> extends Dataset<T> {
    private static final int DEFAULT_ALLOCATION = 1000;
    private final int defaultAllocation;
    private ColumnStoreContainer<T> head;
    private ColumnStoreContainer<T> tail;
    private int size;

    public ColumnStore(String n, Class<T> c) throws BadTypeException {
        this(n, c, DEFAULT_ALLOCATION);
    }

    public ColumnStore(String n, Class<T> c, int da) throws BadTypeException {
        super(n, c);
        defaultAllocation = da;
        head = null;
        tail = null;
        size = 0;
    }

    @Override
    public long size() { return size; }

    @Override
    public void add(T t) throws BadAccessException {
        if (head != null) {
            if (tail.occupied == defaultAllocation) {
                ColumnStoreContainer<T> newTail = allocateContainer();
                newTail.add(t);
                tail.next = newTail;
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

    protected ColumnStoreContainer<T> allocateContainer() {
        if (isSupportedType(getRecordType()))
            return new ColumnStoreContainer<>(getRecordType(),
                    defaultAllocation);
        else
            return new ColumnStoreContainer<>(getRecordType(), getFields(),
                    defaultAllocation);
    }

    @Override
    public T get(long i) throws IndexOutOfBoundsException, BadAccessException {
        long accumulated = 0;
        ColumnStoreContainer<T> current = head;
        while (current != null) {
            if (accumulated + current.occupied > i) {
                return current.get((int) (i - accumulated));
            }
            else {
                accumulated += current.occupied;
                current = current.next;
            }
        }
        throw new IndexOutOfBoundsException("Out of bounds: " + i + " > "
                + size);
    }

    @Override
    public void append(Dataset<T> d) throws BadAccessException {
        if (d.getClass().equals(this.getClass())) {
            ColumnStore<T> cd = (ColumnStore<T>) d;
            tail.next = cd.head;
            tail = cd.tail;
            ColumnStoreContainer<T> current = cd.head;
            while (current != null) {
                size += current.occupied;
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
            ColumnStoreContainer<T> currentContainer = head;
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
                if (currentContainer != null) {
                    return currentContainer.get(currentCounter++);
                }
                return null;
            }
        };
    }

    public static void main(String [] s) {
        try {
            ColumnStore<Integer> lala =
                    new ColumnStore<>("lala", Integer.class, 10);
            ColumnStore<Integer> koko =
                    new ColumnStore<>("koko", Integer.class, 10);
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
