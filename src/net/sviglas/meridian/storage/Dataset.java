package net.sviglas.meridian.storage;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */
public abstract class Dataset<T> implements Iterable<T> {
    private final static Map<Class<?>, Integer> SUPPORTED_TYPES;
    static {
        Map<Class<?>, Integer> theMap = new HashMap<>();
        theMap.put(Byte.class, 1);
        theMap.put(byte.class, 1);
        theMap.put(Short.class, 2);
        theMap.put(short.class, 2);
        theMap.put(Character.class, 2);
        theMap.put(char.class, 2);
        theMap.put(Integer.class, 4);
        theMap.put(int.class, 4);
        theMap.put(Long.class, 8);
        theMap.put(long.class, 8);
        theMap.put(Float.class, 4);
        theMap.put(float.class, 4);
        theMap.put(Double.class, 8);
        theMap.put(double.class, 8);
        SUPPORTED_TYPES = Collections.unmodifiableMap(theMap);
    }
    private final static Catalog catalog = Catalog.getInstance();
    private final String name;
    private Class<T> recordType;
    private int elementSize;
    private Field [] fields;

    /**
     * Constructs a dataset with the given name, hosting element of the given
     * type. The type must have a parameter-less constructor.
     * @param n the name of the dataset.
     * @param c the type of the elements this dataset hosts.
     * @throws BadTypeException if the element type is not acceptable (i.e., it
     * does not comprise primitive types and does not have a parameter-less
     * constructor).
     */
    public Dataset(String n, Class<T> c) throws BadTypeException {
        name = n;
        recordType = c;
        validateType();
    }

    public int getElementSize() {
        return elementSize;
    }

    protected boolean isSupportedType(Class<?> c) {
        return SUPPORTED_TYPES.containsKey(c);
    }

    protected int byteSize(Class<?> c) {
        Integer i = SUPPORTED_TYPES.get(c);
        return i != null ? i : -1;
    }

    protected Field [] getFields() {
        return fields;
    }

    protected void validateType() throws BadTypeException {
        if (isSupportedType(recordType)) {
            elementSize = byteSize(recordType);
            fields = null;
            return;
        }
        try {
            recordType.getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new BadTypeException("Type: " + recordType + " does not "
                    + "have a default constructor.");
        }
        fields = recordType.getDeclaredFields();
        elementSize = 0;
        for (Field field : fields) {
            // hack maybe
            try {
                field.setAccessible(true);
            }
            catch (SecurityException se) {
                throw new BadTypeException("Could not change accessibility "
                        + "through reflection for: " + field.getName() + ": "
                        + se.getMessage());
            }
            Class<?> type = field.getType();
            if (! isSupportedType(type)) {
                throw new BadTypeException("Type: " + type + " is not "
                        + "supported for datasets; the only supported types "
                        + "are: " + SUPPORTED_TYPES.toString());
            }
            elementSize += byteSize(field.getType());
        }
    }

    /**
     * Returns the internal record type.
     * @return the internal record type.
     */
    public Class<T> getRecordType() {
        return recordType;
    }

    /**
     * Returns the catalog responsible for maintaining this dataset.
     * @return the catalog responsible for maintaining this dataset.
     */
    protected Catalog getCatalog() {
        return catalog;
    }

    /**
     * Returns the number of elements in this dataset.
     * @return the number of elements in this dataset.
     */
    public abstract long size();

    /**
     * Adds the given element to this dataset.
     * @param t the element to be added.
     */
    public abstract void add(T t) throws BadAccessException;

    /**
     *
     * Retrieves the element with the given index from this dataset.
     * @param i the index of the element to be retrieved.
     * @return the element at index i.
     * @throws IndexOutOfBoundsException if the index is out of range.
     */
    public abstract T get(long i)
            throws IndexOutOfBoundsException, BadAccessException;

    /**
     * Appends another dataset to this one.
     * @param d the dataset to be appended to this.
     */
    public abstract void append(Dataset<T> d) throws BadAccessException;

    /**
     * Returns an iterator over the elements of this dataset.
     * @return an iterator over this dataset's elements.
     */
    public abstract Iterator<T> iterator();
}
