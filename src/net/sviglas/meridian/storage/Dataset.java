/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */

package net.sviglas.meridian.storage;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * The base class of all in-memory datasets. As a first approximation this is
 * a container that one can add records to, append other datasets to and
 * iterate over.
 *
 * @param <T> the records of the dataset; these records must only consist
 *           of primitive types.
 */

public abstract class Dataset<T> implements Iterable<T> {
    // static map of supported types
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
    // the internal identifier of the dataset (in case we want to build a 
    // catalog) 
    private final UUID identifier;
    // the type of records
    private Class<T> recordType;
    // the size of the record in bytes
    private int recordSize;
    // the fields of the record, as returned through reflections.
    private Field [] fields;

    /**
     * Constructs a dataset with the given name, hosting record of the given
     * type. The type must have a parameter-less constructor.
     * 
     * @param c the type of the records this dataset hosts.
     * @throws BadTypeException if the record type is not acceptable (i.e., it
     * does not comprise primitive types and does not have a parameter-less
     * constructor).
     */
    public Dataset(Class<T> c) throws BadTypeException {
        identifier = UUID.randomUUID();
        recordType = c;
        validateType();
    }

    /**
     * Returns the identifier of this dataset.
     * 
     * @return this dataset's identifier.
     */
    public UUID getIdentifier() { return identifier; }

    /**
     * Returns the size of the dataset's records in bytes.
     * 
     * @return the number of bytes of this dataset's records.
     */
    public int getRecordSize() {
        return recordSize;
    }

    /**
     * Checks whether a given type is supported or not.
     * 
     * @param c the type to check.
     * @return true if the type is supported, false otherwise.
     */
    protected boolean isSupportedType(Class<?> c) {
        return SUPPORTED_TYPES.containsKey(c);
    }

    /**
     * Given a type, return the number of bytes in its fields.
     * 
     * @param c the type.
     * @return the number of bytes necessary to serialize this type.
     */
    protected int byteSize(Class<?> c) {
        Integer i = SUPPORTED_TYPES.get(c);
        return i != null ? i : -1;
    }

    /**
     * Retrieves the fields of this dataset's records.
     * 
     * @return the fields of the records of this dataset.
     */
    protected Field [] getFields() {
        return fields;
    }

    /**
     * Validates that the type of the dataset's records consists of
     * primitive-typed fields only.
     * 
     * @throws BadTypeException if there are non-primitive fields in this
     * dataset's record type.
     */
    protected void validateType() throws BadTypeException {
        if (isSupportedType(recordType)) {
            recordSize = byteSize(recordType);
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
        recordSize = 0;
        for (Field field : fields) {
            // hack maybe?
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
            recordSize += byteSize(field.getType());
        }
    }

    /**
     * Returns the internal record type.
     *
     * @return the internal record type.
     */
    public Class<T> getRecordType() {
        return recordType;
    }

    // /**
    // * Returns the catalog responsible for maintaining this dataset.
    // * @return the catalog responsible for maintaining this dataset.
    // */
    //protected Catalog getCatalog() {
    //    return catalog;
    //}

    /**
     * Returns the number of records in this dataset.
     *
     * @return the number of records in this dataset.
     */
    public abstract long size();

    /**
     * Adds the given record to this dataset.
     *
     * @param t the record to be added.
     * @throws BadAccessException whenever the record cannot be added.
     */
    public abstract void add(T t) throws BadAccessException;

    /**
     * Retrieves the record with the given index from this dataset.
     * @param i the index of the record to be retrieved.
     *
     * @return the record at index i.
     * @throws IndexOutOfBoundsException if the index is out of range.
     * @throws BadAccessException if there are problems with the added record.
     */
    public abstract T get(long i)
            throws IndexOutOfBoundsException, BadAccessException;

    /**
     * Appends another dataset to this one.
     *
     * @param d the dataset to be appended to this.
     */
    public abstract void append(Dataset<T> d) throws BadAccessException;

    /**
     * Returns an iterator over the records of this dataset.
     *
     * @return an iterator over this dataset's records.
     */
    public abstract Iterator<T> iterator();

    /**
     * Prints out the contents of this dataset.
     *
     * @param ps the print stream to print the contents to.
     */
    public void print(PrintStream ps) {
        ps.print("[");
        Iterator<T> it = iterator();
        while (it.hasNext()) {
            ps.print(it.next());
            if (it.hasNext()) ps.print(", ");
        }
        ps.print("]");
    }
}
