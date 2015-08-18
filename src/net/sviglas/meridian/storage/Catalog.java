/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */

package net.sviglas.meridian.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Initial effort from building a catalog of datasets.
 */
public class Catalog {
    // singleton
    private static Catalog ourInstance = new Catalog();
    // map of datasets
    private static final Map<UUID, Dataset<?>> datasets = new HashMap<>();

    /**
     * Retrieves the unique catalog.
     *
     * @return the catalog.
     */
    public static Catalog getInstance() {
        return ourInstance;
    }

    /**
     * Default constructor.
     */
    private Catalog() {
    }

    /**
     * Creates a new array store given a type.
     *
     * @param type the type of the dataset's records.
     * @param <T> the generic type of records.
     * @return an array store for records of the given type.
     * @throws BadTypeException if the type is not a primitive one, or if it
     * does not consist of primitive-typed fields, or if it does not have a
     * parameter-less constructor.
     */
    public static <T> ArrayStore<T> createArrayStore(Class<T> type)
            throws BadTypeException {
        ArrayStore<T> as = new ArrayStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    /**
     * Create a new array store given a type.
     *
     * @param type the type of the dataset's records.
     * @param da the allocation increment.
     * @param <T> the generic type of records.
     * @return an array store for records of the given type.
     * @throws BadTypeException if the type is not a primitive one, or if it
     * does not consist of primitive-typed fields, or if it does not have a
     * parameter-less constructor.
     */
    public static <T> ArrayStore<T> createArrayStore(Class<T> type,
                                                     int da)
            throws BadTypeException {
        ArrayStore<T> as = new ArrayStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    /**
     * Creates a new row store given a type.
     *
     * @param type the type of the dataset's records.
     * @param <T> the generic type of records.
     * @return an array store for records of the given type.
     * @throws BadTypeException if the type is not a primitive one, or if it
     * does not consist of primitive-typed fields, or if it does not have a
     * parameter-less constructor.
     */
    public static <T> RowStore<T> createRowStore(Class<T> type)
            throws BadTypeException {
        RowStore<T> as = new RowStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    /**
     * Create a new row store given a type.
     *
     * @param type the type of the dataset's records.
     * @param da the allocation increment.
     * @param <T> the generic type of records.
     * @return an array store for records of the given type.
     * @throws BadTypeException if the type is not a primitive one, or if it
     * does not consist of primitive-typed fields, or if it does not have a
     * parameter-less constructor.
     */
    public static <T> RowStore<T> createRowStore(Class<T> type,
                                                 int da)
            throws BadTypeException {
        RowStore<T> as = new RowStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    /**
     * Creates a new column store given a type.
     *
     * @param type the type of the dataset's records.
     * @param <T> the generic type of records.
     * @return an array store for records of the given type.
     * @throws BadTypeException if the type is not a primitive one, or if it
     * does not consist of primitive-typed fields, or if it does not have a
     * parameter-less constructor.
     */
    public static <T> ColumnStore<T> createColumnStore(Class<T> type)
            throws BadTypeException {
        ColumnStore<T> as = new ColumnStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    /**
     * Create a new column store given a type.
     *
     * @param type the type of the dataset's records.
     * @param da the allocation increment.
     * @param <T> the generic type of records.
     * @return an array store for records of the given type.
     * @throws BadTypeException if the type is not a primitive one, or if it
     * does not consist of primitive-typed fields, or if it does not have a
     * parameter-less constructor.
     */
    public static <T> ColumnStore<T> createColumnStore(Class<T> type,
                                                       int da)
            throws BadTypeException {
        ColumnStore<T> as = new ColumnStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    /**
     * Retrieves the dataset associated with the given identifier.
     *
     * @param uuid the identifier.
     * @return the dataset associated with the given identifier.
     */
    public static Dataset<?> getDataset(UUID uuid) {
        return datasets.get(uuid);
    }

}
