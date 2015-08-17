package net.sviglas.meridian.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */
public class Catalog {
    private static Catalog ourInstance = new Catalog();
    private static final Map<UUID, Dataset<?>> datasets = new HashMap<>();

    public static Catalog getInstance() {
        return ourInstance;
    }

    private Catalog() {
    }

    public static <T> ArrayStore<T> createArrayStore(Class<T> type)
            throws BadTypeException {
        ArrayStore<T> as = new ArrayStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    public static <T> ArrayStore<T> createArrayStore(Class<T> type,
                                                     int da)
            throws BadTypeException {
        ArrayStore<T> as = new ArrayStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    public static <T> RowStore<T> createRowStore(Class<T> type)
            throws BadTypeException {
        RowStore<T> as = new RowStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    public static <T> RowStore<T> createRowStore(Class<T> type,
                                                 int da)
            throws BadTypeException {
        RowStore<T> as = new RowStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    public static <T> ColumnStore<T> createColumnStore(Class<T> type)
            throws BadTypeException {
        ColumnStore<T> as = new ColumnStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    public static <T> ColumnStore<T> createColumnStore(Class<T> type,
                                                       int da)
            throws BadTypeException {
        ColumnStore<T> as = new ColumnStore<>(type);
        datasets.put(as.getIdentifier(), as);
        return as;
    }

    public static Dataset<?> getDataset(UUID uuid) {
        return datasets.get(uuid);
    }

}
