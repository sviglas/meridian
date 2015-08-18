/*
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */

package net.sviglas.meridian.storage;

/**
 * A container of records used in stores.
 *
 * @param <T> the type of records.
 */
abstract class AbstractStoreContainer<T> {
    // the next container
    private AbstractStoreContainer<T> next;

    /**
     * Constructs a new container.
     */
    public AbstractStoreContainer() { next = null; }

    /**
     * Sets the next container from this one.
     *
     * @param n a reference to the next container.
     */
    public void setNext(AbstractStoreContainer<T> n) { next = n; }

    /**
     * Retrieves the next container from this one.
     *
     * @return a reference to this container's next container.
     */
    public AbstractStoreContainer<T> getNext() { return next; }

    /**
     * Returns the number of records in this container.
     *
     * @return the number of records in this container.
     */
    public abstract int size();

    /**
     * Retrieves the given record of this container.
     *
     * @param i the index of the record to be retrieved.
     * @return the record at the given index of the container.
     * @throws BadAccessException if the record cannot be retrieved.
     */
    public abstract T get(int i) throws BadAccessException;

    /**
     * Adds a new record to this container.
     *
     * @param t the record to be added.
     * @throws BadAccessException if the record cannot be added.
     */
    public abstract void add(T t) throws BadAccessException;
}
