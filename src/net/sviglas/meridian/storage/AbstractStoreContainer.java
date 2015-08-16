package net.sviglas.meridian.storage;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 16/08/15.
 */
abstract class AbstractStoreContainer<T> {
    private AbstractStoreContainer<T> next;

    public AbstractStoreContainer() { next = null; }

    public void setNext(AbstractStoreContainer<T> n) { next = n; }

    public AbstractStoreContainer<T> getNext() { return next; }

    public abstract int size();
    public abstract T get(int i) throws BadAccessException;
    public abstract void add(T t) throws BadAccessException;
}
