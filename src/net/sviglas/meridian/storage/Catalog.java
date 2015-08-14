package net.sviglas.meridian.storage;

/**
 * This is part of the Meridian code base, licensed under the
 * Apache License 2.0 (see also
 * http://www.apache.org/licenses/LICENSE-2.0).
 * <p>
 * Created by sviglas on 12/08/15.
 */
public class Catalog {
    private static Catalog ourInstance = new Catalog();

    public static Catalog getInstance() {
        return ourInstance;
    }

    private Catalog() {
    }
}
