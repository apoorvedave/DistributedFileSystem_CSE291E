package common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Distributed filesystem paths.
 * <p/>
 * <p/>
 * Objects of type <code>Path</code> are used by all filesystem interfaces.
 * Path objects are immutable.
 * <p/>
 * <p/>
 * The string representation of paths is a forward-slash-delimeted sequence of
 * path components. The root directory is represented as a single forward
 * slash.
 * <p/>
 * <p/>
 * The colon (<code>:</code>) and forward slash (<code>/</code>) characters are
 * not permitted within path components. The forward slash is the delimeter,
 * and the colon is reserved as a delimeter for application use.
 */
public class Path implements Iterable<String>, Comparable<Path>, Serializable {
    private ArrayList<String> components;

    /**
     * Creates a new path which represents the root directory.
     */
    public Path() {
        components = new ArrayList<>();
    }

    /**
     * Creates a new path by appending the given component to an existing path.
     *
     * @param path      The existing path.
     * @param component The new component.
     * @throws IllegalArgumentException If <code>component</code> includes the
     *                                  separator, a colon, or
     *                                  <code>component</code> is the empty
     *                                  string.
     */
    public Path(Path path, String component) {
        this();
        if (component == null || component.isEmpty() || component.contains("/") ||
                component.contains(":")) {
            throw new IllegalArgumentException("Invalid argument(s)");
        }
        this.components.addAll(path.components);
        components.add(component);
    }

    /**
     * Creates a new path from a path string.
     * <p/>
     * <p/>
     * The string is a sequence of components delimited with forward slashes.
     * Empty components are dropped. The string must begin with a forward
     * slash.
     *
     * @param path The path string.
     * @throws IllegalArgumentException If the path string does not begin with
     *                                  a forward slash, or if the path
     *                                  contains a colon character.
     */
    public Path(String path) {

        this();

        if (path == null || path.isEmpty() || !path.startsWith("/") || path.contains(":")) {
            throw new IllegalArgumentException("Invalid argument(s)");
        }
        String[] comps = path.split("/");
        for (String comp : comps) {
            if (!comp.isEmpty()) {
                components.add(comp);
            }
        }
    }

    /**
     * Returns an iterator over the components of the path.
     * <p/>
     * <p/>
     * The iterator cannot be used to modify the path object - the
     * <code>remove</code> method is not supported.
     *
     * @return The iterator.
     */
    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {

            Iterator<String> it = components.iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public String next() {
                return it.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not supported");
            }
        };
    }

    /**
     * Lists the paths of all files in a directory tree on the local
     * filesystem.
     *
     * @param directory The root directory of the directory tree.
     * @return An array of relative paths, one for each file in the directory
     * tree.
     * @throws FileNotFoundException    If the root directory does not exist.
     * @throws IllegalArgumentException If <code>directory</code> exists but
     *                                  does not refer to a directory.
     */
    public static Path[] list(File directory) throws FileNotFoundException {

        if (!directory.exists()) {
            throw new FileNotFoundException("The requested directory does not exist");
        }

        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("The requested directory is not a directory");
        }

        final ArrayList<Path> paths = new ArrayList<>();

        final java.nio.file.Path dir = directory.toPath();
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<java.nio.file.Path>() {
                @Override
                public FileVisitResult visitFile(java.nio.file.Path file,
                                                 BasicFileAttributes attrs) throws IOException {
                    paths.add(new Path("/" + dir.relativize(file).toString()));
                    return super.visitFile(file, attrs);
                }
            });
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            System.out.println("Unexpected Exception" + e);
        }
        return paths.toArray(new Path[paths.size()]);
    }

    /**
     * Determines whether the path represents the root directory.
     *
     * @return <code>true</code> if the path does represent the root directory,
     * and <code>false</code> if it does not.
     */
    public boolean isRoot() {
        return components.size() == 0;
    }

    /**
     * Returns the path to the parent of this path.
     *
     * @throws IllegalArgumentException If the path represents the root
     *                                  directory, and therefore has no parent.
     */
    public Path parent() {
        if (isRoot()) {
            throw new IllegalArgumentException("Root does not have parent");
        }
        Path parent = new Path();
        parent.components = new ArrayList<>(components.subList(0, components.size() - 1));
        return parent;
    }

    /**
     * Returns the last component in the path.
     *
     * @throws IllegalArgumentException If the path represents the root
     *                                  directory, and therefore has no last
     *                                  component.
     */
    public String last() {
        if (isRoot()) {
            throw new IllegalArgumentException("Root does not have last component");
        }
        return components.get(components.size() - 1);
    }

    /**
     * Determines if the given path is a subpath of this path.
     * <p/>
     * <p/>
     * The other path is a subpath of this path if it is a prefix of this path.
     * Note that by this definition, each path is a subpath of itself.
     *
     * @param other The path to be tested.
     * @return <code>true</code> If and only if the other path is a subpath of
     * this path.
     */
    public boolean isSubpath(Path other) {
        if (other == null || other.components.size() > components.size()) {
            return false;
        }

        for (int i = 0; i < other.components.size(); i++) {
            if (!other.components.get(i).equals(components.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Converts the path to <code>File</code> object.
     *
     * @param root The resulting <code>File</code> object is created relative
     *             to this directory.
     * @return The <code>File</code> object.
     */
    public File toFile(File root) {
        return new File(root, this.toString());
    }

    /**
     * Compares this path to another.
     * <p/>
     * <p/>
     * An ordering upon <code>Path</code> objects is provided to prevent
     * deadlocks between applications that need to lock multiple filesystem
     * objects simultaneously. By convention, paths that need to be locked
     * simultaneously are locked in increasing order.
     * <p/>
     * <p/>
     * Because locking a path requires locking every component along the path,
     * the order is not arbitrary. For example, suppose the paths were ordered
     * first by length, so that <code>/etc</code> precedes
     * <code>/bin/cat</code>, which precedes <code>/etc/dfs/conf.txt</code>.
     * <p/>
     * <p/>
     * Now, suppose two users are running two applications, such as two
     * instances of <code>cp</code>. One needs to work with <code>/etc</code>
     * and <code>/bin/cat</code>, and the other with <code>/bin/cat</code> and
     * <code>/etc/dfs/conf.txt</code>.
     * <p/>
     * <p/>
     * Then, if both applications follow the convention and lock paths in
     * increasing order, the following situation can occur: the first
     * application locks <code>/etc</code>. The second application locks
     * <code>/bin/cat</code>. The first application tries to lock
     * <code>/bin/cat</code> also, but gets blocked because the second
     * application holds the lock. Now, the second application tries to lock
     * <code>/etc/dfs/conf.txt</code>, and also gets blocked, because it would
     * need to acquire the lock for <code>/etc</code> to do so. The two
     * applications are now deadlocked.
     *
     * @param other The other path.
     * @return Zero if the two paths are equal, a negative number if this path
     * precedes the other path, or a positive number if this path
     * follows the other path.
     */
    @Override
    public int compareTo(Path other) {
        return this.toString().compareTo(other.toString());
    }

    /**
     * Compares two paths for equality.
     * <p/>
     * <p/>
     * Two paths are equal if they share all the same components.
     *
     * @param other The other path.
     * @return <code>true</code> if and only if the two paths are equal.
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof Path) {
            return this.toString().equals(other.toString());
        }
        return false;
    }

    /**
     * Returns the hash code of the path.
     */
    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * Converts the path to a string.
     * <p/>
     * <p/>
     * The string may later be used as an argument to the
     * <code>Path(String)</code> constructor.
     *
     * @return The string representation of the path.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("/");
        Iterator<String> it = components.iterator();
        while (it.hasNext()) {
            sb.append(it.next());
            if (it.hasNext()) {
                sb.append("/");
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {

        Path p = new Path("/Users/apoorve/Documents/documents");
        Path q = new Path(p, "books");
        System.out.println(p);
        System.out.println(q.isSubpath(p));
        System.out.println(p.isSubpath(q));

        File f = new File("/Users/apoorve/Documents/documents");
        System.out.println(Arrays.toString(Path.list(f)));
        Iterator<String> it = p.iterator();
        it.next();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
        System.out.println(new Path("/users").parent());
        System.out.println(Path.list(f)[1].components);
    }
}
