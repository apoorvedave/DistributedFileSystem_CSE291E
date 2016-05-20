package naming;

import common.Path;
import rmi.RMIException;
import rmi.Skeleton;
import storage.Command;
import storage.Storage;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Naming server.
 * <p/>
 * <p/>
 * Each instance of the filesystem is centered on a single naming server. The
 * naming server maintains the filesystem directory tree. It does not store any
 * file data - this is done by separate storage servers. The primary purpose of
 * the naming server is to map each file name (path) to the storage server
 * which hosts the file's contents.
 * <p/>
 * <p/>
 * The naming server provides two interfaces, <code>Service</code> and
 * <code>Registration</code>, which are accessible through RMI. Storage servers
 * use the <code>Registration</code> interface to inform the naming server of
 * their existence. Clients use the <code>Service</code> interface to perform
 * most filesystem operations. The documentation accompanying these interfaces
 * provides details on the methods supported.
 * <p/>
 * <p/>
 * Stubs for accessing the naming server must typically be created by directly
 * specifying the remote network address. To make this possible, the client and
 * registration interfaces are available at well-known ports defined in
 * <code>NamingStubs</code>.
 */
public class NamingServerCopy implements Service, Registration {

    //Skeleton for Service and Registration Interfaces
    private Skeleton<Service> service;
    private Skeleton<Registration> registration;

    // Maintains a set of available storageServers
    private Set<StorageServer> storageServers;

    // Maintains mapping of path to set of storageServers containing that path
    private Map<Path, Set<StorageServer>> fileMap;

    // Maintains set of available directories in the system
    private Set<Path> directorySet;

    /**
     * Creates the naming server object.
     * <p/>
     * <p/>
     * The naming server is not started.
     */
    public NamingServerCopy() {
        storageServers = new HashSet<>();
        directorySet = new HashSet<>();
        directorySet.add(new Path("/"));

        fileMap = new HashMap<>();
        service = new Skeleton<>(Service.class, this,
                new InetSocketAddress(NamingStubs.SERVICE_PORT));
        registration = new Skeleton<>(Registration.class, this,
                new InetSocketAddress(NamingStubs.REGISTRATION_PORT));
    }

    /**
     * Starts the naming server.
     * <p/>
     * <p/>
     * After this method is called, it is possible to access the client and
     * registration interfaces of the naming server remotely.
     *
     * @throws RMIException If either of the two skeletons, for the client or
     *                      registration server interfaces, could not be
     *                      started. The user should not attempt to start the
     *                      server again if an exception occurs.
     */
    public synchronized void start() throws RMIException {
        registration.start();
        service.start();
    }

    /**
     * Stops the naming server.
     * <p/>
     * <p/>
     * This method commands both the client and registration interface
     * skeletons to stop. It attempts to interrupt as many of the threads that
     * are executing naming server code as possible. After this method is
     * called, the naming server is no longer accessible remotely. The naming
     * server should not be restarted.
     */
    public void stop() {
        registration.stop();
        service.stop();
        stopped(null);
    }

    /**
     * Indicates that the server has completely shut down.
     * <p/>
     * <p/>
     * This method should be overridden for error reporting and application
     * exit purposes. The default implementation does nothing.
     *
     * @param cause The cause for the shutdown, or <code>null</code> if the
     *              shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause) {
    }

    // The following public methods are documented in Service.java.
    @Override
    public void lock(Path path, boolean exclusive) throws FileNotFoundException {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void unlock(Path path, boolean exclusive) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException {
        if (path.isRoot() || directorySet.contains(path)) {
            return true;
        }
        if (!fileMap.containsKey(path)) {
            throw new FileNotFoundException(path.toString());
        }
        return false;
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException {
        //
        if (!directorySet.contains(directory) && !directory.isRoot()) {
            throw new FileNotFoundException(directory.toString());
        }
        Path[] hiers = getHierarchy(directory);
        ArrayList<String> children = new ArrayList<>();
        for (Path p : hiers) {
            if (p.parent().equals(directory)) {
                children.add(p.toString());
            }
        }
        return children.toArray(new String[children.size()]);
    }

    @Override
    public boolean createFile(Path file)
            throws RMIException, FileNotFoundException {
        // check if a file/dir with this name already exists
        if (fileMap.containsKey(file) || directorySet.contains(file)
                || !isDirectory(file.parent())) {
            return false;
        }
        // check if storage servers are available
        if (storageServers.isEmpty()) {
            throw new IllegalStateException("No Storage Servers connected");
        }
        // create file in a randomly selected ss
        StorageServer ss = getRandomStorageServer(null);
        if (!ss.command_stub.create(file)) {
            return true;
        }
        addToMap(file, ss);
        return true;
    }

    @Override
    public boolean createDirectory(Path directory) throws FileNotFoundException {
        // check if a file/dir with this name already exists
        if (fileMap.containsKey(directory) || directorySet.contains(directory)
                || !isDirectory(directory.parent())) {
            return false;
        }
        // Just add an entry in the directories.
        // We will randomly choose an ss while creating a file
        directorySet.add(directory);
        return true;
    }

    @Override
    public boolean delete(Path path) throws FileNotFoundException, RMIException {

        if (path.isRoot()) {
            return false;
        }

        // get heirarchy. delete files and directories simply. send delete
        // requests to storage stubs for files. they have to take care of
        // deleting the files and empty directories.

        // delete all entries in the hierarchy
        for (Path p : getHierarchy(path)) {
            if (!isDirectory(p)) {
                for (StorageServer ss : fileMap.get(p)) {
                    ss.command_stub.delete(p);
                }
                fileMap.remove(p);
            } else {
                directorySet.remove(p);
            }
        }
        return true;
    }

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException {
        // ASSUMING file is not a directory
        if (isDirectory(file)) {
            throw new IllegalArgumentException("It is a directory, not a file.");
        }
        return getRandomStorageServer(fileMap.get(file)).storage_stub;
    }

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub,
                           Path[] files) {
        // Everything should be non-null
        if (client_stub == null || command_stub == null || files == null) {
            throw new NullPointerException();
        }

        StorageServer ss = new StorageServer(client_stub, command_stub);

        // Avoiding duplicates
        if (storageServers.contains(ss)) {
            throw new IllegalStateException("Duplicate entry");
        }
        storageServers.add(ss);

        // Adding valid files and directories, collecting files to be deleted on ss
        ArrayList<Path> pathsToDelete = new ArrayList<>();
        for (Path path : files) {

            // -ignore if root is sent
            // -if similar path exists, mark for deletion. Ignore root request
            if (fileMap.containsKey(path) || directorySet.contains(path) || path.isRoot()) {
                if (!path.isRoot()) {
                    pathsToDelete.add(path);
                }
                continue;
            }

            // else add this file path to fileMap ...
            addToMap(path, ss);
            // ... and directories to directorySet
            Path p = path.parent();
            while (!p.isRoot()) {
                if (directorySet.contains(p)) {
                    break;
                }
                directorySet.add(p);
                p = p.parent();
            }
        }
        return pathsToDelete.toArray(new Path[pathsToDelete.size()]);
    }

    //#########################################################################
    // Helper functions to addToMap entry to fileMap
    private void addToMap(Path path, StorageServer storageServer) {
        if (!fileMap.containsKey(path)) {
            fileMap.put(path, new HashSet<StorageServer>());
        }
        fileMap.get(path).add(storageServer);
    }

    // Helper function to give list of all files and directories rooted
    // at <code>directory</code>
    private Path[] getHierarchy(Path directory) {
        ArrayList<Path> hiers = new ArrayList<>();

        // add paths from fileMap
        for (Path p : fileMap.keySet()) {
            if (p.isSubpath(directory)) {
                hiers.add(p);
            }
        }

        // add paths from directorySet
        for (Path p : directorySet) {
            if (p.isSubpath(directory)) {
                hiers.add(p);
            }
        }
        return hiers.toArray(new Path[hiers.size()]);
    }

    // Helper method to randomly pick a storage server from available servers
    // null parameter means pick from all available storageServers.
    private StorageServer getRandomStorageServer(Set<StorageServer> storageServers) {
        if (storageServers == null) {
            storageServers = this.storageServers;
        }
        StorageServer[] servers = storageServers.toArray(
                new StorageServer[storageServers.size()]);
        return servers[new Random().nextInt(storageServers.size())];
    }

    /*
    This class is just a data class which maintains the pair command_stub
    and storage_stub together.
     */
    private class StorageServer {
        Storage storage_stub;
        Command command_stub;

        private StorageServer(final Storage storage_stub, final Command command_stub) {
            this.storage_stub = storage_stub;
            this.command_stub = command_stub;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof StorageServer)) return false;

            StorageServer that = (StorageServer) o;

            if (!storage_stub.equals(that.storage_stub)) return false;
            return command_stub.equals(that.command_stub);

        }

        @Override
        public int hashCode() {
            int result = storage_stub.hashCode();
            result = 31 * result + command_stub.hashCode();
            return result;
        }
    }
}
