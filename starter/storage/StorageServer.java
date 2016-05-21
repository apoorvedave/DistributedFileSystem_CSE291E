package storage;

import common.Path;
import naming.Registration;
import rmi.RMIException;
import rmi.Skeleton;
import rmi.Stub;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Storage server.
 * <p/>
 * <p/>
 * Storage servers respond to client file access requests. The files accessible
 * through a storage server are those accessible under a given directory of the
 * local filesystem.
 */
public class StorageServer implements Storage, Command {
    private File root;
    //Skeleton for Service and Registration Interfaces
    private Skeleton<Storage> storageSkeleton;
    private Skeleton<Command> commandSkeleton;

    /**
     * Creates a storage server, given a directory on the local filesystem, and
     * ports to use for the client and command interfaces.
     * <p/>
     * <p/>
     * The ports may have to be specified if the storage server is running
     * behind a firewall, and specific ports are open.
     *
     * @param root         Directory on the local filesystem. The contents of this
     *                     directory will be accessible through the storage server.
     * @param client_port  Port to use for the client interface, or zero if the
     *                     system should decide the port.
     * @param command_port Port to use for the command interface, or zero if
     *                     the system should decide the port.
     * @throws NullPointerException If <code>root</code> is <code>null</code>.
     */
    public StorageServer(File root, int client_port, int command_port) {
        if (root == null) {
            throw new NullPointerException("Root file null");
        }
        this.root = root;
        this.storageSkeleton = new Skeleton<>(Storage.class, this,
                new InetSocketAddress(client_port));
        this.commandSkeleton = new Skeleton<>(Command.class, this,
                new InetSocketAddress(command_port));
    }

    /**
     * Creats a storage server, given a directory on the local filesystem.
     * <p/>
     * <p/>
     * This constructor is equivalent to
     * <code>StorageServer(root, 0, 0)</code>. The system picks the ports on
     * which the interfaces are made available.
     *
     * @param root Directory on the local filesystem. The contents of this
     *             directory will be accessible through the storage server.
     * @throws NullPointerException If <code>root</code> is <code>null</code>.
     */
    public StorageServer(File root) {
        this(root, 0, 0);
    }

    /**
     * Starts the storage server and registers it with the given naming
     * server.
     *
     * @param hostname      The externally-routable hostname of the local host on
     *                      which the storage server is running. This is used to
     *                      ensure that the stub which is provided to the naming
     *                      server by the <code>start</code> method carries the
     *                      externally visible hostname or address of this storage
     *                      server.
     * @param naming_server Remote interface for the naming server with which
     *                      the storage server is to register.
     * @throws UnknownHostException  If a stub cannot be created for the storage
     *                               server because a valid address has not been
     *                               assigned.
     * @throws FileNotFoundException If the directory with which the server was
     *                               created does not exist or is in fact a
     *                               file.
     * @throws RMIException          If the storage server cannot be started, or if it
     *                               cannot be registered.
     */
    public synchronized void start(String hostname, Registration naming_server)
            throws RMIException, UnknownHostException, FileNotFoundException {
        storageSkeleton.start();
        Storage client_stub = Stub.create(Storage.class, storageSkeleton);

        commandSkeleton.start();
        Command command_stub = Stub.create(Command.class, commandSkeleton);

        // register
        Path[] files;
        Path[] filesToDelete;
        try {
            files = Path.list(root);
            filesToDelete = naming_server.register(client_stub, command_stub, files);
        } catch (IllegalArgumentException e) {
            throw new FileNotFoundException(e.toString());
        }

        // delete filesToDelete and prune tree
        for (Path p : filesToDelete) {
            delete(p);
        }
    }

    /**
     * Stops the storage server.
     * <p/>
     * <p/>
     * The server should not be restarted.
     */
    public void stop() {
        storageSkeleton.stop();
        commandSkeleton.stop();
        stopped(null);
    }

    /**
     * Called when the storage server has shut down.
     *
     * @param cause The cause for the shutdown, if any, or <code>null</code> if
     *              the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause) {
    }

    // The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException {
        if (file == null) {
            throw new NullPointerException("Path null");
        }

        File localFile = file.toFile(root);
        if (!localFile.isFile()) {
            throw new FileNotFoundException(file.toString());
        }
        return localFile.length();
    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length)
            throws FileNotFoundException, IOException {
        if (file == null) {
            throw new NullPointerException("Path null");
        }
        if (!file.toFile(root).exists()) {
            throw new FileNotFoundException();
        }
        if (offset < 0 || length < 0 || offset + length > size(file)) {
            throw new IndexOutOfBoundsException(
                    String.format("File size: %d, offset: %d, len: %d", size(file), offset, length));
        }

        byte[] b = new byte[length];
        RandomAccessFile localFile = new RandomAccessFile(file.toFile(root), "r");
        localFile.seek(offset);
        localFile.read(b);
        localFile.close();
        return b;
    }

    @Override
    public synchronized void write(Path file, long offset, byte[] data)
            throws FileNotFoundException, IOException {
        if (file == null) {
            throw new NullPointerException("Path null");
        }
        if (offset < 0) {
            throw new IndexOutOfBoundsException("Negative offset");
        }
        if (!file.toFile(root).exists()) {
            throw new FileNotFoundException("File not found" + file);
        }
        RandomAccessFile localFile = new RandomAccessFile(file.toFile(root), "rw");
        localFile.seek(offset);
        localFile.write(data);
        localFile.close();
    }

    // The following methods are documented in Command.java.
    @Override
    public synchronized boolean create(Path file) {
        if (file == null) {
            throw new NullPointerException("Path null");
        }
        File localFile;
        if (file.isRoot() || (localFile = file.toFile(root)).exists()) {
            //System.out.println("File is either root or already exists" + file.toFile(root) );
            return false;
        }
        try {
            // create any parent files if not made already
            localFile.getParentFile().mkdirs();
            // create this file and send return value
            return localFile.createNewFile();
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public synchronized boolean delete(Path path) {
        if (path == null) {
            throw new NullPointerException("Path null");
        }

        // Cannot delete root, non existent files
        if (path.isRoot() || !path.toFile(root).exists()) {
            return false;
        }
        File file = path.toFile(root);

        // if file is Directory, recursively delete all files and directories inside
        if (file.isDirectory()) {
            java.nio.file.Path start = file.toPath();
            try {
                Files.walkFileTree(start, new SimpleFileVisitor<java.nio.file.Path>() {
                    @Override
                    public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException e)
                            throws IOException {
                        if (e == null) {
                            Files.delete(dir);
                            return FileVisitResult.CONTINUE;
                        } else {
                            // directory iteration failed
                            throw e;
                        }
                    }
                });
            } catch (IOException e) {
                System.out.println("exception occurred while deleting the file tree");
                e.printStackTrace();
                return false;
            }
        }
        // else if it is a file, delete it and delete all empty ancestors
        else {
            Path parent = path.parent();
            // delete file
            try {
                Files.delete(file.toPath());
                while (!parent.isRoot() &&
                        (Path.list(parent.toFile(root)) == null ||
                                Path.list(parent.toFile(root)).length == 0)) {

                    File temp = parent.toFile(root);
                    parent = parent.parent();
                    Files.delete(temp.toPath());
                }
            } catch (IOException e) {
                System.out.println("exception occurred while deleting the file tree");
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized boolean copy(Path file, Storage server)
            throws RMIException, FileNotFoundException, IOException {
        if (file == null || server == null) {
            throw new NullPointerException();
        }
        File localFile = file.toFile(root);
        try {
            // get file size on remote server, (also checks if file exists there)
            long size = server.size(file);
            // create any parent files if not made already
            localFile.getParentFile().mkdirs();
            // delete local file if exists
            if (localFile.exists()) {
                localFile.delete();
            }
            // create this file and send return value
            localFile.createNewFile();
            OutputStream out = new FileOutputStream(localFile);
            long offset = 0;
            int length = 1024;
            if (size > length) {
                while (true) {
                    int bytes = (int) Math.min((long) length, size - offset);
                    byte[] buf = server.read(file, offset, bytes);
                    if (buf.length > 0) {
                        out.write(buf, 0, bytes);
                        offset += length;
                    } else {
                        break;
                    }
                }
            } else {
                byte[] buf = server.read(file, 0, (int) size);
                if (buf.length > 0) {
                    out.write(buf, 0, (int) size);
                }
            }
            out.close();
            return true;
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            localFile.delete();
            e.printStackTrace();
            return false;
        }
    }
}
