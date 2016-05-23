package rmi;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 RMI skeleton

 <p>
 A skeleton encapsulates a multithreaded TCP server. The server's clients are
 intended to be RMI stubs created using the <code>Stub</code> class.

 <p>
 The skeleton class is parametrized by a type variable. This type variable
 should be instantiated with an interface. The skeleton will accept from the
 stub requests for calls to the methods of this interface. It will then
 forward those requests to an object. The object is specified when the
 skeleton is constructed, and must implement the remote interface. Each
 method in the interface should be marked as throwing
 <code>RMIException</code>, in addition to any other exceptions that the user
 desires.

 <p>
 Exceptions may occur at the top level in the listening and service threads.
 The skeleton's response to these exceptions can be customized by deriving
 a class from <code>Skeleton</code> and overriding <code>listen_error</code>
 or <code>service_error</code>.
 */
public class Skeleton<T> {

    /* Private member variables */
    private Class<T> interfaceClass;
    private T server;
    private ServerSocket serverSocket;
    private InetSocketAddress address;
    private ListeningThread listeningThread;
    private final Set<ServiceThread> serviceThreads = new HashSet<>();

    /**
     Creates a <code>Skeleton</code> with no initial server address. The
     address will be determined by the system when <code>start</code> is
     called. Equivalent to using <code>Skeleton(null)</code>.

     <p>
     This constructor is for skeletons that will not be used for
     bootstrapping RMI - those that therefore do not require a well-known
     port.

     @param interfaceClass An object representing the class of the interface for which the
     skeleton server is to handle method call requests.
     @param server An object implementing said interface. Requests for method
     calls are forwarded by the skeleton to this object.
     @throws Error If <code>interfaceClass</code> does not represent a remote interface -
     an interface whose methods are all marked as throwing
     <code>RMIException</code>.
     @throws NullPointerException If either of <code>c</code> or
     <code>server</code> is <code>null</code>.
     */
    public Skeleton(Class<T> interfaceClass, T server) {
        validateInputs(interfaceClass, server);
        //Set the member variables to the parameters passed
        this.interfaceClass = interfaceClass;
        this.server = server;
    }

    /**
     Creates a <code>Skeleton</code> with the given initial server address.

     <p>
     This constructor should be used when the port number is significant.

     @param interfaceClass An object representing the class of the interface for which the
     skeleton server is to handle method call requests.
     @param server An object implementing said interface. Requests for method
     calls are forwarded by the skeleton to this object.
     @param address The address at which the skeleton is to run. If
     <code>null</code>, the address will be chosen by the
     system when <code>start</code> is called.
     @throws Error If <code>c</code> does not represent a remote interface -
     an interface whose methods are all marked as throwing
     <code>RMIException</code>.
     @throws NullPointerException If either of <code>c</code> or
     <code>server</code> is <code>null</code>.
     */
    public Skeleton(Class<T> interfaceClass, T server, InetSocketAddress address) {
        this(interfaceClass, server);
        this.address = address;
    }

    /**
     Called when the listening thread exits.

     <p>
     The listening thread may exit due to a top-level exception, or due to a
     call to <code>stop</code>.

     <p>
     When this method is called, the calling thread owns the lock on the
     <code>Skeleton</code> object. Care must be taken to avoid deadlocks when
     calling <code>start</code> or <code>stop</code> from different threads
     during this call.

     <p>
     The default implementation does nothing.

     @param cause The exception that stopped the skeleton, or
     <code>null</code> if the skeleton stopped normally.
     */
    protected void stopped(Throwable cause) {
        while (!serviceThreads.isEmpty()) {
            try {
                Thread t;
                synchronized (serviceThreads) {
                    if (!serviceThreads.isEmpty()) {
                        try {
                            t = serviceThreads.iterator().next();
                        } catch (NoSuchElementException e) {
                            return;
                        }
                    } else {
                        return;
                    }
                }
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    /**
     Called when an exception occurs at the top level in the listening
     thread.

     <p>
     The intent of this method is to allow the user to report exceptions in
     the listening thread to another thread, by a mechanism of the user's
     choosing. The user may also ignore the exceptions. The default
     implementation simply stops the server. The user should not use this
     method to stop the skeleton. The exception will again be provided as the
     argument to <code>stopped</code>, which will be called later.

     @param exception The exception that occurred.
     @return <code>true</code> if the server is to resume accepting
     connections, <code>false</code> if the server is to shut down.
     */
    protected boolean listen_error(Exception exception) {
        //TODO: stop(); the server
        // FIXME:
        return false;
    }

    /**
     Called when an exception occurs at the top level in a service thread.

     <p>
     The default implementation does nothing.

     @param exception The exception that occurred.
     */
    protected void service_error(RMIException exception) {
    }

    /**
     Starts the skeleton server.

     <p>
     A thread is created to listen for connection requests, and the method
     returns immediately. Additional threads are created when connections are
     accepted. The network address used for the server is determined by which
     constructor was used to create the <code>Skeleton</code> object.

     @throws RMIException When the listening socket cannot be created or
     bound, when the listening thread cannot be created,
     or when the server has already been started and has
     not since stopped.
     */
    public synchronized void start() throws RMIException {

        // Checks
        if (listeningThread != null && listeningThread.isAlive()) {
            throw new RMIException("Server already running");
        }

        listeningThread = new ListeningThread();
        // Create Server socket if not present else use it
        try {
            if (address == null) {
                String localIp = InetAddress.getLocalHost().getHostAddress();
                // Get some free port and assign: moving with this
                serverSocket = new ServerSocket(0);
                address = new InetSocketAddress(localIp, serverSocket.getLocalPort());

            } else if (serverSocket == null || serverSocket.isClosed()) {
                serverSocket = new ServerSocket(address.getPort());
                address = new InetSocketAddress(address.getHostName(), serverSocket.getLocalPort());
            }

            // Starting listening thread
            listeningThread.start();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     Stops the skeleton server, if it is already running.

     <p>
     The listening thread terminates. Threads created to service connections
     may continue running until their invocations of the <code>service</code>
     method return. The server stops at some later time; the method
     <code>stopped</code> is called at that point. The server may then be
     restarted.
     */
    public synchronized void stop() {
        if (listeningThread != null && listeningThread.isAlive()) {
            listeningThread.stopSignal = true;
            try {
                serverSocket.close();
                listeningThread.join();
                stopped(null);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public void setAddress(InetSocketAddress address) {
        this.address = address;
    }

    private void validateInputs(Class<T> interfaceClass, T server) {
        //Checks for null and unsupported class
        if (server == null) {
            throw new NullPointerException("Server is null");
        }
        if (interfaceClass == null) {
            throw new NullPointerException("Class is null");
        }

        if (!isRemoteInterface(interfaceClass)) {
            throw new Error("Given class does not represent remote interface");
        }
    }

    private boolean isRemoteInterface(Class c) {

        if (!c.isInterface()) {
            return false;
        }

        for (Method method : c.getDeclaredMethods()) {
            Class<?>[] exceptionTypes = method.getExceptionTypes();

            if (!Arrays.asList(exceptionTypes).contains(RMIException.class)) {
                return false;
            }
        }
        return true;
    }

    //Definition of ListeningThread class
    private class ListeningThread extends Thread {

        private boolean stopSignal = false;

        @Override
        public void run() {
            try {
                // Run Server
                while (true) {
                    try {
                        ServiceThread new_thread = new ServiceThread(serverSocket.accept());
                        new_thread.start();
                    } catch (IOException e) {

                        if (stopSignal) {
                            // User closed the server by choice
                            // stopped(null);
                            break;
                        } else {
                            // Some unforseen exception occurred
                            if (listen_error(e)) {
                                // if true, server needs to continue accepting connections
                                continue;
                            } else {
                                // else server needs to stop
                                stopped(e);
                                break;
                            }
                        }
                    }
                }
            } finally {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    System.out.println("Could not close server socket, " + e);
                }
            }
        }
    }

    //Definition of ServiceThread class
    private class ServiceThread extends Thread {
        private Socket socket;

        public ServiceThread(Socket socket) {
            this.socket = socket;
            serviceThreads.add(this);
        }

        @Override
        public void run() {
            ObjectInputStream in = null;
            ObjectOutputStream out = null;
            try {
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                in = new ObjectInputStream(socket.getInputStream());

                String methodName = (String) in.readObject();
                Class[] parameterTypes = (Class[]) in.readObject();
                Object[] args = (Object[]) in.readObject();

                Method method;
                method = interfaceClass.getMethod(methodName, parameterTypes);
                Object result = null;
                try {
                    result = method.invoke(server, args);
                    out.writeObject("PASSED");

                    Class returnType = method.getReturnType();
                    if (!returnType.equals(Void.TYPE)) {
                        // if result type is void, do nothing.
                        if ((result instanceof Serializable && isRemoteInterface(returnType)) || !isRemoteInterface(returnType)) {
                            // if result type is not void, and not remote interface, serialize the return object
                            out.writeObject(result);
                        } else {
                            // Object is ROR
                            // create and start skeleton and return stub of this skeleton
                            Skeleton rorSkeleton = new Skeleton(returnType, result);
                            rorSkeleton.start();
                            out.writeObject(Stub.create(returnType, rorSkeleton.getAddress()));
                        }
                    }
                } catch (InvocationTargetException e) {
                    out.writeObject("FAILED");
                    out.writeObject(e.getTargetException());
                }
            } catch (Exception e) {
                service_error(new RMIException(e));
            } finally {
                serviceThreads.remove(this);
                try {
                    if (out != null) {
                        out.flush();
                        out.close();
                    }
                    if (in != null) {
                        in.close();
                    }
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
