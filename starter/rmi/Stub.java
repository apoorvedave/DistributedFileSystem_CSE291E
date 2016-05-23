package rmi;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 RMI stub factory.

 <p>
 RMI stubs hide network communication with the remote server and provide a
 simple object-like interface to their users. This class provides methods for
 creating stub objects dynamically, when given pre-defined interfaces.

 <p>
 The network address of the remote server is set when a stub is created, and
 may not be modified afterwards. Two stubs are equal if they implement the
 same interface and carry the same remote server address - and would
 therefore connect to the same skeleton. Stubs are serializable.
 */
public abstract class Stub implements Serializable {
    /**
     Creates a stub, given a skeleton with an assigned adress.

     <p>
     The stub is assigned the address of the skeleton. The skeleton must
     either have been created with a fixed address, or else it must have
     already been started.

     <p>
     This method should be used when the stub is created together with the
     skeleton. The stub may then be transmitted over the network to enable
     communication with the skeleton.

     @param c A <code>Class</code> object representing the interface
     implemented by the remote object.
     @param skeleton The skeleton whose network address is to be used.
     @return The stub created.
     @throws IllegalStateException If the skeleton has not been assigned an
     address by the user and has not yet been
     started.
     @throws UnknownHostException When the skeleton address is a wildcard and
     a port is assigned, but no address can be
     found for the local host.
     @throws NullPointerException If any argument is <code>null</code>.
     @throws Error If <code>c</code> does not represent a remote interface
     - an interface in which each method is marked as throwing
     <code>RMIException</code>, or if an object implementing
     this interface cannot be dynamically created.
     */
    public static <T> T create(Class<T> c, Skeleton<T> skeleton) throws UnknownHostException {

        if (c == null || skeleton == null) {
            throw new NullPointerException("Either of Class<T> or Skeleton<T> is null");
        }

        validateClass(c);

        if (null == skeleton.getAddress()) {
            throw new IllegalStateException("Skeleton not initialized with address");
        }

        try {
            InvocationHandler handler = new StubInvocationHandler(skeleton.getAddress(), c);
            return (T) Proxy.newProxyInstance(c.getClassLoader(), new Class[]{c}, handler);
        } catch (Exception e) {
            throw new Error("Stub for remote interface " + c.getCanonicalName() + " could not be created: " + e);
        }
    }

    /**
     Creates a stub, given a skeleton with an assigned address and a hostname
     which overrides the skeleton's hostname.

     <p>
     The stub is assigned the port of the skeleton and the given hostname.
     The skeleton must either have been started with a fixed port, or else
     it must have been started to receive a system-assigned port, for this
     method to succeed.

     <p>
     This method should be used when the stub is created together with the
     skeleton, but firewalls or private networks prevent the system from
     automatically assigning a valid externally-routable address to the
     skeleton. In this case, the creator of the stub has the option of
     obtaining an externally-routable address by other means, and specifying
     this hostname to this method.

     @param c A <code>Class</code> object representing the interface
     implemented by the remote object.
     @param skeleton The skeleton whose port is to be used.
     @param hostname The hostname with which the stub will be created.
     @return The stub created.
     @throws IllegalStateException If the skeleton has not been assigned a
     port.
     @throws NullPointerException If any argument is <code>null</code>.
     @throws Error If <code>c</code> does not represent a remote interface
     - an interface in which each method is marked as throwing
     <code>RMIException</code>, or if an object implementing
     this interface cannot be dynamically created.
     */
    public static <T> T create(Class<T> c, Skeleton<T> skeleton, String hostname) {

        if (c == null || skeleton == null || hostname == null || hostname.equals("")) {
            throw new NullPointerException("Either of Class<T> or Skeleton<T> or hostname is null");
        }

        validateClass(c);

        if (null == skeleton.getAddress() || 0 == skeleton.getAddress().getPort()) {
            throw new IllegalStateException("Skeleton not initialized with port number");
        }

        try {
            InetSocketAddress address = new InetSocketAddress(hostname, skeleton.getAddress().getPort());
            InvocationHandler handler = new StubInvocationHandler(address, c);
            return (T) Proxy.newProxyInstance(c.getClassLoader(), new Class[]{c}, handler);
        } catch (Exception e) {
            throw new Error("Stub for remote interface " + c.getCanonicalName() + " could not be created: " + e);
        }
    }

    /**
     Creates a stub, given the address of a remote server.

     <p>
     This method should be used primarily when bootstrapping RMI. In this
     case, the server is already running on a remote host but there is
     not necessarily a direct way to obtain an associated stub.

     @param c A <code>Class</code> object representing the interface
     implemented by the remote object.
     @param address The network address of the remote skeleton.
     @return The stub created.
     @throws NullPointerException If any argument is <code>null</code>.
     @throws Error If <code>c</code> does not represent a remote interface
     - an interface in which each method is marked as throwing
     <code>RMIException</code>, or if an object implementing
     this interface cannot be dynamically created.
     */
    public static <T> T create(Class<T> c, InetSocketAddress address) {

        if (c == null || address == null) {
            throw new NullPointerException("Either Class<T> or InetSocketAddress object null");
        }
        validateClass(c);

        try {
            InvocationHandler handler = new StubInvocationHandler(address, c);
            return (T) Proxy.newProxyInstance(c.getClassLoader(), new Class[]{c}, handler);
        } catch (Exception e) {
            throw new Error("Stub for remote interface " + c.getCanonicalName() + " could not be created: " + e);
        }
    }

    private static class StubInvocationHandler<T> implements InvocationHandler, Serializable {

        private Class<T> interfaceClass;
        private InetSocketAddress address;

        private StubInvocationHandler(InetSocketAddress address, Class interfaceClass) {
            this.address = address;
            this.interfaceClass = interfaceClass;
        }

        public Class getInterfaceClass() {
            return interfaceClass;
        }

        public InetSocketAddress getAddress() {
            return address;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

            if (method.equals(Object.class.getMethod("equals", Object.class))) {
                if (args[0] instanceof Proxy) {
                    StubInvocationHandler handler = (StubInvocationHandler) Proxy.getInvocationHandler((Proxy) args[0]);
                    return interfaceClass.equals(handler.getInterfaceClass()) && address.equals(handler.getAddress());
                }
                return false;
                // alternately can match tostrings of both
            }

            if (method.equals(Object.class.getMethod("hashCode"))) {
                return interfaceClass.hashCode() * address.hashCode();
            }

            if (method.equals(Object.class.getMethod("toString"))) {
                return interfaceClass.getCanonicalName() + " " + address.toString();
            }

            try {
                Socket socket = new Socket(address.getHostName(), address.getPort());
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                out.writeObject(method.getName());
                out.writeObject(method.getParameterTypes());
                out.writeObject(args);
                out.flush();

                // Read result pass or fail
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                Object success = in.readObject();

                if (success.equals("FAILED")) {
                    Object error = in.readObject();
                    in.close();
                    out.close();
                    socket.close();
                    throw (Exception) error;
                }

                Object result = null;

                if (!method.getReturnType().equals(Void.TYPE)) {
                    result = in.readObject();
                }

                in.close();
                out.close();
                socket.close();
                return result;
            } catch (Exception e) {
                if (e instanceof RuntimeException || Arrays.asList(method.getExceptionTypes()).contains(e.getClass())){
                    throw e;
                }
                throw new RMIException(e);
            }
        }
    }

    private static <T> void validateClass(Class<T> c) {
        for (Method m : c.getMethods()) {
            if (!Arrays.asList(m.getExceptionTypes()).contains(RMIException.class)) {
                throw new Error("Every method of " + c.getCanonicalName() +
                        " must throw " + RMIException.class.getCanonicalName() + " to be used as remote interface");
            }
        }
    }
}
