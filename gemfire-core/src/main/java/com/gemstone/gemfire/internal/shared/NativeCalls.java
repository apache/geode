/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal.shared;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketImpl;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.SystemFailure;

/**
 * Encapsulates native C/C++ calls via JNA. To obtain an instance of
 * implementation for a platform, use {@link NativeCalls#getInstance()}.
 * 
 * This class is also referenced by ODBC/.NET drivers so it should not refer to
 * any classes other than standard JDK or those within the same package.
 * 
 * @author swale
 * @since 8.0
 */
public abstract class NativeCalls {

  /**
   * Static instance of NativeCalls implementation. This can be one of JNA
   * implementations in <code>NativeCallsJNAImpl</code> or can fallback to a
   * generic implementation in case JNA is not available for the platform.
   * 
   * Note: this variable is deliberately not final since other drivers like
   * those for ADO.NET or ODBC will plugin their own native implementations of
   * NativeCalls.
   */
  protected static NativeCalls instance;

  static {
    NativeCalls inst;
    try {
      // try to load JNA implementation first
      // we do it via reflection since some clients like ADO.NET/ODBC
      // may not have it
      final Class<?> c = Class
          .forName("com.gemstone.gemfire.internal.shared.NativeCallsJNAImpl");
      inst = (NativeCalls)c.getMethod("getInstance").invoke(null);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      inst = null;
    }
    if (inst == null) {
      // In case JNA implementations cannot be loaded, fallback to generic
      // implementations.
      // Other clients like ADO.NET/ODBC will plugin their own implementations.
      try {
        // using reflection to get the implementation based on OSProcess
        // since this is also used by GemFireXD client; at some point all the
        // functionality of OSProcess should be folded into the JNA impl
        final Class<?> c = Class
            .forName("com.gemstone.gemfire.internal.OSProcess$NativeOSCalls");
        inst = (NativeCalls)c.newInstance();
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable t) {
        SystemFailure.checkFailure();
        // fallback to generic impl in case of a problem
        inst = new NativeCallsGeneric();
      }
    }
    instance = inst;
  }

  public NativeCalls() {
  }

  /**
   * Get an instance of implementation of {@link NativeCalls} for the current
   * platform.
   */
  public static NativeCalls getInstance() {
    return instance;
  }

  @SuppressWarnings("unchecked")
  protected static final Map<String, String> getModifiableJavaEnv() {
    final Map<String, String> env = System.getenv();
    try {
      final Field m = env.getClass().getDeclaredField("m");
      m.setAccessible(true);
      return (Map<String, String>)m.get(env);
    } catch (Exception ex) {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  protected static final Map<String, String> getModifiableJavaEnvWIN() {
    try {
      final Field envField = Class.forName("java.lang.ProcessEnvironment")
          .getDeclaredField("theCaseInsensitiveEnvironment");
      envField.setAccessible(true);
      return (Map<String, String>)envField.get(null);
    } catch (Exception ex) {
      return null;
    }
  }

  /**
   * Get the native kernel descriptor given the java Socket. This is a horribly
   * implementation dependent code checking various cases to get to the
   * underlying kernel socket descriptor but works for the JDK's we support or
   * intend to support directly or indirectly (e.g. GCJ for ODBC clients).
   * 
   * @param sock
   *          the java socket
   * @param sockStream
   *          the {@link InputStream} of the java socket, if available
   * 
   * @throws UnsupportedOperationException
   *           if the kernel descriptor could not be extracted
   */
  protected int getSocketKernelDescriptor(Socket sock, InputStream sockStream)
      throws UnsupportedOperationException {
    Method m;
    Field f;
    Object obj;
    FileDescriptor fd = null;
    // in some cases (for SSL) the Socket can be a wrapper one
    try {
      f = getAnyField(sock.getClass(), "self");
      if (f != null) {
        f.setAccessible(true);
        final Object self = f.get(sock);
        if (self instanceof Socket) {
          sock = (Socket)self;
          sockStream = sock.getInputStream();
        }
      }
    } catch (NoSuchFieldException fe) {
      // ignore if there is no such field
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception ex) {
      throw new UnsupportedOperationException(ex);
    }

    // first try using SocketInputStream
    if (sockStream instanceof FileInputStream) {
      try {
        fd = ((FileInputStream)sockStream).getFD();
      } catch (Exception e) {
        // go the fallback route
      }
    }
    // else fallback to SocketImpl route
    try {
      if (fd == null) {
        try {
          // package private Socket.getImpl() to get SocketImpl
          m = getAnyMethod(sock.getClass(), "getImpl");
        } catch (Exception ex) {
          try {
            m = getAnyMethod(sock.getClass(), "getPlainSocketImpl");
          } catch (Exception e) {
            // try forcing the InputStream route
            m = null;
            if (sockStream == null) {
              sockStream = sock.getInputStream();
              if (sockStream instanceof FileInputStream) {
                fd = ((FileInputStream)sockStream).getFD();
              }
            }
            else {
              throw e;
            }
          }
        }
        if (m != null) {
          m.setAccessible(true);
          final SocketImpl sockImpl = (SocketImpl)m.invoke(sock);
          if (sockImpl != null) {
            try {
              m = getAnyMethod(sockImpl.getClass(), "getFileDescriptor");
              if (m != null) {
                m.setAccessible(true);
                fd = (FileDescriptor)m.invoke(sockImpl);
              }
            } catch (NoSuchMethodException nme) {
              // go to field reflection route
            }
          }
        }
      }
      if (fd != null) {
        // get the kernel descriptor using reflection
        f = getAnyField(fd.getClass(), "fd");
        if (f != null) {
          f.setAccessible(true);
          obj = f.get(fd);
          if (obj instanceof Integer) {
            return ((Integer)obj).intValue();
          }
        }
      }
      throw new UnsupportedOperationException();
    } catch (SecurityException se) {
      throw new UnsupportedOperationException(se);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception ex) {
      throw new UnsupportedOperationException(ex);
    }
  }

  protected static Method getAnyMethod(Class<?> c, String name,
      Class<?>... parameterTypes) throws NoSuchMethodException,
      SecurityException {
    NoSuchMethodException firstEx = null;
    for (;;) {
      try {
        return c.getDeclaredMethod(name, parameterTypes);
      } catch (NoSuchMethodException nsme) {
        if (firstEx == null) {
          firstEx = nsme;
        }
        if ((c = c.getSuperclass()) == null) {
          throw firstEx;
        }
        // else continue searching in superClass
      }
    }
  }

  protected static Field getAnyField(Class<?> c, String name)
      throws NoSuchFieldException, SecurityException {
    NoSuchFieldException firstEx = null;
    for (;;) {
      try {
        return c.getDeclaredField(name);
      } catch (NoSuchFieldException nsfe) {
        if (firstEx == null) {
          firstEx = nsfe;
        }
        if ((c = c.getSuperclass()) == null) {
          throw firstEx;
        }
        // else continue searching in superClass
      }
    }
  }

  protected String getUnsupportedSocketOptionMessage(TCPSocketOptions opt) {
    return "setSocketOption(): socket option " + opt
        + " not supported by current platform " + getOSType();
  }

  /**
   * Get the {@link OSType} of current system.
   */
  public abstract OSType getOSType();

  /**
   * Get the value of given environment variable. This is different from
   * {@link System#getenv(String)} in that it returns the current value of the
   * environment variable in the process rather than from a static unmodifiable
   * map created on the first call.
   * 
   * @param name
   *          the name of the environment variable to be modified
   */
  public abstract String getEnvironment(String name);

  /**
   * Set the value of an environment variable. This modifies both the value in
   * the process, and the cached static map maintained by JVM on the first call
   * so further calls to {@link System#getenv(String)} will also return the
   * modified value.
   * 
   * @param name
   *          the name of the environment variable to be modified
   * @param value
   *          the new value of the environment variable; a value of null clears
   *          the existing value
   */
  public abstract void setEnvironment(String name, String value);

  /**
   * Get the process ID of the current process.
   */
  public abstract int getProcessId();

  /**
   * Check whether a process with given ID is still running.
   * 
   * @throws UnsupportedOperationException
   *           if no native API to determine the process status could be invoked
   */
  public abstract boolean isProcessActive(int processId)
      throws UnsupportedOperationException;

  /**
   * Kill the process with given process ID immediately (i.e. without giving it
   * a chance to cleanup properly).
   * 
   * @param processId
   *          the PID of the process to be kill
   * 
   * @throws UnsupportedOperationException
   *           if no native API to kill the process could be invoked
   */
  public abstract boolean killProcess(int processId)
      throws UnsupportedOperationException;

  /**
   * Perform the steps necessary to make the current JVM a proper UNIX daemon.
   * 
   * @param callback
   *          register callback to be invoked on catching a SIGHUP signal;
   *          SIGHUP signal is ignored if the callback is null
   * 
   * @throws UnsupportedOperationException
   *           if the native calls could not be completed for some reason or are
   *           not available
   * @throws IllegalStateException
   *           for a non-UNIX platform
   */
  public void daemonize(RehashServerOnSIGHUP callback)
      throws UnsupportedOperationException, IllegalStateException {
    throw new UnsupportedOperationException(
        "daemonize() not available in base implementation");
  }

  public void preBlow(String path, long maxSize, boolean preAllocate)
      throws IOException {
    RandomAccessFile raf = new RandomAccessFile(path, "rw");
    try {
      raf.setLength(maxSize);
    } finally {
      raf.close();
    }
  }
  
  /**
   * This will return whether the path passed in as arg is
   * part of a local file system or a remote file system.
   * This method is mainly used by the DiskCapacityMonitor thread
   * and we don't want to monitor remote fs available space as
   * due to network problems/firewall issues the call to getUsableSpace
   * can hang. See bug #49155. On platforms other than Linux this will
   * return false even if it on local file system for now.
   */
  public boolean isOnLocalFileSystem(final String path) {
    return false;
  }
  /**
   * Set given extended socket options on a Java {@link Socket}.
   * 
   * @throws UnsupportedOperationException
   *           if the native API to set the option could not be found or invoked
   * 
   * @return the unsupported {@link TCPSocketOptions} for the current platform
   *         and the underlying exception
   * 
   * @see TCPSocketOptions
   */
  public abstract Map<TCPSocketOptions, Throwable> setSocketOptions(
      Socket sock, InputStream sockStream,
      Map<TCPSocketOptions, Object> optValueMap)
      throws UnsupportedOperationException;

  // IPPROTO_TCP is used by setsockopt to denote a TCP option
  protected static final int OPT_IPPROTO_TCP = 6;
  // indicates an unsupported TCPSocketOptions enum
  protected static final int UNSUPPORTED_OPTION = Integer.MIN_VALUE;
  /**
   * A generic implementation of {@link #setSocketOptions} for POSIX like
   * systems that requires the child classes to implement a few platform
   * specific methods.
   */
  protected final Map<TCPSocketOptions, Throwable> setGenericSocketOptions(
      Socket sock, InputStream sockStream,
      Map<TCPSocketOptions, Object> optValueMap)
      throws UnsupportedOperationException {
    final Set<Map.Entry<TCPSocketOptions, Object>> optValueEntries =
        optValueMap.entrySet();
    for (Map.Entry<TCPSocketOptions, Object> e : optValueEntries) {
      TCPSocketOptions opt = e.getKey();
      Object value = e.getValue();
      // just to check for unsupported option
      getPlatformOption(opt);
      // all options currently require an integer argument
      if (value == null || !(value instanceof Integer)) {
        throw new IllegalArgumentException("bad argument type "
            + (value != null ? value.getClass().getName() : "NULL") + " for "
            + opt);
      }
    }

    Map<TCPSocketOptions, Throwable> failures =
        new HashMap<TCPSocketOptions, Throwable>(4);
    final int sockfd = getSocketKernelDescriptor(sock, sockStream);
    for (Map.Entry<TCPSocketOptions, Object> e : optValueEntries) {
      TCPSocketOptions opt = e.getKey();
      Object value = e.getValue();
      final int optName = getPlatformOption(opt);
      if (optName == UNSUPPORTED_OPTION) {
        failures.put(opt, new UnsupportedOperationException(
            getUnsupportedSocketOptionMessage(opt)));
        continue;
      }
      final int optSize = Integer.SIZE / Byte.SIZE;
      try {
        if (setPlatformSocketOption(sockfd, OPT_IPPROTO_TCP, optName, opt,
            (Integer)value, optSize) != 0) {
          failures.put(opt, new SocketException(getOSType()
              + ": error setting option " + opt + " to " + value));
        }
      } catch (NativeErrorException ne) {
        // check if the error indicates that option is not supported
        if (isNoProtocolOptionCode(ne.getErrorCode())) {
          failures.put(opt, new UnsupportedOperationException(
              getUnsupportedSocketOptionMessage(opt), ne));
        }
        else {
          final SocketException se = new SocketException(getOSType()
              + ": failed to set " + opt + " to " + value);
          se.initCause(ne);
          failures.put(opt, se);
        }
      }
    }
    return failures.size() > 0 ? failures : null;
  }

  protected int getPlatformOption(TCPSocketOptions opt)
      throws UnsupportedOperationException {
    // no generic POSIX specification for this
    throw new UnsupportedOperationException(
        "setSocketOption not supported for generic POSIX platform");
  }

  protected int setPlatformSocketOption(int sockfd, int level, int optName,
      TCPSocketOptions opt, Integer optVal, int optSize)
      throws UnsupportedOperationException, NativeErrorException {
    // no generic POSIX specification for this
    throw new UnsupportedOperationException(
        "setSocketOption not supported for generic POSIX platform");
  }

  protected boolean isNoProtocolOptionCode(int errno)
      throws UnsupportedOperationException {
    // no generic POSIX specification for this
    throw new UnsupportedOperationException(
        "setSocketOption not supported for generic POSIX platform");
  }

  /**
   * Callback invoked when an OS-level SIGHUP signal is caught after handler has
   * been installed by {@link NativeCalls#daemonize}. This is provided to allow
   * for re-reading configuration files or any other appropriate actions on
   * receiving HUP signal as is the convention in other servers.
   * 
   * @author swale
   * @since 8.0
   */
  public static interface RehashServerOnSIGHUP {

    /**
     * Perform the actions required to "rehash" the server.
     */
    public void rehash();
  }

  /**
   * whether o/s supports high resolution clock or equivalent 
   * perf counter.
   * 
   * @return true if implemented, otherwise false.
   */
  public boolean isNativeTimerEnabled() {
    return false;
  }
  
  /**
   * This is fall back for jni based library implementation of NanoTimer which
   * is more efficient than current impl through jna.
   * 
   * Linux impls create temporary timespec object and marshals that for invoking
   * native api. Shouldn't be used if to be called too many times, instead jni
   * implementation is more desirable.
   * 
   * @param clock_id
   * @return nanosecond precision performance counter.
   */
  public long nanoTime(int clock_id) {
    return System.nanoTime();
  }
  
  public long clockResolution(int clock_id) {
    return 0;
  }
  
  public boolean isTTY() {
    return false;
  }

  /**
   * A generic fallback implementation of {@link NativeCalls} when no JNA based
   * implementation could be initialized (e.g. if JNA itself does not provide an
   * implementation for the platform, or JNA is not found).
   * 
   * @author swale
   * @since 8.0
   */
  public static class NativeCallsGeneric extends NativeCalls {

    private static final Map<String, String> javaEnv;

    private static final boolean isWin;

    static {
      isWin = System.getProperty("os.name").indexOf("Windows") >= 0;
      javaEnv = isWin ? getModifiableJavaEnvWIN() : getModifiableJavaEnv();
    }

    /**
     * @see NativeCalls#getOSType()
     */
    @Override
    public OSType getOSType() {
      return isWin ? OSType.WIN : OSType.GENERIC;
    }

    /**
     * @see NativeCalls#getEnvironment(String)
     */
    @Override
    public String getEnvironment(final String name) {
      return System.getenv(name);
    }

    /**
     * @see NativeCalls#setEnvironment(String, String)
     */
    @Override
    public void setEnvironment(final String name, final String value) {
      // just change the cached map in java env if possible
      if (javaEnv != null) {
        if (value != null) {
          javaEnv.put(name, value);
        }
        else {
          javaEnv.remove(name);
        }
      }
    }

    /**
     * @see NativeCalls#getProcessId()
     */
    @Override
    public int getProcessId() {
      final String name = java.lang.management.ManagementFactory
          .getRuntimeMXBean().getName();
      final int idx = name.indexOf('@');
      if (idx > 0) {
        try {
          return Integer.parseInt(name.substring(0, idx));
        } catch (NumberFormatException nfe) {
          // something changed in the RuntimeMXBean name
        }
      }
      return 0;
    }

    /**
     * @see NativeCalls#isProcessActive(int)
     */
    @Override
    public boolean isProcessActive(int processId)
        throws UnsupportedOperationException {
      throw new UnsupportedOperationException(
          "isProcessActive() not available in generic implementation");
    }

    /**
     * @see NativeCalls#killProcess(int)
     */
    @Override
    public boolean killProcess(int processId)
        throws UnsupportedOperationException {
      throw new UnsupportedOperationException(
          "killProcess() not available in generic implementation");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<TCPSocketOptions, Throwable> setSocketOptions(Socket sock,
        InputStream sockStream, Map<TCPSocketOptions, Object> optValueMap)
        throws UnsupportedOperationException {
      throw new UnsupportedOperationException("setting native socket options "
          + optValueMap.keySet() + " not possible in generic implementation");
    }
  }
}
