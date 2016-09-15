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

package org.apache.geode.internal.shared;

import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.process.signal.Signal;
import org.apache.geode.internal.shared.NativeCalls.RehashServerOnSIGHUP;
import com.sun.jna.Callback;
import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.win32.StdCallLibrary;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


/**
 * Implementation of {@link NativeCalls} interface that encapsulates native C
 * calls via JNA. To obtain an instance of JNA based implementation for the
 * current platform, use {@link NativeCallsJNAImpl#getInstance()}.
 * 
 * BridJ is supposed to be cleaner, faster but it does not support Solaris/SPARC
 * yet and its not a mature library yet, so not using it. Can revisit once this
 * changes.
 * 
 * @since GemFire 8.0
 */
public final class NativeCallsJNAImpl {

  // no instance allowed
  private NativeCallsJNAImpl() {
  }

  /**
   * The static instance of the JNA based implementation for this platform.
   */
  private static final NativeCalls instance = getImplInstance();

  private static final NativeCalls getImplInstance() {
    if (Platform.isLinux()) {
      return new LinuxNativeCalls();
    }
    if (Platform.isWindows()) {
      return new WinNativeCalls();
    }
    if (Platform.isSolaris()) {
      return new SolarisNativeCalls();
    }
    if (Platform.isMac()) {
      return new MacOSXNativeCalls();
    }
    if (Platform.isFreeBSD()) {
      return new FreeBSDNativeCalls();
    }
    return new POSIXNativeCalls();
  }

  /**
   * Get an instance of JNA based implementation of {@link NativeCalls} for the
   * current platform.
   */
  public static NativeCalls getInstance() {
    return instance;
  }

  /**
   * Implementation of {@link NativeCalls} for POSIX compatible platforms.
   */
  private static class POSIXNativeCalls extends NativeCalls {

    static {
      Native.register("c");
    }

    public static native int setenv(String name, String value, int overwrite)
        throws LastErrorException;

    public static native int unsetenv(String name) throws LastErrorException;

    public static native String getenv(String name);

    public static native int getpid();

    public static native int kill(int processId, int signal)
        throws LastErrorException;

    public static native int setsid() throws LastErrorException;

    public static native int umask(int mask);

    public static native int signal(int signum, SignalHandler handler);

    public static native int setsockopt(int sockfd, int level, int optName,
        IntByReference optVal, int optSize) throws LastErrorException;

    public static native int close(int fd) throws LastErrorException;

    public static native int isatty(int fd) throws LastErrorException;
    
    static final int EPERM = 1;
    static final int ENOSPC = 28;

    static final Map<String, String> javaEnv = getModifiableJavaEnv();

    /** Signal callback handler for <code>signal</code> native call. */
    static interface SignalHandler extends Callback {
      void callback(int signum);
    }

    /**
     * holds a reference of {@link SignalHandler} installed in
     * {@link #daemonize} for SIGHUP to avoid it being GCed. Assumes that the
     * NativeCalls instance itself is a singleton and never GCed.
     */
    SignalHandler hupHandler;

    /**
     * the {@link RehashServerOnSIGHUP} instance sent to
     * {@link #daemonize}
     */
    RehashServerOnSIGHUP rehashCallback;

    /**
     * @see NativeCalls#getOSType()
     */
    @Override
    public OSType getOSType() {
      return OSType.GENERIC_POSIX;
    }

    /**
     * @see NativeCalls#setEnvironment(String, String)
     */
    @Override
    public synchronized void setEnvironment(final String name,
        final String value) {
      if (name == null) {
        throw new UnsupportedOperationException(
            "setEnvironment() for name=NULL");
      }
      int res = -1;
      Throwable cause = null;
      try {
        if (value != null) {
          res = setenv(name, value, 1);
        }
        else {
          res = unsetenv(name);
        }
      } catch (LastErrorException le) {
        cause = new NativeErrorException(le.getMessage(), le.getErrorCode(),
            le.getCause());
      }
      if (res != 0) {
        throw new IllegalArgumentException("setEnvironment: given name=" + name
            + " (value=" + value + ')', cause);
      }
      // also change in java cached map
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
     * @see NativeCalls#getEnvironment(String)
     */
    @Override
    public synchronized String getEnvironment(final String name) {
      if (name == null) {
        throw new UnsupportedOperationException(
            "getEnvironment() for name=NULL");
      }
      return getenv(name);
    }

    /**
     * @see NativeCalls#getProcessId()
     */
    @Override
    public int getProcessId() {
      return getpid();
    }

    /**
     * @see NativeCalls#isProcessActive(int)
     */
    @Override
    public boolean isProcessActive(final int processId) {
      try {
        return kill(processId, 0) == 0;
      } catch (LastErrorException le) {
        if (le.getErrorCode() == EPERM) {
          // Process exists; it probably belongs to another user (bug 27698).
          return true;
        }
      }
      return false;
    }

    /**
     * @see NativeCalls#killProcess(int)
     */
    @Override
    public boolean killProcess(final int processId) {
      try {
        return kill(processId, 9) == 0;
      } catch (LastErrorException le) {
        return false;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void daemonize(RehashServerOnSIGHUP callback)
        throws UnsupportedOperationException {
      UnsupportedOperationException err = null;
      try {
        setsid();
      } catch (LastErrorException le) {
        // errno=EPERM indicates already group leader
        if (le.getErrorCode() != EPERM) {
          err = new UnsupportedOperationException(
              "Failed in setsid() in daemonize() due to " + le.getMessage()
                  + " (errno=" + le.getErrorCode() + ')');
        }
      }
      // set umask to something consistent for servers
      final int newMask = 022;
      int oldMask = umask(newMask);
      // check if old umask was more restrictive, and if so then set it back
      if ((oldMask & 077) > newMask) {
        umask(oldMask);
      }
      // catch the SIGHUP signal and invoke any callback provided
      this.rehashCallback = callback;
      this.hupHandler = new SignalHandler() {
        @Override
        public void callback(int signum) {
          // invoke the rehash function if provided
          final RehashServerOnSIGHUP rehashCb = rehashCallback;
          if (signum == Signal.SIGHUP.getNumber() && rehashCb != null) {
            rehashCb.rehash();
          }
        }
      };
      signal(Signal.SIGHUP.getNumber(), this.hupHandler);
      // ignore SIGCHLD and SIGINT
      signal(Signal.SIGCHLD.getNumber(), this.hupHandler);
      signal(Signal.SIGINT.getNumber(), this.hupHandler);
      if (err != null) {
        throw err;
      }
    }

    @Override
    public void preBlow(String path, long maxSize, boolean preAllocate)
        throws IOException {
      final org.apache.geode.LogWriter logger; 
      if (InternalDistributedSystem.getAnyInstance() != null) {
        logger = InternalDistributedSystem.getAnyInstance().getLogWriter();
      }
      else {
        logger = null;
      }
      
      if (logger != null && logger.fineEnabled()) {
        logger.fine("DEBUG preBlow called for path = " + path);
      }
      if (!preAllocate || !hasFallocate(path)) {
        super.preBlow(path, maxSize, preAllocate);
        if (logger != null && logger.fineEnabled()) {
          logger.fine("DEBUG preBlow super.preBlow 1 called for path = " + path);
        }
        return;
      }
      int fd = -1;
      boolean unknownError = false;
      try {
        fd = createFD(path, 00644);
        if (!isOnLocalFileSystem(path)) {
          super.preBlow(path, maxSize, preAllocate);
          if (logger != null && logger.fineEnabled()) {
            logger.fine("DEBUG preBlow super.preBlow 2 called as path = "
                + path + " not on local file system");
          }
          if (DiskStoreImpl.TEST_NO_FALLOC_DIRS != null) {
            DiskStoreImpl.TEST_NO_FALLOC_DIRS.add(path);
          }
          return;
        }
        fallocateFD(fd, 0L, maxSize);
        if (DiskStoreImpl.TEST_CHK_FALLOC_DIRS != null) {
          DiskStoreImpl.TEST_CHK_FALLOC_DIRS.add(path);
        }
        if (logger != null && logger.fineEnabled()) {
          logger.fine("DEBUG preBlow posix_fallocate called for path = " + path + " and ret = 0 maxsize = " + maxSize);
        }
      } catch (LastErrorException le) {
        if (logger != null && logger.fineEnabled()) {
          logger.fine("DEBUG preBlow posix_fallocate called for path = " + path + " and ret = " + le.getErrorCode() + " maxsize = " + maxSize);
        }
        // check for no space left on device
        if (le.getErrorCode() == ENOSPC) {
          unknownError = false;
          throw new IOException("Not enough space left on device");
        }
        else {
          unknownError = true;
        }
      } finally {
        if (fd >= 0) {
          try {
            close(fd);
          } catch (Exception e) {
            // ignore
          }
        }
        if (unknownError) {
          super.preBlow(path, maxSize, preAllocate);
          if (logger != null && logger.infoEnabled()) {
            logger.fine("DEBUG preBlow super.preBlow 3 called for path = " + path);
          }
        }
      }
    }

    protected boolean hasFallocate(String path) {
      return false;
    }

    protected int createFD(String path, int flags) throws LastErrorException {
      throw new UnsupportedOperationException("not expected to be invoked");
    }

    protected void fallocateFD(int fd, long offset, long len)
        throws LastErrorException {
      throw new UnsupportedOperationException("not expected to be invoked");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<TCPSocketOptions, Throwable> setSocketOptions(Socket sock,
        InputStream sockStream, Map<TCPSocketOptions, Object> optValueMap)
        throws UnsupportedOperationException {
      return super.setGenericSocketOptions(sock, sockStream, optValueMap);
    }

    @Override
    protected int setPlatformSocketOption(int sockfd, int level, int optName,
        TCPSocketOptions opt, Integer optVal, int optSize)
        throws NativeErrorException {
      try {
        return setsockopt(sockfd, level, optName,
            new IntByReference(optVal.intValue()), optSize);
      } catch (LastErrorException le) {
        throw new NativeErrorException(le.getMessage(), le.getErrorCode(),
            le.getCause());
      }
    }
    
    public boolean isTTY() {
      try {
        return isatty(0) == 1;
      } catch (Exception e) {
        throw new RuntimeException("Couldn't find tty impl. ", e);
      }
    }
    
  }

  /**
   * Implementation of {@link NativeCalls} for Linux platform.
   */
  private static class LinuxNativeCalls extends POSIXNativeCalls {

    static {
      Native.register("c");
    }

    // #define values for keepalive options in /usr/include/netinet/tcp.h
    static final int OPT_TCP_KEEPIDLE = 4;
    static final int OPT_TCP_KEEPINTVL = 5;
    static final int OPT_TCP_KEEPCNT = 6;

    static final int ENOPROTOOPT = 92;
    static final int ENOPROTOOPT_ALPHA = 42;
    static final int ENOPROTOOPT_MIPS = 99;
    static final int ENOPROTOOPT_PARISC = 220;

    /** posix_fallocate returns error number rather than setting errno */
    public static native int posix_fallocate64(int fd, long offset, long len);

    public static native int creat64(String path, int flags)
        throws LastErrorException;

    /**
     * {@inheritDoc}
     */
    @Override
    public OSType getOSType() {
      return OSType.LINUX;
    }

    @Override
    protected int getPlatformOption(TCPSocketOptions opt)
        throws UnsupportedOperationException {
      switch (opt) {
        case OPT_KEEPIDLE:
          return OPT_TCP_KEEPIDLE;
        case OPT_KEEPINTVL:
          return OPT_TCP_KEEPINTVL;
        case OPT_KEEPCNT:
          return OPT_TCP_KEEPCNT;
        default:
          throw new UnsupportedOperationException("unknown option " + opt);
      }
    }

    @Override
    protected boolean isNoProtocolOptionCode(int errno) {
      switch (errno) {
        case ENOPROTOOPT: return true;
        case ENOPROTOOPT_ALPHA: return true;
        case ENOPROTOOPT_MIPS: return true;
        case ENOPROTOOPT_PARISC: return true;
        default: return false;
      }
    }
    
    static {
      if (Platform.is64Bit()) {
        StatFS64.dummy();
      }
      else {
        StatFS.dummy();
      }
    }
    
    private ThreadLocal<Structure> tSpecs = new ThreadLocal<Structure>();
    
    private static boolean isStatFSEnabled;

    public static class FSIDIntArr2 extends Structure {

      public int[] fsid = new int[2];

      protected List getFieldOrder() {
        return Arrays.asList(new String[] { "fsid" });
      }
    }

    public static class FSPAREIntArr5 extends Structure {

      public int[] fspare = new int[5];

      protected List getFieldOrder() {
        return Arrays.asList(new String[] { "fspare" });
      }
    }

    public static class StatFS extends Structure {
      public int f_type;

      public int f_bsize;

      public int f_blocks;

      public int f_bfree;

      public int f_bavail;

      public int f_files;

      public int f_ffree;

      public FSIDIntArr2 f_fsid;

      public int f_namelen;

      public int f_frsize;

      public FSPAREIntArr5 f_spare;

      static {
        try {
          Native.register("rt");
          StatFS struct = new StatFS();
          int ret = statfs(".", struct);
          if (ret == 0) {
            isStatFSEnabled = true;
          }
          else {
            isStatFSEnabled = false;
          }
        } catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        } catch (Throwable t) {
          SystemFailure.checkFailure();
          isStatFSEnabled = false;
        }
      }

      public static native int statfs(String path, StatFS statfs)
          throws LastErrorException;

      @Override
      protected List getFieldOrder() {
        return Arrays.asList(new String[] { "f_type", "f_bsize", "f_blocks",
            "f_bfree", "f_bavail", "f_files", "f_ffree", "f_fsid", "f_namelen",
            "f_frsize", "f_spare" });
      }

      // KN: TODO need to add more types which are type of remote.
      // Not sure about these file types which needs to be cheked and added in the list below
      // COH, DEVFS, ISOFS, OPENPROM_SUPER_MAGIC, PROC_SUPER_MAGIC, UDF_SUPER_MAGIC
      // Do man 2 statfs on linux and will give all the file types.
      // Following identified as remote file system types
      // CIFS_MAGIC_NUMBER, CODA_SUPER_MAGIC, NCP_SUPER_MAGIC, NFS_SUPER_MAGIC, SMB_SUPER_MAGIC, TMPFS_MAGIC
      // 0xFF534D42       , 0x73757245      , 0x564c         , 0x6969         , 0x517B         , 0x01021994
      // 4283649346       , 1937076805      , 22092          , 26985          , 20859          , 16914836
      private static int[] REMOTE_TYPES= new int[] { /*4283649346,*/ 1937076805, 22092, 26985, 20859, 16914836 };
      
      public boolean isTypeLocal() {
        for(int i=0; i<REMOTE_TYPES.length; i++) {
          if (REMOTE_TYPES[i] == f_type) {
            return false;
          }
        }
        return true;
      }
      
      public static void dummy() {
        
      }
    }

    public static class FSPARELongArr5 extends Structure {

      public long[] fspare = new long[5];

      protected List getFieldOrder() {
        return Arrays.asList(new String[] { "fspare" });
      }
    }

    public static class StatFS64 extends Structure {
      public long f_type;

      public long f_bsize;

      public long f_blocks;

      public long f_bfree;

      public long f_bavail;

      public long f_files;

      public long f_ffree;

      public FSIDIntArr2 f_fsid;

      public long f_namelen;

      public long f_frsize;

      public FSPARELongArr5 f_spare;

      // KN: TODO need to add more types which are type of remote.
      // Not sure about these file types which needs to be checked and added in the list below
      // COH, DEVFS, ISOFS, OPENPROM_SUPER_MAGIC, PROC_SUPER_MAGIC, UDF_SUPER_MAGIC
      // Do man 2 statfs on linux and will give all the file types.
      // Following identified as remote file system types
      // CIFS_MAGIC_NUMBER, CODA_SUPER_MAGIC, NCP_SUPER_MAGIC, NFS_SUPER_MAGIC, SMB_SUPER_MAGIC, TMPFS_MAGIC
      // 0xFF534D42       , 0x73757245      , 0x564c         , 0x6969         , 0x517B         , 0x01021994
      // 4283649346       , 1937076805      , 22092          , 26985          , 20859          , 16914836
      private static long[] REMOTE_TYPES= new long[] { 4283649346l, 1937076805l, 22092l, 26985l, 20859l, 16914836l };
      
      static {
        try {
          Native.register("rt");
          StatFS64 struct = new StatFS64();
          int ret = statfs(".", struct);
          if (ret == 0) {
            isStatFSEnabled = true;
          }
          else {
            isStatFSEnabled = false;
          }
        } catch (Throwable t) {
          System.out.println("got error t: " + t.getMessage());
          t.printStackTrace();
          isStatFSEnabled = false;
        }
      }

      public static native int statfs(String path, StatFS64 statfs)
          throws LastErrorException;

      @Override
      protected List getFieldOrder() {
        return Arrays.asList(new String[] { "f_type", "f_bsize", "f_blocks",
            "f_bfree", "f_bavail", "f_files", "f_ffree", "f_fsid", "f_namelen",
            "f_frsize", "f_spare" });
      }

      
      public boolean isTypeLocal() {
        for(int i=0; i<REMOTE_TYPES.length; i++) {
          if (REMOTE_TYPES[i] == f_type) {
            return false;
          }
        }
        return true;
      }
      
      public static void dummy() {
        
      }
    }

    /**
     * Get the file store type of a path.
     * for example, /dev/sdd1(store name) /w2-gst-dev40d(mount point) ext4(type)
     * @param path
     * @return file store type
     */
    public String getFileStoreType(final String path) {
      File diskFile = new File(path);
      if (!diskFile.exists()) {
        diskFile = diskFile.getParentFile();
      }
      Path currentPath = diskFile.toPath();
      if (currentPath.isAbsolute() && Files.exists(currentPath)) {
        try {
          FileStore store = Files.getFileStore(currentPath);
          return store.type();
        } catch (IOException e) {
          return null;
        }
      }
      return null;
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
      final org.apache.geode.LogWriter logger; 
      if (InternalDistributedSystem.getAnyInstance() != null) {
        logger = InternalDistributedSystem.getAnyInstance().getLogWriter();
      }
      else {
        logger = null;
      }
      if (!isStatFSEnabled) {
//        if (logger != null && logger.fineEnabled()) {
//          logger.info("DEBUG isOnLocalFileSystem returning false 1 for path = " + path);
//        }
        return false;
      }
      final int numTries = 10;
      for (int i = 1; i <= numTries; i++) {
        try {
          if (Platform.is64Bit()) {
            StatFS64 stat = new StatFS64();
            StatFS64.statfs(path, stat);
//            if (logger != null && logger.fineEnabled()) {
//              logger.info("DEBUG 64 isOnLocalFileSystem returning " + stat.isTypeLocal() + " for path = " + path);
//            }
            return stat.isTypeLocal();
          }
          else {
            StatFS stat = new StatFS();
            StatFS.statfs(path, stat);
//            if (logger != null && logger.fineEnabled()) {
//              logger.fine("DEBUG 32 isOnLocalFileSystem returning " + stat.isTypeLocal() + " for path = " + path);
//            }
            return stat.isTypeLocal();
          }
        } catch (LastErrorException le) {
           // ignoring it as NFS mounted can give this exception
           // and we just want to retry to remove transient problem.
          if (logger != null && logger.fineEnabled()) {
            logger.fine("DEBUG isOnLocalFileSystem got ex = "+ le + " msg = " + le.getMessage());
          }
        }
      }
      return false;
    }

    public final static String[] FallocateFileSystems = {"ext4", "xfs", "btrfs", "ocfs2"};

    @Override
    protected boolean hasFallocate(String path) {
      String fstype = getFileStoreType(path);
      for (String type:FallocateFileSystems) {
        if (type.equalsIgnoreCase(fstype)) {
          return true;
        }
      }
      return false;
    }

    @Override
    protected int createFD(String path, int flags) throws LastErrorException {
      return creat64(path, flags);
    }

    @Override
    protected void fallocateFD(int fd, long offset, long len)
        throws LastErrorException {
      int errno = posix_fallocate64(fd, offset, len);
      if (errno != 0) {
        throw new LastErrorException(errno);
      }
    }
  }

  /**
   * Implementation of {@link NativeCalls} for Solaris platform.
   */
  private static class SolarisNativeCalls extends POSIXNativeCalls {

    static {
      Native.register("nsl");
      Native.register("socket");
    }

    // #define values for keepalive options in /usr/include/netinet/tcp.h
    // Below are only available on Solaris 11 and above but older platforms will
    // throw an exception which higher layers will handle appropriately
    static final int OPT_TCP_KEEPALIVE_THRESHOLD = 0x16;
    static final int OPT_TCP_KEEPALIVE_ABORT_THRESHOLD = 0x17;

    static final int ENOPROTOOPT = 99;

    /**
     * {@inheritDoc}
     */
    @Override
    public OSType getOSType() {
      return OSType.SOLARIS;
    }

    @Override
    protected int getPlatformOption(TCPSocketOptions opt)
        throws UnsupportedOperationException {
      switch (opt) {
        case OPT_KEEPIDLE:
          return OPT_TCP_KEEPALIVE_THRESHOLD;
        case OPT_KEEPINTVL:
        case OPT_KEEPCNT:
          return UNSUPPORTED_OPTION;
        default:
          throw new UnsupportedOperationException("unknown option " + opt);
      }
    }

    @Override
    protected int setPlatformSocketOption(int sockfd, int level, int optName,
        TCPSocketOptions opt, Integer optVal, int optSize)
        throws NativeErrorException {
      try {
        switch (optName) {
          case OPT_TCP_KEEPALIVE_THRESHOLD:
            // value required is in millis
            final IntByReference timeout = new IntByReference(
                optVal.intValue() * 1000);
            int result = setsockopt(sockfd, level, optName, timeout, optSize);
            if (result == 0) {
              // setting ABORT_THRESHOLD to be same as KEEPALIVE_THRESHOLD
              return setsockopt(sockfd, level,
                  OPT_TCP_KEEPALIVE_ABORT_THRESHOLD, timeout, optSize);
            }
            else {
              return result;
            }
          default:
            throw new UnsupportedOperationException("unsupported option " + opt);
        }
      } catch (LastErrorException le) {
        throw new NativeErrorException(le.getMessage(), le.getErrorCode(),
            le.getCause());
      }
    }

    @Override
    protected boolean isNoProtocolOptionCode(int errno) {
      return (errno == ENOPROTOOPT);
    }
  }

  /**
   * Implementation of {@link NativeCalls} for MacOSX platform.
   */
  private static class MacOSXNativeCalls extends POSIXNativeCalls {

    // #define values for keepalive options in /usr/include/netinet/tcp.h
    static final int OPT_TCP_KEEPALIVE = 0x10;

    static final int ENOPROTOOPT = 42;

    /**
     * {@inheritDoc}
     */
    @Override
    public OSType getOSType() {
      return OSType.MACOSX;
    }

    @Override
    protected int getPlatformOption(TCPSocketOptions opt)
        throws UnsupportedOperationException {
      switch (opt) {
        case OPT_KEEPIDLE:
          return OPT_TCP_KEEPALIVE;
        case OPT_KEEPINTVL:
        case OPT_KEEPCNT:
          return UNSUPPORTED_OPTION;
        default:
          throw new UnsupportedOperationException("unknown option " + opt);
      }
    }

    @Override
    protected boolean isNoProtocolOptionCode(int errno) {
      return (errno == ENOPROTOOPT);
    }
  }

  /**
   * Implementation of {@link NativeCalls} for FreeBSD platform.
   */
  private static class FreeBSDNativeCalls extends POSIXNativeCalls {

    // #define values for keepalive options in /usr/include/netinet/tcp.h
    static final int OPT_TCP_KEEPALIVE = 0x100;
    static final int OPT_TCP_KEEPINTVL = 0x200;
    static final int OPT_TCP_KEEPCNT = 0x400;

    static final int ENOPROTOOPT = 42;

    /**
     * {@inheritDoc}
     */
    @Override
    public OSType getOSType() {
      return OSType.FREEBSD;
    }

    @Override
    protected int getPlatformOption(TCPSocketOptions opt)
        throws UnsupportedOperationException {
      switch (opt) {
        case OPT_KEEPIDLE:
          return OPT_TCP_KEEPALIVE;
        case OPT_KEEPINTVL:
          return OPT_TCP_KEEPINTVL;
        case OPT_KEEPCNT:
          return OPT_TCP_KEEPCNT;
        default:
          throw new UnsupportedOperationException("unknown option " + opt);
      }
    }

    @Override
    protected boolean isNoProtocolOptionCode(int errno) {
      return (errno == ENOPROTOOPT);
    }
  }

  /**
   * Implementation of {@link NativeCalls} for Windows platforms.
   */
  private static final class WinNativeCalls extends NativeCalls {

    static {
      // for socket operations
      Native.register("Ws2_32");
    }

    @SuppressWarnings("unused")
    public static final class TcpKeepAlive extends Structure {
      public int onoff;
      public int keepalivetime;
      public int keepaliveinterval;

      @Override
      protected List<?> getFieldOrder() {
        return Arrays.asList(new String[] { "onoff", "keepalivetime",
            "keepaliveinterval" });
      }
    }

    public static native int WSAIoctl(NativeLong sock, int controlCode,
        TcpKeepAlive value, int valueSize, Pointer outValue, int outValueSize,
        IntByReference bytesReturned, Pointer overlapped,
        Pointer completionRoutine) throws LastErrorException;

    static final int WSAENOPROTOOPT = 10042;
    static final int SIO_KEEPALIVE_VALS = -1744830460;

    private static final class Kernel32 {

      static {
        // kernel32 requires stdcall calling convention
        Map<String, Object> kernel32Options = new HashMap<String, Object>();
        kernel32Options.put(Library.OPTION_CALLING_CONVENTION,
            StdCallLibrary.STDCALL_CONVENTION);
        kernel32Options.put(Library.OPTION_FUNCTION_MAPPER,
            StdCallLibrary.FUNCTION_MAPPER);
        final NativeLibrary kernel32Lib = NativeLibrary.getInstance("kernel32",
            kernel32Options);
        Native.register(kernel32Lib);
      }

      // Values below from windows.h header are hard-coded since there
      // does not seem any simple way to get those at build or run time.
      // Hopefully these will never change else all hell will break
      // loose in Windows world ...
      static final int PROCESS_QUERY_INFORMATION = 0x0400;
      static final int PROCESS_TERMINATE = 0x0001;
      static final int STILL_ACTIVE = 259;
      static final int INVALID_HANDLE = -1;

      public static native boolean SetEnvironmentVariableA(String name,
          String value) throws LastErrorException;

      public static native int GetEnvironmentVariableA(String name,
          byte[] pvalue, int psize);

      public static native int GetCurrentProcessId();

      public static native Pointer OpenProcess(int desiredAccess,
          boolean inheritHandle, int processId) throws LastErrorException;

      public static native boolean TerminateProcess(Pointer processHandle,
          int exitCode) throws LastErrorException;

      public static native boolean GetExitCodeProcess(Pointer processHandle,
          IntByReference exitCode) throws LastErrorException;

      public static native boolean CloseHandle(Pointer handle)
          throws LastErrorException;
    }

    private static final Map<String, String> javaEnv =
        getModifiableJavaEnvWIN();

    /**
     * @see NativeCalls#getOSType()
     */
    @Override
    public OSType getOSType() {
      return OSType.WIN;
    }

    /**
     * @see NativeCalls#setEnvironment(String, String)
     */
    @Override
    public synchronized void setEnvironment(final String name,
        final String value) {
      if (name == null) {
        throw new UnsupportedOperationException(
            "setEnvironment() for name=NULL");
      }
      boolean res = false;
      Throwable cause = null;
      try {
        res = Kernel32.SetEnvironmentVariableA(name, value);
      } catch (LastErrorException le) {
        // error code ERROR_ENVVAR_NOT_FOUND (203) indicates variable was not
        // found so ignore
        if (value == null && le.getErrorCode() == 203) {
          res = true;
        }
        else {
          cause = new NativeErrorException(le.getMessage(), le.getErrorCode(),
              le.getCause());
        }
      }
      if (!res) {
        throw new IllegalArgumentException("setEnvironment: given name=" + name
            + " (value=" + value + ')', cause);
      }
      // also change in java cached map
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
     * @see NativeCalls#getEnvironment(String)
     */
    @Override
    public synchronized String getEnvironment(final String name) {
      if (name == null) {
        throw new UnsupportedOperationException(
            "getEnvironment() for name=NULL");
      }
      int psize = Kernel32.GetEnvironmentVariableA(name, null, 0);
      if (psize > 0) {
        for (;;) {
          byte[] result = new byte[psize];
          psize = Kernel32.GetEnvironmentVariableA(name, result, psize);
          if (psize == (result.length - 1)) {
            return new String(result, 0, psize);
          }
          else if (psize <= 0) {
            return null;
          }
        }
      }
      else {
        return null;
      }
    }

    /**
     * @see NativeCalls#getProcessId()
     */
    @Override
    public int getProcessId() {
      return Kernel32.GetCurrentProcessId();
    }

    /**
     * @see NativeCalls#isProcessActive(int)
     */
    @Override
    public boolean isProcessActive(final int processId) {
      try {
        final Pointer procHandle = Kernel32.OpenProcess(
            Kernel32.PROCESS_QUERY_INFORMATION, false, processId);
        final long hval;
        if (procHandle == null || (hval = Pointer.nativeValue(procHandle)) ==
            Kernel32.INVALID_HANDLE || hval == 0) {
          return false;
        }
        else {
          final IntByReference status = new IntByReference();
          final boolean result = Kernel32.GetExitCodeProcess(procHandle, status)
              && status != null && status.getValue() == Kernel32.STILL_ACTIVE;
          Kernel32.CloseHandle(procHandle);
          return result;
        }
      } catch (LastErrorException le) {
        // some problem in getting process status
        return false;
      }
    }

    /**
     * @see NativeCalls#killProcess(int)
     */
    @Override
    public boolean killProcess(final int processId) {
      try {
        final Pointer procHandle = Kernel32.OpenProcess(
            Kernel32.PROCESS_TERMINATE, false, processId);
        final long hval;
        if (procHandle == null || (hval = Pointer.nativeValue(procHandle)) ==
            Kernel32.INVALID_HANDLE || hval == 0) {
          return false;
        }
        else {
          final boolean result = Kernel32.TerminateProcess(procHandle, -1);
          Kernel32.CloseHandle(procHandle);
          return result;
        }
      } catch (LastErrorException le) {
        // some problem in killing the process
        return false;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void daemonize(RehashServerOnSIGHUP callback)
        throws UnsupportedOperationException, IllegalStateException {
      throw new IllegalStateException(
          "daemonize() not applicable for Windows platform");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<TCPSocketOptions, Throwable> setSocketOptions(Socket sock,
        InputStream sockStream, Map<TCPSocketOptions, Object> optValueMap)
        throws UnsupportedOperationException {
      final TcpKeepAlive optValue = new TcpKeepAlive();
      final int optSize = (Integer.SIZE / Byte.SIZE) * 3;
      TCPSocketOptions errorOpt = null;
      Throwable error = null;
      for (Map.Entry<TCPSocketOptions, Object> e : optValueMap.entrySet()) {
        TCPSocketOptions opt = e.getKey();
        Object value = e.getValue();
        // all options currently require an integer argument
        if (value == null || !(value instanceof Integer)) {
          throw new IllegalArgumentException("bad argument type "
              + (value != null ? value.getClass().getName() : "NULL") + " for "
              + opt);
        }
        switch (opt) {
          case OPT_KEEPIDLE:
            optValue.onoff = 1;
            // in millis
            optValue.keepalivetime = ((Integer)value).intValue() * 1000;
            break;
          case OPT_KEEPINTVL:
            optValue.onoff = 1;
            // in millis
            optValue.keepaliveinterval = ((Integer)value).intValue() * 1000;
            break;
          case OPT_KEEPCNT:
            errorOpt = opt;
            error = new UnsupportedOperationException(
                getUnsupportedSocketOptionMessage(opt));
            break;
          default:
            throw new UnsupportedOperationException("unknown option " + opt);
        }
      }
      final int sockfd = getSocketKernelDescriptor(sock, sockStream);
      final IntByReference nBytes = new IntByReference(0);
      try {
        if (WSAIoctl(new NativeLong(sockfd), SIO_KEEPALIVE_VALS, optValue,
            optSize, null, 0, nBytes, null, null) != 0) {
          errorOpt = TCPSocketOptions.OPT_KEEPIDLE; // using some option here
          error = new SocketException(getOSType() + ": error setting options: "
              + optValueMap);
        }
      } catch (LastErrorException le) {
        // check if the error indicates that option is not supported
        errorOpt = TCPSocketOptions.OPT_KEEPIDLE; // using some option here
        if (le.getErrorCode() == WSAENOPROTOOPT) {
          error = new UnsupportedOperationException(
              getUnsupportedSocketOptionMessage(errorOpt),
              new NativeErrorException(le.getMessage(), le.getErrorCode(),
                  le.getCause()));
        }
        else {
          final SocketException se = new SocketException(getOSType()
              + ": failed to set options: " + optValueMap);
          se.initCause(le);
          error = se;
        }
      }
      return errorOpt != null ? Collections.singletonMap(errorOpt, error)
          : null;
    }
  }
}
