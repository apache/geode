/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.shared;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.sun.jna.Callback;
import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Structure;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinBase;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import com.sun.jna.ptr.IntByReference;
import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.process.signal.Signal;
import org.apache.geode.logging.internal.log4j.api.LogService;


/**
 * Implementation of {@link NativeCalls} interface that encapsulates native C calls via JNA. To
 * obtain an instance of JNA based implementation for the current platform, use
 * {@link NativeCallsJNAImpl#getInstance()}.
 *
 * BridJ is supposed to be cleaner, faster but it does not support Solaris/SPARC yet and its not a
 * mature library yet, so not using it. Can revisit once this changes.
 *
 * @since GemFire 8.0
 */
public class NativeCallsJNAImpl {

  private static final Logger logger = LogService.getLogger();

  // no instance allowed
  private NativeCallsJNAImpl() {}

  /**
   * The static instance of the JNA based implementation for this platform.
   */
  @Immutable
  private static final NativeCalls instance = getImplInstance();

  private static NativeCalls getImplInstance() {
    if (Platform.isLinux()) {
      return new LinuxNativeCalls();
    }
    if (Platform.isWindows()) {
      return new WinNativeCalls();
    }
    return new POSIXNativeCalls();
  }

  /**
   * Get an instance of JNA based implementation of {@link NativeCalls} for the current platform.
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

    public static native String getenv(String name);

    public static native int getpid();

    public static native int kill(int processId, int signal) throws LastErrorException;

    public static native int setsid() throws LastErrorException;

    public static native int umask(int mask);

    public static native int signal(int signum, SignalHandler handler);

    public static native int close(int fd) throws LastErrorException;

    static final int EPERM = 1;
    static final int ENOSPC = 28;

    /** Signal callback handler for <code>signal</code> native call. */
    interface SignalHandler extends Callback {
      void callback(int signum);
    }

    /**
     * holds a reference of {@link SignalHandler} installed in {@link #daemonize} for SIGHUP to
     * avoid it being GCed. Assumes that the NativeCalls instance itself is a singleton and never
     * GCed.
     */
    SignalHandler hupHandler;

    RehashServerOnSIGHUP rehashCallback;

    @Override
    public synchronized String getEnvironment(final String name) {
      if (name == null) {
        throw new UnsupportedOperationException("getEnvironment() for name=NULL");
      }
      return getenv(name);
    }

    @Override
    public int getProcessId() {
      return getpid();
    }

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

    @Override
    public boolean killProcess(final int processId) {
      try {
        return kill(processId, 9) == 0;
      } catch (LastErrorException le) {
        return false;
      }
    }

    @Override
    public void daemonize(RehashServerOnSIGHUP callback) throws UnsupportedOperationException {
      UnsupportedOperationException err = null;
      try {
        setsid();
      } catch (LastErrorException le) {
        // errno=EPERM indicates already group leader
        if (le.getErrorCode() != EPERM) {
          err = new UnsupportedOperationException("Failed in setsid() in daemonize() due to "
              + le.getMessage() + " (errno=" + le.getErrorCode() + ')');
        }
      }
      // set umask to something consistent for servers
      @SuppressWarnings("OctalInteger")
      final int newMask = 022;
      int oldMask = umask(newMask);
      // check if old umask was more restrictive, and if so then set it back
      @SuppressWarnings("OctalInteger")
      final int OCTAL_077 = 077;
      if ((oldMask & OCTAL_077) > newMask) {
        umask(oldMask);
      }
      // catch the SIGHUP signal and invoke any callback provided
      rehashCallback = callback;
      hupHandler = signum -> {
        // invoke the rehash function if provided
        final RehashServerOnSIGHUP rehashCb = rehashCallback;
        if (signum == Signal.SIGHUP.getNumber() && rehashCb != null) {
          rehashCb.rehash();
        }
      };
      signal(Signal.SIGHUP.getNumber(), hupHandler);
      // ignore SIGCHLD and SIGINT
      signal(Signal.SIGCHLD.getNumber(), hupHandler);
      signal(Signal.SIGINT.getNumber(), hupHandler);
      if (err != null) {
        throw err;
      }
    }

    @Override
    public void preBlow(String path, long maxSize, boolean preAllocate) throws IOException {
      if (logger.isDebugEnabled()) {
        logger.debug("DEBUG preBlow called for path = " + path);
      }
      if (!preAllocate || !hasFallocate(path)) {
        super.preBlow(path, maxSize, preAllocate);
        if (logger.isDebugEnabled()) {
          logger.debug("DEBUG preBlow super.preBlow 1 called for path = " + path);
        }
        return;
      }
      int fd = -1;
      boolean unknownError = false;
      try {
        @SuppressWarnings("OctalInteger")
        final int OCTAL_0644 = 00644;
        fd = createFD(path, OCTAL_0644);
        if (!isOnLocalFileSystem(path)) {
          super.preBlow(path, maxSize, preAllocate);
          if (logger.isDebugEnabled()) {
            logger.debug("DEBUG preBlow super.preBlow 2 called as path = " + path
                + " not on local file system");
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
        if (logger.isDebugEnabled()) {
          logger.debug("DEBUG preBlow posix_fallocate called for path = " + path
              + " and ret = 0 maxsize = " + maxSize);
        }
      } catch (LastErrorException le) {
        if (logger.isDebugEnabled()) {
          logger.debug("DEBUG preBlow posix_fallocate called for path = " + path + " and ret = "
              + le.getErrorCode() + " maxsize = " + maxSize);
        }
        // check for no space left on device
        if (le.getErrorCode() == ENOSPC) {
          throw new IOException("Not enough space left on device");
        } else {
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
          if (logger.isDebugEnabled()) {
            logger.debug("DEBUG preBlow super.preBlow 3 called for path = " + path);
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

    protected void fallocateFD(int fd, long offset, long len) throws LastErrorException {
      throw new UnsupportedOperationException("not expected to be invoked");
    }

  }

  /**
   * Implementation of {@link NativeCalls} for Linux platform.
   */
  private static class LinuxNativeCalls extends POSIXNativeCalls {

    static {
      Native.register("c");
    }

    /** posix_fallocate returns error number rather than setting errno */
    public static native int posix_fallocate64(int fd, long offset, long len);

    public static native int creat64(String path, int flags) throws LastErrorException;

    static {
      if (Platform.is64Bit()) {
        StatFS64.dummy();
      } else {
        StatFS.dummy();
      }
    }

    @MakeNotStatic
    private static boolean isStatFSEnabled;

    @SuppressWarnings("unused")
    public static class FSIDIntArr2 extends Structure {
      public int[] fsid = new int[2];

      @Override
      protected List<String> getFieldOrder() {
        return singletonList("fsid");
      }
    }

    @SuppressWarnings("unused")
    public static class FSPAREIntArr5 extends Structure {
      public int[] fspare = new int[5];

      @Override
      protected List<String> getFieldOrder() {
        return singletonList("fspare");
      }
    }

    @SuppressWarnings("unused")
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
          isStatFSEnabled = ret == 0;
        } catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        } catch (Throwable t) {
          SystemFailure.checkFailure();
          isStatFSEnabled = false;
        }
      }

      public static native int statfs(String path, StatFS statfs) throws LastErrorException;

      @Override
      protected List<String> getFieldOrder() {
        return asList("f_type", "f_bsize", "f_blocks", "f_bfree", "f_bavail",
            "f_files", "f_ffree", "f_fsid", "f_namelen", "f_frsize", "f_spare");
      }

      // KN: TODO need to add more types which are type of remote.
      // Not sure about these file types which needs to be cheked and added in the list below
      // COH, DEVFS, ISOFS, OPENPROM_SUPER_MAGIC, PROC_SUPER_MAGIC, UDF_SUPER_MAGIC
      // Do man 2 statfs on linux and will give all the file types.
      // Following identified as remote file system types
      // CIFS_MAGIC_NUMBER, CODA_SUPER_MAGIC, NCP_SUPER_MAGIC, NFS_SUPER_MAGIC, SMB_SUPER_MAGIC,
      // TMPFS_MAGIC
      // 0xFF534D42 , 0x73757245 , 0x564c , 0x6969 , 0x517B , 0x01021994
      // 4283649346 , 1937076805 , 22092 , 26985 , 20859 , 16914836
      @Immutable
      private static final int[] REMOTE_TYPES =
          new int[] { /* 4283649346, */ 1937076805, 22092, 26985, 20859, 16914836};

      public boolean isTypeLocal() {
        for (int remoteType : REMOTE_TYPES) {
          if (remoteType == f_type) {
            return false;
          }
        }
        return true;
      }

      public static void dummy() {

      }
    }

    @SuppressWarnings("unused")
    public static class FSPARELongArr5 extends Structure {

      public long[] fspare = new long[5];

      @Override
      protected List<String> getFieldOrder() {
        return singletonList("fspare");
      }
    }

    @SuppressWarnings("unused")
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
      // CIFS_MAGIC_NUMBER, CODA_SUPER_MAGIC, NCP_SUPER_MAGIC, NFS_SUPER_MAGIC, SMB_SUPER_MAGIC,
      // TMPFS_MAGIC
      // 0xFF534D42 , 0x73757245 , 0x564c , 0x6969 , 0x517B , 0x01021994
      // 4283649346 , 1937076805 , 22092 , 26985 , 20859 , 16914836
      @Immutable
      private static final long[] REMOTE_TYPES =
          new long[] {4283649346L, 1937076805L, 22092L, 26985L, 20859L, 16914836L};

      static {
        try {
          Native.register("rt");
          StatFS64 struct = new StatFS64();
          int ret = statfs(".", struct);
          isStatFSEnabled = ret == 0;
        } catch (Throwable t) {
          System.out.println("got error t: " + t.getMessage());
          t.printStackTrace();
          isStatFSEnabled = false;
        }
      }

      public static native int statfs(String path, StatFS64 statfs) throws LastErrorException;

      @Override
      protected List<String> getFieldOrder() {
        return asList("f_type", "f_bsize", "f_blocks", "f_bfree", "f_bavail",
            "f_files", "f_ffree", "f_fsid", "f_namelen", "f_frsize", "f_spare");
      }


      public boolean isTypeLocal() {
        for (long remoteType : REMOTE_TYPES) {
          if (remoteType == f_type) {
            return false;
          }
        }
        return true;
      }

      public static void dummy() {

      }
    }

    /**
     * Get the file store type of a path. for example, /dev/sdd1(store name) /w2-gst-dev40d(mount
     * point) ext4(type)
     *
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
     * This will return whether the path passed in as arg is part of a local file system or a remote
     * file system. This method is mainly used by the DiskCapacityMonitor thread and we don't want
     * to monitor remote fs available space as due to network problems/firewall issues the call to
     * getUsableSpace can hang. See bug #49155. On platforms other than Linux this will return false
     * even if it on local file system for now.
     */
    @Override
    public boolean isOnLocalFileSystem(final String path) {
      if (!isStatFSEnabled) {
        // if (logger != null && logger.fineEnabled()) {
        // logger.info("DEBUG isOnLocalFileSystem returning false 1 for path = " + path);
        // }
        return false;
      }
      final int numTries = 10;
      for (int i = 1; i <= numTries; i++) {
        try {
          if (Platform.is64Bit()) {
            StatFS64 stat = new StatFS64();
            StatFS64.statfs(path, stat);
            // if (logger != null && logger.fineEnabled()) {
            // logger.info("DEBUG 64 isOnLocalFileSystem returning " + stat.isTypeLocal() + " for
            // path = " + path);
            // }
            return stat.isTypeLocal();
          } else {
            StatFS stat = new StatFS();
            StatFS.statfs(path, stat);
            // if (logger != null && logger.fineEnabled()) {
            // logger.fine("DEBUG 32 isOnLocalFileSystem returning " + stat.isTypeLocal() + " for
            // path = " + path);
            // }
            return stat.isTypeLocal();
          }
        } catch (LastErrorException le) {
          // ignoring it as NFS mounted can give this exception
          // and we just want to retry to remove transient problem.
          if (logger.isDebugEnabled()) {
            logger.debug("DEBUG isOnLocalFileSystem got ex = " + le + " msg = " + le.getMessage());
          }
        }
      }
      return false;
    }

    @Immutable
    private static final String[] FallocateFileSystems = {"ext4", "xfs", "btrfs", "ocfs2"};

    @Override
    protected boolean hasFallocate(String path) {
      String fstype = getFileStoreType(path);
      for (String type : FallocateFileSystems) {
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
    protected void fallocateFD(int fd, long offset, long len) throws LastErrorException {
      int errno = posix_fallocate64(fd, offset, len);
      if (errno != 0) {
        throw new LastErrorException(errno);
      }
    }
  }

  /**
   * Implementation of {@link NativeCalls} for Windows platforms.
   */
  private static class WinNativeCalls extends NativeCalls {

    @Override
    public synchronized String getEnvironment(final String name) {
      if (name == null) {
        throw new UnsupportedOperationException("getEnvironment() for name=NULL");
      }
      int psize = Kernel32.INSTANCE.GetEnvironmentVariable(name, null, 0);
      if (psize > 0) {
        for (;;) {
          char[] result = new char[psize];
          psize = Kernel32.INSTANCE.GetEnvironmentVariable(name, result, psize);
          if (psize == (result.length - 1)) {
            return new String(result, 0, psize);
          } else if (psize <= 0) {
            return null;
          }
        }
      } else {
        return null;
      }
    }

    @Override
    public int getProcessId() {
      return Kernel32.INSTANCE.GetCurrentProcessId();
    }

    @Override
    public boolean isProcessActive(final int processId) {
      try {
        final HANDLE procHandle =
            Kernel32.INSTANCE.OpenProcess(Kernel32.PROCESS_QUERY_INFORMATION, false, processId);
        if (procHandle == null || WinBase.INVALID_HANDLE_VALUE.equals(procHandle)) {
          return false;
        } else {
          final IntByReference status = new IntByReference();
          final boolean result = Kernel32.INSTANCE.GetExitCodeProcess(procHandle, status)
              && status.getValue() == Kernel32.STILL_ACTIVE;
          Kernel32.INSTANCE.CloseHandle(procHandle);
          return result;
        }
      } catch (LastErrorException le) {
        // some problem in getting process status
        return false;
      }
    }

    @Override
    public boolean killProcess(final int processId) {
      try {
        final HANDLE procHandle =
            Kernel32.INSTANCE.OpenProcess(Kernel32.PROCESS_TERMINATE, false, processId);
        if (procHandle == null || Kernel32.INVALID_HANDLE_VALUE.equals(procHandle)) {
          return false;
        } else {
          final boolean result = Kernel32.INSTANCE.TerminateProcess(procHandle, -1);
          Kernel32.INSTANCE.CloseHandle(procHandle);
          return result;
        }
      } catch (LastErrorException le) {
        // some problem in killing the process
        return false;
      }
    }

    @Override
    public void daemonize(RehashServerOnSIGHUP callback)
        throws UnsupportedOperationException, IllegalStateException {
      throw new IllegalStateException("daemonize() not applicable for Windows platform");
    }
  }
}
