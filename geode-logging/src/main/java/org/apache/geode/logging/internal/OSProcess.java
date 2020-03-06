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
package org.apache.geode.logging.internal;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.zip.GZIPOutputStream;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;
import org.apache.geode.util.internal.TeePrintStream;

/**
 * Used to interact with operating system processes. Use <code>exec</code> to create a new process
 * by executing a command. Use <code>kill</code> to kill a process.
 *
 */
// TODO: In the next major release, we should remove the variables and logic related to the system
// properties used to determine whether output redirection is allowed or not
// (DISABLE_OUTPUT_REDIRECTION_PROPERTY, ENABLE_OUTPUT_REDIRECTION_PROPERTY,
// DISABLE_REDIRECTION_CONFIGURATION_PROPERTY, ENABLE_OUTPUT_REDIRECTION, DISABLE_OUTPUT_REDIRECTION
// and DISABLE_REDIRECTION_CONFIGURATION). GFSH should always use the new redirect-output flag.
public class OSProcess {
  private static final Logger logger = LogService.getLogger();

  /**
   * @deprecated use GFSH redirect-output flag instead.
   */
  @Deprecated
  public static final String DISABLE_OUTPUT_REDIRECTION_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "OSProcess.DISABLE_OUTPUT_REDIRECTION";

  /**
   * @deprecated use GFSH redirect-output flag instead.
   */
  @Deprecated
  public static final String ENABLE_OUTPUT_REDIRECTION_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "OSProcess.ENABLE_OUTPUT_REDIRECTION";

  /**
   * @deprecated use GFSH redirect-output flag instead.
   */
  @Deprecated
  public static final String DISABLE_REDIRECTION_CONFIGURATION_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "OSProcess.DISABLE_REDIRECTION_CONFIGURATION";

  /**
   * @deprecated use GFSH redirect-output flag instead.
   */
  @Deprecated
  private static final boolean ENABLE_OUTPUT_REDIRECTION =
      Boolean.getBoolean(ENABLE_OUTPUT_REDIRECTION_PROPERTY);

  /**
   * @deprecated use GFSH redirect-output flag instead.
   */
  @Deprecated
  private static final boolean DISABLE_OUTPUT_REDIRECTION =
      Boolean.getBoolean(DISABLE_OUTPUT_REDIRECTION_PROPERTY);

  /**
   * @deprecated use GFSH redirect-output flag instead.
   */
  @Deprecated
  private static final boolean DISABLE_REDIRECTION_CONFIGURATION =
      Boolean.getBoolean(DISABLE_REDIRECTION_CONFIGURATION_PROPERTY);

  /**
   * Starts a background command writing its stdout and stderr to the specified log file.
   *
   * @param cmdarray An array of strings that specify the command to run. The first element must be
   *        the executable. Each additional command line argument should have its own entry in the
   *        array.
   * @param workdir the current directory of the created process
   * @param logfile the file the created process will write stdout and stderr to.
   * @param inheritLogfile can be set to false if the child process is willing to create its own log
   *        file. Setting to false can help on Windows because it keeps the child process from
   *        inheriting handles from the parent.
   * @return the process id of the created process; -1 on failure
   * @exception IOException if a child process could not be created.
   */
  private static native int bgexecInternal(String[] cmdarray, String workdir, String logfile,
      boolean inheritLogfile) throws IOException;

  /**
   * Starts execution of the specified command and arguments in a separate detached process in the
   * specified working directory writing output to the specified log file.
   * <p>
   * If there is a security manager, its <code>checkExec</code> method is called with the first
   * component of the array <code>cmdarray</code> as its argument. This may result in a security
   * exception.
   * <p>
   * Given an array of strings <code>cmdarray</code>, representing the tokens of a command line,
   * this method creates a new process in which to execute the specified command.
   *
   * @param cmdarray array containing the command to call and its arguments.
   * @param workdir the current directory of the created process; null causes working directory to
   *        default to the current directory.
   * @param logfile the file the created process will write stdout and stderr to; null causes a
   *        default log file name to be used.
   * @param inheritLogfile can be set to false if the child process is willing to create its own log
   *        file. Setting to false can help on Windows because it keeps the child process from
   *        inheriting handles from the parent.
   * @param env any extra environment variables as key,value map; these will be in addition to those
   *        inherited from the parent process and will overwrite same keys
   * @return the process id of the created process; -1 on failure
   * @exception SecurityException if the current thread cannot create a subprocess.
   * @see java.lang.SecurityException
   * @see java.lang.SecurityManager#checkExec(java.lang.String)
   */
  public static int bgexec(String cmdarray[], File workdir, File logfile, boolean inheritLogfile,
      Map<String, String> env) throws IOException {
    String commandShell =
        System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "commandShell", "bash");
    if (cmdarray.length == 0) {
      throw new java.lang.IndexOutOfBoundsException();
    }
    boolean isWindows = false;
    String os = System.getProperty("os.name");
    if (os != null) {
      if (os.indexOf("Windows") != -1) {
        isWindows = true;
      }
    }
    for (int i = 0; i < cmdarray.length; i++) {
      if (cmdarray[i] == null) {
        throw new NullPointerException();
      }
      if (isWindows) {
        if (i == 0) {
          // do the following before quotes get added.
          File cmd = new File(cmdarray[0]);
          if (!cmd.exists()) {
            cmd = new File(cmdarray[0] + ".exe");
            if (cmd.exists()) {
              cmdarray[0] = cmd.getPath();
            }
          }
        }
        String s = cmdarray[i];
        if (i != 0) {
          if (s.length() == 0) {
            cmdarray[i] = "\"\""; // fix for bug 22207
          } else if ((s.indexOf(' ') >= 0 || s.indexOf('\t') >= 0)) {
            String unquotedS = s;
            if (s.indexOf('\"') != -1) {
              // Note that Windows provides no way to embed a double
              // quote in a double quoted string so need to remove
              // any internal quotes and let the outer quotes
              // preserve the whitespace.
              StringBuffer b = new StringBuffer(s);
              int quoteIdx = s.lastIndexOf('\"');
              while (quoteIdx != -1) {
                b.deleteCharAt(quoteIdx);
                quoteIdx = s.lastIndexOf('\"', quoteIdx - 1);
              }
              unquotedS = b.toString();
            }
            // It has whitespace and its not quoted
            cmdarray[i] = '"' + unquotedS + '"';
          }
        }
      }
    }
    File cmd = new File(cmdarray[0]);
    if (!cmd.exists()) {
      throw new IOException(String.format("the executable %s does not exist",
          cmd.getPath()));
    }
    SecurityManager security = System.getSecurityManager();
    if (security != null) {
      security.checkExec(cmdarray[0]);
    }
    if (workdir != null && !workdir.isDirectory()) {
      String curDir = new File("").getAbsolutePath();
      System.out.println(
          String.format("WARNING: %s is not a directory. Defaulting to current directory %s.",
              new Object[] {workdir, curDir}));
      workdir = null;
    }
    if (workdir == null) {
      workdir = new File("").getAbsoluteFile();
    }
    if (logfile == null) {
      logfile = File.createTempFile("bgexec", ".log", workdir);
    }
    if (!logfile.isAbsolute()) {
      // put it in the working directory
      logfile = new File(workdir, logfile.getPath());
    }
    // fix for bug 24575
    if (logfile.exists()) {
      // it already exists so make sure its a file and can be written
      if (!logfile.isFile()) {
        throw new IOException(String.format("The log file %s was not a normal file.",
            logfile.getPath()));
      }
      if (!logfile.canWrite()) {
        throw new IOException(String.format("Need write access for the log file %s.",
            logfile.getPath()));
      }
    } else {
      try {
        logfile.createNewFile();
      } catch (IOException io) {
        throw new IOException(String.format("Could not create log file %s because: %s.",
            new Object[] {logfile.getPath(), io.getMessage()}));
      }
    }
    String trace = System.getProperty("org.apache.geode.logging.internal.OSProcess.trace");
    if (trace != null && trace.length() > 0) {
      for (int i = 0; i < cmdarray.length; i++) {
        System.out.println("cmdarray[" + i + "] = " + cmdarray[i]);
      }
      System.out.println("workdir=" + workdir.getPath());
      System.out.println("logfile=" + logfile.getPath());
    }
    int result = 0;

    StringBuffer sb = new StringBuffer();
    Vector<String> cmdVec = new Vector<>();
    // Add shell code to spawn a process silently
    if (isWindows) {
      cmdVec.add("cmd.exe");
      cmdVec.add("/c");
      sb.append("start /b \"\" ");
    } else {
      // to address issue with users that don't have bash shell installed
      if (commandShell.equals("bash")) {
        cmdVec.add("bash");
        cmdVec.add("--norc");
        cmdVec.add("-c");
      } else {
        cmdVec.add(commandShell);
      }
    }
    // Add the actual command
    for (int i = 0; i < cmdarray.length; i++) {
      if (i != 0)
        sb.append(" ");
      if (cmdarray[i].length() != 0 && cmdarray[i].charAt(0) == '\"') {
        // The token has already been quoted, see bug 40835
        sb.append(cmdarray[i]);
      } else {
        sb.append("\"");
        sb.append(cmdarray[i]);
        sb.append("\"");
      }
    }
    // Add the IO redirction code, this prevents hangs and IO blocking
    sb.append(" >> ");
    sb.append(logfile.getPath());
    sb.append(" 2>&1");
    if (isWindows) {
      sb.append(" <NUL");
    } else {
      sb.append(" </dev/null &");
    }
    cmdVec.add(sb.toString());

    String[] cmdStrings = cmdVec.toArray(new String[0]);
    if (trace != null && trace.length() > 0) {
      for (int i = 0; i < cmdStrings.length; i++) {
        System.out.println("cmdStrings[" + i + "] = " + cmdStrings[i]);
      }
      System.out.println("workdir=" + workdir.getPath());
      System.out.println("logfile=" + logfile.getPath());
    }
    final ProcessBuilder procBuilder = new ProcessBuilder(cmdStrings);
    if (env != null && env.size() > 0) {
      // adjust the environment variables inheriting from parent
      procBuilder.environment().putAll(env);
    }
    procBuilder.directory(workdir);
    final Process process = procBuilder.start();
    try {
      process.getInputStream().close();
    } catch (IOException ignore) {
    }
    try {
      process.getOutputStream().close();
    } catch (IOException ignore) {
    }
    try {
      process.getErrorStream().close();
    } catch (IOException ignore) {
    }
    try {
      // short count = 1000;
      boolean processIsStillRunning = true;
      while (processIsStillRunning) {
        Thread.sleep(10);
        try {
          process.exitValue();
          processIsStillRunning = false;
        } catch (IllegalThreadStateException itse) {
          // Ignore this, we are polling the exitStatus
          // instead of using the blocking Process#waitFor()
        }
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }

    return result; // Always 0 for pureJava
  }

  /**
   * Checks to make sure that we are operating on a valid process id. Sending signals to processes
   * with <code>pid</code> 0 or -1 can have unintended consequences.
   *
   * @throws IllegalArgumentException If <code>pid</code> is not positive
   *
   * @since GemFire 4.0
   */
  private static void checkPid(int pid) {
    if (pid <= 0) {
      throw new IllegalArgumentException(
          String.format("Should not send a signal to pid %s",
              pid));
    }
  }

  /**
   * Ask a process to shut itself down. The process may catch and ignore this shutdown request.
   *
   * @param pid the id of the process to shutdown
   * @return true if the request was sent to the process; false if the process does not exist or can
   *         not be asked to shutdown.
   */
  public static boolean shutdown(int pid) {
    throw new RuntimeException(
        "shutdown not allowed in pure java mode");
  }

  private static native boolean _shutdown(int pid);

  /**
   * Terminate a process without warning and without a chance of an orderly shutdown. This method
   * should only be used as a last resort. The {@link #shutdown(int)} method should be used in most
   * cases.
   *
   * @param pid the id of the process to kill
   * @return true if the process was killed; false if it does not exist or can not be killed.
   */
  public static boolean kill(int pid) {
    throw new RuntimeException(
        "kill not allowed in pure java mode");
  }

  private static native boolean _kill(int pid);

  /**
   * Tells a process to print its stacks to its standard output
   *
   * @param pid the id of the process that will print its stacks, or zero for the current process
   * @return true if the process was told; false if it does not exist or can not be told.
   */
  public static boolean printStacks(int pid) {
    return printStacks(pid, false);
  }

  /**
   * Tells a process to print its stacks to its standard output or the given log writer
   *
   * @param pid the id of the process that will print its stacks, or zero for the current process
   * @param useNative if true we attempt to use native code, which goes to stdout
   * @return true if the process was told; false if it does not exist or can not be told.
   */
  public static boolean printStacks(int pid, boolean useNative) {
    if (!useNative) {
      if (pid > 0 && pid != myPid[0]) {
        return false;
      }
      CharArrayWriter cw = new CharArrayWriter(50000);
      PrintWriter sb = new PrintWriter(cw, true);
      sb.append("\n******** full thread dump ********\n");
      ThreadMXBean bean = ManagementFactory.getThreadMXBean();
      long[] threadIds = bean.getAllThreadIds();
      ThreadInfo[] infos = bean.getThreadInfo(threadIds, true, true);
      long thisThread = Thread.currentThread().getId();
      for (int i = 0; i < infos.length; i++) {
        if (i != thisThread && infos[i] != null) {
          formatThreadInfo(infos[i], sb);
        }
      }
      sb.flush();
      logger.warn(cw.toString());
      return true;
    } else {
      if (pid < 0)
        checkPid(pid);
      return _printStacks(pid);
    }
  }

  /** dumps this vm's stacks and returns gzipped result */
  public static byte[] zipStacks() throws IOException {
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    long[] threadIds = bean.getAllThreadIds();
    ThreadInfo[] infos = bean.getThreadInfo(threadIds, true, true);
    long thisThread = Thread.currentThread().getId();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    GZIPOutputStream zipOut = new GZIPOutputStream(baos, 10000);
    PrintWriter pw = new PrintWriter(zipOut, true);
    for (int i = 0; i < infos.length; i++) {
      if (i != thisThread && infos[i] != null) {
        formatThreadInfo(infos[i], pw);
      }
    }
    pw.flush();
    zipOut.close();
    return baos.toByteArray();
  }

  private static native boolean _printStacks(int pid);

  static final int MAX_STACK_FRAMES = 75;

  private static void formatThreadInfo(ThreadInfo t, PrintWriter pw) {
    // this is largely copied from the JDK's ThreadInfo.java, but it limits the
    // stacks to 8 elements
    pw.append("\"").append(t.getThreadName()).append("\"").append(" tid=0x")
        .append(Long.toHexString(t.getThreadId()));
    // this is in the stack trace elements so we don't need to add it
    // if (t.getLockName() != null) {
    // pw.append(" ");
    // pw.append(StringUtils.toLowerCase(t.getThreadState().toString()));
    // pw.append(" on " + t.getLockName());
    // }
    // priority is not known
    // daemon status is not known
    if (t.isSuspended()) {
      pw.append(" (suspended)");
    }
    if (t.isInNative()) {
      pw.append(" (in native)");
    }
    if (t.getLockOwnerName() != null) {
      pw.append(" owned by \"").append(t.getLockOwnerName()).append("\" tid=0x")
          .append(Long.toHexString(t.getLockOwnerId()));
    }
    pw.append('\n');
    pw.append("    java.lang.Thread.State: ").append(String.valueOf(t.getThreadState()))
        .append("\n");
    int i = 0;
    StackTraceElement[] stackTrace = t.getStackTrace();
    for (; i < stackTrace.length && i < MAX_STACK_FRAMES; i++) {
      StackTraceElement ste = stackTrace[i];
      pw.append("\tat ").append(ste.toString());
      pw.append('\n');
      if (i == 0 && t.getLockInfo() != null) {
        Thread.State ts = t.getThreadState();
        switch (ts) {
          case BLOCKED:
            pw.append("\t-  blocked on ").append(String.valueOf(t.getLockInfo()));
            pw.append('\n');
            break;
          case WAITING:
            pw.append("\t-  waiting on ").append(String.valueOf(t.getLockInfo()));
            pw.append('\n');
            break;
          case TIMED_WAITING:
            pw.append("\t-  waiting on ").append(String.valueOf(t.getLockInfo()));
            pw.append('\n');
            break;
          default:
        }
      }

      for (MonitorInfo mi : t.getLockedMonitors()) {
        if (mi.getLockedStackDepth() == i) {
          pw.append("\t-  locked ").append(String.valueOf(mi));
          pw.append('\n');
        }
      }
    }
    if (i < stackTrace.length) {
      pw.append("\t...");
      pw.append('\n');
    }

    LockInfo[] locks = t.getLockedSynchronizers();
    if (locks.length > 0) {
      pw.append("\n\tNumber of locked synchronizers = ").append(String.valueOf(locks.length));
      pw.append('\n');
      for (LockInfo li : locks) {
        pw.append("\t- ").append(String.valueOf(li));
        pw.append('\n');
      }
    }
    pw.append('\n');
  }

  /**
   * Find out if a process exists.
   *
   * @param pid the id of the process to check for
   * @return true if the process exists; false if it does not.
   */
  public static boolean exists(int pid) {
    throw new RuntimeException(
        "exists not allowed in pure java mode");
  }

  private static native boolean nativeExists(int pid);

  // Private stuff
  /**
   * Waits for a child process to die and reaps it.
   */
  private static native void waitForPid(int pid);

  /**
   * Waits until the identified process exits. If the process does not exist then returns
   * immediately.
   */
  public static void waitForPidToExit(int pid) {
    throw new RuntimeException(
        "waitForPidToExit not allowed in pure java mode");
  }

  /**
   * Sets the current directory of this process.
   *
   * @return true if current directory was set; false if not.
   */
  public static boolean setCurrentDirectory(File curDir) {
    throw new RuntimeException(
        "setCurrentDirectory not allowed in pure java mode");
  }

  /**
   * Returns true on success. Returns false and current directory is unchanged on failure.
   */
  private static native boolean jniSetCurDir(String dir);

  /**
   * Reaps a child process if it has died. Does not wait for the child.
   *
   * @param pid the id of the process to reap
   * @return true if it was reaped or lost (someone else reaped it); false if the child still
   *         exists. HACK: If pid is -1 then returns true if this platform needs reaping.
   */
  protected static native boolean reapPid(int pid);

  @MakeNotStatic
  private static Thread reaperThread;
  @MakeNotStatic
  protected static Set pids = null;

  // myPid caches result of getProcessId . To provide a stable processId
  // on Linux, where processId may differ per thread, we cache the
  // processId of the reaper thread .
  @MakeNotStatic
  static final int[] myPid = new int[1]; // cache of my processId

  @MakeNotStatic
  static boolean reaperStarted = false; // true if cache is valid

  /**
   * On Linux, getProcessId returns the processId of the calling thread
   */
  static native int getProcessId();

  static {
    // just initialize the pid cache
    synchronized (myPid) {
      int pid = 0;
      // Windows checks have been disabled as the ManagementFactory hack
      // to find the PID has been seen to work on Windows 7. Add checks
      // for more specific versions of Windows if this fails on them
      // if(! System.getProperty("os.name", "").startsWith("Windows")) {
      String name = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
      int idx = name.indexOf('@');
      try {
        pid = Integer.parseInt(name.substring(0, idx));
      } catch (NumberFormatException nfe) {
        // something changed in the RuntimeMXBean name
      }
      // }
      myPid[0] = pid;
      reaperStarted = true;
    }
  }

  /**
   * Get the vm's process id. On Linux, this returns the processId of the reaper thread.
   *
   * @return the vm's process id.
   */
  public static int getId() {
    boolean done = false;
    int result = -1;
    for (;;) {
      synchronized (myPid) {
        done = reaperStarted;
        result = myPid[0];
      }
      if (done)
        break;

      // wait for reaper thread to initialize myPid
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
    }
    return result;
  }

  public static PrintStream redirectOutput(File newOutput) throws IOException {
    return redirectOutput(newOutput, true);
  }

  public static PrintStream redirectOutput(File newOutput, boolean setOut) throws IOException {
    FileOutputStream newFileStream = null;
    try {
      newFileStream = new FileOutputStream(newOutput, true);
    } catch (FileNotFoundException e) {
      throw new IOException("File not found: " + newOutput, e);
    }
    final PrintStream newPrintStream =
        new PrintStream(new BufferedOutputStream(newFileStream, 128), true);
    if (((DISABLE_REDIRECTION_CONFIGURATION)
        || (ENABLE_OUTPUT_REDIRECTION && !DISABLE_OUTPUT_REDIRECTION)) && setOut) {
      System.setOut(newPrintStream);
      if (System.err instanceof TeePrintStream) {
        ((TeePrintStream) System.err).getTeeOutputStream()
            .setBranchOutputStream(new BufferedOutputStream(newFileStream, 128));
      } else {
        System.setErr(newPrintStream);
      }
    }
    return newPrintStream;
  }

  private static native void redirectCOutput(String file);

  /**
   * Registers a signal handler for SIGQUIT on UNIX platforms.
   */
  private static native void registerSigQuitHandler();
}
