package com.gemstone.gemfire.test.process;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.logging.LogService;

import com.gemstone.gemfire.test.process.*;

import junit.framework.Assert;

/**
 * Wraps spawned process to capture output and provide hooks to the
 * {@link java.lang.Process} object.
 *
 * @author Kirk Lund
 * @since 4.1.1
 */
public class ProcessWrapper extends Assert {
  private static final Logger logger = LogService.getLogger();

  protected static final String TIMEOUT_MILLIS_PROPERTY = "quickstart.test.TIMEOUT_MILLIS";
  private static final long TIMEOUT_MILLIS = Long.getLong(TIMEOUT_MILLIS_PROPERTY, 5 * 60 * 1000L); 
  private static final boolean JAVA_AWT_HEADLESS = true;

  private final String[] jvmArgs;  
  
  private final Class<?> mainClass;
  private final String[] mainArgs;

  private volatile Process process;
  private volatile Throwable processException;
  private volatile ProcessOutputReader outputReader;

  private final boolean useMainLauncher;
  
  private final List<String> allLines;
  private final BlockingQueue<String> lineBuffer;

  private final AtomicInteger exitValue = new AtomicInteger(-1);
  private boolean starting = false;
  private boolean started = false;
  private boolean stopped = false;
  private boolean interrupted = false;
  private Thread processThread;
  private ProcessStreamReader stdout;
  private ProcessStreamReader stderr;
  
  public ProcessWrapper(Class<?> main, String[] mainArgs) {
    this(main, mainArgs, true);
  }

  public ProcessWrapper(Class<?> main, String[] mainArgs, boolean useMainLauncher) {
    this(null, main, mainArgs, true);
  }
  
  public ProcessWrapper(String[] jvmArgs, Class<?> main, String[] mainArgs, boolean useMainLauncher) {
    this.jvmArgs = jvmArgs;
    
    this.mainClass = main;
    this.mainArgs = mainArgs;

    this.lineBuffer = new LinkedBlockingQueue<String>();
    this.allLines = Collections.synchronizedList(new ArrayList<String>());
    
    this.useMainLauncher = useMainLauncher;
  }
  
  public ProcessStreamReader getStandardOutReader() { // TODO:protected
    synchronized (this.exitValue) {
      return stdout;
    }
  }
  
  public ProcessStreamReader getStandardErrorReader() { // TODO:protected
    synchronized (this.exitValue) {
      return stderr;
    }
  }
  
  private void waitForProcessStart() throws InterruptedException {
    long start = System.currentTimeMillis();
    boolean done = false;
    while (!done) {
      synchronized (this.exitValue) {
        done = (this.process != null || this.processException != null) && 
            (this.started || this.exitValue.get() > -1 || this.interrupted);
      }
      if (!done && System.currentTimeMillis() > start + TIMEOUT_MILLIS) {
        fail("Timed out launching process");
      }
      Thread.sleep(100);
    }
  }
  
  public boolean isAlive() throws InterruptedException {
    checkStarting();
    waitForProcessStart();
    
    synchronized (this.exitValue) {
      if (this.interrupted) { // TODO: do we want to do this?
        throw new InterruptedException("Process was interrupted");
      }
      return this.exitValue.get() == -1 && this.started && !this.stopped && !this.interrupted && this.processThread.isAlive();
    }
  }
  
  public ProcessWrapper destroy() {
    if (this.process != null) {
      this.process.destroy();
    }
    return this;
  }

  public int waitFor(long timeout, boolean throwOnTimeout) throws InterruptedException {
    checkStarting();
    Thread thread = getThread();
    thread.join(timeout);
    synchronized (this.exitValue) {
      if (throwOnTimeout) {
        checkStopped();
      }
      return this.exitValue.get();
    }
  }
  
  public int waitFor(long timeout) throws InterruptedException {
    return waitFor(timeout, false);
  }
  
  public int waitFor(boolean throwOnTimeout) throws InterruptedException {
    return waitFor(TIMEOUT_MILLIS, throwOnTimeout);
  }
  
  public int waitFor() throws InterruptedException {
    return waitFor(TIMEOUT_MILLIS, false);
  }
  
  public String getOutput() { 
    return getOutput(false);
  }

  public String getOutput(boolean ignoreStopped) { 
    checkStarting();
    if (!ignoreStopped) {
      checkStopped();
    }
    StringBuffer sb = new StringBuffer();
    Iterator<String> iterator = allLines.iterator();
    while (iterator.hasNext()) {
      sb.append(iterator.next() + "\n");
    }
    return sb.toString();
  }

  public ProcessWrapper sendInput() {
    checkStarting();
    sendInput("");
    return this;
  }

  public ProcessWrapper sendInput(String input) {
    checkStarting();
    PrintStream ps = new PrintStream(this.process.getOutputStream());
    ps.println(input);
    ps.flush();
    return this;
  }

  public ProcessWrapper failIfOutputMatches(String patternString, long timeoutMillis) throws InterruptedException {
    checkStarting();
    checkOk();

    Pattern pattern = Pattern.compile(patternString);
    
    logger.debug("failIfOutputMatches waiting for \"{}\"...", patternString);
    long start = System.currentTimeMillis();
    while(System.currentTimeMillis() <= start+timeoutMillis) {
      String line = lineBuffer.poll(timeoutMillis, TimeUnit.MILLISECONDS);

      if (line != null && pattern.matcher(line).matches()) {
        fail("failIfOutputMatches Matched pattern \"" + patternString + "\" against output \"" + line + "\". Output: " + this.allLines);
      }
    }
    return this;
  }
  
  /*
   * Waits for the process stdout or stderr stream to contain the specified 
   * text. Uses the specified timeout for debugging purposes.
   */
  public ProcessWrapper waitForOutputToMatch(String patternString, long timeoutMillis) throws InterruptedException {
    checkStarting();
    checkOk();

    Pattern pattern = Pattern.compile(patternString);
    
    logger.debug("ProcessWrapper:waitForOutputToMatch waiting for \"{}\"...", patternString);
    while(true) {
      String line = this.lineBuffer.poll(timeoutMillis, TimeUnit.MILLISECONDS);

      if (line == null) {
        fail("Timed out waiting for output \"" + patternString + "\" after " + TIMEOUT_MILLIS + " ms. Output: " + new OutputFormatter(this.allLines));
      }
      
      if (pattern.matcher(line).matches()) {
        logger.debug("ProcessWrapper:waitForOutputToMatch Matched pattern \"{}\" against output \"{}\"", patternString, line);
        break;
      } else {
        logger.debug("ProcessWrapper:waitForOutputToMatch Did not match pattern \"{}\" against output \"{}\"", patternString, line);
      }
    }
    return this;
  }
  
  /*
   * Waits for the process stdout or stderr stream to contain the specified 
   * text. Uses the default timeout.
   */
  public ProcessWrapper waitForOutputToMatch(String patternString) throws InterruptedException {
    return waitForOutputToMatch(patternString, TIMEOUT_MILLIS);
  }

  public ProcessWrapper execute() throws InterruptedException {
    return execute(null, new File(System.getProperty("user.dir")));
  }

  public ProcessWrapper execute(Properties props) throws InterruptedException {
    return execute(props, new File(System.getProperty("user.dir")));
  }
  
  public ProcessWrapper execute(final Properties props, final File workingDirectory) throws InterruptedException {
    synchronized (this.exitValue) {
      if (this.starting) {
        throw new IllegalStateException("ProcessWrapper can only be executed once");
      }
      this.starting = true;
      this.processThread = new Thread(new Runnable() {
        public void run() {
          exec(props, workingDirectory);
        }
      }, "ProcessWrapper Process Thread");
    }
    this.processThread.start();

    waitForProcessStart();

    synchronized (this.exitValue) {
      if (this.processException != null) {
        System.out.println("ProcessWrapper:execute failed with " + this.processException);
        this.processException.printStackTrace();
      }
    }

    if (this.useMainLauncher) {
      sendInput(); // to trigger MainLauncher delegation to inner main
    }
    return this;
  }

  private void exec(Properties dsProps, final File workingDirectory) {
    List<String> vmArgList = new ArrayList<String>();

    if (dsProps != null) {
      for (Map.Entry<Object, Object> entry : dsProps.entrySet()) {
        if (!entry.getKey().equals("log-file")) {
          vmArgList.add("-D" + entry.getKey() + "=" + entry.getValue());
        }
      }
    }

    if (JAVA_AWT_HEADLESS) {
      vmArgList.add("-Djava.awt.headless=true");
    }
    
    if (this.jvmArgs != null) {
      for (String vmArg: this.jvmArgs) {
        vmArgList.add(vmArg);
      }
    }
    
    String[] vmArgs = vmArgList.toArray(new String[vmArgList.size()]);

    try {
      synchronized (this.exitValue) {
        String[] command = defineCommand(vmArgs);
        this.process = new ProcessBuilder(command).directory(workingDirectory).start();
        
        StringBuilder processCommand = new StringBuilder();
        boolean addSpace = false;
        for (String string : command) {
          if (addSpace) {
            processCommand.append(" ");
          }
          processCommand.append(string);
          addSpace = true;
        }
        String commandString = processCommand.toString();
        System.out.println("Executing " + commandString);
        final ProcessStreamReader stdOut = new ProcessStreamReader(commandString, this.process.getInputStream(), this.lineBuffer, this.allLines);
        final ProcessStreamReader stdErr = new ProcessStreamReader(commandString, this.process.getErrorStream(), this.lineBuffer, this.allLines);
  
        this.stdout = stdOut;
        this.stderr = stdErr;
  
        this.outputReader = new ProcessOutputReader(this.process, stdOut, stdErr, this.allLines);
      
        this.started = true;
      }
      
      this.outputReader.waitFor();
      int code = this.process.waitFor();
      
      synchronized (this.exitValue) {
        this.exitValue.set(code);
        this.stopped = true;
      }
      
    } catch (InterruptedException e) {
      synchronized (this.exitValue) {
        this.interrupted = true;
        this.processException = e;
      }
    } catch (Throwable t) {
      synchronized (this.exitValue) {
        this.processException = t;
      }
    }
  }
  
  private String[] defineCommand(String[] vmArgs) {
    File javabindir = new File(System.getProperty("java.home"), "bin");
    File javaexe = new File(javabindir, "java");

    List<String> argList = new ArrayList<String>();
    argList.add(javaexe.getPath());
    argList.add("-classpath");
    argList.add(System.getProperty("java.class.path"));

    // -d64 is not a valid option for windows and results in failure
    int bits = Integer.getInteger("sun.arch.data.model", 0).intValue();
    if (bits == 64 && !(System.getProperty("os.name").toLowerCase().contains("windows"))) {
      argList.add("-d64");
    }

    argList.add("-Djava.library.path=" + System.getProperty("java.library.path"));

    if (vmArgs != null) {
      argList.addAll(Arrays.asList(vmArgs));
    }

    if (this.useMainLauncher) {
      argList.add(MainLauncher.class.getName());
    }
    argList.add(mainClass.getName());
    
    if (mainArgs != null) {
      argList.addAll(Arrays.asList(mainArgs));
    }

    String[] cmd = argList.toArray(new String[argList.size()]);
    return cmd;
  }
  
  private String toString(String[] strings) {
    if (strings == null || strings.length < 1) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (String string : strings) {
      sb.append(string).append("\n");
    }
    return sb.toString();
  }

  private void checkStarting() throws IllegalStateException {
    synchronized (this.exitValue) {
      if (!this.starting) {
        throw new IllegalStateException("Process has not been launched");
      }
    }
  }
  
  private void checkStopped() throws IllegalStateException {
    synchronized (this.exitValue) {
      if (!this.stopped) {
        throw new IllegalStateException("Process has not stopped");
      }
    }
  }
  
  private void checkOk() throws RuntimeException {
    if (this.processException != null) {
      RuntimeException rt = new RuntimeException("Failed to launch process", this.processException);
      throw rt;
    }
  }

  private Thread getThread() {
    synchronized (this.exitValue) {
      return this.processThread;
    }
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append(this.mainClass);
    sb.append("}");
    return sb.toString();
  }

  public static class Builder {
    private String[] jvmArgs = null;
    private Class<?> main;
    private String[] mainArgs = null;
    private boolean useMainLauncher = true;
    public Builder() {
      //nothing
    }
    public Builder jvmArgs(String[] jvmArgs) {
      this.jvmArgs = jvmArgs;
      return this;
    }
    public Builder main(Class<?> main) { 
      this.main = main;
      return this;
    }
    public Builder mainArgs(String[] mainArgs) {
      this.mainArgs = mainArgs;
      return this;
    }
    public Builder useMainLauncher(boolean useMainLauncher) {
      this.useMainLauncher = useMainLauncher;
      return this;
    }
    public ProcessWrapper build() {
      return new ProcessWrapper(jvmArgs, main, mainArgs, useMainLauncher);
    }
  }
}
