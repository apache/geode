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
package org.apache.geode.test.process;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Wraps spawned {@link Process} to capture output and provide interaction with the process.
 *
 * @since GemFire 4.1.1
 */
public class ProcessWrapper implements Consumer<String> {
  private static final Logger logger = LogService.getLogger();

  private static final long PROCESS_TIMEOUT_MILLIS = GeodeAwaitility.getTimeout().toMillis();
  private static final long DELAY = 10;

  private final boolean headless;
  private final long timeoutMillis;
  private final File directory;

  private final String[] jvmArguments;

  private final Class<?> mainClass;
  private final String[] mainArguments;

  private volatile Process process;
  private volatile Throwable processException;
  private volatile ProcessOutputReader outputReader;

  private final boolean useMainLauncher;

  private final List<String> allLines;
  private final BlockingQueue<String> lineBuffer;

  private final AtomicInteger exitValue = new AtomicInteger(-1);
  private boolean starting;
  private boolean started;
  private boolean stopped;
  private boolean interrupted;
  private Thread processThread;
  private ProcessStreamReader stdout;
  private ProcessStreamReader stderr;
  private Consumer<String> consumer;

  private ProcessWrapper(final String[] jvmArguments, final Class<?> mainClass,
      final String[] mainArguments, final boolean useMainLauncher, final boolean headless,
      final long timeoutMillis, final File directory) {
    this.jvmArguments = jvmArguments;
    this.mainClass = mainClass;
    this.mainArguments = mainArguments;
    this.useMainLauncher = useMainLauncher;
    this.headless = headless;
    this.timeoutMillis = timeoutMillis;
    this.directory = directory;

    lineBuffer = new LinkedBlockingQueue<>();
    allLines = Collections.synchronizedList(new ArrayList<>());
  }

  public void setConsumer(Consumer<String> consumer) {
    this.consumer = consumer;
  }

  @Override
  public void accept(String line) {
    allLines.add(line);
    lineBuffer.offer(line);

    if (consumer != null) {
      consumer.accept(line);
    }
  }

  public ProcessStreamReader getStandardOutReader() {
    synchronized (exitValue) {
      return stdout;
    }
  }

  public ProcessStreamReader getStandardErrorReader() {
    synchronized (exitValue) {
      return stderr;
    }
  }

  private void waitForProcessStart() throws InterruptedException, TimeoutException {
    final long start = System.currentTimeMillis();
    boolean done = false;
    while (!done) {
      synchronized (exitValue) {
        done = (process != null || processException != null)
            && (started || exitValue.get() > -1 || interrupted);
      }
      if (!done && System.currentTimeMillis() > start + timeoutMillis) {
        throw new TimeoutException("Timed out launching process");
      }
      Thread.sleep(DELAY);
    }
  }

  public boolean isAlive() throws InterruptedException, TimeoutException {
    checkStarting();
    waitForProcessStart();

    synchronized (exitValue) {
      if (interrupted) {
        throw new InterruptedException("Process was interrupted");
      }
      return exitValue.get() == -1 && started && !stopped && !interrupted
          && processThread.isAlive();
    }
  }

  public ProcessWrapper destroy() {
    if (process != null) {
      process.destroy();
    }
    return this;
  }

  public int waitFor(final long timeout, final boolean throwOnTimeout) throws InterruptedException {
    checkStarting();
    final Thread thread = getThread();
    thread.join(timeout);
    synchronized (exitValue) {
      if (throwOnTimeout) {
        checkStopped();
      }
      return exitValue.get();
    }
  }

  public int waitFor(final long timeout) throws InterruptedException {
    return waitFor(timeout, false);
  }

  public int waitFor(final boolean throwOnTimeout) throws InterruptedException {
    return waitFor(timeoutMillis, throwOnTimeout);
  }

  public int waitFor() throws InterruptedException {
    return waitFor(timeoutMillis, false);
  }

  public String getOutput() {
    return getOutput(false);
  }

  public String getOutput(final boolean ignoreStopped) {
    checkStarting();
    if (!ignoreStopped) {
      checkStopped();
    }
    final StringBuffer sb = new StringBuffer();
    final Iterator<String> iterator = allLines.iterator();
    while (iterator.hasNext()) {
      sb.append(iterator.next() + System.lineSeparator());
    }
    return sb.toString();
  }

  public ProcessWrapper sendInput() {
    checkStarting();
    sendInput("");
    return this;
  }

  public ProcessWrapper sendInput(final String input) {
    checkStarting();
    final PrintStream ps = new PrintStream(process.getOutputStream());
    ps.println(input);
    ps.flush();
    return this;
  }

  public ProcessWrapper failIfOutputMatches(final String patternString, final long timeoutMillis)
      throws InterruptedException {
    checkStarting();
    checkOk();

    final Pattern pattern = Pattern.compile(patternString);
    logger.debug("failIfOutputMatches waiting for \"{}\"...", patternString);
    final long start = System.currentTimeMillis();

    while (System.currentTimeMillis() <= start + timeoutMillis) {
      final String line = lineBuffer.poll(timeoutMillis, MILLISECONDS);
      if (line != null && pattern.matcher(line).matches()) {
        fail("failIfOutputMatches Matched pattern \"" + patternString + "\" against output \""
            + line + "\". Output: " + allLines);
      }
    }
    return this;
  }

  /*
   * Waits for the process stdout or stderr stream to contain the specified text. Uses the specified
   * timeout for debugging purposes.
   */
  public ProcessWrapper waitForOutputToMatch(final String patternString, final long timeoutMillis)
      throws InterruptedException {
    checkStarting();
    checkOk();

    logger.debug("ProcessWrapper:waitForOutputToMatch waiting for \"{}\"...", patternString);
    final Pattern pattern = Pattern.compile(patternString);

    while (true) {
      final String line = lineBuffer.poll(timeoutMillis, MILLISECONDS);
      if (line == null) {
        fail("Timed out waiting for output \"" + patternString + "\" after " + timeoutMillis +
            " ms from process \"" + toString(process) + "\" in \"" + this + "\". Output: " +
            new OutputFormatter(allLines));
      }

      if (pattern.matcher(line).matches()) {
        logger.debug(
            "ProcessWrapper:waitForOutputToMatch Matched pattern \"{}\" against output \"{}\"",
            patternString, line);
        break;
      }
      logger.debug(
          "ProcessWrapper:waitForOutputToMatch Did not match pattern \"{}\" against output \"{}\"",
          patternString, line);
    }
    return this;
  }

  private String toString(Process process) {
    StringBuilder sb = new StringBuilder(process.getClass().getSimpleName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("alive=").append(process.isAlive());
    sb.append("}");
    return sb.toString();
  }

  /*
   * Waits for the process stdout or stderr stream to contain the specified text. Uses the default
   * timeout.
   */
  public ProcessWrapper waitForOutputToMatch(final String patternString)
      throws InterruptedException {
    return waitForOutputToMatch(patternString, timeoutMillis);
  }

  public ProcessWrapper execute() throws InterruptedException, TimeoutException {
    return execute(null, directory);
  }

  public ProcessWrapper execute(final Properties properties)
      throws InterruptedException, TimeoutException {
    return execute(properties, directory);
  }

  public ProcessWrapper execute(final Properties properties, final File workingDirectory)
      throws InterruptedException, TimeoutException {
    synchronized (exitValue) {
      if (starting) {
        throw new IllegalStateException("ProcessWrapper can only be executed once");
      }
      starting = true;
      processThread =
          new Thread(() -> start(properties, workingDirectory), "ProcessWrapper Process Thread");
    }
    processThread.start();

    waitForProcessStart();

    synchronized (exitValue) {
      if (processException != null) {
        logger.error("ProcessWrapper:execute failed with " + processException);
        processException.printStackTrace();
      }
    }

    if (useMainLauncher) {
      // to trigger MainLauncher delegation to inner main
      sendInput();
    }
    return this;
  }

  private void start(final Properties properties, final File workingDirectory) {
    final List<String> jvmArgumentsList = new ArrayList<>();

    if (properties != null) {
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        if (!entry.getKey().equals(LOG_FILE)) {
          jvmArgumentsList.add("-D" + entry.getKey() + "=" + entry.getValue());
        }
      }
    }

    if (headless) {
      jvmArgumentsList.add("-Djava.awt.headless=true");
    }

    if (jvmArguments != null) {
      Collections.addAll(jvmArgumentsList, jvmArguments);
    }

    try {
      synchronized (exitValue) {
        final String[] command =
            defineCommand(jvmArgumentsList.toArray(new String[jvmArgumentsList.size()]),
                workingDirectory.getCanonicalPath());
        process = new ProcessBuilder(command).directory(workingDirectory).start();

        final StringBuilder processCommand = new StringBuilder();
        boolean addSpace = false;

        for (String string : command) {
          if (addSpace) {
            processCommand.append(" ");
          }
          processCommand.append(string);
          addSpace = true;
        }

        final String commandString = processCommand.toString();
        logger.info("Starting " + commandString);

        final ProcessStreamReader stdOut = new ProcessStreamReader(commandString,
            process.getInputStream(), this);
        final ProcessStreamReader stdErr = new ProcessStreamReader(commandString,
            process.getErrorStream(), this);

        stdout = stdOut;
        stderr = stdErr;
        outputReader = new ProcessOutputReader(process, stdOut, stdErr);
        started = true;
      }

      outputReader.start();
      outputReader.waitFor(PROCESS_TIMEOUT_MILLIS, MILLISECONDS);
      boolean exited = process.waitFor(PROCESS_TIMEOUT_MILLIS, MILLISECONDS);

      synchronized (exitValue) {
        exitValue.set(exited ? process.exitValue() : 0);
        stopped = exited;
      }

    } catch (InterruptedException e) {
      synchronized (exitValue) {
        interrupted = true;
        processException = e;
      }
    } catch (Throwable t) {
      synchronized (exitValue) {
        processException = t;
      }
    }
  }

  private String[] defineCommand(final String[] jvmArguments, String workingDir)
      throws IOException {
    final File javaBinDir = new File(System.getProperty("java.home"), "bin");
    final File javaExe = new File(javaBinDir, "java");

    String classPath = System.getProperty("java.class.path");
    List<String> parts = Arrays.asList(classPath.split(File.pathSeparator));
    String manifestJar = createManifestJar(parts, workingDir);

    final List<String> argumentList = new ArrayList<>();
    argumentList.add(javaExe.getPath());
    argumentList.add("-classpath");
    argumentList.add(manifestJar);

    // -d64 is not a valid option for windows and results in failure
    // -d64 is not a valid option for java 9 and above
    final int bits = Integer.getInteger("sun.arch.data.model", 0);
    if (bits == 64 && !System.getProperty("os.name").toLowerCase().contains("windows")
        && !SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) {
      argumentList.add("-d64");
    }

    argumentList.add("-Djava.library.path=" + System.getProperty("java.library.path"));

    if (jvmArguments != null) {
      argumentList.addAll(Arrays.asList(jvmArguments));
    }

    if (useMainLauncher) {
      argumentList.add(MainLauncher.class.getName());
    }
    argumentList.add(mainClass.getName());

    if (mainArguments != null) {
      argumentList.addAll(Arrays.asList(mainArguments));
    }

    return argumentList.toArray(new String[0]);
  }

  private void checkStarting() throws IllegalStateException {
    synchronized (exitValue) {
      if (!starting) {
        throw new IllegalStateException("Process has not been launched");
      }
    }
  }

  private void checkStopped() throws IllegalStateException {
    synchronized (exitValue) {
      if (!stopped) {
        throw new IllegalStateException("Process has not stopped");
      }
    }
  }

  private void checkOk() throws RuntimeException {
    if (processException != null) {
      throw new RuntimeException("Failed to launch process", processException);
    }
  }

  private Thread getThread() {
    synchronized (exitValue) {
      return processThread;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("mainClass=").append(mainClass).append(", ");
    sb.append("jvmArguments=").append(Arrays.toString(jvmArguments)).append(", ");
    sb.append("mainArguments=").append(Arrays.toString(mainArguments));
    sb.append("}");
    return sb.toString();
  }

  public Process getProcess() {
    return process;
  }

  /**
   * Method to create a manifest jar from a list of jars or directories. The provided entries are
   * first converted to absolute paths and then converted to relative paths, relative to the
   * location provided. This is to support the Manifest's requirement that class-paths only be
   * relative. For example, if a jar is given as /a/b/c/foo.jar and the location is /tmp/app, the
   * following will happen:
   * - the manifest jar will be created as /tmp/app/manifest.jar
   * - the class-path attribute will be ../../a/b/c/foo.jar
   *
   * @return the path to the created manifest jar
   */
  public static String createManifestJar(List<String> entries, String location) throws IOException {
    // Must use the canonical path so that symbolic links are resolved correctly
    Path locationPath = new File(location).getCanonicalFile().toPath();
    Files.createDirectories(locationPath);

    List<String> manifestEntries = new ArrayList<>();
    for (String jarEntry : entries) {
      Path jarEntryAbsolutePath = Paths.get(jarEntry).toAbsolutePath();
      Path jarEntryRelativizedPath = locationPath.relativize(jarEntryAbsolutePath);
      if (jarEntryAbsolutePath.toFile().isDirectory()) {
        manifestEntries.add(jarEntryRelativizedPath + File.separator);
      } else {
        manifestEntries.add(jarEntryRelativizedPath.toString());
      }
    }

    Manifest manifest = new Manifest();
    Attributes attributes = manifest.getMainAttributes();
    attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
    attributes.put(new Attributes.Name("Class-Path"), String.join(" ", manifestEntries));

    // Generate a 'unique' 8 char name
    String uuid = UUID.randomUUID().toString().substring(0, 8);
    Path manifestJarPath = Paths.get(location, "manifest-" + uuid + ".jar");
    File manifestJarFile = manifestJarPath.toFile();
    manifestJarFile.deleteOnExit();

    try (JarOutputStream jos =
        new JarOutputStream(new FileOutputStream(manifestJarFile), manifest)) {
      // the above try-with-resource writes the manifest to the manifestJarFile
    }

    return manifestJarPath.toFile().getAbsolutePath();
  }

  public static class Builder {

    private String[] jvmArguments;
    private Class<?> mainClass;
    private String[] mainArguments;
    private boolean useMainLauncher = true;
    private boolean headless = true;
    private long timeoutMillis = PROCESS_TIMEOUT_MILLIS;
    private boolean inline;
    private File directory = new File(System.getProperty("user.dir"));

    public Builder jvmArguments(final String[] jvmArguments) {
      this.jvmArguments = jvmArguments;
      return this;
    }

    public Builder mainClass(final Class<?> mainClass) {
      this.mainClass = mainClass;
      return this;
    }

    public Builder mainArguments(final String[] mainArguments) {
      this.mainArguments = mainArguments;
      return this;
    }

    public Builder useMainLauncher(final boolean useMainLauncher) {
      this.useMainLauncher = useMainLauncher;
      return this;
    }

    public Builder headless(final boolean headless) {
      this.headless = headless;
      return this;
    }

    public Builder timeoutMillis(final long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
      return this;
    }

    public Builder inline(final boolean inline) {
      this.inline = inline;
      return this;
    }

    public Builder directory(final File directory) {
      this.directory = directory;
      return this;
    }

    public ProcessWrapper build() {
      return new ProcessWrapper(jvmArguments, mainClass, mainArguments, useMainLauncher, headless,
          timeoutMillis, directory);
    }
  }
}
