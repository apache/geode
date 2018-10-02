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
package org.apache.geode.internal.process;

import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notNull;

import java.io.InputStream;

import org.apache.commons.lang.SystemUtils;

import org.apache.geode.internal.logging.LoggingThread;

/**
 * Reads the output stream of a Process.
 *
 * @since GemFire 7.0
 */
public abstract class ProcessStreamReader implements Runnable {

  private static final int DEFAULT_PROCESS_OUTPUT_WAIT_TIME_MILLIS = 5000;

  protected final Process process;
  protected final InputStream inputStream;
  protected final InputListener inputListener;

  private Thread thread;

  protected ProcessStreamReader(final Builder builder) {
    notNull(builder, "Invalid builder '" + builder + "' specified");

    this.process = builder.process;
    this.inputStream = builder.inputStream;

    if (builder.inputListener == null) {
      this.inputListener = new InputListener() {
        @Override
        public void notifyInputLine(String line) {
          // do nothing
        }

        @Override
        public String toString() {
          return "NullInputListener";
        }
      };
    } else {
      this.inputListener = builder.inputListener;
    }
  }

  public ProcessStreamReader start() {
    synchronized (this) {
      if (thread == null) {
        thread = new LoggingThread(createThreadName(), this);
        thread.start();
      } else if (thread.isAlive()) {
        throw new IllegalStateException(this + " has already started");
      } else {
        throw new IllegalStateException(this + " was stopped and cannot be restarted");
      }
    }
    return this;
  }

  public ProcessStreamReader stop() {
    synchronized (this) {
      if (thread != null && thread.isAlive()) {
        thread.interrupt();
      }
    }
    return this;
  }

  public ProcessStreamReader stopAsync(final long delayMillis) {
    Runnable delayedStop = () -> {
      try {
        Thread.sleep(delayMillis);
      } catch (InterruptedException ignored) {
      } finally {
        stop();
      }
    };

    String threadName =
        getClass().getSimpleName() + " stopAfterDelay Thread @" + Integer.toHexString(hashCode());
    Thread thread = new LoggingThread(threadName, delayedStop);
    thread.start();
    return this;
  }

  public boolean isRunning() {
    synchronized (this) {
      if (thread != null) {
        return thread.isAlive();
      }
    }
    return false;
  }

  public ProcessStreamReader join() throws InterruptedException {
    Thread thread;
    synchronized (this) {
      thread = this.thread;
    }
    if (thread != null) {
      thread.join();
    }
    return this;
  }

  public ProcessStreamReader join(final long millis) throws InterruptedException {
    Thread thread;
    synchronized (this) {
      thread = this.thread;
    }
    if (thread != null) {
      thread.join(millis);
    }
    return this;
  }

  public ProcessStreamReader join(final long millis, final int nanos) throws InterruptedException {
    Thread thread;
    synchronized (this) {
      thread = this.thread;
    }
    if (thread != null) {
      thread.join(millis, nanos);
    }
    return this;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append(" Thread").append(" #").append(System.identityHashCode(this));
    sb.append(" alive=").append(isRunning());
    sb.append(" listener=").append(inputListener);
    return sb.toString();
  }

  private String createThreadName() {
    return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode());
  }

  /**
   * Defines the callback for lines of output found in the stream.
   */
  public interface InputListener {
    void notifyInputLine(final String line);
  }

  /** Default ReadingMode is BLOCKING */
  public enum ReadingMode {
    BLOCKING, NON_BLOCKING
  }

  private static String waitAndCaptureProcessStandardOutputStream(final Process process,
      final long waitTimeMilliseconds) {
    notNull(process, "Invalid process '" + process + "' specified");

    return waitAndCaptureProcessStream(process, process.getInputStream(), waitTimeMilliseconds);
  }

  public static String waitAndCaptureProcessStandardErrorStream(final Process process) {
    return waitAndCaptureProcessStandardErrorStream(process,
        DEFAULT_PROCESS_OUTPUT_WAIT_TIME_MILLIS);
  }

  public static String waitAndCaptureProcessStandardErrorStream(final Process process,
      final long waitTimeMilliseconds) {
    return waitAndCaptureProcessStream(process, process.getErrorStream(), waitTimeMilliseconds);
  }

  private static String waitAndCaptureProcessStream(final Process process,
      final InputStream processInputStream, final long waitTimeMilliseconds) {
    StringBuffer buffer = new StringBuffer();

    InputListener inputListener = line -> {
      buffer.append(line);
      buffer.append(SystemUtils.LINE_SEPARATOR);
    };

    ProcessStreamReader reader = new ProcessStreamReader.Builder(process)
        .inputStream(processInputStream).inputListener(inputListener).build();

    try {
      reader.start();

      long endTime = System.currentTimeMillis() + waitTimeMilliseconds;

      while (System.currentTimeMillis() < endTime) {
        try {
          reader.join(waitTimeMilliseconds);
        } catch (InterruptedException ignore) {
        }
      }
    } finally {
      reader.stop();
    }

    return buffer.toString();
  }

  /**
   * Builds a ProcessStreamReader.
   *
   * @since GemFire 8.2
   */
  public static class Builder {

    final Process process;
    InputStream inputStream;
    InputListener inputListener;
    long continueReadingMillis = 0;
    ReadingMode readingMode = ReadingMode.BLOCKING;

    public Builder(final Process process) {
      this.process = process;
    }

    public Builder inputStream(final InputStream inputStream) {
      this.inputStream = inputStream;
      return this;
    }

    /** InputListener callback to invoke with read data */
    public Builder inputListener(final InputListener inputListener) {
      this.inputListener = inputListener;
      return this;
    }

    /** millis to continue reading InputStream after Process terminates */
    public Builder continueReadingMillis(final long continueReadingMillis) {
      this.continueReadingMillis = continueReadingMillis;
      return this;
    }

    /** ReadingMode to use for reading InputStream */
    public Builder readingMode(final ReadingMode readingMode) {
      this.readingMode = readingMode;
      return this;
    }

    public ProcessStreamReader build() {
      notNull(process, "Invalid process '" + process + "' specified");
      notNull(inputStream, "Invalid inputStream '" + inputStream + "' specified");
      isTrue(continueReadingMillis >= 0,
          "Invalid continueReadingMillis '" + continueReadingMillis + "' specified");

      switch (readingMode) {
        case NON_BLOCKING:
          return new NonBlockingProcessStreamReader(this);
        default:
          return new BlockingProcessStreamReader(this);
      }
    }
  }
}
