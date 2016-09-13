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
package com.gemstone.gemfire.internal.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Reads the output stream of a Process.
 * 
 * @since GemFire 7.0
 */
public abstract class ProcessStreamReader implements Runnable {
  private static final Logger logger = LogService.getLogger();
 
  protected final Process process;
  protected final InputStream inputStream;
  protected final InputListener inputListener;

  private Thread thread;

  protected ProcessStreamReader(final Builder builder) {
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
  
  @Override
  public void run() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Running {}", this);
    }
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(inputStream));
      String line;
      while ((line = reader.readLine()) != null) {
        this.inputListener.notifyInputLine(line);
      }
    } catch (IOException e) {
      if (isDebugEnabled) {
        logger.debug("Failure reading from buffered input stream: {}", e.getMessage(), e);
      }
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        if (isDebugEnabled) {
          logger.debug("Failure closing buffered input stream reader: {}", e.getMessage(), e);
        }
      }
      if (isDebugEnabled) {
        logger.debug("Terminating {}", this);
      }
    }
  }

  public ProcessStreamReader start() {
    synchronized (this) {
      if (this.thread == null) {
        this.thread = new Thread(this, createThreadName());
        this.thread.setDaemon(true);
        this.thread.start();
      } else if (this.thread.isAlive()){
        throw new IllegalStateException(this + " has already started");
      } else {
        throw new IllegalStateException(this + " was stopped and cannot be restarted");
      }
    }
    return this;
  }

  public ProcessStreamReader stop() {
    synchronized (this) {
      if (this.thread != null && this.thread.isAlive()) {
        this.thread.interrupt();
      } else if (this.thread != null){
        if (logger.isDebugEnabled()) {
          logger.debug("{} has already been stopped", this);
        }
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("{} has not been started", this);
        }
      }
    }
    return this;
  }

  public ProcessStreamReader stopAsync(final long delayMillis) {
    Runnable delayedStop = new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
        } finally {
          stop();
        }
      }
    };
    String threadName = getClass().getSimpleName() + " stopAfterDelay Thread @" + Integer.toHexString(hashCode());
    Thread thread = new Thread(delayedStop, threadName);
    thread.setDaemon(true);
    thread.start();
    return this;
  }
  
  public boolean isRunning() {
    synchronized (this) {
      if (this.thread != null) {
        return this.thread.isAlive();
      }
    }
    return false;
  }
  
  public void join() throws InterruptedException {
    Thread thread;
    synchronized (this) {
      thread = this.thread;
    }
    if (thread != null) {
      thread.join();
    }
  }
  
  public void join(final long millis) throws InterruptedException {
    Thread thread;
    synchronized (this) {
      thread = this.thread;
    }
    if (thread != null) {
      thread.join(millis);
    }
  }
  
  public void join(final long millis, final int nanos) throws InterruptedException {
    Thread thread;
    synchronized (this) {
      thread = this.thread;
    }
    if (thread != null) {
      thread.join(millis, nanos);
    }
  }
  
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append(" Thread").append(" #").append(System.identityHashCode(this));
    sb.append(" alive=").append(isRunning()); //this.thread == null ? false : this.thread.isAlive());
    sb.append(" listener=").append(this.inputListener);
    return sb.toString();
  }
  
  private String createThreadName() {
    return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode());
  }
  
  /**
   * Defines the callback for  lines of output found in the stream.
   */
  public static interface InputListener {
    public void notifyInputLine(String line);
  }

  /** Default ReadingMode is BLOCKING */
  public static enum ReadingMode {
    BLOCKING,
    NON_BLOCKING;
  }
  
  /**
   * Builds a ProcessStreamReader.
   * 
   * @since GemFire 8.2
   */
  public static class Builder {
    protected Process process;
    protected InputStream inputStream;
    protected InputListener inputListener;
    protected long continueReadingMillis = 0;
    protected ReadingMode readingMode = ReadingMode.BLOCKING;
    
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
      if (process == null) {
        throw new NullPointerException("process may not be null");
      }
      if (inputStream == null) {
        throw new NullPointerException("inputStream may not be null");
      }
      if (continueReadingMillis < 0) {
        throw new IllegalArgumentException("continueReadingMillis must zero or positive");
      }
      switch (this.readingMode) {
        case NON_BLOCKING: return new NonBlockingProcessStreamReader(this);
        default: return new BlockingProcessStreamReader(this);
      }
    }
  }
}
