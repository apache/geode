/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Reads the output stream of a Process.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public final class ProcessStreamReader implements Runnable {
  private static final Logger logger = LogService.getLogger();
 
  private final InputStream inputStream;
  private final InputListener listener;

  private Thread thread;

  public ProcessStreamReader(final InputStream inputStream) {
    this.inputStream = inputStream;
    this.listener = new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        // do nothing
      }
      @Override
      public String toString() {
        return "NullInputListener";
      }
    };
  }

  public ProcessStreamReader(final InputStream inputStream, final InputListener listener) {
    this.inputStream = inputStream;
    this.listener = listener;
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
        this.listener.notifyInputLine(line);
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
    sb.append(" listener=").append(this.listener);
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
}
