/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.sequencelog.io.OutputStreamAppender;

/**
 * @author dsmith
 *
 */
public class SequenceLoggerImpl implements SequenceLogger {

  private static final SequenceLogger INSTANCE;
  
  public static final String ENABLED_TYPES_PROPERTY = "gemfire.GraphLoggerImpl.ENABLED_TYPES";
  
  private final EnumSet<GraphType> enabledTypes;
  
  static {
    SequenceLoggerImpl logger = new SequenceLoggerImpl();
    logger.start();
    INSTANCE = logger;
  }
  
  //TODO - this might be too much synchronization for recording all region
  //operations. Maybe we should use a ConcurrentLinkedQueue instead?
  private final LinkedBlockingQueue<Transition> edges = new LinkedBlockingQueue<Transition>();
  
  private volatile OutputStreamAppender appender;

  private ConsumerThread consumerThread;
  
  public static SequenceLogger getInstance() {
    return INSTANCE;
  }

  /**
   * Should be invoked when GemFire cache is closing or closed. 
   */
  public static void signalCacheClose() {
    if (INSTANCE != null) {
      ((SequenceLoggerImpl)INSTANCE).consumerThread.interrupt();
    }
  }

  public boolean isEnabled(GraphType type) {
    return enabledTypes.contains(type);
  }

  public void logTransition(GraphType type, Object graphName, Object edgeName,
      Object state, Object source, Object dest) {
    if(isEnabled(type)) {
      Transition edge = new Transition(type, graphName, edgeName, state, source, dest);
      edges.add(edge);
    }
  }
  
  public void flush() throws InterruptedException {
    FlushToken token = new FlushToken();
    edges.add(token);
    token.cdl.await();
  }

  private SequenceLoggerImpl() {
    String enabledTypesString = System.getProperty(ENABLED_TYPES_PROPERTY, "");
    this.enabledTypes = GraphType.parse(enabledTypesString);
    if(!enabledTypes.isEmpty()) {
      try {
        String name = "states" + OSProcess.getId() + ".graph";
        appender = new OutputStreamAppender(new File(name));
      } catch (FileNotFoundException e) {
      }
    }
  }
  
  private void start() {
    consumerThread = new ConsumerThread();
    consumerThread.start();
  }

  private class ConsumerThread extends Thread {
    
    public ConsumerThread() {
      super("State Logger Consumer Thread");
      setDaemon(true);
    }

    @Override
    public void run() {
      Transition edge;
      while(true) {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        if (cache == null || cache.isClosed()) {
          if (appender != null) {
            appender.close();
          }
          break;
        }
        try {
          edge = edges.take();
          if(edge instanceof FlushToken) {
            ((FlushToken) edge).cdl.countDown();
            continue;
          }

          if(appender != null) {
            appender.write(edge);
          }
        } catch (InterruptedException e) {
          //do nothing
        } catch (Throwable t) {
          t.printStackTrace();
          appender.close();
          appender = null;
        }
      }
    }
  }
  
  private static class FlushToken extends Transition {
    CountDownLatch cdl = new CountDownLatch(1);
    
    public FlushToken() {
      super(null, null, null, null, null, null);
    }
  }
}
