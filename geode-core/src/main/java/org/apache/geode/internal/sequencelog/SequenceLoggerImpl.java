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
package org.apache.geode.internal.sequencelog;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.sequencelog.io.OutputStreamAppender;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.util.internal.GeodeGlossary;

public class SequenceLoggerImpl implements SequenceLogger {

  @Immutable
  private static final SequenceLoggerImpl INSTANCE;

  public static final String ENABLED_TYPES_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "GraphLoggerImpl.ENABLED_TYPES";

  private final EnumSet<GraphType> enabledTypes;

  static {
    SequenceLoggerImpl logger = new SequenceLoggerImpl();
    logger.start();
    INSTANCE = logger;
  }

  // TODO - this might be too much synchronization for recording all region
  // operations. Maybe we should use a ConcurrentLinkedQueue instead?
  private final LinkedBlockingQueue<Transition> edges = new LinkedBlockingQueue<>();

  private volatile OutputStreamAppender appender;

  private ConsumerThread consumerThread;

  public static SequenceLogger getInstance() {
    return INSTANCE;
  }

  /**
   * Should be invoked when GemFire cache is closing or closed.
   */
  public static void signalCacheClose() {
    if (INSTANCE != null && INSTANCE.consumerThread != null) {
      INSTANCE.consumerThread.interrupt();
    }
  }

  @Override
  public boolean isEnabled(GraphType type) {
    return enabledTypes.contains(type);
  }

  @Override
  public void logTransition(GraphType type, Object graphName, Object edgeName, Object state,
      Object source, Object dest) {
    if (isEnabled(type)) {
      Transition edge = new Transition(type, graphName, edgeName, state, source, dest);
      edges.add(edge);
    }
  }

  @Override
  public void flush() throws InterruptedException {
    FlushToken token = new FlushToken();
    edges.add(token);
    token.cdl.await();
  }

  private SequenceLoggerImpl() {
    String enabledTypesString = System.getProperty(ENABLED_TYPES_PROPERTY, "");
    enabledTypes = GraphType.parse(enabledTypesString);
    if (!enabledTypes.isEmpty()) {
      try {
        String name = "states" + OSProcess.getId() + ".graph";
        appender = new OutputStreamAppender(new File(name));
      } catch (FileNotFoundException e) {
      }
    }
  }

  private void start() {
    if (!enabledTypes.isEmpty()) {
      consumerThread = new ConsumerThread();
      consumerThread.start();
    }
  }

  private class ConsumerThread extends Thread {

    public ConsumerThread() {
      super("State Logger Consumer Thread");
      setDaemon(true);
    }

    @Override
    public void run() {
      Transition edge;
      while (true) {
        try {
          edge = edges.take();
          if (edge instanceof FlushToken) {
            ((FlushToken) edge).cdl.countDown();
            continue;
          }

          if (appender != null) {
            appender.write(edge);
          }
        } catch (InterruptedException e) {
          // do nothing
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
