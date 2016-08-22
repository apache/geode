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

package com.gemstone.gemfire.modules.session.internal.filter.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Factory to create named threads for easy identification at runtime.
 */
public class NamedThreadFactory implements ThreadFactory {

  private static final Logger LOG = Logger.getLogger(
      NamedThreadFactory.class.getName());

  private final String id;

  private final AtomicLong serial = new AtomicLong();

  /**
   * Create a new thread factory, using the specified pool ID as a basis for
   * naming each thread.
   *
   * @param poolID pool name/ID
   */
  public NamedThreadFactory(final String poolID) {
    id = poolID;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * This implementation sets the name of the thread, sets the thread to be a
   * daemon thread, and adds an uncaught exception handler.
   */
  @Override
  public Thread newThread(Runnable r) {
    Thread thr = new Thread(r);
    thr.setDaemon(true);
    thr.setName(id + " - " + serial.incrementAndGet());
    thr.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        LOG.log(Level.WARNING,
            "Uncaught Exception in thread: " + t.getName(), e);
      }
    });
    return thr;
  }
}
