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
package org.apache.geode.internal.cache.partitioned;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.mockito.ArgumentCaptor;

public class MockAppender implements AutoCloseable {

  private final Logger logger;
  private final Appender mockAppender;

  public MockAppender(Class<?> loggerClass) {
    mockAppender = mock(Appender.class);
    when(mockAppender.getName()).thenReturn("MockAppender");
    when(mockAppender.isStarted()).thenReturn(true);
    when(mockAppender.isStopped()).thenReturn(false);
    logger = (Logger) LogManager.getLogger(loggerClass);
    logger.addAppender(mockAppender);
    logger.setLevel(Level.WARN);
  }

  public Appender getMock() {
    return this.mockAppender;
  }

  public List<LogEvent> getLogs() {
    ArgumentCaptor<LogEvent> loggingEventCaptor = ArgumentCaptor.forClass(LogEvent.class);
    verify(mockAppender, atLeast(0)).append(loggingEventCaptor.capture());
    return loggingEventCaptor.getAllValues();
  }

  public void close() {
    logger.removeAppender(mockAppender);
  }


}
