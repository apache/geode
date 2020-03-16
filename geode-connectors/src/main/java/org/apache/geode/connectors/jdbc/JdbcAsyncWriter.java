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
package org.apache.geode.connectors.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CopyHelper;
import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.connectors.jdbc.internal.AbstractJdbcCallback;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.PdxInstance;

/**
 * This class provides write behind cache semantics for a JDBC data source using AsyncEventListener.
 *
 * @since Geode 1.4
 */
@Experimental
public class JdbcAsyncWriter extends AbstractJdbcCallback implements AsyncEventListener {
  private static final Logger logger = LogService.getLogger();

  private final LongAdder totalEvents = new LongAdder();
  private final LongAdder successfulEvents = new LongAdder();
  private final LongAdder failedEvents = new LongAdder();
  private final LongAdder ignoredEvents = new LongAdder();

  @SuppressWarnings("unused")
  public JdbcAsyncWriter() {
    super();
  }

  // Constructor for test purposes only
  JdbcAsyncWriter(SqlHandler sqlHandler, InternalCache cache) {
    super(sqlHandler, cache);
  }

  @Override
  public boolean processEvents(@SuppressWarnings("rawtypes") List<AsyncEvent> events) {
    changeTotalEvents(events.size());

    if (!events.isEmpty()) {
      try {
        checkInitialized(events.get(0).getRegion());
      } catch (RuntimeException ex) {
        changeFailedEvents(events.size());
        logger.error("Exception initializing JdbcAsyncWriter", ex);
        return true;
      }
    }

    Boolean initialPdxReadSerialized = cache.getPdxReadSerializedOverride();
    cache.setPdxReadSerializedOverride(true);
    try {
      processEventsList(events);
    } finally {
      cache.setPdxReadSerializedOverride(initialPdxReadSerialized);
    }
    return true;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void processEventsList(List<AsyncEvent> events) {
    for (AsyncEvent event : events) {
      if (eventCanBeIgnored(event.getOperation())) {
        changeIgnoredEvents(1);
        continue;
      }
      try {
        getSqlHandler().write(event.getRegion(), event.getOperation(), event.getKey(),
            getPdxInstance(event));
        changeSuccessfulEvents(1);
      } catch (SQLException | RuntimeException ex) {
        changeFailedEvents(1);
        logger.error("Exception processing event {}", event, ex);
      }
    }
  }

  long getTotalEvents() {
    return totalEvents.longValue();
  }

  long getSuccessfulEvents() {
    return successfulEvents.longValue();
  }

  long getFailedEvents() {
    return failedEvents.longValue();
  }

  long getIgnoredEvents() {
    return ignoredEvents.longValue();
  }

  private void changeSuccessfulEvents(long delta) {
    successfulEvents.add(delta);
  }

  private void changeFailedEvents(long delta) {
    failedEvents.add(delta);
  }

  private void changeTotalEvents(long delta) {
    totalEvents.add(delta);
  }

  private void changeIgnoredEvents(long delta) {
    ignoredEvents.add(delta);
  }

  /**
   * precondition: DefaultQuery.setPdxReadSerialized(true)
   */
  private PdxInstance getPdxInstance(@SuppressWarnings("rawtypes") AsyncEvent event) {
    Object value = event.getDeserializedValue();
    if (!(value instanceof PdxInstance)) {
      value = CopyHelper.copy(value);
    }
    return (PdxInstance) value;
  }
}
