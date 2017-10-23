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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CopyHelper;
import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.connectors.jdbc.internal.AbstractJdbcCallback;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.PdxInstance;

/**
 * This class provides write behind cache semantics for a JDBC data source using AsyncEventListener.
 *
 * @since Geode 1.4
 */
@Experimental
public class JdbcAsyncWriter extends AbstractJdbcCallback implements AsyncEventListener {
  private static final Logger logger = LogService.getLogger();

  private AtomicLong totalEvents = new AtomicLong();
  private AtomicLong successfulEvents = new AtomicLong();

  @SuppressWarnings("unused")
  public JdbcAsyncWriter() {
    super();
  }

  // Constructor for test purposes only
  JdbcAsyncWriter(SqlHandler sqlHandler) {
    super(sqlHandler);
  }

  @Override
  public boolean processEvents(List<AsyncEvent> events) {
    changeTotalEvents(events.size());

    if (!events.isEmpty()) {
      checkInitialized((InternalCache) events.get(0).getRegion().getRegionService());
    }

    DefaultQuery.setPdxReadSerialized(true);
    try {
      for (AsyncEvent event : events) {
        try {
          getSqlHandler().write(event.getRegion(), event.getOperation(), event.getKey(),
              getPdxInstance(event));
          changeSuccessfulEvents(1);
        } catch (RuntimeException ex) {
          logger.error("Exception processing event {}", event, ex);
        }
      }
    } finally {
      DefaultQuery.setPdxReadSerialized(false);
    }

    return true;
  }

  long getTotalEvents() {
    return totalEvents.get();
  }

  long getSuccessfulEvents() {
    return successfulEvents.get();
  }

  private void changeSuccessfulEvents(long delta) {
    successfulEvents.addAndGet(delta);
  }

  private void changeTotalEvents(long delta) {
    totalEvents.addAndGet(delta);
  }

  /**
   * precondition: DefaultQuery.setPdxReadSerialized(true)
   */
  private PdxInstance getPdxInstance(AsyncEvent event) {
    Object value = event.getDeserializedValue();
    if (!(value instanceof PdxInstance)) {
      value = CopyHelper.copy(value);
    }
    return (PdxInstance) value;
  }
}
