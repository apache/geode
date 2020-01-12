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
import java.util.concurrent.atomic.LongAdder;

import org.apache.geode.CopyHelper;
import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.SerializedCacheValue;
import org.apache.geode.connectors.jdbc.internal.AbstractJdbcCallback;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;

/**
 * This class provides synchronous write through to a data source using JDBC.
 *
 * @since Geode 1.4
 */
@Experimental
public class JdbcWriter<K, V> extends AbstractJdbcCallback implements CacheWriter<K, V> {

  private final LongAdder totalEvents = new LongAdder();

  @SuppressWarnings("unused")
  public JdbcWriter() {
    super();
  }

  // Constructor for test purposes only
  JdbcWriter(SqlHandler sqlHandler, InternalCache cache) {
    super(sqlHandler, cache);
  }


  @Override
  public void beforeUpdate(EntryEvent<K, V> event) throws CacheWriterException {
    writeEvent(event);
  }

  @Override
  public void beforeCreate(EntryEvent<K, V> event) throws CacheWriterException {
    writeEvent(event);
  }

  @Override
  public void beforeDestroy(EntryEvent<K, V> event) throws CacheWriterException {
    writeEvent(event);
  }

  @Override
  public void beforeRegionDestroy(RegionEvent<K, V> event) throws CacheWriterException {
    // this event is not sent to JDBC
  }

  @Override
  public void beforeRegionClear(RegionEvent<K, V> event) throws CacheWriterException {
    // this event is not sent to JDBC
  }

  private void writeEvent(EntryEvent<K, V> event) {
    if (eventCanBeIgnored(event.getOperation())) {
      return;
    }
    checkInitialized(event.getRegion());
    totalEvents.add(1);
    try {
      getSqlHandler().write(event.getRegion(), event.getOperation(), event.getKey(),
          getPdxNewValue(event));
    } catch (SQLException e) {
      throw JdbcConnectorException.createException(e);
    }
  }

  private PdxInstance getPdxNewValue(EntryEvent<K, V> event) {
    Boolean initialPdxReadSerialized = cache.getPdxReadSerializedOverride();
    cache.setPdxReadSerializedOverride(true);
    try {
      Object newValue = event.getNewValue();
      if (!(newValue instanceof PdxInstance)) {
        SerializedCacheValue<V> serializedNewValue = event.getSerializedNewValue();
        if (serializedNewValue != null) {
          newValue = serializedNewValue.getDeserializedValue();
        } else {
          newValue = CopyHelper.copy(newValue);
        }
        if (newValue != null && !(newValue instanceof PdxInstance)) {
          String valueClassName = newValue.getClass().getName();
          throw new IllegalArgumentException(getClass().getSimpleName()
              + " only supports PDX values; newValue is " + valueClassName);
        }
      }
      return (PdxInstance) newValue;
    } finally {
      cache.setPdxReadSerializedOverride(initialPdxReadSerialized);
    }
  }

  long getTotalEvents() {
    return totalEvents.longValue();
  }
}
