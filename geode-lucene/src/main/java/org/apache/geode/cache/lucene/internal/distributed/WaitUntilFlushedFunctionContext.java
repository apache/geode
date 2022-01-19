/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Contains function arguments for WaitUntilFlushed
 */
public class WaitUntilFlushedFunctionContext implements DataSerializableFixedID {
  private String indexName;
  private long timeout;
  private TimeUnit unit;

  public WaitUntilFlushedFunctionContext() {
    this(null, 0, TimeUnit.MILLISECONDS);
  }

  public WaitUntilFlushedFunctionContext(String indexName, long timeout, TimeUnit unit) {
    this.indexName = indexName;
    this.timeout = timeout;
    this.unit = unit;
  }

  /**
   * Get the name of the index
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * Get the timeout
   */
  public long getTimeout() {
    return timeout;
  }

  /*
   * Get the timeout's unit
   */
  public TimeUnit getTimeunit() {
    return unit;
  }

  @Override
  public int getDSFID() {
    return WAIT_UNTIL_FLUSHED_FUNCTION_CONTEXT;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeString(indexName, out);
    out.writeLong(timeout);
    DataSerializer.writeEnum(unit, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException {
    indexName = DataSerializer.readString(in);
    timeout = in.readLong();
    unit = DataSerializer.readEnum(TimeUnit.class, in);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
