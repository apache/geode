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

package com.gemstone.gemfire.modules.session.internal.filter.attributes;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.modules.session.internal.filter.GemfireHttpSession;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation for attributes. Should be sub-classed to provide
 * differing implementations for synchronous or delta propagation. The backing
 * store used is defined by the session manager.
 */
public abstract class AbstractSessionAttributes implements SessionAttributes {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractSessionAttributes.class.getName());

  /**
   * Internal attribute store.
   */
  protected Map<String, Object> attributes =
      Collections.synchronizedMap(new HashMap<String, Object>());

  /**
   * The session to which these attributes belong
   */
  protected transient GemfireHttpSession session;

  /**
   * The last accessed time
   */
  protected long lastAccessedTime;

  /**
   * The maximum inactive interval. Default is 1800 seconds.
   */
  protected int maxInactiveInterval = 60 * 30;

  /**
   * The JVM Id who last committed these attributes
   */
  protected String jvmOwnerId;

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSession(GemfireHttpSession session) {
    this.session = session;
  }

  /**
   * {@inheritDoc} The actual de-serialization of any domain objects is deferred
   * until the point at which they are actually retrieved by the application
   * layer.
   */
  @Override
  public Object getAttribute(String name) {
    Object value = attributes.get(name);

    // If the value is a byte[] (meaning it came from the server),
    // deserialize it and re-add it to attributes map before returning it.
    if (value instanceof byte[]) {
      try {
        value = BlobHelper.deserializeBlob((byte[]) value);
        attributes.put(name, value);
      } catch (Exception iox) {
        LOG.error("Attribute '" + name +
            " contains a byte[] that cannot be deserialized due "
            + "to the following exception", iox);
      }
    }

    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<String> getAttributeNames() {
    return attributes.keySet();
  }

  /**
   * {@inheritDoc} +
   */
  @Override
  public void setMaxInactiveInterval(int interval) {
    maxInactiveInterval = interval;
  }

  @Override
  public int getMaxIntactiveInterval() {
    return maxInactiveInterval;
  }

  @Override
  public void setLastAccessedTime(long time) {
    lastAccessedTime = time;
  }

  @Override
  public long getLastAccessedTime() {
    return lastAccessedTime;
  }

  /**
   * {@inheritDoc} This method calls back into the session to flush the whole
   * session including its attributes.
   */
  @Override
  public void flush() {
    session.putInRegion();
  }

  /**
   * Use DeltaEvents to propagate the actual attribute data - DeltaEvents turn
   * the values into byte arrays which means that the actual domain classes are
   * not required on the server.
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(maxInactiveInterval);
    out.writeLong(lastAccessedTime);

    synchronized (attributes) {
      out.writeInt(attributes.size());
      for (Map.Entry<String, Object> entry : attributes.entrySet()) {
        DeltaEvent delta = new DeltaEvent(true, entry.getKey(),
            entry.getValue());
        DataSerializer.writeObject(delta, out);
      }
    }

    out.writeUTF(jvmOwnerId);
  }

  @Override
  public void fromData(
      DataInput in) throws IOException, ClassNotFoundException {
    maxInactiveInterval = in.readInt();
    lastAccessedTime = in.readLong();
    int size = in.readInt();
    while (size-- > 0) {
      DeltaEvent event = DataSerializer.readObject(in);
      attributes.put(event.getName(), event.getValue());
    }
    jvmOwnerId = in.readUTF();
  }

  @Override
  public void setJvmOwnerId(String jvmId) {
    this.jvmOwnerId = jvmId;
  }

  @Override
  public String getJvmOwnerId() {
    return jvmOwnerId;
  }
}
