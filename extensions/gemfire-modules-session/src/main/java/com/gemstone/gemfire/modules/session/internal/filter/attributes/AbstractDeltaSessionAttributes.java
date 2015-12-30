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
import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class contains the structures and methods to handle delta
 * updates to attributes.
 */
public abstract class AbstractDeltaSessionAttributes
    extends AbstractSessionAttributes implements Delta {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractDeltaSessionAttributes.class.getName());

  /**
   * This map holds the updates to attributes
   */
  protected transient Map<String, DeltaEvent> deltas =
      Collections.synchronizedMap(new HashMap<String, DeltaEvent>());

  @Override
  public boolean hasDelta() {
    return true;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    out.writeInt(maxInactiveInterval);
    out.writeLong(lastAccessedTime);

    synchronized (deltas) {
      DataSerializer.writeInteger(deltas.size(), out);
      for (Map.Entry<String, DeltaEvent> e : deltas.entrySet()) {
        DataSerializer.writeString(e.getKey(), out);
        DataSerializer.writeObject(e.getValue(), out);
      }
      deltas.clear();
    }

    out.writeUTF(jvmOwnerId);
  }

  @Override
  public void fromDelta(DataInput in)
      throws IOException, InvalidDeltaException {
    maxInactiveInterval = in.readInt();
    lastAccessedTime = in.readLong();
    Map<String, DeltaEvent> localDeltas = new HashMap<String, DeltaEvent>();
    try {
      int size = DataSerializer.readInteger(in);
      for (int i = 0; i < size; i++) {
        String key = DataSerializer.readString(in);
        DeltaEvent evt = DataSerializer.readObject(in);
        localDeltas.put(key, evt);
      }
    } catch (ClassNotFoundException ex) {
      LOG.error("Unable to de-serialize delta events", ex);
      return;
    }

    LOG.debug("Processing {} delta events for {}",
        localDeltas.size(), session);
    for (DeltaEvent e : localDeltas.values()) {
      if (e.isUpdate()) {
        attributes.put(e.getName(), e.getValue());
        if (session.getNativeSession() != null) {
          session.getNativeSession().setAttribute(e.getName(), e.getValue());
        }
      } else {
        attributes.remove(e.getName());
        if (session.getNativeSession() != null) {
          session.getNativeSession().setAttribute(e.getName(), null);
        }
      }
    }
    jvmOwnerId = in.readUTF();
  }
}
