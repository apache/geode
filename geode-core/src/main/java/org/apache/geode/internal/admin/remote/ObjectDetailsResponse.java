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


package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireCacheException;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Responds to {@link ObjectDetailsRequest}.
 */
public class ObjectDetailsResponse extends AdminResponse implements Cancellable {
  // instance variables
  private Object objectValue;
  private Object userAttribute;
  private RemoteCacheStatistics stats;
  private transient boolean cancelled;

  /**
   * Returns a <code>ObjectValueResponse</code> that will be returned to the specified recipient.
   * The message will contains a copy of the local manager's system config.
   */
  public static ObjectDetailsResponse create(DistributionManager dm,
      InternalDistributedMember recipient) {
    ObjectDetailsResponse m = new ObjectDetailsResponse();
    m.setRecipient(recipient);
    return m;
  }

  void buildDetails(Region r, Object objName, int inspectionType) {
    try {
      objName = getObjectName(r, objName);
      if (cancelled) {
        return;
      }
      if (r.containsKey(objName)) {
        if (cancelled) {
          return;
        }
        // @todo darrel: race condition; could be unloaded between isPresent and get call.
        Region.Entry e = r.getEntry(objName);
        Object v = e.getValue();
        if (cancelled) {
          return;
        }
        objectValue = CacheDisplay.getCachedObjectDisplay(v, inspectionType);
        if (cancelled) {
          return;
        }
        userAttribute = CacheDisplay.getCachedObjectDisplay(e.getUserAttribute(), inspectionType);
        if (cancelled) {
          return;
        }
        try {
          stats = new RemoteCacheStatistics(e.getStatistics());
        } catch (StatisticsDisabledException ignore) {
        }
      }
    } catch (CacheException ex) {
      throw new GemFireCacheException(ex);
    }
  }

  @Override
  public synchronized void cancel() {
    cancelled = true;
  }

  // instance methods
  public Object getObjectValue() {
    return this.objectValue;
  }

  public Object getUserAttribute() {
    return this.userAttribute;
  }

  public CacheStatistics getStatistics() {
    return this.stats;
  }

  @Override
  public int getDSFID() {
    return OBJECT_DETAILS_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(this.objectValue, out);
    DataSerializer.writeObject(this.userAttribute, out);
    DataSerializer.writeObject(this.stats, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.objectValue = DataSerializer.readObject(in);
    this.userAttribute = DataSerializer.readObject(in);
    this.stats = (RemoteCacheStatistics) DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "ObjectDetailsResponse from " + this.getRecipient();
  }


  // Holds the last result of getObjectName to optimize the next call
  @MakeNotStatic
  private static Object lastObjectNameFound = null;

  static Object getObjectName(Region r, Object objName) throws CacheException {
    if (objName instanceof RemoteObjectName) {
      synchronized (ObjectDetailsResponse.class) {
        if (objName.equals(lastObjectNameFound)) {
          return lastObjectNameFound;
        }
      }
      Object obj = null;
      Set keys = r.keySet();
      synchronized (r) {
        Iterator it = keys.iterator();
        while (it.hasNext()) {
          Object o = it.next();
          if (objName.equals(o)) {
            synchronized (ObjectDetailsResponse.class) {
              lastObjectNameFound = o;
            }
            obj = o;
            break;
          }
        }
      }
      if (obj != null) {
        return obj;
      }
      // Didn't find it so just return the input RemoteObjectName instance.
      // This should fail on the lookup and give a reasonable error.
      synchronized (ObjectDetailsResponse.class) {
        lastObjectNameFound = objName;
      }
    }
    return objName;
  }
}
