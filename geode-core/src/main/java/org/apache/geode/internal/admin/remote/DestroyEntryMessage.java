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

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.logging.LogService;

/**
 * A message that is sent to a particular distribution manager to let it know that the sender is an
 * administation console that just connected.
 */
public class DestroyEntryMessage extends RegionAdminMessage {

  private static final Logger logger = LogService.getLogger();

  private Object key;
  private ExpirationAction action;

  public static DestroyEntryMessage create(Object key, ExpirationAction action) {
    DestroyEntryMessage m = new DestroyEntryMessage();
    m.action = action;
    m.key = key;
    return m;
  }

  @Override
  public void process(ClusterDistributionManager dm) {
    Region r = getRegion(dm.getSystem());
    if (r != null) {
      try {
        if (action == ExpirationAction.LOCAL_DESTROY) {
          r.localDestroy(key);
        } else if (action == ExpirationAction.DESTROY) {
          r.destroy(key);
        } else if (action == ExpirationAction.INVALIDATE) {
          r.invalidate(key);
        } else if (action == ExpirationAction.LOCAL_INVALIDATE) {
          r.localInvalidate(key);
        }
      } catch (Exception e) {
        logger.warn("Failed attempt to destroy or invalidate entry {} {} from console at {}",
            new Object[] {r.getFullPath(), key, this.getSender()});
      }
    }
  }

  public int getDSFID() {
    return DESTROY_ENTRY_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.action, out);
    DataSerializer.writeObject(this.key, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.action = (ExpirationAction) DataSerializer.readObject(in);
    this.key = DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return String.format("DestroyEntryMessage from %s",
        this.getSender());
  }
}
