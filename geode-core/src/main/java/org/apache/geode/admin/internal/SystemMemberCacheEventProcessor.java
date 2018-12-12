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
package org.apache.geode.admin.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.admin.SystemMemberCacheEvent;
import org.apache.geode.admin.SystemMemberCacheListener;
import org.apache.geode.admin.SystemMemberRegionEvent;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.LogService;

/**
 * This class processes the message to be delivered to admin node. [This needs to be redesigned and
 * reimplemented... see 32887]
 *
 * @since GemFire 5.0
 */
public class SystemMemberCacheEventProcessor {
  private static final Logger logger = LogService.getLogger();


  /*
   * Sends cache create/close message to Admin VMs
   */
  public static void send(Cache c, Operation op) {
    send(c, null, op);
  }

  /*
   * Sends region creation/destroy message to Admin VMs
   */
  public static void send(Cache c, Region region, Operation op) {
    InternalDistributedSystem system = (InternalDistributedSystem) c.getDistributedSystem();
    Set recps = system.getDistributionManager().getAdminMemberSet();
    // @todo darrel: find out if any of these admin members have region listeners
    if (recps.isEmpty()) {
      return;
    }
    SystemMemberCacheMessage msg = new SystemMemberCacheMessage();
    if (region == null) {
      msg.regionPath = null;
    } else {
      msg.regionPath = region.getFullPath();
    }
    msg.setRecipients(recps);
    msg.op = op;
    system.getDistributionManager().putOutgoing(msg);
  }


  public static class SystemMemberCacheMessage extends HighPriorityDistributionMessage {
    protected String regionPath;
    protected Operation op;

    @Override
    protected void process(ClusterDistributionManager dm) {
      AdminDistributedSystemImpl admin = AdminDistributedSystemImpl.getConnectedInstance();
      if (admin == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Ignoring message because there is no admin distributed system present: {}",
              this);
        }
        return; // probably shutting down or still connecting
      }
      List listeners = admin.getCacheListeners();
      Iterator itr = listeners.iterator();
      SystemMemberCacheListener listener = null;
      while (itr.hasNext()) {
        listener = (SystemMemberCacheListener) itr.next();
        if (this.regionPath == null) {
          SystemMemberCacheEvent event = new SystemMemberCacheEventImpl(getSender(), this.op);
          if (this.op == Operation.CACHE_CREATE) {
            listener.afterCacheCreate(event);
          } else {
            listener.afterCacheClose(event);
          }
        } else {
          SystemMemberRegionEvent event =
              new SystemMemberRegionEventImpl(getSender(), this.op, this.regionPath);
          if (this.op.isRegionDestroy()) {
            listener.afterRegionLoss(event);
          } else {
            listener.afterRegionCreate(event);
          }
        }
      }
    }

    public int getDSFID() {
      return ADMIN_CACHE_EVENT_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.regionPath = DataSerializer.readString(in);
      this.op = Operation.fromOrdinal(in.readByte());
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.regionPath, out);
      out.writeByte(this.op.ordinal);
    }

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append("SystemMemberCacheMessage (region='");
      buff.append(this.regionPath);
      buff.append("'; sender=");
      buff.append(this.sender);
      buff.append("; op=");
      buff.append(this.op);
      buff.append(")");
      return buff.toString();
    }
  }
}
