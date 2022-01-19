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

package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;


/** Creates a new instance of CloseCacheMessage */
public class CloseCacheMessage extends HighPriorityDistributionMessage implements MessageWithReply {
  private static final Logger logger = LogService.getLogger();

  private int processorId;

  @Override
  public int getProcessorId() {
    return processorId;
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    // Now that Cache.close calls close on each region we don't need
    // any of the following code so we can just do an immediate ack.
    boolean systemError = false;
    try {
      try {
        PartitionedRegionHelper.cleanUpMetaDataOnNodeFailure(dm.getCache(), getSender());
      } catch (VirtualMachineError err) {
        systemError = true;
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        if (logger.isDebugEnabled()) {
          logger.debug("Throwable caught while processing cache close message from:{}", getSender(),
              t);
        }
      }
    } finally {
      if (!systemError) {
        ReplyMessage.send(getSender(), processorId, null, dm, false, false, true);
      }
    }
  }

  public void setProcessorId(int id) {
    processorId = id;
  }

  @Override
  public String toString() {
    return super.toString() + " (processorId=" + processorId + ")";
  }

  @Override
  public int getDSFID() {
    return CLOSE_CACHE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    processorId = in.readInt();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(processorId);
  }
}
