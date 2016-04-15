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
package com.gemstone.gemfire.cache.query.internal.aggregate.uda;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * Used for sending UDA creation / removal message
 * @author ashahid
 * @since 9.0 
 */
public class UDAMessage extends DistributionMessage {
  private boolean isCreate;
  private String udaName;
  private String udaClass;

  public UDAMessage() {}

  public UDAMessage(String name, String udaClass) {
    this.isCreate = true;
    this.udaName = name;
    this.udaClass = udaClass;
  }

  public UDAMessage(String name) {
    this.isCreate = false;
    this.udaName = name;
  }

  @Override
  public int getDSFID() {
    return UDA_MESSAGE;
  }

  @Override
  public int getProcessorType() {
    return DistributionManager.SERIAL_EXECUTOR;
  }

  @Override
  protected void process(DistributionManager dm) {
    if (this.isCreate) {
      try {
        GemFireCacheImpl.getExisting().getUDAManager().createUDALocally(this.udaName, this.udaClass);
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      GemFireCacheImpl.getExisting().getUDAManager().removeUDALocally(this.udaName);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.isCreate = DataSerializer.readPrimitiveBoolean(in);
    this.udaName = DataSerializer.readString(in);
    if (this.isCreate) {
      this.udaClass = DataSerializer.readString(in);
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writePrimitiveBoolean(this.isCreate, out);
    DataSerializer.writeString(this.udaName, out);
    if (this.isCreate) {
      DataSerializer.writeString(this.udaClass, out);
    }
  }

  public void send() {
    GemFireCacheImpl gfc = GemFireCacheImpl.getExisting();
    DistributionAdvisor advisor = gfc.getDistributionAdvisor();
    final Set<InternalDistributedMember> recipients = new HashSet<InternalDistributedMember>(advisor.adviseGeneric());
    recipients.remove(gfc.getDistributionManager().getId());
    this.setRecipients(recipients);
    gfc.getDistributionManager().putOutgoing(this);
  }
}
