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
package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;

public class HeartbeatRequestMessage extends HighPriorityDistributionMessage{

  int requestId;
  InternalDistributedMember target;
  
  public HeartbeatRequestMessage(InternalDistributedMember neighbour, int id) {
    requestId = id;
    this.target = neighbour;
  }
  
  public HeartbeatRequestMessage(){}
  
  public InternalDistributedMember getTarget() {
    return target;
  }

  /**
   * If no response is desired the requestId can be reset by invoking
   * this method
   */
  public void clearRequestId() {
    requestId = -1;
  }
  
  @Override
  public int getDSFID() {
    return HEARTBEAT_REQUEST;
  }

  @Override
  public void process(DistributionManager dm) {
    throw new IllegalStateException("this message is not intended to execute in a thread pool");
  }   

  @Override
  public String toString() {
    return getClass().getSimpleName()+" [requestId=" + requestId + "]";
  }

  public int getRequestId() {
    return requestId;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }  
  
  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(requestId);
    DataSerializer.writeObject(target, out);
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    requestId = in.readInt();
    target = DataSerializer.readObject(in);
  }
}
