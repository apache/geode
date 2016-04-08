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
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;

/**
 * 
 * This ShutdownAllGatewayHubsRequest just reply with ignored bit true so that
 * old version member's request will be ignored and no exception will be thrown.
 * 
 * From 9.0 old wan support is removed. Ideally ShutdownAllGatewayHubsRequest
 * should be removed but it it there for rolling upgrade support when request
 * come from old version member to shut down hubs.
 * 
 * @since 9.0
 *
 */
public class ShutdownAllGatewayHubsRequest extends DistributionMessage {
  
  protected int rpid;

  @Override
  public int getDSFID() {
    return SHUTDOWN_ALL_GATEWAYHUBS_REQUEST;
  }

  @Override
  public int getProcessorType() {
    return DistributionManager.STANDARD_EXECUTOR;
  }

  @Override
  protected void process(DistributionManager dm) {
    ReplyMessage.send(getSender(), this.rpid, null, dm, true /*ignored*/, false, false);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.rpid = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.rpid);
  }
}
