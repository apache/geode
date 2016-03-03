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
package com.gemstone.gemfire.distributed.internal.membership.gms.mgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.SerialDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;


/**
  LocalViewMessage is used to pass a new membership view to the GemFire cache
  in an orderly manner.  It is intended to be queued with serially
  executed messages so that the view takes effect at the proper time.
  
 */

public final class LocalViewMessage extends SerialDistributionMessage
{

  private GMSMembershipManager manager;
  private long viewId;
  private NetView view;
  
  public LocalViewMessage(
    InternalDistributedMember addr,
    long viewId,
    NetView view,
    GMSMembershipManager manager
    )
  {
    super();
    this.sender = addr;
    this.viewId = viewId;
    this.view = view;
    this.manager = manager;
  }
  
  @Override
  final public int getProcessorType() {
    return DistributionManager.VIEW_EXECUTOR;
  }


  @Override
  protected void process(DistributionManager dm) {
    //dm.getLogger().info("view message processed", new Exception());
    manager.processView(viewId, view);
  }

  // These "messages" are never DataSerialized 

  public int getDSFID() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException();
  }
}    
