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
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.List;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderAttributes;

public class ParallelGatewaySenderCreation extends AbstractGatewaySender implements GatewaySender{

  public ParallelGatewaySenderCreation(Cache cache, GatewaySenderAttributes attrs) {
    super(cache, attrs);
  }

  public void distribute(EnumListenerEvent operation, EntryEventImpl event, List<Integer> remoteDSIds) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void start() {
    // TODO Auto-generated method stub
    
  }

  public void stop() {
    // TODO Auto-generated method stub
    
  }

  public void rebalance() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void destroy() {
  }
  
  public void fillInProfile(Profile profile) {
    // TODO Auto-generated method stub
    
  }

  public CancelCriterion getCancelCriterion() {
    // TODO Auto-generated method stub
    return null;
  }

  public DistributionAdvisor getDistributionAdvisor() {
    // TODO Auto-generated method stub
    return null;
  }

  public DM getDistributionManager() {
    // TODO Auto-generated method stub
    return null;
  }

  public String getFullPath() {
    // TODO Auto-generated method stub
    return null;
  }

  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  public DistributionAdvisee getParentAdvisee() {
    // TODO Auto-generated method stub
    return null;
  }

  public Profile getProfile() {
    // TODO Auto-generated method stub
    return null;
  }

  public int getSerialNumber() {
    // TODO Auto-generated method stub
    return 0;
  }

  public InternalDistributedSystem getSystem() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender#setModifiedEventId(com.gemstone.gemfire.internal.cache.EntryEventImpl)
   */
  @Override
  protected void setModifiedEventId(EntryEventImpl clonedEvent) {
    // TODO Auto-generated method stub
    
  }
  
}
