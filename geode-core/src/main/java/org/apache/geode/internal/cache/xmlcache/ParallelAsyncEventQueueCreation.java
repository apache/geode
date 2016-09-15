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
package org.apache.geode.internal.cache.xmlcache;

import java.util.List;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;

public class ParallelAsyncEventQueueCreation extends AbstractGatewaySender implements GatewaySender{

  public ParallelAsyncEventQueueCreation(Cache cache, GatewaySenderAttributes attrs) {
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
   * @see org.apache.geode.internal.cache.wan.AbstractGatewaySender#setModifiedEventId(org.apache.geode.internal.cache.EntryEventImpl)
   */
  @Override
  protected void setModifiedEventId(EntryEventImpl clonedEvent) {
    // TODO Auto-generated method stub
    
  }
  
}
