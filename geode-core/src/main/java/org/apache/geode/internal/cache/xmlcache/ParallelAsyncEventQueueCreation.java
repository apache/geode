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
package org.apache.geode.internal.cache.xmlcache;

import java.util.List;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;

public class ParallelAsyncEventQueueCreation extends AbstractGatewaySender
    implements GatewaySender {

  public ParallelAsyncEventQueueCreation(InternalCache cache, GatewaySenderAttributes attrs) {
    super(cache, attrs);
  }

  public void distribute(EnumListenerEvent operation, EntryEventImpl event,
      List<Integer> remoteDSIds) {}

  @Override
  public void start() {}

  public void stop() {}

  public void rebalance() {
    throw new UnsupportedOperationException();
  }

  public void fillInProfile(Profile profile) {}

  public CancelCriterion getCancelCriterion() {
    return null;
  }

  public DistributionAdvisor getDistributionAdvisor() {
    return null;
  }

  public DistributionManager getDistributionManager() {
    return null;
  }

  public String getFullPath() {
    return null;
  }

  public String getName() {
    return null;
  }

  public DistributionAdvisee getParentAdvisee() {
    return null;
  }

  public Profile getProfile() {
    return null;
  }

  public int getSerialNumber() {
    return 0;
  }

  public InternalDistributedSystem getSystem() {
    return null;
  }

  @Override
  public void setModifiedEventId(EntryEventImpl clonedEvent) {}
}
