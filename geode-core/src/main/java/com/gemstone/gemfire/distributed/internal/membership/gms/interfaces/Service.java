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
package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;

/**
 * Services in GMS all implement this interface
 *
 */
public interface Service {
  void init(Services s);

  /**
   * called after all services have been initialized 
   * with init() and all services are available via Services
   */
  void start();

  /**
   * called after all servers have been started
   */
  void started();

  /**
   * called when the GMS is stopping
   */
  void stop();

  /**
   * called after all services have been stopped
   */
  void stopped();

  /**
   * called when a new view is installed by Membership
   */
  void installView(NetView v);

  /**
   * test method for simulating a sick/dead member
   */
  void beSick();
  
  /**
   * test method for simulating a sick/dead member
   */
  void playDead();
  
  /**
   * test method for simulating a sick/dead member
   */
  void beHealthy();
  
  /**
   * shut down threads, cancel timers, etc.
   */
  void emergencyClose();
  
  /**
   * a member is suspected of having crashed
   */
  void memberSuspected(InternalDistributedMember initiator, InternalDistributedMember suspect, String reason);


}
