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
package org.apache.geode.distributed.internal.membership.gms.interfaces;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberStartupException;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.services.module.ModuleService;

/**
 * Membership services in GMS all implement this interface
 *
 */
public interface Service<ID extends MemberIdentifier> {
  void init(Services<ID> s, ModuleService moduleService) throws MembershipConfigurationException;

  /**
   * called after all services have been initialized with init() and all services are available via
   * Services
   */
  void start() throws MemberStartupException;

  /**
   * called after all servers have been started
   */
  void started() throws MemberStartupException;

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
  void installView(GMSMembershipView<ID> v);

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
  void memberSuspected(ID initiator, ID suspect,
      String reason);


  default void setLocalAddress(ID address) {}
}
