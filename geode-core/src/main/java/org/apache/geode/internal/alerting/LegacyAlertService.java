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
package org.apache.geode.internal.alerting;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * TODO:KIRK: delete LegacyAlertService after replacing calls
 */
public class LegacyAlertService {

  // TODO:KIRK: shuttingDown (AgentImpl, GMSMembershipManager)
  public static void shuttingDown() {}

  public static boolean removeAlertListener(InternalDistributedMember member) {
    return false;
  }

  public static void onConnect(InternalDistributedSystem system) {}

  public static boolean hasAlertListener(DistributedMember member, int alertLevel) {
    return false;
  }

  public static boolean isThreadAlerting() {
    return false;
  }

  public static void addAlertListener(final DistributedMember member, final int alertLevel) {}

  public static boolean isAlertingDisabled() {
    return false;
  }

  public static void setAlertingDisabled(final boolean alertingDisabled) {}

  public static void setIsAlerting(boolean isAlerting) {}
}
