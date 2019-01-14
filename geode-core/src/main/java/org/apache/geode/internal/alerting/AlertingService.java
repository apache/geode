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


import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;

/**
 * Adds and removes {@code AlertListeners} for local and remote members that need to receive
 * notification of system {@code Alerts}.
 *
 * <p>
 * The {@code AlertingService} looks up the registered {@code AlertingProvider} from
 * {@code AlertingProviderRegistry} and delegates all calls to that provider.
 */
public class AlertingService {

  private final AlertingProviderRegistry providerRegistry;

  public AlertingService() {
    this(AlertingProviderRegistry.get());
  }

  AlertingService(final AlertingProviderRegistry providerRegistry) {
    this.providerRegistry = providerRegistry;
  }

  public void addAlertListener(final DistributedMember member, final AlertLevel alertLevel) {
    providerRegistry.getAlertingProvider().addAlertListener(member, alertLevel);
  }

  public boolean removeAlertListener(final DistributedMember member) {
    return providerRegistry.getAlertingProvider().removeAlertListener(member);
  }

  public boolean hasAlertListener(final DistributedMember member, final AlertLevel alertLevel) {
    return providerRegistry.getAlertingProvider().hasAlertListener(member, alertLevel);
  }

  @VisibleForTesting
  AlertingProviderRegistry getAlertingProviderRegistry() {
    return providerRegistry;
  }
}
