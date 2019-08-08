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
package org.apache.geode.alerting.internal;

import org.apache.geode.alerting.AlertingService;
import org.apache.geode.alerting.spi.AlertLevel;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.DistributedMember;

/**
 * Null implementation of {@link AlertingService} that does nothing.
 */
public class NullAlertingService extends AlertingService {

  @Immutable
  private static final NullAlertingService INSTANCE = new NullAlertingService();

  public static NullAlertingService get() {
    return INSTANCE;
  }

  private NullAlertingService() {
    // nothing
  }

  @Override
  public void addAlertListener(final DistributedMember member, final AlertLevel alertLevel) {
    // nothing
  }

  @Override
  public boolean removeAlertListener(final DistributedMember member) {
    return false;
  }

  @Override
  public boolean hasAlertListener(final DistributedMember member, final AlertLevel alertLevel) {
    return false;
  }
}
