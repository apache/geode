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

package org.apache.geode.management.internal.rest;

import static org.mockito.Mockito.mock;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.internal.api.LocatorClusterManagementService;

public class MockLocatorContextLoader extends BaseLocatorContextLoader {
  private SecurityService securityService;
  private LocatorClusterManagementService cms;

  public MockLocatorContextLoader() {
    securityService = mock(SecurityService.class);
    cms = mock(LocatorClusterManagementService.class);
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public int getPort() {
    return 0;
  }

  @Override
  public SecurityService getSecurityService() {
    return securityService;
  }

  @Override
  public ClusterManagementService getClusterManagementService() {
    return cms;
  }
}
