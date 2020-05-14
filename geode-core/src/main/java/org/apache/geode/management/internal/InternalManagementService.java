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
package org.apache.geode.management.internal;

import javax.management.ObjectName;

import org.apache.geode.management.IManagementService;

public interface InternalManagementService extends IManagementService {

  ObjectName getCacheServerMBeanName(int serverPort, String member);

  ObjectName getDiskStoreMBeanName(String member, String diskName);

  ObjectName getGatewayReceiverMBeanName(String member);

  ObjectName getGatewaySenderMBeanName(String member, String gatewaySenderId);

  ObjectName getAsyncEventQueueMBeanName(String member, String queueId);

  ObjectName getLockServiceMBeanName(String member, String lockServiceName);

  ObjectName getMemberMBeanName(String member);

  ObjectName getRegionMBeanName(String member, String regionPath);

  ObjectName getLocatorMBeanName(String member);
}
