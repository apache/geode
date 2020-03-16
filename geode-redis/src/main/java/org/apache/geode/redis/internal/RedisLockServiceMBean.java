/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF licenses this file to You
 * under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.apache.geode.redis.internal;

import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

/**
 * MBean to expose number of locks in RedisLockService
 */

@ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
    operation = ResourcePermission.Operation.MANAGE)
public interface RedisLockServiceMBean {
  String OBJECTNAME__REDISLOCKSERVICE_MBEAN = "GemFire:service=RedisLockService,type=Member";

  int getLockCount();
}
