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

package org.apache.geode.management.internal.security;

import static org.apache.geode.security.ResourcePermission.Operation.MANAGE;
import static org.apache.geode.security.ResourcePermission.Operation.READ;
import static org.apache.geode.security.ResourcePermission.Operation.WRITE;
import static org.apache.geode.security.ResourcePermission.Resource.CLUSTER;
import static org.apache.geode.security.ResourcePermission.Resource.DATA;
import static org.apache.geode.security.ResourcePermission.Target.DEPLOY;
import static org.apache.geode.security.ResourcePermission.Target.DISK;
import static org.apache.geode.security.ResourcePermission.Target.GATEWAY;
import static org.apache.geode.security.ResourcePermission.Target.QUERY;

import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public final class ResourcePermissions {
  public static final ResourcePermission ALL = new ResourcePermission(Resource.ALL, Operation.ALL);
  public static final ResourcePermission DATA_ALL = new ResourcePermission(DATA, Operation.ALL);
  public static final ResourcePermission CLUSTER_ALL =
      new ResourcePermission(CLUSTER, Operation.ALL);
  public static final ResourcePermission DATA_READ = new ResourcePermission(DATA, READ);
  public static final ResourcePermission DATA_WRITE = new ResourcePermission(DATA, WRITE);
  public static final ResourcePermission DATA_MANAGE = new ResourcePermission(DATA, MANAGE);
  public static final ResourcePermission CLUSTER_READ = new ResourcePermission(CLUSTER, READ);
  public static final ResourcePermission CLUSTER_WRITE = new ResourcePermission(CLUSTER, WRITE);
  public static final ResourcePermission CLUSTER_MANAGE = new ResourcePermission(CLUSTER, MANAGE);
  public static final ResourcePermission CLUSTER_READ_QUERY =
      new ResourcePermission(CLUSTER, READ, QUERY);
  public static final ResourcePermission CLUSTER_MANAGE_QUERY =
      new ResourcePermission(CLUSTER, MANAGE, QUERY);
  public static final ResourcePermission CLUSTER_MANAGE_DEPLOY =
      new ResourcePermission(CLUSTER, MANAGE, DEPLOY);
  public static final ResourcePermission CLUSTER_MANAGE_DISK =
      new ResourcePermission(CLUSTER, MANAGE, DISK);
  public static final ResourcePermission CLUSTER_MANAGE_GATEWAY =
      new ResourcePermission(CLUSTER, MANAGE, GATEWAY);

  private ResourcePermissions() {}
}
