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

package org.apache.geode.security.templates;

import java.security.Principal;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AccessControl;
import org.apache.geode.security.NotAuthorizedException;

/**
 * A test implementation of the legacy {@link org.apache.geode.security.AccessControl}.
 * An authenticated user's permissions are defined by the username itself, e.g. user "dataRead"
 * has permissions DATA:READ and user "data,cluster" has permissions DATA and CLUSTER.
 */
@SuppressWarnings("deprecation")
public class SimpleAccessController implements AccessControl {
  private static Principal principal;

  @Override
  public void init(Principal principal, DistributedMember remoteMember, Cache cache)
      throws NotAuthorizedException {
    SimpleAccessController.principal = principal;
  }

  public static AccessControl create() {
    return new SimpleAccessController();
  }

  @Override
  public boolean authorizeOperation(String regionName, OperationContext context) {
    switch (context.getOperationCode()) {
      case GET:
      case REGISTER_INTEREST:
      case UNREGISTER_INTEREST:
      case CONTAINS_KEY:
      case KEY_SET:
      case QUERY:
      case EXECUTE_CQ:
      case STOP_CQ:
      case CLOSE_CQ:
        return authorize(principal, "DATA:READ");
      case PUT:
      case PUTALL:
      case REMOVEALL:
      case DESTROY:
      case INVALIDATE:
        return authorize(principal, "DATA:WRITE");
      case REGION_CLEAR:
      case REGION_CREATE:
      case REGION_DESTROY:
      case EXECUTE_FUNCTION:
      case GET_DURABLE_CQS:
        return false;
      default:
        return false;
    }
  }

  private boolean authorize(Principal principal, String permission) {
    String[] principals = principal.toString().toLowerCase().split(",");
    for (String role : principals) {
      String permissionString = permission.replace(":", "").toLowerCase();
      if (permissionString.startsWith(role))
        return true;
    }
    return false;
  }

  @Override
  public void close() {
    principal = null;
  }
}
