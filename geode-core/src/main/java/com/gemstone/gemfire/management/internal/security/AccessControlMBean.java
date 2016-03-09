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
package com.gemstone.gemfire.management.internal.security;

import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.security.AccessControl;

import javax.management.remote.JMXPrincipal;
import javax.security.auth.Subject;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.Set;

/**
 * AccessControlMBean Implementation. This retrieves JMXPrincipal from AccessController
 * and performs authorization for given role using gemfire AccessControl Plugin
 *
 * @author tushark
 * @since 9.0
 */
public class AccessControlMBean implements AccessControlMXBean {

  private ManagementInterceptor interceptor;

  public AccessControlMBean(ManagementInterceptor interceptor) {
    this.interceptor = interceptor;
  }

  @Override
  public boolean authorize(String role) {
    AccessControlContext acc = AccessController.getContext();
    Subject subject = Subject.getSubject(acc);
    Set<JMXPrincipal> principals = subject.getPrincipals(JMXPrincipal.class);
    if (principals == null || principals.isEmpty()) {
      throw new SecurityException("Access denied");
    }
    Principal principal = principals.iterator().next();
    AccessControl gemAccControl = interceptor.getAccessControl(principal, false);
    boolean authorized = gemAccControl.authorizeOperation(null,
        new ResourceOperationContext(Resource.DEFAULT, OperationContext.OperationCode.valueOf(role)));
    return authorized;
  }

}
