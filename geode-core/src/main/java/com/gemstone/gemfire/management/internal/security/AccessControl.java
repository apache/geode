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

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.Set;

import javax.management.remote.JMXPrincipal;
import javax.security.auth.Subject;

public class AccessControl implements AccessControlMXBean {

  private ManagementInterceptor interceptor;

  public AccessControl(ManagementInterceptor interceptor) {
    this.interceptor = interceptor;
  }

  @Override
  public boolean authorize(String role) {
    AccessControlContext acc = AccessController.getContext();
    Subject subject = Subject.getSubject(acc);
    Set<JMXPrincipal> principals = subject.getPrincipals(JMXPrincipal.class);
    Set<Object> pubCredentials = subject.getPublicCredentials();
    if (principals == null || principals.isEmpty()) {
      throw new SecurityException("Access denied");
    }
    Principal principal = principals.iterator().next();
    com.gemstone.gemfire.security.AccessControl gemAccControl = interceptor.getAccessControl(principal);
    boolean authorized = gemAccControl.authorizeOperation(null,
        new com.gemstone.gemfire.management.internal.security.AccessControlContext(role));
    return authorized;
  }

}
