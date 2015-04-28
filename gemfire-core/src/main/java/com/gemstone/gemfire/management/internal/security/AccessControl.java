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
