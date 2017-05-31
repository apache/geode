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
package org.apache.geode.internal.security;

import java.io.IOException;
import java.io.Serializable;
import java.security.AccessController;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.StringUtils;
import org.apache.geode.GemFireIOException;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.shiro.GeodeAuthenticationToken;
import org.apache.geode.internal.security.shiro.ShiroPrincipal;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.ShiroException;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.support.SubjectThreadState;
import org.apache.shiro.util.ThreadContext;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;

public class CustomSecurityService implements SecurityService {
  private static Logger logger = LogService.getLogger(LogService.SECURITY_LOGGER_NAME);

  private final PostProcessor postProcessor;

  CustomSecurityService(PostProcessor postProcessor) {
    this.postProcessor = postProcessor;
  }

  @Override
  public void initSecurity(final Properties securityProps) {
    if (this.postProcessor != null) {
      this.postProcessor.init(securityProps);
    }
  }

  @Override
  public ThreadState bindSubject(final Subject subject) {
    if (subject == null) {
      return null;
    }

    ThreadState threadState = new SubjectThreadState(subject);
    threadState.bind();
    return threadState;
  }

  @Override
  public Subject getSubject() {
    Subject currentUser;

    // First try get the principal out of AccessControlContext instead of Shiro's Thread context
    // since threads can be shared between JMX clients.
    javax.security.auth.Subject jmxSubject =
        javax.security.auth.Subject.getSubject(AccessController.getContext());

    if (jmxSubject != null) {
      Set<ShiroPrincipal> principals = jmxSubject.getPrincipals(ShiroPrincipal.class);
      if (principals.size() > 0) {
        ShiroPrincipal principal = principals.iterator().next();
        currentUser = principal.getSubject();
        ThreadContext.bind(currentUser);
        return currentUser;
      }
    }

    // in other cases like rest call, client operations, we get it from the current thread
    currentUser = SecurityUtils.getSubject();

    if (currentUser == null || currentUser.getPrincipal() == null) {
      throw new GemFireSecurityException("Error: Anonymous User");
    }

    return currentUser;
  }

  @Override
  public Subject login(final Properties credentials) {
    if (credentials == null) {
      return null;
    }

    // this makes sure it starts with a clean user object
    ThreadContext.remove();

    Subject currentUser = SecurityUtils.getSubject();
    GeodeAuthenticationToken token = new GeodeAuthenticationToken(credentials);
    try {
      logger.debug("Logging in " + token.getPrincipal());
      currentUser.login(token);
    } catch (ShiroException e) {
      logger.info(e.getMessage(), e);
      throw new AuthenticationFailedException(
          "Authentication error. Please check your credentials.", e);
    }

    return currentUser;
  }

  @Override
  public void logout() {
    Subject currentUser = getSubject();
    if (currentUser == null) {
      return;
    }

    try {
      logger.debug("Logging out " + currentUser.getPrincipal());
      currentUser.logout();
    } catch (ShiroException e) {
      logger.info(e.getMessage(), e);
      throw new GemFireSecurityException(e.getMessage(), e);
    }
    // clean out Shiro's thread local content
    ThreadContext.remove();
  }

  @Override
  public Callable associateWith(final Callable callable) {
    Subject currentUser = getSubject();
    if (currentUser == null) {
      return callable;
    }

    return currentUser.associateWith(callable);
  }

  @Override
  public void authorize(final ResourceOperation resourceOperation) {
    if (resourceOperation == null) {
      return;
    }

    authorize(resourceOperation.resource().name(), resourceOperation.operation().name(), null);
  }

  @Override
  public void authorizeClusterManage() {
    authorize("CLUSTER", "MANAGE");
  }

  @Override
  public void authorizeClusterWrite() {
    authorize("CLUSTER", "WRITE");
  }

  @Override
  public void authorizeClusterRead() {
    authorize("CLUSTER", "READ");
  }

  @Override
  public void authorizeDataManage() {
    authorize("DATA", "MANAGE");
  }

  @Override
  public void authorizeDataWrite() {
    authorize("DATA", "WRITE");
  }

  @Override
  public void authorizeDataRead() {
    authorize("DATA", "READ");
  }

  @Override
  public void authorizeRegionManage(final String regionName) {
    authorize("DATA", "MANAGE", regionName);
  }

  @Override
  public void authorizeRegionManage(final String regionName, final String key) {
    authorize("DATA", "MANAGE", regionName, key);
  }

  @Override
  public void authorizeRegionWrite(final String regionName) {
    authorize("DATA", "WRITE", regionName);
  }

  @Override
  public void authorizeRegionWrite(final String regionName, final String key) {
    authorize("DATA", "WRITE", regionName, key);
  }

  @Override
  public void authorizeRegionRead(final String regionName) {
    authorize("DATA", "READ", regionName);
  }

  @Override
  public void authorizeRegionRead(final String regionName, final String key) {
    authorize("DATA", "READ", regionName, key);
  }

  @Override
  public void authorize(final String resource, final String operation) {
    authorize(resource, operation, null);
  }

  @Override
  public void authorize(final String resource, final String operation, final String regionName) {
    authorize(resource, operation, regionName, null);
  }

  @Override
  public void authorize(final String resource, final String operation, String regionName,
      final String key) {
    regionName = StringUtils.stripStart(regionName, "/");
    authorize(new ResourcePermission(resource, operation, regionName, key));
  }

  @Override
  public void authorize(final ResourcePermission context) {
    Subject currentUser = getSubject();
    if (currentUser == null) {
      return;
    }
    if (context == null) {
      return;
    }
    if (context.getResource() == Resource.NULL && context.getOperation() == Operation.NULL) {
      return;
    }

    try {
      currentUser.checkPermission(context);
    } catch (ShiroException e) {
      String msg = currentUser.getPrincipal() + " not authorized for " + context;
      logger.info(msg);
      throw new NotAuthorizedException(msg, e);
    }
  }

  @Override
  public void close() {
    ThreadContext.remove();
    SecurityUtils.setSecurityManager(null);
  }

  @Override
  public boolean needPostProcess() {
    return this.postProcessor != null;
  }

  @Override
  public Object postProcess(final String regionPath, final Object key, final Object value,
      final boolean valueIsSerialized) {
    return postProcess(null, regionPath, key, value, valueIsSerialized);
  }

  @Override
  public Object postProcess(Object principal, final String regionPath, final Object key,
      final Object value, final boolean valueIsSerialized) {
    if (!needPostProcess()) {
      return value;
    }

    if (principal == null) {
      Subject subject = getSubject();
      if (subject == null) {
        return value;
      }
      principal = (Serializable) subject.getPrincipal();
    }

    String regionName = StringUtils.stripStart(regionPath, "/");
    Object newValue;

    // if the data is a byte array, but the data itself is supposed to be an object, we need to
    // deserialize it before we pass it to the callback.
    if (valueIsSerialized && value instanceof byte[]) {
      try {
        Object oldObj = EntryEventImpl.deserialize((byte[]) value);
        Object newObj = this.postProcessor.processRegionValue(principal, regionName, key, oldObj);
        newValue = BlobHelper.serializeToBlob(newObj);
      } catch (IOException | SerializationException e) {
        throw new GemFireIOException("Exception de/serializing entry value", e);
      }
    } else {
      newValue = this.postProcessor.processRegionValue(principal, regionName, key, value);
    }

    return newValue;
  }

  @Override
  public boolean isClientSecurityRequired() {
    return true;
  }

  @Override
  public boolean isIntegratedSecurity() {
    return true;
  }

  @Override
  public boolean isPeerSecurityRequired() {
    return true;
  }

  @Override
  public SecurityManager getSecurityManager() {
    return null;
  }

  @Override
  public PostProcessor getPostProcessor() {
    return this.postProcessor;
  }
}
