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

import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.StringUtils;
import org.apache.geode.GemFireIOException;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.shiro.GeodeAuthenticationToken;
import org.apache.geode.internal.security.shiro.SecurityManagerProvider;
import org.apache.geode.internal.security.shiro.ShiroPrincipal;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;
import org.apache.geode.security.SecurityManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.ShiroException;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.support.SubjectThreadState;
import org.apache.shiro.util.ThreadContext;
import org.apache.shiro.util.ThreadState;

import java.io.IOException;
import java.security.AccessController;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Security service with SecurityManager and an optional PostProcessor.
 */
public class IntegratedSecurityService implements SecurityService {
  private static Logger logger = LogService.getLogger(LogService.SECURITY_LOGGER_NAME);

  private final PostProcessor postProcessor;
  private final SecurityManager securityManager;

  /**
   * this creates a security service using a SecurityManager
   * 
   * @param provider this provides shiro security manager
   * @param postProcessor this can be null
   */
  IntegratedSecurityService(SecurityManagerProvider provider, PostProcessor postProcessor) {
    // provider must provide a shiro security manager, otherwise, this is not integrated security
    // service at all.
    assert provider.getShiroSecurityManager() != null;
    SecurityUtils.setSecurityManager(provider.getShiroSecurityManager());

    this.securityManager = provider.getSecurityManager();
    this.postProcessor = postProcessor;
  }

  @Override
  public PostProcessor getPostProcessor() {
    return this.postProcessor;
  }

  @Override
  public SecurityManager getSecurityManager() {
    return this.securityManager;
  }

  /**
   * It first looks the shiro subject in AccessControlContext since JMX will use multiple threads to
   * process operations from the same client, then it looks into Shiro's thead context.
   *
   * @return the shiro subject, null if security is not enabled
   */
  @Override
  public Subject getSubject() {
    Subject currentUser;

    // First try get the principal out of AccessControlContext instead of Shiro's Thread context
    // since threads can be shared between JMX clients.
    javax.security.auth.Subject jmxSubject =
        javax.security.auth.Subject.getSubject(AccessController.getContext());

    if (jmxSubject != null) {
      Set<ShiroPrincipal> principals = jmxSubject.getPrincipals(ShiroPrincipal.class);
      if (!principals.isEmpty()) {
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

  /**
   * @return return a shiro subject
   */
  @Override
  public Subject login(final Properties credentials) {
    if (credentials == null) {
      throw new AuthenticationRequiredException("credentials are null");
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
    return currentUser.associateWith(callable);
  }

  /**
   * Binds the passed-in subject to the executing thread. Usage:
   *
   * <pre>
   * ThreadState state = null;
   * try {
   *   state = securityService.bindSubject(subject);
   *   // do the rest of the work as this subject
   * } finally {
   *   if (state != null)
   *     state.clear();
   * }
   * </pre>
   */
  @Override
  public ThreadState bindSubject(final Subject subject) {
    if (subject == null) {
      throw new GemFireSecurityException("Error: Anonymous User");
    }

    ThreadState threadState = new SubjectThreadState(subject);
    threadState.bind();
    return threadState;
  }

  @Override
  public void authorizeClusterManage() {
    authorize(Resource.CLUSTER, Operation.MANAGE, Target.ALL, ResourcePermission.ALL);
  }

  @Override
  public void authorizeClusterWrite() {
    authorize(Resource.CLUSTER, Operation.WRITE, Target.ALL, ResourcePermission.ALL);
  }

  @Override
  public void authorizeClusterRead() {
    authorize(Resource.CLUSTER, Operation.READ, Target.ALL, ResourcePermission.ALL);
  }

  @Override
  public void authorizeDataManage() {
    authorize(Resource.DATA, Operation.MANAGE, Target.ALL, ResourcePermission.ALL);
  }

  @Override
  public void authorizeDataWrite() {
    authorize(Resource.DATA, Operation.WRITE, Target.ALL, ResourcePermission.ALL);
  }

  @Override
  public void authorizeDataRead() {
    authorize(Resource.DATA, Operation.READ, Target.ALL, ResourcePermission.ALL);
  }

  @Override
  public void authorizeDiskManage() {
    authorize(Resource.CLUSTER, Operation.MANAGE, Target.DISK, ResourcePermission.ALL);
  }

  @Override
  public void authorizeGatewayManage() {
    authorize(Resource.CLUSTER, Operation.MANAGE, Target.GATEWAY, ResourcePermission.ALL);
  }

  @Override
  public void authorizeJarManage() {
    authorize(Resource.CLUSTER, Operation.MANAGE, Target.JAR, ResourcePermission.ALL);
  }

  @Override
  public void authorizeQueryManage() {
    authorize(Resource.CLUSTER, Operation.MANAGE, Target.QUERY, ResourcePermission.ALL);
  }

  @Override
  public void authorizeRegionManage(final String regionName) {
    authorize(Resource.DATA, Operation.MANAGE, regionName, ResourcePermission.ALL);
  }

  @Override
  public void authorizeRegionManage(final String regionName, final String key) {
    authorize(Resource.DATA, Operation.MANAGE, regionName, key);
  }

  @Override
  public void authorizeRegionWrite(final String regionName) {
    authorize(Resource.DATA, Operation.WRITE, regionName, ResourcePermission.ALL);
  }

  @Override
  public void authorizeRegionWrite(final String regionName, final String key) {
    authorize(Resource.DATA, Operation.WRITE, regionName, key);
  }

  @Override
  public void authorizeRegionRead(final String regionName) {
    authorize(Resource.DATA, Operation.READ, regionName, ResourcePermission.ALL);
  }

  @Override
  public void authorizeRegionRead(final String regionName, final String key) {
    authorize(Resource.DATA, Operation.READ, regionName, key);
  }

  @Override
  public void authorize(Resource resource, Operation operation, Target target, String key) {
    authorize(resource, operation, target.getName(), key);
  }

  @Override
  public void authorize(Resource resource, Operation operation, Target target) {
    authorize(resource, operation, target, ResourcePermission.ALL);
  }

  @Override
  public void authorize(Resource resource, Operation operation, String target, String key) {
    authorize(new ResourcePermission(resource, operation, target, key));
  }

  @Override
  public void authorize(final ResourcePermission context) {
    if (context == null) {
      return;
    }
    if (context.getResource() == Resource.NULL && context.getOperation() == Operation.NULL) {
      return;
    }

    Subject currentUser = getSubject();
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
    if (this.securityManager != null) {
      this.securityManager.close();
    }
    if (this.postProcessor != null) {
      this.postProcessor.close();
    }

    ThreadContext.remove();
    SecurityUtils.setSecurityManager(null);
  }

  /**
   * postProcess call already has this logic built in, you don't need to call this everytime you
   * call postProcess. But if your postProcess is pretty involved with preparations and you need to
   * bypass it entirely, call this first.
   */
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
      principal = getSubject().getPrincipal();
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
  public boolean isIntegratedSecurity() {
    return true;
  }

  @Override
  public boolean isClientSecurityRequired() {
    return true;
  }

  @Override
  public boolean isPeerSecurityRequired() {
    return true;
  }
}
