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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.logging.internal.spi.LoggingProvider.SECURITY_LOGGER_NAME;

import java.io.IOException;
import java.security.AccessController;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.ShiroException;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.support.SubjectThreadState;
import org.apache.shiro.util.ThreadContext;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.GemFireIOException;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.security.shiro.GeodeAuthenticationToken;
import org.apache.geode.internal.security.shiro.SecurityManagerProvider;
import org.apache.geode.internal.security.shiro.ShiroPrincipal;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
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

/**
 * Security service with SecurityManager and an optional PostProcessor.
 */
public class IntegratedSecurityService implements SecurityService {
  private static final Logger logger = LogService.getLogger(SECURITY_LOGGER_NAME);

  public static final String CREDENTIALS_SESSION_ATTRIBUTE = "credentials";

  private final AtomicBoolean closed = new AtomicBoolean();
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

    securityManager = provider.getSecurityManager();
    this.postProcessor = postProcessor;
  }

  @Override
  public PostProcessor getPostProcessor() {
    return postProcessor;
  }

  @Override
  public SecurityManager getSecurityManager() {
    return securityManager;
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
      throw new AuthenticationRequiredException("Failed to find the authenticated user.");
    }

    return currentUser;
  }

  /**
   * Returns the current principal if one exists or null.
   *
   * @return the principal
   */
  @Override
  public Object getPrincipal() {
    try {
      return getSubject().getPrincipal();
    } catch (Exception ex) {
      return null;
    }
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
      logger.info("error logging in: " + token.getPrincipal());
      throw new AuthenticationFailedException(
          "Authentication error. Please check your credentials.", e);
    }

    Session currentSession = currentUser.getSession();
    currentSession.setAttribute(CREDENTIALS_SESSION_ATTRIBUTE, credentials);
    return currentUser;
  }

  @Override
  public void logout() {
    Subject currentUser = getSubject();
    try {
      logger.debug("Logging out " + currentUser.getPrincipal());
      currentUser.logout();
    } catch (ShiroException e) {
      logger.info("error logging out: " + currentUser.getPrincipal());
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
   *   if (state != null) {
   *     state.clear();
   *   }
   * }
   * </pre>
   */
  @Override
  public ThreadState bindSubject(final Subject subject) {
    if (subject == null) {
      throw new AuthenticationRequiredException("Failed to find the authenticated user.");
    }

    ThreadState threadState = new SubjectThreadState(subject);
    threadState.bind();
    return threadState;
  }

  @Override
  public void authorize(Resource resource, Operation operation) {
    authorize(resource, operation, Target.ALL, ResourcePermission.ALL);
  }

  @Override
  public void authorize(Resource resource, Operation operation, Target target) {
    authorize(resource, operation, target, ResourcePermission.ALL);
  }

  @Override
  public void authorize(Resource resource, Operation operation, String target) {
    authorize(resource, operation, target, ResourcePermission.ALL);
  }

  @Override
  public void authorize(Resource resource, Operation operation, Target target, String key) {
    authorize(new ResourcePermission(resource, operation, target, key));
  }

  @Override
  public void authorize(Resource resource, Operation operation, String target, Object key) {
    String keystr = null;
    if (key != null) {
      keystr = key.toString();
    }
    authorize(new ResourcePermission(resource, operation, target, keystr));
  }

  @Override
  public void authorize(final ResourcePermission context) {
    Subject currentUser = getSubject();
    authorize(context, currentUser);
  }

  @Override
  public void authorize(ResourcePermission context, Subject currentUser) {
    if (context == null) {
      return;
    }
    if (context.getResource() == Resource.NULL && context.getOperation() == Operation.NULL) {
      return;
    }

    try {
      currentUser.checkPermission(context);
    } catch (ShiroException e) {
      String message = currentUser.getPrincipal() + " not authorized for " + context;
      logger.info("NotAuthorizedException: {}", message);
      throw new NotAuthorizedException(message, e);
    }
  }

  @Override
  public void close() {
    // subsequent calls to close are no-op
    if (closed.compareAndSet(false, true)) {
      if (securityManager != null) {
        securityManager.close();
      }
      if (postProcessor != null) {
        postProcessor.close();
      }

      ThreadContext.remove();
      SecurityUtils.setSecurityManager(null);
    }
  }

  /**
   * postProcess call already has this logic built in, you don't need to call this everytime you
   * call postProcess. But if your postProcess is pretty involved with preparations and you need to
   * bypass it entirely, call this first.
   */
  @Override
  public boolean needPostProcess() {
    return postProcessor != null;
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

    String regionName = StringUtils.stripStart(regionPath, SEPARATOR);
    Object newValue;

    // if the data is a byte array, but the data itself is supposed to be an object, we need to
    // deserialize it before we pass it to the callback.
    if (valueIsSerialized && value instanceof byte[]) {
      try {
        Object oldObj = EntryEventImpl.deserialize((byte[]) value);
        Object newObj = postProcessor.processRegionValue(principal, regionName, key, oldObj);
        newValue = BlobHelper.serializeToBlob(newObj);
      } catch (IOException | SerializationException e) {
        throw new GemFireIOException("Exception de/serializing entry value", e);
      }
    } else {
      newValue = postProcessor.processRegionValue(principal, regionName, key, value);
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
