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

package org.apache.geode.cache.query.internal;

import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.query.internal.xml.QueryMethodAuthorizerCreation;
import org.apache.geode.cache.query.security.JavaBeanAccessorMethodAuthorizer;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RegExMethodAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

public class QueryConfigurationServiceImpl implements QueryConfigurationService {

  private static final Logger logger = LogService.getLogger();
  private static final String UPDATE_ERROR_MESSAGE =
      "Exception while updating MethodInvocationAuthorizer: ";
  private static final String NULL_CLASS_NAME =
      "Null class name found for MethodInvocationAuthorizer. ";
  private static final String NO_CLASS_FOUND =
      "No MethodInvocationAuthorizer class found with name ";
  private static final String NO_VALID_CONSTRUCTOR =
      "No valid MethodInvocationAuthorizer constructor available. ";
  private static final String INSTANTIATION_ERROR =
      "Error occurred while instantiating MethodInvocationAuthorizer. ";
  private static final String AUTHORIZER_NOT_UPDATED = "The authorizer was not updated.";

  private MethodInvocationAuthorizer authorizer;

  /**
   * Instead of the below property, please use the UnrestrictedMethodAuthorizer
   * implementation of MethodInvocationAuthorizer, which provides the same functionality.
   */
  @Deprecated
  public final boolean ALLOW_UNTRUSTED_METHOD_INVOCATION;

  public static final String DEPRECATION_WARNING = "The property " + GEMFIRE_PREFIX +
      "QueryService.allowUntrustedMethodInvocation is deprecated. " +
      "Please use the UnrestrictedMethodAuthorizer implementation of MethodInvocationAuthorizer " +
      "instead";

  public QueryConfigurationServiceImpl() {
    ALLOW_UNTRUSTED_METHOD_INVOCATION = Boolean.parseBoolean(
        System.getProperty(GEMFIRE_PREFIX + "QueryService.allowUntrustedMethodInvocation"));
  }

  public static MethodInvocationAuthorizer getNoOpAuthorizer() {
    // A no-op authorizer, allow method invocation
    return ((Method m, Object t) -> true);
  }

  @Override
  public boolean init(Cache cache) {
    if (cache == null) {
      throw new IllegalArgumentException("cache must not be null");
    }

    if (System
        .getProperty(GEMFIRE_PREFIX + "QueryService.allowUntrustedMethodInvocation") != null) {
      logger.warn(DEPRECATION_WARNING);
    }

    if (isSecurityDisabled((InternalCache) cache) || ALLOW_UNTRUSTED_METHOD_INVOCATION) {
      this.authorizer = getNoOpAuthorizer();
    } else {
      this.authorizer = new RestrictedMethodAuthorizer(cache);
    }
    return true;
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return QueryConfigurationService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public MethodInvocationAuthorizer getMethodAuthorizer() {
    return authorizer;
  }

  @Override
  public void updateMethodAuthorizer(Cache cache, QueryMethodAuthorizerCreation creation) {
    updateMethodAuthorizer(cache, creation.getClassName(), creation.getParameters());
  }

  @Override
  public void updateMethodAuthorizer(Cache cache, String className, Set<String> parameters) {
    if (isSecurityDisabled((InternalCache) cache) || ALLOW_UNTRUSTED_METHOD_INVOCATION) {
      return;
    }

    if (className == null) {
      logError(UPDATE_ERROR_MESSAGE + NULL_CLASS_NAME + AUTHORIZER_NOT_UPDATED);
      return;
    }

    if (className.equals(RestrictedMethodAuthorizer.class.getSimpleName())) {
      this.authorizer = new RestrictedMethodAuthorizer(cache);
    } else if (className.equals(UnrestrictedMethodAuthorizer.class.getSimpleName())) {
      this.authorizer = new UnrestrictedMethodAuthorizer(cache);
    } else if (className.equals(JavaBeanAccessorMethodAuthorizer.class.getSimpleName())) {
      this.authorizer = new JavaBeanAccessorMethodAuthorizer(cache, parameters);
    } else if (className.equals(RegExMethodAuthorizer.class.getSimpleName())) {
      this.authorizer = new RegExMethodAuthorizer(cache, parameters);
    } else {
      try {
        @SuppressWarnings("unchecked")
        Class<MethodInvocationAuthorizer> userClass =
            (Class<MethodInvocationAuthorizer>) ClassPathLoader.getLatest().forName(className);
        Constructor<MethodInvocationAuthorizer> userConstructor =
            userClass.getDeclaredConstructor(Cache.class, Set.class);
        this.authorizer = userConstructor.newInstance(cache, parameters);
      } catch (Exception e) {
        logErrorMessage(className, e);
      }
    }
  }

  private boolean isSecurityDisabled(InternalCache cache) {
    return !cache.getSecurityService().isIntegratedSecurity();
  }

  private void logErrorMessage(String className, Exception e) {
    if (e instanceof ClassNotFoundException) {
      logError(UPDATE_ERROR_MESSAGE + NO_CLASS_FOUND + className + ". " + AUTHORIZER_NOT_UPDATED
          + " Reason: " + e.toString());
    } else if (e instanceof NoSuchMethodException || e instanceof SecurityException) {
      logError(UPDATE_ERROR_MESSAGE + NO_VALID_CONSTRUCTOR + AUTHORIZER_NOT_UPDATED + " Reason: "
          + e.toString());
    } else {
      logError(UPDATE_ERROR_MESSAGE + INSTANTIATION_ERROR + AUTHORIZER_NOT_UPDATED + " Reason: "
          + e.toString());
    }
  }

  // For testing
  void logError(String message) {
    logger.error(message);
  }
}
