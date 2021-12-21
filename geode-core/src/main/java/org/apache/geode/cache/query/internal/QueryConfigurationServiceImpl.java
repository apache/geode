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

import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.cache.query.internal.xml.QueryMethodAuthorizerCreation;
import org.apache.geode.cache.query.security.JavaBeanAccessorMethodAuthorizer;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RegExMethodAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

public class QueryConfigurationServiceImpl implements QueryConfigurationService {
  private static final Logger logger = LogService.getLogger();
  static final String NULL_CACHE_ERROR_MESSAGE = "Cache must not be null";
  private static final String UPDATE_ERROR_MESSAGE =
      "Exception while updating MethodInvocationAuthorizer.";
  public static final String INTERFACE_NOT_IMPLEMENTED_MESSAGE =
      "Provided method authorizer %S does not implement interface %S";
  public static final String CONTINUOUS_QUERIES_RUNNING_MESSAGE =
      "There are CQs running which might have method invocations not allowed by the new MethodInvocationAuthorizer, the update operation can not be completed on this member.";

  /**
   * Instead of the below property, please use the UnrestrictedMethodAuthorizer
   * implementation of MethodInvocationAuthorizer, which provides the same functionality.
   */
  @Deprecated
  public final boolean ALLOW_UNTRUSTED_METHOD_INVOCATION;

  /**
   * Delete this once {@code ALLOW_UNTRUSTED_METHOD_INVOCATION} is removed.
   * Keeping it here and public to avoid using using the same constant String across classes.
   */
  @Deprecated
  public static final String ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY =
      GEMFIRE_PREFIX + "QueryService.allowUntrustedMethodInvocation";

  public static final String DEPRECATION_WARNING = "The property "
      + ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY
      + " is deprecated. Please use the UnrestrictedMethodAuthorizer implementation of MethodInvocationAuthorizer instead.";

  private MethodInvocationAuthorizer authorizer;

  @Immutable
  private static final MethodInvocationAuthorizer NO_OP_AUTHORIZER = new NoOpAuthorizer();

  public QueryConfigurationServiceImpl() {
    ALLOW_UNTRUSTED_METHOD_INVOCATION =
        Boolean.parseBoolean(System.getProperty(ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY));
  }

  public static MethodInvocationAuthorizer getNoOpAuthorizer() {
    // A no-op authorizer, allow method invocation
    return NO_OP_AUTHORIZER;
  }

  @Override
  public boolean init(Cache cache) {
    if (cache == null) {
      throw new IllegalArgumentException(NULL_CACHE_ERROR_MESSAGE);
    }

    if (System.getProperty(ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY) != null) {
      logger.warn(DEPRECATION_WARNING);
    }

    if (isSecurityDisabled((InternalCache) cache) || ALLOW_UNTRUSTED_METHOD_INVOCATION) {
      authorizer = NO_OP_AUTHORIZER;
    } else {
      authorizer = new RestrictedMethodAuthorizer(cache);
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
  public void close() {}

  @Override
  public MethodInvocationAuthorizer getMethodAuthorizer() {
    return authorizer;
  }

  @Override
  public void updateMethodAuthorizer(Cache cache, boolean forceUpdate,
      QueryMethodAuthorizerCreation creation) throws QueryConfigurationServiceException {
    updateMethodAuthorizer(cache, forceUpdate, creation.getClassName(), creation.getParameters());
  }

  private boolean isSecurityDisabled(InternalCache cache) {
    return !cache.getSecurityService().isIntegratedSecurity();
  }

  private void invalidateContinuousQueryCache(CqService cqService) {
    cqService.getAllCqs().forEach(cqQuery -> {
      ServerCQ serverCQ = (ServerCQ) cqQuery;
      serverCQ.invalidateCqResultKeys();
    });
  }

  @Override
  public void updateMethodAuthorizer(Cache cache, boolean forceUpdate, String className,
      Set<String> parameters) throws QueryConfigurationServiceException {
    // Return quickly if security is disabled or deprecated flag is set
    if (isSecurityDisabled((InternalCache) cache) || ALLOW_UNTRUSTED_METHOD_INVOCATION) {
      return;
    }

    // Throw exception if there are CQs running and forceUpdate flag is false.
    CqService cqService = ((InternalCache) cache).getCqService();
    if ((!cqService.getAllCqs().isEmpty()) && (!forceUpdate)) {
      throw new QueryConfigurationServiceException(CONTINUOUS_QUERIES_RUNNING_MESSAGE);
    }

    try {
      if (className.equals(RestrictedMethodAuthorizer.class.getName())) {
        authorizer = new RestrictedMethodAuthorizer(cache);
      } else if (className.equals(UnrestrictedMethodAuthorizer.class.getName())) {
        authorizer = new UnrestrictedMethodAuthorizer(cache);
      } else if (className.equals(JavaBeanAccessorMethodAuthorizer.class.getName())) {
        authorizer = new JavaBeanAccessorMethodAuthorizer(cache, parameters);
      } else if (className.equals(RegExMethodAuthorizer.class.getName())) {
        authorizer = new RegExMethodAuthorizer(cache, parameters);
      } else {
        Class<?> userClass = ClassPathLoader.getLatest().forName(className);
        if (!Arrays.asList(userClass.getInterfaces()).contains(MethodInvocationAuthorizer.class)) {
          throw new QueryConfigurationServiceException(
              String.format(INTERFACE_NOT_IMPLEMENTED_MESSAGE, userClass.getName(),
                  MethodInvocationAuthorizer.class.getName()));
        }

        MethodInvocationAuthorizer tmpAuthorizer =
            (MethodInvocationAuthorizer) userClass.newInstance();
        tmpAuthorizer.initialize(cache, parameters);
        authorizer = tmpAuthorizer;
      }

      invalidateContinuousQueryCache(cqService);
    } catch (Exception e) {
      throw new QueryConfigurationServiceException(UPDATE_ERROR_MESSAGE, e);
    }
  }

  private static class NoOpAuthorizer implements MethodInvocationAuthorizer {

    @Override
    public boolean authorize(Method method, Object target) {
      return true;
    }
  }
}
