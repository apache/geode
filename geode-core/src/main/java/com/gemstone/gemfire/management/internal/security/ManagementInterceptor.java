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

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXPrincipal;
import javax.security.auth.Subject;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.gemstone.gemfire.management.internal.security.ResourceConstants.ACCESS_DENIED_MESSAGE;
import static com.gemstone.gemfire.management.internal.security.ResourceConstants.WRONGE_CREDENTIALS_MESSAGE;

/**
 *
 * ManagementInterceptor is central go-to place for all M&M Clients Authentication and Authorization
 * requests
 * @since 9.0
 *
 */
public class ManagementInterceptor implements JMXAuthenticator {

  // FIXME: Merged from GEODE-17. Are they necessary?
	public static final String USER_NAME = "security-username";
	public static final String PASSWORD = "security-password";
	public static final String OBJECT_NAME_ACCESSCONTROL = "GemFire:service=AccessControl,type=Distributed";

	private static final Logger logger = LogManager.getLogger(ManagementInterceptor.class);
//  private Cache cache;
  private String authzFactoryName;
  private String postAuthzFactoryName;
  private String authenticatorFactoryName;
  private ConcurrentMap<Principal, AccessControl> cachedAuthZCallback;
  private ConcurrentMap<Principal, AccessControl> cachedPostAuthZCallback;
  private Properties sysProps = null;

  public ManagementInterceptor(Properties sysProps) {
    this.sysProps = sysProps;
    this.authzFactoryName = sysProps.getProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME);
    this.postAuthzFactoryName = sysProps.getProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_PP_NAME);
    this.authenticatorFactoryName = sysProps.getProperty(DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME);
    this.cachedAuthZCallback = new ConcurrentHashMap<>();
    this.cachedPostAuthZCallback = new ConcurrentHashMap<>();
		registerAccessControlMBean();
    logger.info("Started Management interceptor on JMX connector");
	}

  /**
   * This method registers an AccessControlMBean which allows any remote JMX Client (for example Pulse) to check for
   * access allowed for given Operation Code.
   */
	private void registerAccessControlMBean() {
    try {
      AccessControlMBean acc = new AccessControlMBean(this);
      ObjectName accessControlMBeanON = new ObjectName(ResourceConstants.OBJECT_NAME_ACCESSCONTROL);
      MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

      Set<ObjectName> names = platformMBeanServer.queryNames(accessControlMBeanON, null);
      if(names.isEmpty()) {
        try {
          platformMBeanServer.registerMBean(acc, accessControlMBeanON);
          logger.info("Registered AccessContorlMBean on " + accessControlMBeanON);
        } catch (InstanceAlreadyExistsException e) {
          throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
        } catch (MBeanRegistrationException e) {
          throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
        } catch (NotCompliantMBeanException e) {
          throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource",e);
        }
      }
    } catch (MalformedObjectNameException e) {      
      throw new GemFireConfigException("Error while configuring accesscontrol for jmx resource", e);
    }
  }

  /**
   * Delegates authentication to GemFire Authenticator
   *
   * @throws SecurityException
   *           if authentication fails
   */
  @Override
	public Subject authenticate(Object credentials) {
		String username = null, password = null;
    Properties pr = new Properties();
    if (credentials instanceof String[]) {
			final String[] aCredentials = (String[]) credentials;
			username = aCredentials[0];
			password = aCredentials[1];
		  pr.put(USER_NAME, username);
		  pr.put(PASSWORD, password);
    } else if (credentials instanceof Properties) {
      pr = (Properties) credentials;
    } else {
      throw new SecurityException(WRONGE_CREDENTIALS_MESSAGE);
    }

    try {
      Principal principal = getAuthenticator(sysProps).authenticate(pr);
      return new Subject(true, Collections.singleton(new JMXPrincipal(principal.getName())), Collections.EMPTY_SET,
			    Collections.EMPTY_SET);
    } catch (AuthenticationFailedException e) {
      //wrap inside Security exception. AuthenticationFailedException is gemfire class
      //which generic JMX client can't serialize
      throw new SecurityException("Authentication Failed " + e.getMessage());
	}

  }

  /**
   * Builds ResourceOperationContext for the given JMX MBean Request for delegates Authorization to
   * gemfire AccessControl plugin with context as parameter
   *
   * @throws SecurityException
   *           if access is not granted
   */
  public void authorize(ResourceOperationContext context) {
    if (StringUtils.isBlank(authzFactoryName)){
      return;
    }

    AccessControlContext acc = AccessController.getContext();
    Subject subject = Subject.getSubject(acc);
    Set<JMXPrincipal> principals = subject.getPrincipals(JMXPrincipal.class);

    if (principals == null || principals.isEmpty()) {
      throw new SecurityException(ACCESS_DENIED_MESSAGE + ": No principal found.");
		}

		Principal principal = principals.iterator().next();

    AccessControl accessControl = getAccessControl(principal, false);

    if (!accessControl.authorizeOperation(null, context)) {
      throw new SecurityException(ACCESS_DENIED_MESSAGE + ": Not authorized for "+context);
    }
  }

  public AccessControl getAccessControl(Principal principal, boolean isPost) {
    if (!isPost) {
      if (cachedAuthZCallback.containsKey(principal)) {
        return cachedAuthZCallback.get(principal);
      } else if (!StringUtils.isBlank(authzFactoryName)) {
			try {
          Method authzMethod = ClassLoadUtil.methodFromName(authzFactoryName);
          AccessControl authzCallback = (AccessControl) authzMethod.invoke(null, (Object[]) null);
          authzCallback.init(principal, null);
          cachedAuthZCallback.put(principal, authzCallback);
          return authzCallback;
        } catch (Exception ex) {
          throw new AuthenticationFailedException(
              LocalizedStrings.HandShake_FAILED_TO_ACQUIRE_AUTHENTICATOR_OBJECT.toLocalizedString(), ex);
			}
		}
    } else {
      if (cachedPostAuthZCallback.containsKey(principal)) {
        return cachedPostAuthZCallback.get(principal);
      } else if (!StringUtils.isBlank(postAuthzFactoryName)) {
		try {
          Method authzMethod = ClassLoadUtil.methodFromName(postAuthzFactoryName);
          AccessControl postAuthzCallback = (AccessControl) authzMethod.invoke(null, (Object[]) null);
          postAuthzCallback.init(principal, null);
          cachedPostAuthZCallback.put(principal, postAuthzCallback);
          return postAuthzCallback;
        } catch (Exception ex) {
          throw new AuthenticationFailedException(
              LocalizedStrings.HandShake_FAILED_TO_ACQUIRE_AUTHENTICATOR_OBJECT.toLocalizedString(), ex);
		}
	}
    }
    return null;
  }

  private Authenticator getAuthenticator(Properties gfSecurityProperties) throws AuthenticationFailedException {
    Authenticator auth;
			try {
      Method instanceGetter = ClassLoadUtil.methodFromName(this.authenticatorFactoryName);
      auth = (Authenticator) instanceGetter.invoke(null, (Object[]) null);
    } catch (Exception ex) {
      throw new AuthenticationFailedException(
          LocalizedStrings.HandShake_FAILED_TO_ACQUIRE_AUTHENTICATOR_OBJECT.toLocalizedString(), ex);
			}
    if (auth == null) {
      throw new AuthenticationFailedException(
          LocalizedStrings.HandShake_AUTHENTICATOR_INSTANCE_COULD_NOT_BE_OBTAINED.toLocalizedString());
		}
    auth.init(gfSecurityProperties);
    return auth;
	}

  public void postAuthorize(ResourceOperationContext context) {
    if (StringUtils.isBlank(postAuthzFactoryName)){
      return ;
    }

    AccessControlContext acc = AccessController.getContext();
    Subject subject = Subject.getSubject(acc);
    Set<JMXPrincipal> principals = subject.getPrincipals(JMXPrincipal.class);
    if (principals == null || principals.isEmpty()) {
      throw new SecurityException(ACCESS_DENIED_MESSAGE);
    }
    Principal principal = principals.iterator().next();
    AccessControl accessControl = getAccessControl(principal, true);
    if (!accessControl.authorizeOperation(null, context)) {
      throw new SecurityException(ACCESS_DENIED_MESSAGE);
    }
  }

}
