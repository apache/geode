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
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;
import org.apache.logging.log4j.Logger;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXPrincipal;
import javax.management.remote.MBeanServerForwarder;
import javax.security.auth.Subject;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.gemstone.gemfire.management.internal.security.ResourceConstants.*;

/**
 *
 * ManagementInterceptor is central go-to place for all M&M Clients Authentication and Authorization
 * requests
 *
 * @author tushark
 * @since 9.0
 *
 */
public class ManagementInterceptor implements JMXAuthenticator {

  // FIXME: Merged from GEODE-17. Are they necessary?
	public static final String USER_NAME = "security-username";
	public static final String PASSWORD = "security-password";
	public static final String OBJECT_NAME_ACCESSCONTROL = "GemFire:service=AccessControl,type=Distributed";

	private MBeanServerWrapper mBeanServerForwarder;
	private Logger logger;  
  private ObjectName accessControlMBeanON;
  private Cache cache;
  private String authzFactoryName;
  private String postAuthzFactoryName;
  private String authenticatorFactoryName;
  private ConcurrentMap<Principal, AccessControl> cachedAuthZCallback;
  private ConcurrentMap<Principal, AccessControl> cachedPostAuthZCallback;

  public ManagementInterceptor(Cache gemFireCacheImpl, Logger logger) {
    this.cache = gemFireCacheImpl;
		this.logger = logger;		
		this.mBeanServerForwarder = new MBeanServerWrapper(this);
    DistributedSystem system = cache.getDistributedSystem();
    Properties sysProps = system.getProperties();
    this.authzFactoryName = sysProps.getProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME);
    this.postAuthzFactoryName = sysProps.getProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_PP_NAME);
    this.authenticatorFactoryName = sysProps.getProperty(DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME);
    this.cachedAuthZCallback = new ConcurrentHashMap<Principal, AccessControl>();
    this.cachedPostAuthZCallback = new ConcurrentHashMap<Principal, AccessControl>();
		registerAccessContorlMbean();
    logger.info("Started Management interceptor on JMX connector");
	}

  /**
   * This method registers an AccessControlMBean which allows any remote JMX Client (for example Pulse) to check for
   * access allowed for given Operation Code.
   */
	private void registerAccessContorlMbean() {
    try {
      AccessControlMBean acc = new AccessControlMBean(this);
      accessControlMBeanON = new ObjectName(ResourceConstants.OBJECT_NAME_ACCESSCONTROL);
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
			username = (String) aCredentials[0];
			password = (String) aCredentials[1];
		pr.put(USER_NAME, username);
		pr.put(PASSWORD, password);
    } else if (credentials instanceof Properties) {
      pr = (Properties) credentials;
    } else {
      throw new SecurityException(WRONGE_CREDENTIALS_MESSAGE);
    }

    try {
      Principal principal = getAuthenticator(cache.getDistributedSystem().getSecurityProperties()).authenticate(pr,
          cache.getDistributedSystem().getDistributedMember());
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
   *
   * @param name
   * @param methodName
   * @param params
   *
   * @throws SecurityException
   *           if access is not granted
   */
  public ResourceOperationContext authorize(ObjectName name, final String methodName, Object[] params) {

    if (StringUtils.isBlank(authzFactoryName)){
      return com.gemstone.gemfire.management.internal.security.AccessControlContext.ACCESS_GRANTED_CONTEXT;
      }

    if (name.equals(accessControlMBeanON)) {
      return com.gemstone.gemfire.management.internal.security.AccessControlContext.ACCESS_GRANTED_CONTEXT;
    }
	  
    if (!ManagementConstants.OBJECTNAME__DEFAULTDOMAIN.equals(name.getDomain()))
      return com.gemstone.gemfire.management.internal.security.AccessControlContext.ACCESS_GRANTED_CONTEXT;

		AccessControlContext acc = AccessController.getContext();		
		Subject subject = Subject.getSubject(acc);

    // Allow operations performed locally on behalf of the connector server itself
		if (subject == null) {
      return com.gemstone.gemfire.management.internal.security.AccessControlContext.ACCESS_GRANTED_CONTEXT;
		}

    if (methodName.equals(ResourceConstants.CREATE_MBEAN) || methodName.equals(ResourceConstants.UNREGISTER_MBEAN)) {
      throw new SecurityException(ACCESS_DENIED_MESSAGE);
		}

    Set<JMXPrincipal> principals = subject.getPrincipals(JMXPrincipal.class);
		
    if (principals == null || principals.isEmpty()) {
      throw new SecurityException(ACCESS_DENIED_MESSAGE);
		}		
	
		Principal principal = principals.iterator().next();

		
    if (logger.isDebugEnabled()) {
      logger.debug("Name=" + name + " methodName=" + methodName + " principal=" + principal.getName());
    }
		
    AccessControl accessControl = getAccessControl(principal, false);
    String method = methodName;
    if (methodName.equals(GET_ATTRIBUTE)) {
      method = GET_PREFIX + (String) params[0];
    } else if(methodName.equals(GET_ATTRIBUTES)) {
      //Pass to first attribute getter
      String[] attrs = (String[]) params[0];
      method = GET_PREFIX + attrs[0];
    } else if(methodName.equals(SET_ATTRIBUTE)) {
      Attribute attribute = (Attribute) params[0];
      method = SET_PREFIX + attribute.getName();
    }

    if (methodName.equals(SET_ATTRIBUTES)) {
      AttributeList attrList = (AttributeList) params[0];
      List<Attribute> list = attrList.asList();
      ResourceOperationContext setterContext = null;
      SetAttributesOperationContext resourceContext = new SetAttributesOperationContext();
      for (int i = 0; i < list.size(); i++) {
        Attribute attribute = list.get(i);
        String setter = SET_PREFIX + attribute.getName();
        setterContext = buildContext(name, setter, null);
        boolean authorized = accessControl.authorizeOperation(null, setterContext);
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Name=" + name + " methodName=" + methodName + " result=" + authorized + " principal=" + principal.getName());
        }
        if (!authorized) {
          throw new SecurityException(ACCESS_DENIED_MESSAGE);
        } else {
          resourceContext.addAttribute(attribute.getName(), setterContext);
        }
      }
      return resourceContext;
    } else {
      ResourceOperationContext resourceContext = buildContext(name, method, params);
      boolean authorized = accessControl.authorizeOperation(null, resourceContext);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Name=" + name + " methodName=" + methodName + " result=" + authorized + " principal=" + principal.getName());
      }

      if (!authorized) throw new SecurityException(ACCESS_DENIED_MESSAGE);
      return resourceContext;
    }
  }

	public MBeanServerForwarder getMBeanServerForwarder() {
		return mBeanServerForwarder;
	}

  public AccessControl getAccessControl(Principal principal, boolean isPost) {
    if (!isPost) {
      if (cachedAuthZCallback.containsKey(principal)) {
        return cachedAuthZCallback.get(principal);
      } else if (!StringUtils.isBlank(authzFactoryName)) {
			try {
          Method authzMethod = ClassLoadUtil.methodFromName(authzFactoryName);
          AccessControl authzCallback = (AccessControl) authzMethod.invoke(null, (Object[]) null);
          authzCallback.init(principal, null, cache);
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
          postAuthzCallback.init(principal, null, cache);
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
    auth.init(gfSecurityProperties,(InternalLogWriter) this.cache.getLogger(), (InternalLogWriter) this.cache.getSecurityLogger());
    return auth;
	}

  private ResourceOperationContext buildContext(ObjectName name, String methodName, Object[] params) {
    String service = name.getKeyProperty("service");
    if (service == null && PROCESS_COMMAND.equals(methodName)) {
      Object[] array = (Object[]) params[0];
      String command = (String) array[0];
      CLIOperationContext context = new CLIOperationContext(command);
      return context;
    } else {
      ResourceOperationContext context = new JMXOperationContext(name, methodName);
      return context;
    }
  }

  public ObjectName getAccessControlMBeanON() {
    return accessControlMBeanON;
    }

  public void postAuthorize(ObjectName name, final String methodName, ResourceOperationContext context, Object result) {

    if (StringUtils.isBlank(postAuthzFactoryName)){
      return ;
    }

    context.setPostOperationResult(result);

    if (context.equals(com.gemstone.gemfire.management.internal.security.AccessControlContext.ACCESS_GRANTED_CONTEXT))
      return;

    AccessControlContext acc = AccessController.getContext();
    Subject subject = Subject.getSubject(acc);
    Set<JMXPrincipal> principals = subject.getPrincipals(JMXPrincipal.class);
    if (principals == null || principals.isEmpty()) {
      throw new SecurityException(ACCESS_DENIED_MESSAGE);
    }
    Principal principal = principals.iterator().next();
    AccessControl accessControl = getAccessControl(principal, true);
    if (context instanceof SetAttributesOperationContext) {
      SetAttributesOperationContext setterContext = (SetAttributesOperationContext) context;
      for (Entry<String, ResourceOperationContext> e : setterContext.getAttributesContextMap().entrySet()) {
        //TODO : Retrieve proper values from AttributeList and set to its jmxContext
        e.getValue().setPostOperationResult(result);
        boolean authorized = accessControl.authorizeOperation(null, e.getValue());
        if (!authorized)
          throw new SecurityException(ACCESS_DENIED_MESSAGE);
      }
    } else {
      boolean authorized = accessControl.authorizeOperation(null, context);
      if (logger.isDebugEnabled()) {
        logger.debug("postAuthorize: Name=" + name + " methodName=" + methodName + " result=" + authorized
            + " principal=" + principal.getName());
      }
      if (!authorized)
        throw new SecurityException(ACCESS_DENIED_MESSAGE);
    }
  }

}
