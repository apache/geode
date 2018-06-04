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
package org.apache.geode.management.internal.security;

import java.io.ObjectInputStream;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.Descriptor;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.Query;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;
import javax.management.remote.MBeanServerForwarder;

import org.apache.commons.lang.StringUtils;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;

/**
 * This class intercepts all MBean requests for GemFire MBeans and passed it to
 * ManagementInterceptor for authorization
 *
 * @since Geode 1.0
 */
public class MBeanServerWrapper implements MBeanServerForwarder {

  private MBeanServer mbs;

  private final SecurityService securityService;

  public MBeanServerWrapper(SecurityService securityService) {
    this.securityService = securityService;
  }

  private void checkDomain(ObjectName name) {
    if (ManagementConstants.OBJECTNAME__DEFAULTDOMAIN.equals(name.getDomain()))
      throw new SecurityException(ResourceConstants.ACCESS_DENIED_MESSAGE);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name) throws ReflectionException,
      InstanceAlreadyExistsException, MBeanException, NotCompliantMBeanException {
    checkDomain(name);
    return mbs.createMBean(className, name);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanException,
      NotCompliantMBeanException, InstanceNotFoundException {
    checkDomain(name);
    return mbs.createMBean(className, name, loaderName);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, Object[] params,
      String[] signature) throws ReflectionException, InstanceAlreadyExistsException,
      MBeanException, NotCompliantMBeanException {
    checkDomain(name);
    return mbs.createMBean(className, name, params, signature);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName,
      Object[] params, String[] signature)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanException,
      NotCompliantMBeanException, InstanceNotFoundException {
    checkDomain(name);
    return mbs.createMBean(className, name, loaderName, params, signature);
  }

  @Override
  public ObjectInstance registerMBean(Object object, ObjectName name)
      throws InstanceAlreadyExistsException, MBeanRegistrationException,
      NotCompliantMBeanException {
    checkDomain(name);
    return mbs.registerMBean(object, name);
  }

  @Override
  public void unregisterMBean(ObjectName name)
      throws InstanceNotFoundException, MBeanRegistrationException {
    checkDomain(name);
    mbs.unregisterMBean(name);
  }

  @Override
  public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {
    return mbs.getObjectInstance(name);
  }

  private static QueryExp notAccessControlMBean =
      Query.not(Query.isInstanceOf(Query.value(AccessControlMXBean.class.getName())));

  @Override
  public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) {
    // We need to filter out the AccessControlMXBean so that the clients wouldn't see it
    if (query != null)
      return mbs.queryMBeans(name, Query.and(query, notAccessControlMBean));
    else
      return mbs.queryMBeans(name, notAccessControlMBean);
  }

  @Override
  public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
    if (query != null)
      return mbs.queryNames(name, Query.and(query, notAccessControlMBean));
    else
      return mbs.queryNames(name, notAccessControlMBean);
  }

  @Override
  public boolean isRegistered(ObjectName name) {
    return mbs.isRegistered(name);
  }

  @Override
  public Integer getMBeanCount() {
    return mbs.getMBeanCount();
  }

  @Override
  public Object getAttribute(ObjectName name, String attribute)
      throws MBeanException, InstanceNotFoundException, ReflectionException {
    ResourcePermission ctx = getOperationContext(name, attribute, false);
    this.securityService.authorize(ctx);
    Object result;
    try {
      result = mbs.getAttribute(name, attribute);
    } catch (AttributeNotFoundException nex) {
      return null;
    }
    return result;
  }

  @Override
  public AttributeList getAttributes(ObjectName name, String[] attributes)
      throws InstanceNotFoundException, ReflectionException {
    AttributeList results = new AttributeList();
    for (String attribute : attributes) {
      try {
        Object value = getAttribute(name, attribute);
        Attribute att = new Attribute(attribute, value);
        results.add(att);
      } catch (Exception e) {
        throw new GemFireSecurityException("error getting value of " + attribute + " from " + name,
            e);
      }
    }
    return results;
  }

  @Override
  public void setAttribute(ObjectName name, Attribute attribute)
      throws InstanceNotFoundException, AttributeNotFoundException, InvalidAttributeValueException,
      MBeanException, ReflectionException {
    ResourcePermission ctx = getOperationContext(name, attribute.getName(), false);
    this.securityService.authorize(ctx);
    mbs.setAttribute(name, attribute);
  }

  @Override
  public AttributeList setAttributes(ObjectName name, AttributeList attributes)
      throws InstanceNotFoundException, ReflectionException {
    // call setAttribute instead to use the authorization logic
    for (Attribute attribute : attributes.asList()) {
      try {
        setAttribute(name, attribute);
      } catch (Exception e) {
        throw new GemFireSecurityException("error setting attribute " + attribute + " of " + name,
            e);
      }
    }
    return attributes;
  }

  @Override
  public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
      throws InstanceNotFoundException, MBeanException, ReflectionException {

    ResourcePermission ctx = getOperationContext(name, operationName, true);
    this.securityService.authorize(ctx);

    return mbs.invoke(name, operationName, params, signature);
  }

  // TODO: cache this
  private ResourcePermission getOperationContext(ObjectName objectName, String featureName,
      boolean isOp) throws InstanceNotFoundException, ReflectionException {
    MBeanInfo beanInfo;
    try {
      beanInfo = mbs.getMBeanInfo(objectName);
    } catch (IntrospectionException e) {
      throw new GemFireSecurityException("error getting beanInfo of " + objectName, e);
    }
    // If there is no annotation defined either in the class level or method level, we should
    // consider this operation/attribute freely accessible
    ResourcePermission result = null;

    // find the context in the beanInfo if defined in the class level
    result = getOperationContext(beanInfo.getDescriptor(), result);

    MBeanFeatureInfo[] featureInfos;
    if (isOp) {
      featureInfos = beanInfo.getOperations();
    } else {
      featureInfos = beanInfo.getAttributes();
    }
    // still look into the attributes/operations to see if it's defined in the method level
    for (MBeanFeatureInfo info : featureInfos) {
      if (info.getName().equals(featureName)) {
        // found the featureInfo of this method on the bean
        result = getOperationContext(info.getDescriptor(), result);
        break;
      }
    }
    return result;
  }

  private ResourcePermission getOperationContext(Descriptor descriptor,
      ResourcePermission defaultValue) {
    String resource = (String) descriptor.getFieldValue("resource");
    String operationCode = (String) descriptor.getFieldValue("operation");
    String targetCode = (String) descriptor.getFieldValue("target");
    if (resource != null && operationCode != null) {
      if (StringUtils.isBlank(targetCode)) {
        return new ResourcePermission(Resource.valueOf(resource), Operation.valueOf(operationCode));
      } else {
        return new ResourcePermission(Resource.valueOf(resource), Operation.valueOf(operationCode),
            Target.valueOf(targetCode).getName());
      }
    }
    return defaultValue;
  }


  @Override
  public String getDefaultDomain() {
    return mbs.getDefaultDomain();
  }

  @Override
  public String[] getDomains() {
    return mbs.getDomains();
  }

  @Override
  public void addNotificationListener(ObjectName name, NotificationListener listener,
      NotificationFilter filter, Object handback) throws InstanceNotFoundException {
    mbs.addNotificationListener(name, listener, filter, handback);
  }

  @Override
  public void addNotificationListener(ObjectName name, ObjectName listener,
      NotificationFilter filter, Object handback) throws InstanceNotFoundException {
    mbs.addNotificationListener(name, listener, filter, handback);
  }

  @Override
  public void removeNotificationListener(ObjectName name, ObjectName listener)
      throws InstanceNotFoundException, ListenerNotFoundException {
    mbs.removeNotificationListener(name, listener);
  }

  @Override
  public void removeNotificationListener(ObjectName name, ObjectName listener,
      NotificationFilter filter, Object handback)
      throws InstanceNotFoundException, ListenerNotFoundException {
    mbs.removeNotificationListener(name, listener, filter, handback);

  }

  @Override
  public void removeNotificationListener(ObjectName name, NotificationListener listener)
      throws InstanceNotFoundException, ListenerNotFoundException {
    mbs.removeNotificationListener(name, listener);
  }

  @Override
  public void removeNotificationListener(ObjectName name, NotificationListener listener,
      NotificationFilter filter, Object handback)
      throws InstanceNotFoundException, ListenerNotFoundException {
    mbs.removeNotificationListener(name, listener, filter, handback);
  }

  @Override
  public MBeanInfo getMBeanInfo(ObjectName name)
      throws InstanceNotFoundException, IntrospectionException, ReflectionException {
    return mbs.getMBeanInfo(name);
  }

  @Override
  public boolean isInstanceOf(ObjectName name, String className) throws InstanceNotFoundException {
    return mbs.isInstanceOf(name, className);
  }

  @Override
  public Object instantiate(String className) throws ReflectionException, MBeanException {
    return mbs.instantiate(className);
  }

  @Override
  public Object instantiate(String className, ObjectName loaderName)
      throws ReflectionException, MBeanException, InstanceNotFoundException {
    return mbs.instantiate(className, loaderName);
  }

  @Override
  public Object instantiate(String className, Object[] params, String[] signature)
      throws ReflectionException, MBeanException {
    return mbs.instantiate(className, params, signature);
  }

  @Override
  public Object instantiate(String className, ObjectName loaderName, Object[] params,
      String[] signature) throws ReflectionException, MBeanException, InstanceNotFoundException {
    return mbs.instantiate(className, params, signature);
  }

  @SuppressWarnings("deprecation")
  @Override
  public ObjectInputStream deserialize(ObjectName name, byte[] data) throws OperationsException {
    return mbs.deserialize(name, data);
  }

  @Override
  public ObjectInputStream deserialize(String className, byte[] data)
      throws OperationsException, ReflectionException {
    return mbs.deserialize(className, data);
  }

  @SuppressWarnings("deprecation")
  @Override
  public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data)
      throws OperationsException, ReflectionException {
    return mbs.deserialize(className, loaderName, data);
  }

  @Override
  public ClassLoader getClassLoaderFor(ObjectName mbeanName) throws InstanceNotFoundException {
    return mbs.getClassLoaderFor(mbeanName);
  }

  @Override
  public ClassLoader getClassLoader(ObjectName loaderName) throws InstanceNotFoundException {
    return mbs.getClassLoader(loaderName);
  }

  @Override
  public ClassLoaderRepository getClassLoaderRepository() {
    return mbs.getClassLoaderRepository();
  }

  @Override
  public MBeanServer getMBeanServer() {
    return mbs;
  }

  @Override
  public void setMBeanServer(MBeanServer mbs) {
    this.mbs = mbs;
  }

}
