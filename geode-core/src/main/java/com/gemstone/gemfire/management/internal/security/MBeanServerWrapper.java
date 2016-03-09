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

import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.security.GemFireSecurityException;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.Descriptor;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;
import javax.management.remote.MBeanServerForwarder;
import java.io.ObjectInputStream;
import java.util.HashSet;
import java.util.Set;

import static com.gemstone.gemfire.management.internal.security.ResourceConstants.*;

/**
 * This class intercepts all MBean requests for GemFire MBeans and passed it to
 * ManagementInterceptor for authorization
 *
 *
 * @author tushark
 * @since 9.0
 *
 */
public class MBeanServerWrapper implements MBeanServerForwarder {
  
  private MBeanServer mbs;
  private ManagementInterceptor interceptor;
  
  public MBeanServerWrapper(ManagementInterceptor interceptor){
    this.interceptor = interceptor;
  }
  
  private void doAuthorization(ResourceOperationContext context){
    interceptor.authorize(context);
  }

  private void doAuthorizationPost(ResourceOperationContext context){
    interceptor.postAuthorize(context);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name) throws ReflectionException,
      InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
    throw new SecurityException(ACCESS_DENIED_MESSAGE);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
      NotCompliantMBeanException, InstanceNotFoundException {
    throw new SecurityException(ACCESS_DENIED_MESSAGE);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
      NotCompliantMBeanException {
    throw new SecurityException(ACCESS_DENIED_MESSAGE);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params,
      String[] signature) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException,
      MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
    throw new SecurityException(ACCESS_DENIED_MESSAGE);
  }

  @Override
  public ObjectInstance registerMBean(Object object, ObjectName name) throws InstanceAlreadyExistsException,
      MBeanRegistrationException, NotCompliantMBeanException {
    throw new SecurityException(ACCESS_DENIED_MESSAGE);
  }

  @Override
  public void unregisterMBean(ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException {
    throw new SecurityException(ACCESS_DENIED_MESSAGE);
  }

  @Override
  public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {    
    return mbs.getObjectInstance(name);
  }

  @Override
  public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) {
    ResourceOperationContext ctx = new ResourceOperationContext(Resource.MBEAN, OperationCode.QUERY);
    return filterAccessControlMBeanInstance(mbs.queryMBeans(name, query));
  }

  private Set<ObjectInstance> filterAccessControlMBeanInstance(Set<ObjectInstance> queryMBeans) {
    Set<ObjectInstance> set = new HashSet<ObjectInstance>();
    for(ObjectInstance oi : queryMBeans) {
      if(!oi.getObjectName().equals(interceptor.getAccessControlMBeanON())){
        set.add(oi);
      }
    }
    return set;
  }

  @Override
  public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
    return filterAccessControlMBean(mbs.queryNames(name, query));
  }

  private Set<ObjectName> filterAccessControlMBean(Set<ObjectName> queryNames) {
    Set<ObjectName> set = new HashSet<ObjectName>();
    for(ObjectName oi : queryNames) {
      if(!oi.equals(interceptor.getAccessControlMBeanON())){
        set.add(oi);
      }
    }
    return set;
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
  public Object getAttribute(ObjectName name, String attribute) throws MBeanException, AttributeNotFoundException,
      InstanceNotFoundException, ReflectionException {
    ResourceOperationContext ctx = getAttributeContext(name, attribute);
    doAuthorization(ctx);
    Object result = mbs.getAttribute(name, attribute);
    ctx.setPostOperationResult(result);
    doAuthorizationPost(ctx);
    return result;
  }

  @Override
  public AttributeList getAttributes(ObjectName name, String[] attributes) throws InstanceNotFoundException,
      ReflectionException {
    AttributeList results = new AttributeList();
    for(String attribute:attributes){
      try {
        Object value = getAttribute(name, attribute);
        Attribute att = new Attribute(attribute, value);
        results.add(att);
      } catch (Exception e) {
        throw new GemFireSecurityException("error getting value of "+attribute+" from "+name, e);
      }
    }
    return results;
  }

  @Override
  public void setAttribute(ObjectName name, Attribute attribute) throws InstanceNotFoundException,
      AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
    ResourceOperationContext ctx = getAttributeContext(name, attribute.getName());
    doAuthorization(ctx);
    mbs.setAttribute(name, attribute);
  }

  // TODO: cache this
  private ResourceOperationContext getAttributeContext(ObjectName name, String attribute)
      throws InstanceNotFoundException, ReflectionException {
    MBeanInfo beanInfo = null;
    try {
      beanInfo = mbs.getMBeanInfo(name);
    } catch (IntrospectionException e) {
      throw new GemFireSecurityException("error getting beanInfo of "+name);
    }
    MBeanAttributeInfo[] attributeInfos = beanInfo.getAttributes();
    for(MBeanAttributeInfo attributeInfo:attributeInfos){
      if(attributeInfo.getName().equals(attribute)){
        // found the operationInfo of this method on the bean
        Descriptor descriptor = attributeInfo.getDescriptor();
        Resource resource = (Resource)descriptor.getFieldValue("resource");
        OperationCode operationCode = (OperationCode)descriptor.getFieldValue("operation");
        if(resource!=null && operationCode!=null){
          return new ResourceOperationContext(resource, operationCode);
        }
      }
    }
    return new ResourceOperationContext(Resource.DEFAULT, OperationCode.LIST_DS);
  }

  // TODO: cache this
  private ResourceOperationContext getOperationContext(ObjectName name, String operationName)
      throws InstanceNotFoundException, ReflectionException {
    MBeanInfo beanInfo = null;
    try {
      beanInfo = mbs.getMBeanInfo(name);
    } catch (IntrospectionException e) {
      throw new GemFireSecurityException("error getting beanInfo of "+name);
    }
    MBeanOperationInfo[] opInfos = beanInfo.getOperations();
    for(MBeanOperationInfo opInfo:opInfos){
      if(opInfo.getName().equals(operationName)){
        // found the operationInfo of this method on the bean
        Descriptor descriptor = opInfo.getDescriptor();
        String resource = (String)descriptor.getFieldValue("resource");
        String operationCode = (String)descriptor.getFieldValue("operation");
        if(resource!=null && operationCode!=null){
          return new ResourceOperationContext(resource, operationCode);
        }
      }
    }
    return new ResourceOperationContext(Resource.DEFAULT, OperationCode.LIST_DS);
  }

  @Override
  public AttributeList setAttributes(ObjectName name, AttributeList attributes) throws InstanceNotFoundException,
      ReflectionException {
    // call setAttribute instead to use the authorization logic
    for(Attribute attribute:attributes.asList()){
      try {
        setAttribute(name, attribute);
      } catch (Exception e) {
        throw new GemFireSecurityException("error setting attribute "+attribute+" of "+name);
      }
    }
    return attributes;
  }

  @Override
  public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
      throws InstanceNotFoundException, MBeanException, ReflectionException {
    ResourceOperationContext ctx = getOperationContext(name, operationName);
    doAuthorization(ctx);
    Object result = mbs.invoke(name, operationName, params, signature);
    ctx.setPostOperationResult(result);
    doAuthorizationPost(ctx);
    return result;
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
  public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter,
      Object handback) throws InstanceNotFoundException {
    mbs.addNotificationListener(name, listener, filter, handback);
  }

  @Override
  public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback)
      throws InstanceNotFoundException {
    mbs.addNotificationListener(name, listener, filter, handback);
  }

  @Override
  public void removeNotificationListener(ObjectName name, ObjectName listener) throws InstanceNotFoundException,
      ListenerNotFoundException {
    mbs.removeNotificationListener(name, listener);
  }

  @Override
  public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter,
      Object handback) throws InstanceNotFoundException, ListenerNotFoundException {
    mbs.removeNotificationListener(name, listener, filter, handback);

  }

  @Override
  public void removeNotificationListener(ObjectName name, NotificationListener listener)
      throws InstanceNotFoundException, ListenerNotFoundException {
    mbs.removeNotificationListener(name, listener);
  }

  @Override
  public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter,
      Object handback) throws InstanceNotFoundException, ListenerNotFoundException {    
    mbs.removeNotificationListener(name, listener, filter, handback);
  }

  @Override
  public MBeanInfo getMBeanInfo(ObjectName name) throws InstanceNotFoundException, IntrospectionException,
      ReflectionException {
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
  public Object instantiate(String className, ObjectName loaderName) throws ReflectionException, MBeanException,
      InstanceNotFoundException {
    return mbs.instantiate(className, loaderName);
  }

  @Override
  public Object instantiate(String className, Object[] params, String[] signature) throws ReflectionException,
      MBeanException {
    return mbs.instantiate(className, params, signature);
  }

  @Override
  public Object instantiate(String className, ObjectName loaderName, Object[] params, String[] signature)
      throws ReflectionException, MBeanException, InstanceNotFoundException {
    return mbs.instantiate(className, params, signature);
  }

  @SuppressWarnings("deprecation")
  @Override
  public ObjectInputStream deserialize(ObjectName name, byte[] data) throws InstanceNotFoundException,
      OperationsException {
    return mbs.deserialize(name, data);
  }

  @Override
  public ObjectInputStream deserialize(String className, byte[] data) throws OperationsException, ReflectionException {    
    return deserialize(className, data);
  }

  @SuppressWarnings("deprecation")
  @Override
  public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data)
      throws InstanceNotFoundException, OperationsException, ReflectionException {    
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
