package com.gemstone.gemfire.management.internal.security;

import java.io.ObjectInputStream;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
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

public class MBeanServerWrapper implements MBeanServerForwarder {
  
  private MBeanServer mbs;
  private ManagementInterceptor interceptor;
  
  public MBeanServerWrapper(ManagementInterceptor interceptor){
    this.interceptor = interceptor;
  }
  
  private void doAuthorization(ObjectName name, String methodName, Object[] methodParams){
    interceptor.authorize(name,methodName, methodParams);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name) throws ReflectionException,
      InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
    doAuthorization(name, "createMBean", new Object[]{name});
    return mbs.createMBean(className, name);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
      NotCompliantMBeanException, InstanceNotFoundException {
    doAuthorization(name, "createMBean", new Object[]{name});
    return mbs.createMBean(className, name, loaderName);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
      NotCompliantMBeanException {
    doAuthorization(name, "createMBean", new Object[]{name, params});
    return mbs.createMBean(className,name,params,signature);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params,
      String[] signature) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException,
      MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
    doAuthorization(name, "createMBean", new Object[]{name});
    return mbs.createMBean(className, name, loaderName, params, signature);
  }

  @Override
  public ObjectInstance registerMBean(Object object, ObjectName name) throws InstanceAlreadyExistsException,
      MBeanRegistrationException, NotCompliantMBeanException {
    doAuthorization(name, "registerMBean", new Object[]{name});
    return mbs.registerMBean(object, name);
  }

  @Override
  public void unregisterMBean(ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException {
    doAuthorization(name, "registerMBean", new Object[]{});
    mbs.unregisterMBean(name);
  }

  @Override
  public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {    
    return mbs.getObjectInstance(name);
  }

  @Override
  public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) {
    return mbs.queryMBeans(name, query);
  }

  @Override
  public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
    return mbs.queryNames(name, query);
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
    doAuthorization(name, "getAttribute",  new Object[]{attribute});
    return mbs.getAttribute(name, attribute);
  }

  @Override
  public AttributeList getAttributes(ObjectName name, String[] attributes) throws InstanceNotFoundException,
      ReflectionException {
    doAuthorization(name, "getAttributes", new Object[]{attributes});
    return mbs.getAttributes(name, attributes);
  }

  @Override
  public void setAttribute(ObjectName name, Attribute attribute) throws InstanceNotFoundException,
      AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
    doAuthorization(name, "setAttribute", new Object[]{attribute});
    mbs.setAttribute(name, attribute);
  }

  @Override
  public AttributeList setAttributes(ObjectName name, AttributeList attributes) throws InstanceNotFoundException,
      ReflectionException {
    doAuthorization(name, "setAttributes", new Object[]{attributes});
    return mbs.setAttributes(name, attributes);
  }

  @Override
  public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
      throws InstanceNotFoundException, MBeanException, ReflectionException {
    doAuthorization(name, operationName, new Object[]{params, signature});
    return mbs.invoke(name, operationName, params, signature);
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
