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

package org.apache.geode.management.internal;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.security.AccessController;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
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
import javax.security.auth.Subject;

/**
 * MBeanServerForwarder that implements file-based access control for JMX operations.
 * This replaces the JDK internal com.sun.jmx.remote.security.MBeanServerFileAccessController
 * which is not accessible in Java 9+ module system.
 *
 * Access file format: Each line contains:
 * <username> readonly|readwrite [create <classname>,<classname>,...] [unregister]
 */
public class MBeanServerFileAccessController implements MBeanServerForwarder {

  protected MBeanServer mbs;
  private final Map<String, Access> accessMap = new HashMap<>();

  /**
   * Access level enum
   */
  protected enum AccessLevel {
    READONLY, READWRITE
  }

  /**
   * Access configuration for a user
   */
  protected static class Access {
    final AccessLevel level;
    final Set<String> createClasses;
    final boolean unregister;

    Access(AccessLevel level, Set<String> createClasses, boolean unregister) {
      this.level = level;
      this.createClasses = createClasses;
      this.unregister = unregister;
    }
  }

  public MBeanServerFileAccessController(String accessFileName) throws IOException {
    this(loadAccessFile(accessFileName));
  }

  public MBeanServerFileAccessController(Properties accessProps) throws IOException {
    parseAccessFile(accessProps);
  }

  private static Properties loadAccessFile(String accessFileName) throws IOException {
    Properties props = new Properties();
    try (FileInputStream fis = new FileInputStream(accessFileName)) {
      props.load(fis);
    }
    return props;
  }

  private void parseAccessFile(Properties props) throws IOException {
    for (String username : props.stringPropertyNames()) {
      String value = props.getProperty(username);
      accessMap.put(username, parseAccessEntry(value));
    }
  }

  private Access parseAccessEntry(String entry) throws IOException {
    String[] tokens = entry.trim().split("\\s+");
    if (tokens.length == 0) {
      throw new IOException("Invalid access entry: " + entry);
    }

    AccessLevel level;
    if ("readonly".equalsIgnoreCase(tokens[0])) {
      level = AccessLevel.READONLY;
    } else if ("readwrite".equalsIgnoreCase(tokens[0])) {
      level = AccessLevel.READWRITE;
    } else {
      throw new IOException("Invalid access level: " + tokens[0]);
    }

    Set<String> createClasses = new HashSet<>();
    boolean unregister = false;

    for (int i = 1; i < tokens.length; i++) {
      if ("create".equalsIgnoreCase(tokens[i])) {
        if (i + 1 < tokens.length) {
          String[] classes = tokens[++i].split(",");
          for (String cls : classes) {
            createClasses.add(cls.trim());
          }
        }
      } else if ("unregister".equalsIgnoreCase(tokens[i])) {
        unregister = true;
      }
    }

    return new Access(level, createClasses, unregister);
  }

  // Suppress deprecation warning for Subject.getSubject() and AccessController.getContext()
  // These APIs are marked for removal in Java 17+ but are still necessary for JMX security context
  // No suitable replacement exists for accessing the current Subject in MBeanServerForwarder
  @SuppressWarnings("removal")
  protected Access getAccess() {
    Subject subject = Subject.getSubject(AccessController.getContext());
    if (subject == null) {
      return new Access(AccessLevel.READONLY, new HashSet<>(), false);
    }

    Set<?> principals = subject.getPrincipals();
    for (Object principal : principals) {
      String username = principal.toString();
      Access access = accessMap.get(username);
      if (access != null) {
        return access;
      }
    }

    return new Access(AccessLevel.READONLY, new HashSet<>(), false);
  }

  protected void checkRead() {
    // Always allowed
  }

  protected void checkWrite() {
    Access access = getAccess();
    if (access.level != AccessLevel.READWRITE) {
      throw new SecurityException("Access denied: read-only access");
    }
  }

  protected void checkCreate(String className) {
    Access access = getAccess();
    if (access.level != AccessLevel.READWRITE) {
      throw new SecurityException("Access denied: read-only access");
    }
    if (!access.createClasses.isEmpty() && !access.createClasses.contains(className)) {
      throw new SecurityException("Access denied: cannot create " + className);
    }
  }

  protected void checkUnregister() {
    Access access = getAccess();
    if (access.level != AccessLevel.READWRITE) {
      throw new SecurityException("Access denied: read-only access");
    }
    if (!access.unregister) {
      throw new SecurityException("Access denied: cannot unregister MBeans");
    }
  }

  @Override
  public MBeanServer getMBeanServer() {
    return mbs;
  }

  @Override
  public void setMBeanServer(MBeanServer mbs) {
    this.mbs = mbs;
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException,
      MBeanException, NotCompliantMBeanException {
    checkCreate(className);
    return mbs.createMBean(className, name);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException,
      MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
    checkCreate(className);
    return mbs.createMBean(className, name, loaderName);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, Object[] params,
      String[] signature) throws ReflectionException, InstanceAlreadyExistsException,
      MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
    checkCreate(className);
    return mbs.createMBean(className, name, params, signature);
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName,
      Object[] params, String[] signature)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException,
      MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
    checkCreate(className);
    return mbs.createMBean(className, name, loaderName, params, signature);
  }

  @Override
  public void unregisterMBean(ObjectName name)
      throws InstanceNotFoundException, MBeanRegistrationException {
    checkUnregister();
    mbs.unregisterMBean(name);
  }

  @Override
  public void setAttribute(ObjectName name, Attribute attribute)
      throws InstanceNotFoundException, AttributeNotFoundException, InvalidAttributeValueException,
      MBeanException, ReflectionException {
    checkWrite();
    mbs.setAttribute(name, attribute);
  }

  @Override
  public AttributeList setAttributes(ObjectName name, AttributeList attributes)
      throws InstanceNotFoundException, ReflectionException {
    checkWrite();
    return mbs.setAttributes(name, attributes);
  }

  @Override
  public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature)
      throws InstanceNotFoundException, MBeanException, ReflectionException {
    checkWrite();
    return mbs.invoke(name, operationName, params, signature);
  }

  // Read operations - no access check needed

  @Override
  public ObjectInstance registerMBean(Object object, ObjectName name)
      throws InstanceAlreadyExistsException, MBeanRegistrationException,
      NotCompliantMBeanException {
    checkWrite();
    return mbs.registerMBean(object, name);
  }

  @Override
  public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {
    checkRead();
    return mbs.getObjectInstance(name);
  }

  @Override
  public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) {
    checkRead();
    return mbs.queryMBeans(name, query);
  }

  @Override
  public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
    checkRead();
    return mbs.queryNames(name, query);
  }

  @Override
  public boolean isRegistered(ObjectName name) {
    checkRead();
    return mbs.isRegistered(name);
  }

  @Override
  public Integer getMBeanCount() {
    checkRead();
    return mbs.getMBeanCount();
  }

  @Override
  public Object getAttribute(ObjectName name, String attribute)
      throws MBeanException, AttributeNotFoundException, InstanceNotFoundException,
      ReflectionException {
    checkRead();
    return mbs.getAttribute(name, attribute);
  }

  @Override
  public AttributeList getAttributes(ObjectName name, String[] attributes)
      throws InstanceNotFoundException, ReflectionException {
    checkRead();
    return mbs.getAttributes(name, attributes);
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
    checkRead();
    return mbs.getMBeanInfo(name);
  }

  @Override
  public boolean isInstanceOf(ObjectName name, String className) throws InstanceNotFoundException {
    checkRead();
    return mbs.isInstanceOf(name, className);
  }

  @Override
  public Object instantiate(String className) throws ReflectionException, MBeanException {
    checkCreate(className);
    return mbs.instantiate(className);
  }

  @Override
  public Object instantiate(String className, ObjectName loaderName)
      throws ReflectionException, MBeanException, InstanceNotFoundException {
    checkCreate(className);
    return mbs.instantiate(className, loaderName);
  }

  @Override
  public Object instantiate(String className, Object[] params, String[] signature)
      throws ReflectionException, MBeanException {
    checkCreate(className);
    return mbs.instantiate(className, params, signature);
  }

  @Override
  public Object instantiate(String className, ObjectName loaderName, Object[] params,
      String[] signature) throws ReflectionException, MBeanException, InstanceNotFoundException {
    checkCreate(className);
    return mbs.instantiate(className, loaderName, params, signature);
  }

  @Override
  @Deprecated
  public ObjectInputStream deserialize(ObjectName name, byte[] data)
      throws InstanceNotFoundException, OperationsException {
    return mbs.deserialize(name, data);
  }

  @Override
  @Deprecated
  public ObjectInputStream deserialize(String className, byte[] data)
      throws OperationsException, ReflectionException {
    return mbs.deserialize(className, data);
  }

  @Override
  @Deprecated
  public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data)
      throws InstanceNotFoundException, OperationsException, ReflectionException {
    return mbs.deserialize(className, loaderName, data);
  }

  @Override
  public ClassLoaderRepository getClassLoaderRepository() {
    return mbs.getClassLoaderRepository();
  }

  @Override
  public ClassLoader getClassLoaderFor(ObjectName mbeanName) throws InstanceNotFoundException {
    return mbs.getClassLoaderFor(mbeanName);
  }

  @Override
  public ClassLoader getClassLoader(ObjectName loaderName) throws InstanceNotFoundException {
    return mbs.getClassLoader(loaderName);
  }
}
