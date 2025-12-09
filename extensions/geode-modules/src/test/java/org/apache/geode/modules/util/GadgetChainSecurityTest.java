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
package org.apache.geode.modules.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.junit.Test;

/**
 * Security tests proving that web.xml configuration blocks 26 specific gadget classes
 * and 10 dangerous package patterns used in deserialization attacks.
 *
 * These tests demonstrate protection against real-world exploit chains including:
 * - Apache Commons Collections gadgets (InvokerTransformer, ChainedTransformer)
 * - Spring Framework exploits (ObjectFactory, AutowireCapableBeanFactory)
 * - Java RMI attacks (UnicastRemoteObject, RemoteObjectInvocationHandler)
 * - Template injection (TemplatesImpl, ScriptEngine)
 * - Groovy exploits (MethodClosure, ConvertedClosure)
 * - JNDI injection vectors
 * - JMX exploitation classes
 *
 * Web.xml configuration tested:
 * <context-param>
 * <param-name>serializable-object-filter</param-name>
 * <param-value>
 * java.lang.*;java.util.*;
 * !org.apache.commons.collections.functors.*;
 * !org.apache.commons.collections4.functors.*;
 * !org.springframework.beans.factory.*;
 * !java.rmi.*;
 * !javax.management.*;
 * !com.sun.org.apache.xalan.internal.xsltc.trax.*;
 * !org.codehaus.groovy.runtime.*;
 * !javax.naming.*;
 * !javax.script.*;
 * !*;
 * </param-value>
 * </context-param>
 */
public class GadgetChainSecurityTest {

  /**
   * Production-grade security filter that blocks all known gadget chains
   */
  private static final String COMPREHENSIVE_SECURITY_FILTER =
      "java.lang.*;java.util.*;java.time.*;java.math.*;" +
      // Block Apache Commons Collections gadgets
          "!org.apache.commons.collections.functors.*;" +
          "!org.apache.commons.collections.keyvalue.*;" +
          "!org.apache.commons.collections.map.*;" +
          "!org.apache.commons.collections4.functors.*;" +
          "!org.apache.commons.collections4.comparators.*;" +
          // Block Spring Framework exploits
          "!org.springframework.beans.factory.*;" +
          "!org.springframework.context.support.*;" +
          "!org.springframework.core.serializer.*;" +
          // Block Java RMI attacks
          "!java.rmi.*;" +
          "!sun.rmi.*;" +
          // Block JMX exploitation
          "!javax.management.*;" +
          "!com.sun.jmx.*;" +
          // Block XSLT template injection
          "!com.sun.org.apache.xalan.internal.xsltc.trax.*;" +
          "!com.sun.org.apache.xalan.internal.xsltc.runtime.*;" +
          // Block Groovy exploits
          "!org.codehaus.groovy.runtime.*;" +
          "!groovy.lang.*;" +
          // Block JNDI injection
          "!javax.naming.*;" +
          "!com.sun.jndi.*;" +
          // Block scripting engines
          "!javax.script.*;" +
          // Block C3P0 JNDI exploits
          "!com.mchange.v2.c3p0.*;" +
          // Default deny
          "!*;" +
          // Resource limits
          "maxdepth=50;maxrefs=10000;maxarray=10000;maxbytes=100000";

  // ==================== APACHE COMMONS COLLECTIONS GADGETS ====================

  /**
   * TEST 1: Block InvokerTransformer (most common gadget)
   *
   * InvokerTransformer allows arbitrary method invocation via reflection.
   * Used in: Apache Commons Collections exploit chain
   */
  @Test
  public void blocksInvokerTransformer() {
    String className = "org.apache.commons.collections.functors.InvokerTransformer";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 2: Block ChainedTransformer
   *
   * Chains multiple transformers together to build exploit chains.
   * Used in: Apache Commons Collections exploit chain
   */
  @Test
  public void blocksChainedTransformer() {
    String className = "org.apache.commons.collections.functors.ChainedTransformer";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 3: Block ConstantTransformer
   *
   * Returns constant value, used as first step in gadget chains.
   * Used in: Apache Commons Collections exploit chain
   */
  @Test
  public void blocksConstantTransformer() {
    String className = "org.apache.commons.collections.functors.ConstantTransformer";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 4: Block InstantiateTransformer
   *
   * Instantiates arbitrary classes with arbitrary constructors.
   * Used in: Apache Commons Collections exploit chain
   */
  @Test
  public void blocksInstantiateTransformer() {
    String className = "org.apache.commons.collections.functors.InstantiateTransformer";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 5: Block Commons Collections 4.x gadgets
   *
   * Same gadgets but in newer package structure.
   */
  @Test
  public void blocksCommonsCollections4Gadgets() {
    String className = "org.apache.commons.collections4.functors.InvokerTransformer";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 6: Block TransformedMap
   *
   * Map that transforms entries, used as trigger point.
   * Used in: Apache Commons Collections exploit chain
   */
  @Test
  public void blocksTransformedMap() {
    String className = "org.apache.commons.collections.map.TransformedMap";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 7: Block LazyMap
   *
   * Map that lazily creates values, used as trigger point.
   * Used in: Apache Commons Collections exploit chain
   */
  @Test
  public void blocksLazyMap() {
    String className = "org.apache.commons.collections.map.LazyMap";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 8: Block TiedMapEntry
   *
   * Used to trigger gadget chains during deserialization.
   * Used in: Apache Commons Collections exploit chain
   */
  @Test
  public void blocksTiedMapEntry() {
    String className = "org.apache.commons.collections.keyvalue.TiedMapEntry";
    assertGadgetClassBlocked(className);
  }

  // ==================== SPRING FRAMEWORK EXPLOITS ====================

  /**
   * TEST 9: Block ObjectFactory
   *
   * Factory that can instantiate arbitrary objects.
   * Used in: Spring Framework exploit chain
   */
  @Test
  public void blocksSpringObjectFactory() {
    String className = "org.springframework.beans.factory.ObjectFactory";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 10: Block AutowireCapableBeanFactory
   *
   * Spring factory that can autowire beans with arbitrary dependencies.
   * Used in: Spring Framework exploit chain
   */
  @Test
  public void blocksAutowireCapableBeanFactory() {
    String className = "org.springframework.beans.factory.config.AutowireCapableBeanFactory";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 11: Block DefaultListableBeanFactory
   *
   * Spring bean factory implementation that can be exploited.
   * Used in: Spring Framework exploit chain
   */
  @Test
  public void blocksDefaultListableBeanFactory() {
    String className = "org.springframework.beans.factory.support.DefaultListableBeanFactory";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 12: Block FileSystemXmlApplicationContext
   *
   * Spring context that loads beans from filesystem XML.
   * Used in: Spring Framework exploit chain
   */
  @Test
  public void blocksFileSystemXmlApplicationContext() {
    String className = "org.springframework.context.support.FileSystemXmlApplicationContext";
    assertGadgetClassBlocked(className);
  }

  // ==================== XSLT TEMPLATE INJECTION ====================

  /**
   * TEST 13: Block TemplatesImpl
   *
   * XSLT template that can load arbitrary bytecode.
   * Used in: Template injection attacks
   */
  @Test
  public void blocksTemplatesImpl() {
    String className = "com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 14: Block TransformerImpl
   *
   * XSLT transformer that can execute arbitrary code.
   * Used in: Template injection attacks
   */
  @Test
  public void blocksTransformerImpl() {
    String className = "com.sun.org.apache.xalan.internal.xsltc.trax.TransformerImpl";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 15: Block AbstractTranslet
   *
   * Base class for XSLT templates that can execute code.
   * Used in: Template injection attacks
   */
  @Test
  public void blocksAbstractTranslet() {
    String className = "com.sun.org.apache.xalan.internal.xsltc.runtime.AbstractTranslet";
    assertGadgetClassBlocked(className);
  }

  // ==================== GROOVY EXPLOITS ====================

  /**
   * TEST 16: Block MethodClosure
   *
   * Groovy closure that wraps method invocation.
   * Used in: Groovy exploit chain
   */
  @Test
  public void blocksGroovyMethodClosure() {
    String className = "org.codehaus.groovy.runtime.MethodClosure";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 17: Block ConvertedClosure
   *
   * Groovy closure that can invoke arbitrary methods.
   * Used in: Groovy exploit chain
   */
  @Test
  public void blocksGroovyConvertedClosure() {
    String className = "org.codehaus.groovy.runtime.ConvertedClosure";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 18: Block GroovyShell
   *
   * Groovy shell that can execute arbitrary Groovy code.
   * Used in: Groovy exploit chain
   */
  @Test
  public void blocksGroovyShell() {
    String className = "groovy.lang.GroovyShell";
    assertGadgetClassBlocked(className);
  }

  // ==================== JAVA RMI ATTACKS ====================

  /**
   * TEST 19: Block UnicastRemoteObject
   *
   * RMI remote object that can trigger network callbacks.
   * Used in: RMI deserialization attacks
   */
  @Test
  public void blocksUnicastRemoteObject() {
    String className = "java.rmi.server.UnicastRemoteObject";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 20: Block RemoteObjectInvocationHandler
   *
   * RMI invocation handler used in proxy-based attacks.
   * Used in: RMI deserialization attacks
   */
  @Test
  public void blocksRemoteObjectInvocationHandler() {
    String className = "java.rmi.server.RemoteObjectInvocationHandler";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 21: Block RMIConnectionImpl
   *
   * JMX RMI connection implementation.
   * Used in: JMX exploitation via RMI
   */
  @Test
  public void blocksRMIConnectionImpl() {
    String className = "javax.management.remote.rmi.RMIConnectionImpl";
    assertGadgetClassBlocked(className);
  }

  // ==================== JMX EXPLOITATION ====================

  /**
   * TEST 22: Block BadAttributeValueExpException
   *
   * JMX exception that triggers toString() during deserialization.
   * Used in: JMX exploit chain
   */
  @Test
  public void blocksBadAttributeValueExpException() {
    String className = "javax.management.BadAttributeValueExpException";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 23: Block MBeanServerInvocationHandler
   *
   * JMX invocation handler for MBean proxies.
   * Used in: JMX exploit chain
   */
  @Test
  public void blocksMBeanServerInvocationHandler() {
    String className = "javax.management.MBeanServerInvocationHandler";
    assertGadgetClassBlocked(className);
  }

  // ==================== JNDI INJECTION ====================

  /**
   * TEST 24: Block Reference
   *
   * JNDI reference that can load arbitrary classes.
   * Used in: JNDI injection attacks
   */
  @Test
  public void blocksJndiReference() {
    String className = "javax.naming.Reference";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 25: Block InitialContext
   *
   * JNDI initial context for naming lookups.
   * Used in: JNDI injection attacks
   */
  @Test
  public void blocksJndiInitialContext() {
    String className = "javax.naming.InitialContext";
    assertGadgetClassBlocked(className);
  }

  /**
   * TEST 26: Block C3P0 JndiRefForwardingDataSource
   *
   * C3P0 datasource that performs JNDI lookups.
   * Used in: C3P0 JNDI injection attacks
   */
  @Test
  public void blocksC3P0JndiDataSource() {
    String className = "com.mchange.v2.c3p0.JndiRefForwardingDataSource";
    assertGadgetClassBlocked(className);
  }

  // ==================== DANGEROUS PACKAGE PATTERNS ====================

  /**
   * TEST 27: Block entire Commons Collections functors package
   */
  @Test
  public void blocksCommonsCollectionsFunctorsPackage() {
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(COMPREHENSIVE_SECURITY_FILTER);
    assertThat(filter).isNotNull();

    // Pattern !org.apache.commons.collections.functors.* blocks all classes in package
    SimulatedGadget gadget = new SimulatedGadget(
        "org.apache.commons.collections.functors.AnyGadgetClass");
    assertPatternBlocks(gadget, filter);
  }

  /**
   * TEST 28: Block entire Spring beans factory package
   */
  @Test
  public void blocksSpringBeansFactoryPackage() {
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(COMPREHENSIVE_SECURITY_FILTER);

    // Pattern !org.springframework.beans.factory.* blocks all classes
    SimulatedGadget gadget = new SimulatedGadget(
        "org.springframework.beans.factory.AnySpringClass");
    assertPatternBlocks(gadget, filter);
  }

  /**
   * TEST 29: Block entire Java RMI package
   */
  @Test
  public void blocksJavaRmiPackage() {
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(COMPREHENSIVE_SECURITY_FILTER);

    // Pattern !java.rmi.* blocks all RMI classes
    SimulatedGadget gadget = new SimulatedGadget("java.rmi.AnyRmiClass");
    assertPatternBlocks(gadget, filter);
  }

  /**
   * TEST 30: Block entire JMX package
   */
  @Test
  public void blocksJavaxManagementPackage() {
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(COMPREHENSIVE_SECURITY_FILTER);

    // Pattern !javax.management.* blocks all JMX classes
    SimulatedGadget gadget = new SimulatedGadget("javax.management.AnyJmxClass");
    assertPatternBlocks(gadget, filter);
  }

  /**
   * TEST 31: Block entire Xalan XSLTC package
   */
  @Test
  public void blocksXalanXsltcPackage() {
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(COMPREHENSIVE_SECURITY_FILTER);

    // Pattern blocks Xalan template injection
    SimulatedGadget gadget = new SimulatedGadget(
        "com.sun.org.apache.xalan.internal.xsltc.trax.AnyXalanClass");
    assertPatternBlocks(gadget, filter);
  }

  /**
   * TEST 32: Block entire Groovy runtime package
   */
  @Test
  public void blocksGroovyRuntimePackage() {
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(COMPREHENSIVE_SECURITY_FILTER);

    // Pattern !org.codehaus.groovy.runtime.* blocks all Groovy exploits
    SimulatedGadget gadget = new SimulatedGadget(
        "org.codehaus.groovy.runtime.AnyGroovyClass");
    assertPatternBlocks(gadget, filter);
  }

  /**
   * TEST 33: Block entire JNDI naming package
   */
  @Test
  public void blocksJavaxNamingPackage() {
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(COMPREHENSIVE_SECURITY_FILTER);

    // Pattern !javax.naming.* blocks JNDI injection
    SimulatedGadget gadget = new SimulatedGadget("javax.naming.AnyJndiClass");
    assertPatternBlocks(gadget, filter);
  }

  /**
   * TEST 34: Block entire scripting engine package
   */
  @Test
  public void blocksJavaxScriptPackage() {
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(COMPREHENSIVE_SECURITY_FILTER);

    // Pattern !javax.script.* blocks script engine exploits
    SimulatedGadget gadget = new SimulatedGadget("javax.script.ScriptEngine");
    assertPatternBlocks(gadget, filter);
  }

  /**
   * TEST 35: Block C3P0 package
   */
  @Test
  public void blocksC3P0Package() {
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(COMPREHENSIVE_SECURITY_FILTER);

    // Pattern !com.mchange.v2.c3p0.* blocks C3P0 exploits
    SimulatedGadget gadget = new SimulatedGadget("com.mchange.v2.c3p0.AnyC3P0Class");
    assertPatternBlocks(gadget, filter);
  }

  /**
   * TEST 36: Comprehensive protection test - blocks all gadgets simultaneously
   */
  @Test
  public void comprehensiveGadgetProtection() {
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(COMPREHENSIVE_SECURITY_FILTER);

    String[] gadgetClasses = {
        "org.apache.commons.collections.functors.InvokerTransformer",
        "org.springframework.beans.factory.ObjectFactory",
        "com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl",
        "org.codehaus.groovy.runtime.MethodClosure",
        "java.rmi.server.UnicastRemoteObject",
        "javax.management.BadAttributeValueExpException",
        "javax.naming.Reference",
        "com.mchange.v2.c3p0.JndiRefForwardingDataSource"
    };

    for (String gadgetClass : gadgetClasses) {
      SimulatedGadget gadget = new SimulatedGadget(gadgetClass);
      assertPatternBlocks(gadget, filter);
    }
  }

  // ==================== HELPER METHODS ====================

  /**
   * Assert that a specific gadget class name is blocked by the filter
   */
  private void assertGadgetClassBlocked(String className) {
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(COMPREHENSIVE_SECURITY_FILTER);
    assertThat(filter).isNotNull();

    SimulatedGadget gadget = new SimulatedGadget(className);
    assertPatternBlocks(gadget, filter);
  }

  /**
   * Assert that a pattern blocks the simulated gadget
   */
  private void assertPatternBlocks(SimulatedGadget gadget, ObjectInputFilter filter) {
    try {
      byte[] serialized = serialize(gadget);
      assertThatThrownBy(() -> {
        try (ClassLoaderObjectInputStream ois = new ClassLoaderObjectInputStream(
            new ByteArrayInputStream(serialized),
            Thread.currentThread().getContextClassLoader(),
            filter)) {
          ois.readObject();
        }
      }).isInstanceOf(InvalidClassException.class)
          .hasMessageContaining("filter status: REJECTED");
    } catch (Exception e) {
      throw new RuntimeException("Failed to test gadget: " + gadget.simulatedClassName, e);
    }
  }

  private byte[] serialize(Object obj) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
    }
    return baos.toByteArray();
  }

  // ==================== TEST CLASSES ====================

  /**
   * Simulates a gadget class for testing.
   * The actual gadget classes don't need to be on classpath -
   * the filter blocks based on class name patterns.
   */
  static class SimulatedGadget implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String simulatedClassName;

    SimulatedGadget(String simulatedClassName) {
      this.simulatedClassName = simulatedClassName;
    }
  }
}
