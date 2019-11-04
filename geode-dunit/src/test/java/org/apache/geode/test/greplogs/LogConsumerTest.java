/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.test.greplogs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class LogConsumerTest {

  @Rule
  public TestName testName = new TestName();

  private LogConsumer logConsumer;

  @Before
  public void setUp() {
    boolean allowSkipLogMessages = false;
    List<Pattern> expectedStrings = Collections.emptyList();
    String logFileName = getClass().getSimpleName() + "_" + testName.getMethodName();
    int repeatLimit = 2;

    logConsumer = new LogConsumer(allowSkipLogMessages, expectedStrings, logFileName, repeatLimit);
  }

  @Test
  public void consumeReturnsNullIfLineIsOk() {
    StringBuilder value = logConsumer.consume("ok");

    assertThat(value).isNull();
  }

  @Test
  public void consumeReturnsNullIfLineIsEmpty() {
    StringBuilder value = logConsumer.consume("");

    assertThat(value).isNull();
  }

  @Test
  public void consumeThrowsNullPointerExceptionIfLineIsNull() {
    Throwable thrown = catchThrowable(() -> logConsumer.consume(null));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void closeReturnsNullIfLineIsOk() {
    logConsumer.consume("ok");

    StringBuilder value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void closeReturnsNullIfLineIsEmpty() {
    logConsumer.consume("");

    StringBuilder value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void closeReturnsNullIfLineContainsInfoLogStatementWithException() {
    logConsumer.consume("[info 019/06/13 14:41:05.750 PDT <main> tid=0x1] " +
        NullPointerException.class.getName());

    StringBuilder value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void closeReturnsLineIfLineContainsErrorLogStatement() {
    String line = "[error 019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    StringBuilder value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void closeReturnsNullIfLineContainsWarningLogStatement() {
    logConsumer.consume("[warning 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message");

    StringBuilder value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void closeReturnsLineIfLineContainsFatalLogStatement() {
    String line = "[fatal 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    StringBuilder value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void closeReturnsLineIfLineContainsSevereLogStatement() {
    String line = "[severe 2019/06/13 14:41:05.750 PDT <main> tid=0x1] message";
    logConsumer.consume(line);

    StringBuilder value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void closeReturnsLineIfLineContainsMalformedLog4jStatement() {
    String line = "[info 2019/06/13 14:41:05.750 PDT <main> tid=0x1] contains {}";
    logConsumer.consume(line);

    StringBuilder value = logConsumer.close();

    assertThat(value).contains(line);
  }

  @Test
  public void closeReturnsNullIfLineContainsHydraMasterLocatorsWildcard() {
    String line = "hydra.MasterDescription.master.locators={}";
    logConsumer.consume(line);

    StringBuilder value = logConsumer.close();

    assertThat(value).isNull();
  }

  @Test
  public void closeReturnsLineIfLineContains_CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION() {
    logConsumer.consume(CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION);

    StringBuilder value = logConsumer.close();

    assertThat(value).contains(CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION);
  }

  private static final String CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION =
      "[error 2019/11/04 13:09:31.730 PST <RMI TCP Connection(1)-127.0.0.1> tid=0x13] Context initialization failed\n"
          + "org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'managementControllerAdvice' defined in file [/Users/klund/dev/gemfire/geode/geode-cq/dunit/locator/GemFire_klund/services/http/0.0.0.0_7070_management_424997f1/webapp/WEB-INF/classes/org/apache/geode/management/internal/rest/controllers/ManagementControllerAdvice.class]: Instantiation of bean failed; nested exception is java.lang.NoClassDefFoundError: org/apache/geode/logging/internal/log4j/api/LogService\n"
          + "        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.instantiateBean(AbstractAutowireCapableBeanFactory.java:1159)\n"
          + "        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBeanInstance(AbstractAutowireCapableBeanFactory.java:1103)\n"
          + "        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:511)\n"
          + "        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:481)\n"
          + "        at org.springframework.beans.factory.support.AbstractBeanFactory$1.getObject(AbstractBeanFactory.java:312)\n"
          + "        at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:230)\n"
          + "        at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:308)\n"
          + "        at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:197)\n"
          + "        at org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingletons(DefaultListableBeanFactory.java:764)\n"
          + "        at org.springframework.context.support.AbstractApplicationContext.finishBeanFactoryInitialization(AbstractApplicationContext.java:867)\n"
          + "        at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:542)\n"
          + "        at org.springframework.web.servlet.FrameworkServlet.configureAndRefreshWebApplicationContext(FrameworkServlet.java:668)\n"
          + "        at org.springframework.web.servlet.FrameworkServlet.createWebApplicationContext(FrameworkServlet.java:634)\n"
          + "        at org.springframework.web.servlet.FrameworkServlet.createWebApplicationContext(FrameworkServlet.java:682)\n"
          + "        at org.springframework.web.servlet.FrameworkServlet.initWebApplicationContext(FrameworkServlet.java:553)\n"
          + "        at org.springframework.web.servlet.FrameworkServlet.initServletBean(FrameworkServlet.java:494)\n"
          + "        at org.springframework.web.servlet.HttpServletBean.init(HttpServletBean.java:171)\n"
          + "        at javax.servlet.GenericServlet.init(GenericServlet.java:244)\n"
          + "        at org.eclipse.jetty.servlet.ServletHolder.initServlet(ServletHolder.java:599)\n"
          + "        at org.eclipse.jetty.servlet.ServletHolder.initialize(ServletHolder.java:425)\n"
          + "        at org.eclipse.jetty.servlet.ServletHandler.lambda$initialize$0(ServletHandler.java:751)\n"
          + "        at java.util.stream.SortedOps$SizedRefSortingSink.end(SortedOps.java:352)\n"
          + "        at java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:482)\n"
          + "        at java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:471)\n"
          + "        at java.util.stream.StreamSpliterators$WrappingSpliterator.forEachRemaining(StreamSpliterators.java:312)\n"
          + "        at java.util.stream.Streams$ConcatSpliterator.forEachRemaining(Streams.java:743)\n"
          + "        at java.util.stream.Streams$ConcatSpliterator.forEachRemaining(Streams.java:742)\n"
          + "        at java.util.stream.ReferencePipeline$Head.forEach(ReferencePipeline.java:580)\n"
          + "        at org.eclipse.jetty.servlet.ServletHandler.initialize(ServletHandler.java:744)\n"
          + "        at org.eclipse.jetty.servlet.ServletContextHandler.startContext(ServletContextHandler.java:361)\n"
          + "        at org.eclipse.jetty.webapp.WebAppContext.startWebapp(WebAppContext.java:1443)\n"
          + "        at org.eclipse.jetty.webapp.WebAppContext.startContext(WebAppContext.java:1407)\n"
          + "        at org.eclipse.jetty.server.handler.ContextHandler.doStart(ContextHandler.java:821)\n"
          + "        at org.eclipse.jetty.servlet.ServletContextHandler.doStart(ServletContextHandler.java:276)\n"
          + "        at org.eclipse.jetty.webapp.WebAppContext.doStart(WebAppContext.java:524)\n"
          + "        at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)\n"
          + "        at org.eclipse.jetty.util.component.ContainerLifeCycle.start(ContainerLifeCycle.java:169)\n"
          + "        at org.eclipse.jetty.util.component.ContainerLifeCycle.doStart(ContainerLifeCycle.java:117)\n"
          + "        at org.eclipse.jetty.server.handler.AbstractHandler.doStart(AbstractHandler.java:106)\n"
          + "        at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)\n"
          + "        at org.eclipse.jetty.util.component.ContainerLifeCycle.start(ContainerLifeCycle.java:169)\n"
          + "        at org.eclipse.jetty.server.Server.start(Server.java:407)\n"
          + "        at org.eclipse.jetty.util.component.ContainerLifeCycle.doStart(ContainerLifeCycle.java:110)\n"
          + "        at org.eclipse.jetty.server.handler.AbstractHandler.doStart(AbstractHandler.java:106)\n"
          + "        at org.eclipse.jetty.server.Server.doStart(Server.java:371)\n"
          + "        at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)\n"
          + "        at org.apache.geode.internal.cache.InternalHttpService.addWebApplication(InternalHttpService.java:201)\n"
          + "        at org.apache.geode.distributed.internal.InternalLocator.lambda$startClusterManagementService$1(InternalLocator.java:776)\n"
          + "        at java.util.Optional.ifPresent(Optional.java:159)\n"
          + "        at org.apache.geode.distributed.internal.InternalLocator.startClusterManagementService(InternalLocator.java:772)\n"
          + "        at org.apache.geode.distributed.internal.InternalLocator.startCache(InternalLocator.java:735)\n"
          + "        at org.apache.geode.distributed.internal.InternalLocator.startDistributedSystem(InternalLocator.java:714)\n"
          + "        at org.apache.geode.distributed.internal.InternalLocator.startLocator(InternalLocator.java:378)\n"
          + "        at org.apache.geode.distributed.internal.InternalLocator.startLocator(InternalLocator.java:328)\n"
          + "        at org.apache.geode.distributed.Locator.startLocator(Locator.java:252)\n"
          + "        at org.apache.geode.distributed.Locator.startLocatorAndDS(Locator.java:139)\n"
          + "        at org.apache.geode.test.dunit.internal.DUnitLauncher$1.call(DUnitLauncher.java:304)\n"
          + "        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n"
          + "        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\n"
          + "        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n"
          + "        at java.lang.reflect.Method.invoke(Method.java:498)\n"
          + "        at org.apache.geode.test.dunit.internal.MethodInvoker.executeObject(MethodInvoker.java:123)\n"
          + "        at org.apache.geode.test.dunit.internal.MethodInvoker.executeObject(MethodInvoker.java:92)\n"
          + "        at org.apache.geode.test.dunit.internal.RemoteDUnitVM.executeMethodOnObject(RemoteDUnitVM.java:45)\n"
          + "        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n"
          + "        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\n"
          + "        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n"
          + "        at java.lang.reflect.Method.invoke(Method.java:498)\n"
          + "        at sun.rmi.server.UnicastServerRef.dispatch(UnicastServerRef.java:357)\n"
          + "        at sun.rmi.transport.Transport$1.run(Transport.java:200)\n"
          + "        at sun.rmi.transport.Transport$1.run(Transport.java:197)\n"
          + "        at java.security.AccessController.doPrivileged(Native Method)\n"
          + "        at sun.rmi.transport.Transport.serviceCall(Transport.java:196)\n"
          + "        at sun.rmi.transport.tcp.TCPTransport.handleMessages(TCPTransport.java:573)\n"
          + "        at sun.rmi.transport.tcp.TCPTransport$ConnectionHandler.run0(TCPTransport.java:834)\n"
          + "        at sun.rmi.transport.tcp.TCPTransport$ConnectionHandler.lambda$run$0(TCPTransport.java:688)\n"
          + "        at java.security.AccessController.doPrivileged(Native Method)\n"
          + "        at sun.rmi.transport.tcp.TCPTransport$ConnectionHandler.run(TCPTransport.java:687)\n"
          + "        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\n"
          + "        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)\n"
          + "        at java.lang.Thread.run(Thread.java:748)\n"
          + "Caused by: java.lang.NoClassDefFoundError: org/apache/geode/logging/internal/log4j/api/LogService\n"
          + "        at org.apache.geode.management.internal.rest.controllers.ManagementControllerAdvice.<clinit>(ManagementControllerAdvice.java:54)\n"
          + "        at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)\n"
          + "        at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)\n"
          + "        at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)\n"
          + "        at java.lang.reflect.Constructor.newInstance(Constructor.java:423)\n"
          + "        at org.springframework.beans.BeanUtils.instantiateClass(BeanUtils.java:142)\n"
          + "        at org.springframework.beans.factory.support.SimpleInstantiationStrategy.instantiate(SimpleInstantiationStrategy.java:89)\n"
          + "        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.instantiateBean(AbstractAutowireCapableBeanFactory.java:1151)\n"
          + "        ... 80 more\n"
          + "Caused by: java.lang.ClassNotFoundException: org.apache.geode.logging.internal.log4j.api.LogService\n"
          + "        at java.net.URLClassLoader.findClass(URLClassLoader.java:382)\n"
          + "        at java.lang.ClassLoader.loadClass(ClassLoader.java:424)\n"
          + "        at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:349)\n"
          + "        at java.lang.ClassLoader.loadClass(ClassLoader.java:357)\n"
          + "        at org.eclipse.jetty.webapp.WebAppClassLoader.loadClass(WebAppClassLoader.java:543)\n"
          + "        at java.lang.ClassLoader.loadClass(ClassLoader.java:357)\n"
          + "        ... 88 more\n";
}
