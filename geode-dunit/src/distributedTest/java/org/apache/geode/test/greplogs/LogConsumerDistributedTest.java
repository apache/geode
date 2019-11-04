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
package org.apache.geode.test.greplogs;

import static java.lang.System.lineSeparator;
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getLocator;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.greplogs.LogConsumerDistributedTest.TargetVM.ANY_VM;
import static org.apache.geode.test.greplogs.LogConsumerDistributedTest.TargetVM.CONTROLLER;
import static org.apache.geode.test.greplogs.LogConsumerDistributedTest.TargetVM.LOCATOR;
import static org.apache.geode.test.junit.runners.TestRunner.runTest;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Result;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class LogConsumerDistributedTest implements Serializable {

  private static final Logger logger = LogService.getLogger();

  private static final String message = "just a message";

  private static volatile TargetVM targetVM;
  private static volatile SerializableRunnableIF task;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @After
  public void tearDown() {
    for (VM vm : toArray(getAllVMs(), getController(), getLocator())) {
      vm.invoke(() -> {
        targetVM = null;
        task = null;
      });
    }
  }

  @Test
  public void traceLogMessagePasses() {
    given(CONTROLLER, () -> logger.trace(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  @Test
  public void debugLogMessagePasses() {
    given(CONTROLLER, () -> logger.debug(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  @Test
  public void infoLogMessagePasses() {
    given(CONTROLLER, () -> logger.info(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  @Test
  public void warnLogMessagePasses() {
    given(CONTROLLER, () -> logger.warn(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  @Test
  public void errorLogMessageFails() {
    given(CONTROLLER, () -> logger.error(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isFalse();
  }

  @Test
  public void fatalLogMessageFails() {
    given(CONTROLLER, () -> logger.fatal(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isFalse();
  }

  @Test
  public void asyncErrorLogMessageFails() {
    given(CONTROLLER, () -> logger.error(message));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(result.wasSuccessful()).isFalse();
  }

  @Test
  public void asyncFatalLogMessageFails() {
    given(CONTROLLER, () -> logger.fatal(message));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(result.wasSuccessful()).isFalse();
  }

  @Test
  public void errorLogMessageIncludedInFailure() {
    given(CONTROLLER, () -> logger.error(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(message);
  }

  @Test
  public void fatalLogMessageIncludedInFailure() {
    given(CONTROLLER, () -> logger.fatal(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(message);
  }

  @Test
  public void errorLogMessageFailsInVm() {
    given(ANY_VM, () -> logger.error(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(message);
  }

  @Test
  public void fatalLogMessageFailsInVm() {
    given(ANY_VM, () -> logger.fatal(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(message);
  }

  @Test
  public void asyncErrorLogMessageFailsInVm() {
    given(ANY_VM, () -> logger.error(message));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(message);
  }

  @Test
  public void asyncFatalLogMessageFailsInVm() {
    given(ANY_VM, () -> logger.fatal(message));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(message);
  }

  @Test
  public void errorLogMessageFailsInController() {
    given(CONTROLLER, () -> logger.error(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(message);
  }

  @Test
  public void fatalLogMessageFailsInController() {
    given(CONTROLLER, () -> logger.fatal(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(message);
  }

  @Test
  public void asyncErrorLogMessageFailsInController() {
    given(CONTROLLER, () -> logger.error(message));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(message);
  }

  @Test
  public void asyncFatalLogMessageFailsInController() {
    given(CONTROLLER, () -> logger.fatal(message));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(message);
  }

  @Test
  public void errorLogMessageFailsInLocator() {
    given(LOCATOR, () -> logger.error(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(message);
  }

  @Test
  public void fatalLogMessageFailsInLocator() {
    given(LOCATOR, () -> logger.fatal(message));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(message);
  }

  @Test
  public void asyncErrorLogMessageFailsInLocator() {
    given(LOCATOR, () -> logger.error(message));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(message);
  }

  @Test
  public void asyncFatalLogMessageFailsInLocator() {
    given(LOCATOR, () -> logger.fatal(message));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(message);
  }

  @Test
  public void errorLogMessage_loggedInBefore_failsInLocator() {
    given(LOCATOR, () -> logger.error(CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION));

    Result result = runTest(ExecuteTaskBefore.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION);
  }

  @Test
  public void errorLogMessage_loggedInBeforeClass_failsInLocator() {
    given(LOCATOR, () -> logger.error(CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION));

    Result result = runTest(ExecuteTaskBeforeClass.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION);
  }

  /**
   * I was seeing dunit tests pass despite having this stack trace logged by the locator. This test
   * verifies that LogConsumer does match this stack trace with the dunit exclusions in place.
   */
  @Test
  public void CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION_failsInLocator() {
    given(LOCATOR, () -> logger.error(CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION);
  }

  private static void given(TargetVM targetVM, SerializableRunnableIF task) {
    for (VM vm : toArray(getAllVMs(), getController(), getLocator())) {
      vm.invoke(() -> {
        LogConsumerDistributedTest.targetVM = targetVM;
        LogConsumerDistributedTest.task = task;
      });
    }
  }

  private static Throwable getFailure(Result result) {
    assertThat(result.getFailures()).hasSize(1);
    return result.getFailures().get(0).getException();
  }

  private static VM getTargetVm() {
    switch (targetVM) {
      case CONTROLLER:
        return getController();
      case ANY_VM:
        return getVM(0);
      case LOCATOR:
        return getLocator();
      default:
        throw new IllegalStateException("VM for " + targetVM + " not found");
    }
  }

  enum TargetVM {
    CONTROLLER,
    LOCATOR,
    ANY_VM
  }

  public static class ExecuteTaskSync implements Serializable {

    @Rule
    public DistributedRule distributedRule = new DistributedRule();

    @Test
    public void invokeTaskInTargetVm() {
      getTargetVm().invoke(() -> task.run());
    }
  }

  public static class ExecuteTaskAsync implements Serializable {

    private transient AsyncInvocation<Void> asyncInvocation;

    @Rule
    public DistributedRule distributedRule = new DistributedRule();

    @After
    public void tearDown() throws Exception {
      asyncInvocation.await();
    }

    @Test
    public void invokeTaskInTargetVm() {
      asyncInvocation = getTargetVm().invokeAsync(() -> task.run());
    }
  }

  public static class ExecuteTaskBefore implements Serializable {

    @Rule
    public DistributedRule distributedRule = new DistributedRule();

    @Before
    public void invokeTaskInBefore() {
      getTargetVm().invokeAsync(() -> task.run());
    }

    @Test
    public void doNothing() {
      // nothing
    }
  }

  public static class ExecuteTaskBeforeClass implements Serializable {

    @BeforeClass
    public static void invokeTaskInBeforeClass() {
      getTargetVm().invokeAsync(() -> task.run());
    }

    @Rule
    public DistributedRule distributedRule = new DistributedRule();

    @Test
    public void doNothing() {
      // nothing
    }
  }

  private static final String CONTEXT_INITIALIZATION_FAILED_CLASSNOTFOUNDEXCEPTION =
      "[error 2019/11/04 13:09:31.730 PST <RMI TCP Connection(1)-127.0.0.1> tid=0x13] Context initialization failed"
          + lineSeparator()
          + "org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'managementControllerAdvice' defined in file [/Users/klund/dev/gemfire/geode/geode-cq/dunit/locator/GemFire_klund/services/http/0.0.0.0_7070_management_424997f1/webapp/WEB-INF/classes/org/apache/geode/management/internal/rest/controllers/ManagementControllerAdvice.class]: Instantiation of bean failed; nested exception is java.lang.NoClassDefFoundError: org/apache/geode/logging/internal/log4j/api/LogService"
          + lineSeparator()
          + "        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.instantiateBean(AbstractAutowireCapableBeanFactory.java:1159)"
          + lineSeparator()
          + "        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBeanInstance(AbstractAutowireCapableBeanFactory.java:1103)"
          + lineSeparator()
          + "        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:511)"
          + lineSeparator()
          + "        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:481)"
          + lineSeparator()
          + "        at org.springframework.beans.factory.support.AbstractBeanFactory$1.getObject(AbstractBeanFactory.java:312)"
          + lineSeparator()
          + "        at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:230)"
          + lineSeparator()
          + "        at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:308)"
          + lineSeparator()
          + "        at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:197)"
          + lineSeparator()
          + "        at org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingletons(DefaultListableBeanFactory.java:764)"
          + lineSeparator()
          + "        at org.springframework.context.support.AbstractApplicationContext.finishBeanFactoryInitialization(AbstractApplicationContext.java:867)"
          + lineSeparator()
          + "        at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:542)"
          + lineSeparator()
          + "        at org.springframework.web.servlet.FrameworkServlet.configureAndRefreshWebApplicationContext(FrameworkServlet.java:668)"
          + lineSeparator()
          + "        at org.springframework.web.servlet.FrameworkServlet.createWebApplicationContext(FrameworkServlet.java:634)"
          + lineSeparator()
          + "        at org.springframework.web.servlet.FrameworkServlet.createWebApplicationContext(FrameworkServlet.java:682)"
          + lineSeparator()
          + "        at org.springframework.web.servlet.FrameworkServlet.initWebApplicationContext(FrameworkServlet.java:553)"
          + lineSeparator()
          + "        at org.springframework.web.servlet.FrameworkServlet.initServletBean(FrameworkServlet.java:494)"
          + lineSeparator()
          + "        at org.springframework.web.servlet.HttpServletBean.init(HttpServletBean.java:171)"
          + lineSeparator()
          + "        at javax.servlet.GenericServlet.init(GenericServlet.java:244)"
          + lineSeparator()
          + "        at org.eclipse.jetty.servlet.ServletHolder.initServlet(ServletHolder.java:599)"
          + lineSeparator()
          + "        at org.eclipse.jetty.servlet.ServletHolder.initialize(ServletHolder.java:425)"
          + lineSeparator()
          + "        at org.eclipse.jetty.servlet.ServletHandler.lambda$initialize$0(ServletHandler.java:751)"
          + lineSeparator()
          + "        at java.util.stream.SortedOps$SizedRefSortingSink.end(SortedOps.java:352)"
          + lineSeparator()
          + "        at java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:482)"
          + lineSeparator()
          + "        at java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:471)"
          + lineSeparator()
          + "        at java.util.stream.StreamSpliterators$WrappingSpliterator.forEachRemaining(StreamSpliterators.java:312)"
          + lineSeparator()
          + "        at java.util.stream.Streams$ConcatSpliterator.forEachRemaining(Streams.java:743)"
          + lineSeparator()
          + "        at java.util.stream.Streams$ConcatSpliterator.forEachRemaining(Streams.java:742)"
          + lineSeparator()
          + "        at java.util.stream.ReferencePipeline$Head.forEach(ReferencePipeline.java:580)"
          + lineSeparator()
          + "        at org.eclipse.jetty.servlet.ServletHandler.initialize(ServletHandler.java:744)"
          + lineSeparator()
          + "        at org.eclipse.jetty.servlet.ServletContextHandler.startContext(ServletContextHandler.java:361)"
          + lineSeparator()
          + "        at org.eclipse.jetty.webapp.WebAppContext.startWebapp(WebAppContext.java:1443)"
          + lineSeparator()
          + "        at org.eclipse.jetty.webapp.WebAppContext.startContext(WebAppContext.java:1407)"
          + lineSeparator()
          + "        at org.eclipse.jetty.server.handler.ContextHandler.doStart(ContextHandler.java:821)"
          + lineSeparator()
          + "        at org.eclipse.jetty.servlet.ServletContextHandler.doStart(ServletContextHandler.java:276)"
          + lineSeparator()
          + "        at org.eclipse.jetty.webapp.WebAppContext.doStart(WebAppContext.java:524)"
          + lineSeparator()
          + "        at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)"
          + lineSeparator()
          + "        at org.eclipse.jetty.util.component.ContainerLifeCycle.start(ContainerLifeCycle.java:169)"
          + lineSeparator()
          + "        at org.eclipse.jetty.util.component.ContainerLifeCycle.doStart(ContainerLifeCycle.java:117)"
          + lineSeparator()
          + "        at org.eclipse.jetty.server.handler.AbstractHandler.doStart(AbstractHandler.java:106)"
          + lineSeparator()
          + "        at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)"
          + lineSeparator()
          + "        at org.eclipse.jetty.util.component.ContainerLifeCycle.start(ContainerLifeCycle.java:169)"
          + lineSeparator()
          + "        at org.eclipse.jetty.server.Server.start(Server.java:407)" + lineSeparator()
          + "        at org.eclipse.jetty.util.component.ContainerLifeCycle.doStart(ContainerLifeCycle.java:110)"
          + lineSeparator()
          + "        at org.eclipse.jetty.server.handler.AbstractHandler.doStart(AbstractHandler.java:106)"
          + lineSeparator()
          + "        at org.eclipse.jetty.server.Server.doStart(Server.java:371)" + lineSeparator()
          + "        at org.eclipse.jetty.util.component.AbstractLifeCycle.start(AbstractLifeCycle.java:72)"
          + lineSeparator()
          + "        at org.apache.geode.internal.cache.InternalHttpService.addWebApplication(InternalHttpService.java:201)"
          + lineSeparator()
          + "        at org.apache.geode.distributed.internal.InternalLocator.lambda$startClusterManagementService$1(InternalLocator.java:776)"
          + lineSeparator()
          + "        at java.util.Optional.ifPresent(Optional.java:159)" + lineSeparator()
          + "        at org.apache.geode.distributed.internal.InternalLocator.startClusterManagementService(InternalLocator.java:772)"
          + lineSeparator()
          + "        at org.apache.geode.distributed.internal.InternalLocator.startCache(InternalLocator.java:735)"
          + lineSeparator()
          + "        at org.apache.geode.distributed.internal.InternalLocator.startDistributedSystem(InternalLocator.java:714)"
          + lineSeparator()
          + "        at org.apache.geode.distributed.internal.InternalLocator.startLocator(InternalLocator.java:378)"
          + lineSeparator()
          + "        at org.apache.geode.distributed.internal.InternalLocator.startLocator(InternalLocator.java:328)"
          + lineSeparator()
          + "        at org.apache.geode.distributed.Locator.startLocator(Locator.java:252)"
          + lineSeparator()
          + "        at org.apache.geode.distributed.Locator.startLocatorAndDS(Locator.java:139)"
          + lineSeparator()
          + "        at org.apache.geode.test.dunit.internal.DUnitLauncher$1.call(DUnitLauncher.java:304)"
          + lineSeparator()
          + "        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)"
          + lineSeparator()
          + "        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)"
          + lineSeparator()
          + "        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)"
          + lineSeparator()
          + "        at java.lang.reflect.Method.invoke(Method.java:498)" + lineSeparator()
          + "        at org.apache.geode.test.dunit.internal.MethodInvoker.executeObject(MethodInvoker.java:123)"
          + lineSeparator()
          + "        at org.apache.geode.test.dunit.internal.MethodInvoker.executeObject(MethodInvoker.java:92)"
          + lineSeparator()
          + "        at org.apache.geode.test.dunit.internal.RemoteDUnitVM.executeMethodOnObject(RemoteDUnitVM.java:45)"
          + lineSeparator()
          + "        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)"
          + lineSeparator()
          + "        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)"
          + lineSeparator()
          + "        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)"
          + lineSeparator()
          + "        at java.lang.reflect.Method.invoke(Method.java:498)" + lineSeparator()
          + "        at sun.rmi.server.UnicastServerRef.dispatch(UnicastServerRef.java:357)"
          + lineSeparator()
          + "        at sun.rmi.transport.Transport$1.run(Transport.java:200)" + lineSeparator()
          + "        at sun.rmi.transport.Transport$1.run(Transport.java:197)" + lineSeparator()
          + "        at java.security.AccessController.doPrivileged(Native Method)"
          + lineSeparator()
          + "        at sun.rmi.transport.Transport.serviceCall(Transport.java:196)"
          + lineSeparator()
          + "        at sun.rmi.transport.tcp.TCPTransport.handleMessages(TCPTransport.java:573)"
          + lineSeparator()
          + "        at sun.rmi.transport.tcp.TCPTransport$ConnectionHandler.run0(TCPTransport.java:834)"
          + lineSeparator()
          + "        at sun.rmi.transport.tcp.TCPTransport$ConnectionHandler.lambda$run$0(TCPTransport.java:688)"
          + lineSeparator()
          + "        at java.security.AccessController.doPrivileged(Native Method)"
          + lineSeparator()
          + "        at sun.rmi.transport.tcp.TCPTransport$ConnectionHandler.run(TCPTransport.java:687)"
          + lineSeparator()
          + "        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)"
          + lineSeparator()
          + "        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)"
          + lineSeparator()
          + "        at java.lang.Thread.run(Thread.java:748)" + lineSeparator()
          + "Caused by: java.lang.NoClassDefFoundError: org/apache/geode/logging/internal/log4j/api/LogService"
          + lineSeparator()
          + "        at org.apache.geode.management.internal.rest.controllers.ManagementControllerAdvice.<clinit>(ManagementControllerAdvice.java:54)"
          + lineSeparator()
          + "        at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)"
          + lineSeparator()
          + "        at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)"
          + lineSeparator()
          + "        at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)"
          + lineSeparator()
          + "        at java.lang.reflect.Constructor.newInstance(Constructor.java:423)"
          + lineSeparator()
          + "        at org.springframework.beans.BeanUtils.instantiateClass(BeanUtils.java:142)"
          + lineSeparator()
          + "        at org.springframework.beans.factory.support.SimpleInstantiationStrategy.instantiate(SimpleInstantiationStrategy.java:89)"
          + lineSeparator()
          + "        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.instantiateBean(AbstractAutowireCapableBeanFactory.java:1151)"
          + lineSeparator()
          + "        ... 80 more" + lineSeparator()
          + "Caused by: java.lang.ClassNotFoundException: org.apache.geode.logging.internal.log4j.api.LogService"
          + lineSeparator()
          + "        at java.net.URLClassLoader.findClass(URLClassLoader.java:382)"
          + lineSeparator()
          + "        at java.lang.ClassLoader.loadClass(ClassLoader.java:424)" + lineSeparator()
          + "        at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:349)"
          + lineSeparator()
          + "        at java.lang.ClassLoader.loadClass(ClassLoader.java:357)" + lineSeparator()
          + "        at org.eclipse.jetty.webapp.WebAppClassLoader.loadClass(WebAppClassLoader.java:543)"
          + lineSeparator()
          + "        at java.lang.ClassLoader.loadClass(ClassLoader.java:357)" + lineSeparator()
          + "        ... 88 more" + lineSeparator();
}
