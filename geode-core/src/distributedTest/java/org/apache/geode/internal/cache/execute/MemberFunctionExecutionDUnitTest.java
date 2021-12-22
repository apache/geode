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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

@Category({FunctionServiceTest.class})
public class MemberFunctionExecutionDUnitTest extends JUnit4CacheTestCase {

  private static final String TEST_FUNCTION6 = TestFunction.TEST_FUNCTION6;
  private static final String TEST_FUNCTION5 = TestFunction.TEST_FUNCTION5;

  private static final long serialVersionUID = 1L;

  VM member1 = null;
  VM member2 = null;
  VM member3 = null;
  VM member4 = null;

  static InternalDistributedSystem ds = null;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    member1 = host.getVM(0);
    member2 = host.getVM(1);
    member3 = host.getVM(2);
    member4 = host.getVM(3);
  }

  /**
   * Test the execution of function on all members haveResults = true
   *
   */
  @Test
  public void testRemoteMultiKeyExecution() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(() -> MemberFunctionExecutionDUnitTest.excuteOnMembers(5));
  }

  @Test
  public void testRemoteMultiKeyExecution_NotTimedout_byId() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(
        () -> MemberFunctionExecutionDUnitTest.executeOnMembers_NotTimedout_byId(5));
  }

  @Test
  public void testRemoteMultiKeyExecution_Timedout_byId() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1
        .invoke(
            () -> MemberFunctionExecutionDUnitTest.executeOnMembers_Timedout_byId(5));
  }

  @Test
  public void testRemoteMultiKeyExecution_NotTimedout_byInstance() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(
        () -> MemberFunctionExecutionDUnitTest
            .executeOnMembers_NotTimedout_byInstance(5));
  }

  @Test
  public void testRemoteMultiKeyExecution_Timedout_byInstance() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1
        .invoke(() -> MemberFunctionExecutionDUnitTest
            .executeOnMembers_Timedout_byInstance(5));
  }

  @Test
  public void testRemoteMultiKeyExecution_SendException1() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(
        () -> MemberFunctionExecutionDUnitTest.excuteOnMembers_SendException(1));
  }

  @Test
  public void testRemoteMultiKeyExecution_SendException2() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(
        () -> MemberFunctionExecutionDUnitTest.excuteOnMembers_SendException(4));
  }

  @Test
  public void testRemoteMultiKeyExecution_SendException3() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(
        () -> MemberFunctionExecutionDUnitTest.excuteOnMembers_SendException(5));
  }

  @Test
  public void testRemoteMultiKeyExecution_NoLastResult() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(
        () -> MemberFunctionExecutionDUnitTest.excuteOnMembers_NoLastResult(5));
  }

  @Test
  public void testLocalMultiKeyExecution_NoLastResult() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(
        () -> MemberFunctionExecutionDUnitTest.excuteOnMembers_NoLastResult(1));
  }

  /**
   * Test the execution of function on all memebers haveResults = true
   *
   */
  @Test
  public void testRemoteMultiKeyExecution_InlineFunction() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(
        () -> MemberFunctionExecutionDUnitTest.excuteOnMembers_InlineFunction(5));
  }

  @Test
  public void testBug45328() throws Exception {
    createDistributedSystemAndRegisterFunction();
    ClassBuilder classBuilder = new ClassBuilder();

    final String functionId = "MemberFunctionExecutionDUnitFunction";

    // Create a class that has a function and then get a ClassLoader that includes it
    StringBuilder StringBuilder = new StringBuilder();
    StringBuilder.append("import java.util.Properties;import org.apache.geode.cache.Declarable;");
    StringBuilder.append("import org.apache.geode.cache.CacheFactory;");
    StringBuilder.append("import org.apache.geode.cache.execute.Function;");
    StringBuilder.append("import org.apache.geode.cache.execute.FunctionContext;");
    StringBuilder.append("public class ").append(functionId)
        .append(" implements Function, Declarable {");
    StringBuilder.append("public void init(Properties props) {}");
    StringBuilder.append("public boolean hasResult() {return true;}");
    StringBuilder.append(
        "public void execute(FunctionContext context) {context.getResultSender().lastResult(\"GOOD\");}");
    StringBuilder
        .append("public String getId() {return \"MemberFunctionExecutionDUnitFunction\";}");
    StringBuilder.append("public boolean optimizeForWrite() {return false;}");
    StringBuilder.append("public boolean isHA() {return false;}}");

    ClassLoader classLoader = classBuilder.createClassLoaderFromContent(
        "MemberFunctionExecutionDUnitFunction", StringBuilder.toString());
    @SuppressWarnings("unchecked")
    Class<Function> clazz = (Class<Function>) Class.forName(functionId, true, classLoader);
    Constructor<Function> constructor = clazz.getConstructor();
    Function function = constructor.newInstance();

    Execution execution = FunctionService.onMembers();
    ResultCollector resultCollector = execution.execute(function);
    try {
      resultCollector.getResult();
      fail("Should have received FunctionException due to class not found");
    } catch (FunctionException expected) {
      LogWriterUtils.getLogWriter().warning("received wrong exception cause", expected.getCause());
      assertTrue((expected.getCause() instanceof ClassNotFoundException));
    }
  }

  @Test
  public void testBug40714() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest::registerFunction);
    member2.invoke(MemberFunctionExecutionDUnitTest::registerFunction);
    member3.invoke(MemberFunctionExecutionDUnitTest::registerFunction);
    member4.invoke(MemberFunctionExecutionDUnitTest::registerFunction);
    member1.invoke(
        () -> MemberFunctionExecutionDUnitTest.excuteOnMembers_InlineFunction(5));
  }


  @Test
  public void testBug46129() throws Exception {
    Properties props = getDistributedSystemProperties();
    member1.invoke(() -> MemberFunctionExecutionDUnitTest.connectToDistributedSystem(props));
    member2.invoke(() -> MemberFunctionExecutionDUnitTest.connectToDistributedSystem(props));
    member3.invoke(() -> MemberFunctionExecutionDUnitTest.connectToDistributedSystem(props));
    member4.invoke(() -> MemberFunctionExecutionDUnitTest.connectToDistributedSystem(props));
    connectToDistributedSystem(props);
    AbstractExecution exe = (AbstractExecution) FunctionService.onMembers();
    exe.setIgnoreDepartedMembers(true);
    ResultCollector rs = exe.execute(new TestBug46129FN1());
    List resultList = (List) rs.getResult();
    Exception e = (Exception) resultList.get(0);
    assertTrue(e instanceof IllegalArgumentException);
    IllegalArgumentException ex = (IllegalArgumentException) e;
    assertEquals("dummy", ex.getMessage());
  }

  public static void registerFunction() {
    FunctionService.registerFunction(new TestBug46129FN2());
  }

  /**
   * Test the execution of function on all memebers haveResults = false
   *
   */
  @Test
  public void testRemoteMultiKeyExecutionNoResult() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest::excuteOnMembersNoResult);
  }

  /**
   * Test the execution of function on local memebers haveResults = true
   *
   */
  @Test
  public void testRemoteMultiKeyExecutiononLocalMember() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(() -> MemberFunctionExecutionDUnitTest.excuteOnMembers(1));
  }

  /**
   * Test the execution of function on local memebers haveResults = true
   *
   */
  @Test
  public void testRemoteMultiKeyExecutiononLocalMember_InlineFunction() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(
        () -> MemberFunctionExecutionDUnitTest.excuteOnMembers_InlineFunction(1));
  }

  /**
   * Test the execution of function on other memebers haveResults = true
   *
   */
  @Test
  public void testRemoteMultiKeyExecutiononOtherMembers() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(() -> MemberFunctionExecutionDUnitTest.excuteOnMembers(4));
  }

  /**
   * Test the execution of function on other memebers haveResults = true
   *
   */
  @Test
  public void testRemoteMultiKeyExecutiononOtherMembers_InlineFunction() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(
        () -> MemberFunctionExecutionDUnitTest.excuteOnMembers_InlineFunction(4));
  }

  /**
   * This tests make sure that, in case of LonerDistributedSystem we dont get ClassCast Exception.
   * Just making sure that the function executed on lonerDistribuedSystem
   */
  @Test
  public void testBug41118() throws Exception {
    member1.invoke(MemberFunctionExecutionDUnitTest::bug41118);
  }

  @Test
  public void testOnMembersWithoutCache() throws Exception {
    DistributedMember member1Id = (DistributedMember) member1.invoke(new SerializableCallable() {

      @Override
      public Object call() {
        disconnectFromDS();
        return getSystem().getDistributedMember();
      }
    });

    member2.invoke(new SerializableRunnable() {

      @Override
      public void run() {
        getSystem();
        ResultCollector<?, ?> rc =
            FunctionService.onMember(member1Id).execute(new TestOnMembersWithoutCacheFN1());

        try {
          rc.getResult(30, TimeUnit.SECONDS);
          fail("Should have seen an exception");
        } catch (Exception e) {
          if (!(e.getCause() instanceof FunctionInvocationTargetException)) {
            Assert.fail("failed", e);
          }
        }

      }
    });
  }

  public static void bug41118() {
    ds = new MemberFunctionExecutionDUnitTest().getSystem();
    assertNotNull(ds);
    ds.disconnect();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    ds = (InternalDistributedSystem) DistributedSystem.connect(props);

    DistributionManager dm = ds.getDistributionManager();
    assertEquals("Distributed System is not loner", true, dm instanceof LonerDistributionManager);

    DistributedMember localmember = ds.getDistributedMember();
    Execution memberExcution = null;
    memberExcution = FunctionService.onMember(localmember);
    Execution executor = memberExcution.setArguments("Key");
    try {
      ResultCollector rc = executor.execute(new Bug41118FN1());
      List li = (ArrayList) rc.getResult();
      LogWriterUtils.getLogWriter()
          .info("MemberFunctionExecutionDUnitTest#excuteOnMembers: Result : " + li);
      assertEquals(li.size(), 1);
      for (Object obj : li) {
        assertEquals(obj, "Success");
      }
      ds.disconnect();
    } catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception Occurred : " + e.getMessage());
      e.printStackTrace();
      Assert.fail("Test failed", e);
    }
  }

  private static Execution getExecutionObject(Integer noOfMembers) {
    InternalExecution memberExcution = null;
    Execution executor = null;
    Map memArgs = new HashMap();
    if (noOfMembers == 1) { // Local VM
      DistributedMember localmember = ds.getDistributedMember();
      memberExcution = (InternalExecution) FunctionService.onMember(localmember);
      memArgs.put(localmember.getId(), localmember.getId());
      MemberMappedArgument args = new MemberMappedArgument("Key", memArgs);
      executor = memberExcution.withMemberMappedArgument(args);
    } else if (noOfMembers == 5) {
      memberExcution = (InternalExecution) FunctionService.onMembers();
      Set memberSet = new HashSet(ds.getDistributionManager().getNormalDistributionManagerIds());
      for (final Object o : memberSet) {
        InternalDistributedMember member = (InternalDistributedMember) o;
        memArgs.put(member.getId(), member.getId());
      }
      MemberMappedArgument args = new MemberMappedArgument("Key", memArgs);
      executor = memberExcution.withMemberMappedArgument(args);
    } else {
      Set memberSet = new HashSet(ds.getDistributionManager().getNormalDistributionManagerIds());
      InternalDistributedMember localVM = ds.getDistributionManager().getDistributionManagerId();
      memberSet.remove(localVM);
      for (final Object o : memberSet) {
        InternalDistributedMember member = (InternalDistributedMember) o;
        memArgs.put(member.getId(), member.getId());
      }
      MemberMappedArgument args = new MemberMappedArgument("Key", memArgs);
      memberExcution = (InternalExecution) FunctionService.onMembers(memberSet);
      executor = memberExcution.withMemberMappedArgument(args);
    }
    return executor;
  }

  /*
   * Execute Function
   */
  public static void excuteOnMembers(Integer noOfMembers) {
    assertNotNull(ds);
    Function function = new TestFunction(true, TestFunction.MEMBER_FUNCTION);
    Execution executor = getExecutionObject(noOfMembers);
    try {
      ResultCollector rc = executor.execute(function.getId());
      List li = (ArrayList) rc.getResult();
      LogWriterUtils.getLogWriter()
          .info("MemberFunctionExecutionDUnitTest#excuteOnMembers: Result : " + li);
      assertEquals(li.size(), noOfMembers.intValue());
      for (Object obj : li) {
        assertEquals(obj, "Success");
      }
    } catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception Occurred : " + e.getMessage());
      e.printStackTrace();
      Assert.fail("Test failed", e);
    }
  }

  public static void excuteOnMembers_SendException(Integer noOfMembers) {
    assertNotNull(ds);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
    InternalExecution memberExcution = null;
    Execution executor = null;
    Map memArgs = new HashMap();
    if (noOfMembers == 1) { // Local VM
      DistributedMember localmember = ds.getDistributedMember();
      memberExcution = (InternalExecution) FunctionService.onMember(localmember);
    } else if (noOfMembers == 5) {
      memberExcution = (InternalExecution) FunctionService.onMembers();
    } else {
      Set memberSet = new HashSet(ds.getDistributionManager().getNormalDistributionManagerIds());
      InternalDistributedMember localVM = ds.getDistributionManager().getDistributionManagerId();
      memberSet.remove(localVM);
      for (final Object o : memberSet) {
        InternalDistributedMember member = (InternalDistributedMember) o;
      }
      memberExcution = (InternalExecution) FunctionService.onMembers(memberSet);
    }
    try {
      ResultCollector rc = memberExcution.setArguments(Boolean.TRUE).execute(function);
      List li = (ArrayList) rc.getResult();
      LogWriterUtils.getLogWriter()
          .info("MemberFunctionExecutionDUnitTest#excuteOnMembers: Result : " + li);
      assertEquals(noOfMembers.intValue(), li.size());
      for (Object obj : li) {
        assertTrue(obj instanceof MyFunctionExecutionException);
      }
    } catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception Occurred : " + e.getMessage());
      e.printStackTrace();
      Assert.fail("Test failed", e);
    }
  }

  public static void excuteOnMembers_NoLastResult(Integer noOfMembers) {
    assertNotNull(ds);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_NO_LASTRESULT);
    Execution executor = getExecutionObject(noOfMembers);
    try {
      ResultCollector rc = executor.execute(function.getId());
      rc.getResult();
      fail("Expcted FunctionException : Function did not send last result");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("did not send last result")
          || ex.getCause().getMessage().contains("did not send last result"));
    }
  }

  /*
   * Execute Function
   */
  public static void excuteOnMembers_InlineFunction(Integer noOfMembers) {
    assertNotNull(ds);
    Execution memberExcution = null;
    if (noOfMembers == 1) { // Local VM
      DistributedMember localmember = ds.getDistributedMember();
      memberExcution = FunctionService.onMember(localmember);
    } else if (noOfMembers == 5) {
      memberExcution = FunctionService.onMembers();
    } else {
      Set memberSet = new HashSet(ds.getDistributionManager().getNormalDistributionManagerIds());
      InternalDistributedMember localVM = ds.getDistributionManager().getDistributionManagerId();
      memberSet.remove(localVM);
      memberExcution = FunctionService.onMembers(memberSet);
    }
    Execution executor = memberExcution.setArguments("Key");
    try {
      ResultCollector rc = executor.execute(new FunctionAdapter() {
        @Override
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult("Success");
          } else {
            context.getResultSender().lastResult("Failure");
          }
        }

        @Override
        public String getId() {
          return getClass().getName();
        }

        @Override
        public boolean hasResult() {
          return true;
        }
      });
      List li = (ArrayList) rc.getResult();
      LogWriterUtils.getLogWriter()
          .info("MemberFunctionExecutionDUnitTest#excuteOnMembers: Result : " + li);
      assertEquals(li.size(), noOfMembers.intValue());
      for (Object obj : li) {
        assertEquals(obj, "Success");
      }
    } catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception Occurred : " + e.getMessage());
      e.printStackTrace();
      Assert.fail("Test failed", e);
    }
  }

  /*
   * Execute Function
   */
  public static void excuteOnMembersNoResult() {
    assertNotNull(ds);
    Function function = new TestFunction(false, TEST_FUNCTION6);
    Execution memberExcution = FunctionService.onMembers();
    Execution executor = memberExcution.setArguments("Key");
    try {
      ResultCollector rc = executor.execute(function.getId());
      rc.getResult();
      fail("Test Failed");
    } catch (Exception expected) {
      LogWriterUtils.getLogWriter().info("Exception Occurred : " + expected.getMessage());
      // boolean check = expected.getMessage().equals("Cannot return any result, as
      // Function.hasResult() is false");
      assertTrue(expected.getMessage()
          .equals(String.format("Cannot %s result as the Function#hasResult() is false",
              "return any")));
    }
  }

  /*
   * Execute Function
   */
  public static void executeOnMembers_NotTimedout_byId(Integer noOfMembers) {
    assertNotNull(ds);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_RUNNING_FOR_LONG_TIME);
    Execution executor = getExecutionObject(noOfMembers);
    try {
      long timeoutMs = 6000;
      ResultCollector rc = executor.setArguments(Boolean.TRUE).execute(function.getId(), timeoutMs,
          TimeUnit.MILLISECONDS);
      List li = (ArrayList) rc.getResult();
      LogWriterUtils.getLogWriter()
          .info("MemberFunctionExecutionDUnitTest#excuteOnMembers: Result : " + li);
      assertEquals(li.size(), noOfMembers.intValue());
      for (Object obj : li) {
        assertEquals(obj, "Ran executeFunctionRunningForLongTime for 2000");
      }
    } catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception Occurred : " + e.getMessage());
      e.printStackTrace();
      Assert.fail("Test failed", e);
    }
  }

  /*
   * Execute Function
   */
  public static void executeOnMembers_Timedout_byId(Integer noOfMembers) {
    assertNotNull(ds);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_RUNNING_FOR_LONG_TIME);
    Execution executor = getExecutionObject(noOfMembers);
    try {
      long timeoutMs = 1000;
      ResultCollector rc = executor.execute(function.getId(), timeoutMs, TimeUnit.MILLISECONDS);
      fail("Expected FunctionException: Function did not timeout");
    } catch (Exception ex) {
      if (!(ex.getCause() instanceof FunctionException)) {
        Assert.fail("Function should have timed out", ex);
      }
    }
  }

  /*
   * Execute Function
   */
  public static void executeOnMembers_NotTimedout_byInstance(Integer noOfMembers) {
    assertNotNull(ds);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_RUNNING_FOR_LONG_TIME);
    Execution executor = getExecutionObject(noOfMembers);
    try {
      long timeoutMs = 6000;
      ResultCollector rc = executor.setArguments(Boolean.TRUE).execute(function, timeoutMs,
          TimeUnit.MILLISECONDS);
      List li = (ArrayList) rc.getResult();
      LogWriterUtils.getLogWriter()
          .info("MemberFunctionExecutionDUnitTest#excuteOnMembers: Result : " + li);
      assertEquals(li.size(), noOfMembers.intValue());
      for (Object obj : li) {
        assertEquals(obj, "Ran executeFunctionRunningForLongTime for 2000");
      }
    } catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception Occurred : " + e.getMessage());
      e.printStackTrace();
      Assert.fail("Test failed", e);
    }
  }

  /*
   * Execute Function
   */
  public static void executeOnMembers_Timedout_byInstance(Integer noOfMembers) {
    assertNotNull(ds);
    Function function = new TestFunction(true, TestFunction.TEST_FUNCTION_RUNNING_FOR_LONG_TIME);
    Execution executor = getExecutionObject(noOfMembers);
    try {
      long timeoutMs = 1000;
      ResultCollector rc = executor.execute(function, timeoutMs, TimeUnit.MILLISECONDS);
      fail("Expected FunctionException: Function did not timeout");
    } catch (Exception ex) {
      if (!(ex.getCause() instanceof FunctionException)) {
        Assert.fail("Function should have timed out", ex);
      }
    }
  }


  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.execute.MyFunctionExecutionException"
            + ";org.apache.geode.internal.cache.execute.MemberFunctionExecutionDUnitTest*"
            + ";org.apache.geode.test.dunit.**" + ";org.apache.geode.test.junit.rules.**");
    return props;
  }

  /*
   * Create Disturbued System and Register the Function
   */
  private void createDistributedSystemAndRegisterFunction() {
    final Properties props = getDistributedSystemProperties();
    connectToDistributedSystem(props);
    List<VM> members = new ArrayList<>(4);
    members.add(member1);
    members.add(member2);
    members.add(member3);
    members.add(member4);
    for (VM member : members) {
      member.invoke(() -> MemberFunctionExecutionDUnitTest.connectToDistributedSystem(props));
      member
          .invoke(() -> MemberFunctionExecutionDUnitTest.registerExpectedExceptions(Boolean.TRUE));
    }
  }

  public static void registerExpectedExceptions(boolean add) {
    final String action = add ? "add" : "remove";
    LogWriter log = InternalDistributedSystem.getLogger();
    if (log != null) {
      log.info(
          "<ExpectedException action=" + action + ">ClassNotFoundException</ExpectedException>");
    }
  }

  public static void connectToDistributedSystem(Properties props) {
    new MemberFunctionExecutionDUnitTest().createSystem(props);
  }

  private InternalDistributedSystem createSystem(Properties props) {
    try {
      ds = getSystem(props);
      assertNotNull(ds);
      FunctionService.registerFunction(new TestFunction(true, TEST_FUNCTION5));
      FunctionService.registerFunction(new TestFunction(false, TEST_FUNCTION6));
      FunctionService.registerFunction(new TestFunction(true, TestFunction.MEMBER_FUNCTION));
      FunctionService
          .registerFunction(new TestFunction(true, TestFunction.TEST_FUNCTION_NO_LASTRESULT));
      FunctionService.registerFunction(
          new TestFunction(true, TestFunction.TEST_FUNCTION_RUNNING_FOR_LONG_TIME));

    } catch (Exception e) {
      Assert.fail("Failed while creating the Distribued System", e);
    }
    return ds;
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    List<VM> members = new ArrayList<>(4);
    members.add(member1);
    members.add(member2);
    members.add(member3);
    members.add(member4);
    for (VM member : members) {
      member
          .invoke(() -> MemberFunctionExecutionDUnitTest.registerExpectedExceptions(Boolean.FALSE));
    }
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  public static class TestBug46129FN1 extends FunctionAdapter {
    @Override
    public void execute(FunctionContext context) {
      try {
        Object arg = context.getArguments();
        if (arg != null && arg instanceof Map) {
          context.getResultSender().lastResult("ok");
        } else {
          throw new IllegalArgumentException("dummy");
        }
      } catch (Exception e) {
        context.getResultSender().sendException(e);
      }
    }

    @Override
    public String getId() {
      return "testBug46129";
    }
  }

  public static class TestBug46129FN2 extends FunctionAdapter {
    @Override
    public void execute(FunctionContext context) {
      if (context.getArguments() instanceof String) {
        context.getResultSender().lastResult("Failure");
      } else if (context.getArguments() instanceof Boolean) {
        context.getResultSender().lastResult(Boolean.FALSE);
      }
    }

    @Override
    public String getId() {
      return "Function";
    }

    @Override
    public boolean hasResult() {
      return true;
    }
  }

  public static class Bug41118FN1 extends FunctionAdapter {
    @Override
    public void execute(FunctionContext context) {
      if (context.getArguments() instanceof String) {
        context.getResultSender().lastResult("Success");
      } else {
        context.getResultSender().lastResult("Failure");
      }
    }

    @Override
    public String getId() {
      return getClass().getName();
    }

    @Override
    public boolean hasResult() {
      return true;
    }
  }

  public static class TestOnMembersWithoutCacheFN1 extends FunctionAdapter {

    @Override
    public String getId() {
      return getClass().getName();
    }

    @Override
    public void execute(FunctionContext context) {
      // This will throw an exception because the cache is not yet created.
      CacheFactory.getAnyInstance();
    }
  }
}
