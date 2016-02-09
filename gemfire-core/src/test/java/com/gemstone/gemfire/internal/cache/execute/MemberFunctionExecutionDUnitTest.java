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
package com.gemstone.gemfire.internal.cache.execute;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.LonerDistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.ClassBuilder;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class MemberFunctionExecutionDUnitTest extends CacheTestCase {
  private static final String TEST_FUNCTION6 = TestFunction.TEST_FUNCTION6;
  private static final String TEST_FUNCTION5 = TestFunction.TEST_FUNCTION5;

  private static final long serialVersionUID = 1L;
  
  VM member1 = null;
  VM member2 = null;
  VM member3 = null;
  VM member4 = null;
  
  static InternalDistributedSystem ds = null;

  public MemberFunctionExecutionDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    member1 = host.getVM(0);
    member2 = host.getVM(1);
    member3 = host.getVM(2);
    member4 = host.getVM(3);
  }
  
  /**
   * Test the execution of function on all memebers
   * haveResults = true 
   * @throws Exception
   */
  public void testRemoteMultiKeyExecution()
      throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembers",new Object[]{new Integer(5)});
  }
  
  public void testRemoteMultiKeyExecution_SendException1() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembers_SendException",
        new Object[] { new Integer(1) });
  }
  
  public void testRemoteMultiKeyExecution_SendException2() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembers_SendException",
        new Object[] { new Integer(4) });
  }
  
  public void testRemoteMultiKeyExecution_SendException3() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembers_SendException",
        new Object[] { new Integer(5) });
  }
  
  public void testRemoteMultiKeyExecution_NoLastResult() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembers_NoLastResult",
        new Object[] { new Integer(5) });
  }
  
  public void testLocalMultiKeyExecution_NoLastResult() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembers_NoLastResult",
        new Object[] { new Integer(1) });
  }
  
  /**
   * Test the execution of function on all memebers
   * haveResults = true 
   * @throws Exception
   */
  public void testRemoteMultiKeyExecution_InlineFunction()
      throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembers_InlineFunction",new Object[]{new Integer(5)});
  }

  public void testBug45328() throws Exception {
    createDistributedSystemAndRegisterFunction();
    ClassBuilder classBuilder = new ClassBuilder();

    final String functionId = "MemberFunctionExecutionDUnitFunction";

    // Create a class that has a function and then get a ClassLoader that includes it
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("import java.util.Properties;import com.gemstone.gemfire.cache.Declarable;");
    stringBuffer.append("import com.gemstone.gemfire.cache.CacheFactory;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.Function;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.FunctionContext;");
    stringBuffer.append("public class ").append(functionId).append(" implements Function, Declarable {");
    stringBuffer.append("public void init(Properties props) {}");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer.append("public void execute(FunctionContext context) {context.getResultSender().lastResult(\"GOOD\");}");
    stringBuffer.append("public String getId() {return \"MemberFunctionExecutionDUnitFunction\";}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");

    ClassLoader classLoader = classBuilder.createClassLoaderFromContent("MemberFunctionExecutionDUnitFunction", stringBuffer
        .toString());
    @SuppressWarnings("unchecked")
    Class<Function> clazz = (Class<Function>) Class.forName(functionId, true, classLoader);
    Constructor<Function> constructor = clazz.getConstructor();
    Function function = (Function) constructor.newInstance();

    DistributedSystem distributedSystem = getSystem();
    Execution execution = FunctionService.onMembers(distributedSystem);
    ResultCollector resultCollector = execution.execute(function);
    try {
      resultCollector.getResult();
      fail("Should have received FunctionException due to class not found");
    } catch (FunctionException expected) {
      LogWriterUtils.getLogWriter().warning("received wrong exception cause", expected.getCause());
      assertTrue((expected.getCause() instanceof ClassNotFoundException));
    }
  }

  public void testBug40714() throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "registerFunction");
    member2.invoke(MemberFunctionExecutionDUnitTest.class, "registerFunction");
    member3.invoke(MemberFunctionExecutionDUnitTest.class, "registerFunction");
    member4.invoke(MemberFunctionExecutionDUnitTest.class, "registerFunction");
    member1.invoke(MemberFunctionExecutionDUnitTest.class,
        "excuteOnMembers_InlineFunction", new Object[] { new Integer(5) });
  }

  
  public void testBug46129() throws Exception {
    Properties props = new Properties();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "connectToDistributedSystem",  new Object[] { props });
    member2.invoke(MemberFunctionExecutionDUnitTest.class, "connectToDistributedSystem",  new Object[] { props });
    member3.invoke(MemberFunctionExecutionDUnitTest.class, "connectToDistributedSystem",  new Object[] { props });
    member4.invoke(MemberFunctionExecutionDUnitTest.class, "connectToDistributedSystem",  new Object[] { props });
    connectToDistributedSystem(props);
    AbstractExecution exe = (AbstractExecution)FunctionService.onMembers(getSystem());
    exe.setIgnoreDepartedMembers(true);
    ResultCollector rs = exe.execute(new FunctionAdapter(){
      @Override
      public void execute(FunctionContext context) {
        try {
          Object arg = context.getArguments();
          if (arg != null && arg instanceof Map) {
            context.getResultSender().lastResult("ok");
          } else {
            throw new IllegalArgumentException("dummy");
          }
        } catch (Exception e){
          context.getResultSender().sendException(e);
        }
      }

      @Override
      public String getId() {
        return "testBug46129";
      }      
    });
    List resultList = (List)rs.getResult();
    Exception e = (Exception)resultList.get(0);
    assertTrue(e instanceof IllegalArgumentException);
    IllegalArgumentException ex = (IllegalArgumentException)e;
    assertEquals("dummy", ex.getMessage());
  }     
  
  public static void registerFunction() {
    FunctionService.registerFunction(new FunctionAdapter() {
      @Override
      public void execute(FunctionContext context) {
        if (context.getArguments() instanceof String) {
          context.getResultSender().lastResult("Failure");
        }
        else if (context.getArguments() instanceof Boolean) {
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
    });
  }

  /**
   * Test the execution of function on all memebers
   * haveResults = false 
   * @throws Exception
   */
  public void testRemoteMultiKeyExecutionNoResult()
      throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembersNoResult");
  }
  /**
   * Test the execution of function on local memebers
   * haveResults = true 
   * @throws Exception
   */
  public void testRemoteMultiKeyExecutiononLocalMember()
      throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembers",new Object[]{new Integer(1)});
  }
  
  /**
   * Test the execution of function on local memebers
   * haveResults = true 
   * @throws Exception
   */
  public void testRemoteMultiKeyExecutiononLocalMember_InlineFunction()
      throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembers_InlineFunction",new Object[]{new Integer(1)});
  }
  
  /**
   * Test the execution of function on other memebers
   * haveResults = true 
   * @throws Exception
   */
  public void testRemoteMultiKeyExecutiononOtherMembers()
      throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembers",new Object[]{new Integer(4)});
  }
  
  /**
   * Test the execution of function on other memebers
   * haveResults = true 
   * @throws Exception
   */
  public void testRemoteMultiKeyExecutiononOtherMembers_InlineFunction()
      throws Exception {
    createDistributedSystemAndRegisterFunction();
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "excuteOnMembers_InlineFunction",new Object[]{new Integer(4)});
  }
  
  /**
   * This tests make sure that, in case of LonerDistributedSystem we dont get ClassCast Exception.
   * Just making sure that the function executed on lonerDistribuedSystem
   */
  public void testBug41118()
      throws Exception {
    member1.invoke(MemberFunctionExecutionDUnitTest.class, "bug41118");
  }
  
  public void testOnMembersWithoutCache()
      throws Exception {
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
        ResultCollector<?, ?> rc = FunctionService.onMember(member1Id).execute(new FunctionAdapter() {
          
          @Override
          public String getId() {
            return getClass().getName();
          }
          
          @Override
          public void execute(FunctionContext context) {
            //This will throw an exception because the cache is not yet created.
            CacheFactory.getAnyInstance();
          }
        });
        
        try {
          rc.getResult(30, TimeUnit.SECONDS);
          fail("Should have seen an exception");
        } catch (Exception e) {
          if(!(e.getCause() instanceof FunctionInvocationTargetException)) {
            Assert.fail("failed", e);
          }
        }
        
      }
    });
  }
  
  public static void bug41118(){
    ds = new MemberFunctionExecutionDUnitTest("temp").getSystem();
    assertNotNull(ds);
    ds.disconnect();
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    ds = (InternalDistributedSystem)DistributedSystem.connect(props);
    
    DM dm = ds.getDistributionManager();
    assertEquals("Distributed System is not loner", true, dm instanceof LonerDistributionManager);
    
    DistributedMember localmember = ds.getDistributedMember();
    Execution memberExcution = null;
    memberExcution = FunctionService.onMember(ds,localmember);  
    Execution executor = memberExcution.withArgs("Key");
    try {
      ResultCollector rc = executor.execute(new FunctionAdapter(){
        @Override
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult("Success");
          }else{
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
      List li = (ArrayList)rc.getResult();
      LogWriterUtils.getLogWriter().info(
          "MemberFunctionExecutionDUnitTest#excuteOnMembers: Result : " + li);
      assertEquals(li.size(), 1);
      for (Object obj : li) {
        assertEquals(obj, "Success");
      }
      ds.disconnect();
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception Occured : "+ e.getMessage());
      e.printStackTrace();
      Assert.fail("Test failed",e);
    }
  }
  /*
   * Execute Function
   */
  public static void excuteOnMembers(Integer noOfMembers) {
    assertNotNull(ds);
    Function function = new TestFunction(true,TestFunction.MEMBER_FUNCTION);
    InternalExecution memberExcution = null;
    Execution executor = null;
    Map memArgs = new HashMap();
    if(noOfMembers.intValue() == 1){  //Local VM
      DistributedMember localmember = ds.getDistributedMember();
      memberExcution = (InternalExecution)FunctionService.onMember(ds,localmember);
      memArgs.put(localmember.getId(), localmember.getId());
      MemberMappedArgument args = new MemberMappedArgument("Key", memArgs);
      executor = memberExcution.withMemberMappedArgument(args);
    }
    else if(noOfMembers.intValue() == 5){
      memberExcution = (InternalExecution)FunctionService.onMembers(ds);
      Set memberSet = new HashSet(ds.getDistributionManager().getNormalDistributionManagerIds());
      Iterator iter = memberSet.iterator();
      while(iter.hasNext()){
        InternalDistributedMember member = (InternalDistributedMember)iter.next();
        memArgs.put(member.getId(), member.getId());
      }      
      MemberMappedArgument args = new MemberMappedArgument("Key", memArgs);
      executor = memberExcution.withMemberMappedArgument(args);
    }
    else{
      Set memberSet = new HashSet(ds.getDistributionManager().getNormalDistributionManagerIds());
      InternalDistributedMember localVM = ds.getDistributionManager().getDistributionManagerId();
      memberSet.remove(localVM);
      Iterator iter = memberSet.iterator();
      while(iter.hasNext()){
        InternalDistributedMember member = (InternalDistributedMember)iter.next();
        memArgs.put(member.getId(), member.getId());
      }      
      MemberMappedArgument args = new MemberMappedArgument("Key", memArgs);
      memberExcution = (InternalExecution)FunctionService.onMembers(ds,memberSet);
      executor = memberExcution.withMemberMappedArgument(args);
    }
    try {
      ResultCollector rc = executor.execute(function.getId());
      List li = (ArrayList)rc.getResult();
      LogWriterUtils.getLogWriter().info(
          "MemberFunctionExecutionDUnitTest#excuteOnMembers: Result : " + li);
      assertEquals(li.size(), noOfMembers.intValue());
      for (Object obj : li) {
        assertEquals(obj, "Success");
      }
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception Occured : "+ e.getMessage());
      e.printStackTrace();
      Assert.fail("Test failed",e);
    }
  }
  
  public static void excuteOnMembers_SendException(Integer noOfMembers) {
    assertNotNull(ds);
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_SEND_EXCEPTION);
    InternalExecution memberExcution = null;
    Execution executor = null;
    Map memArgs = new HashMap();
    if(noOfMembers.intValue() == 1){  //Local VM
      DistributedMember localmember = ds.getDistributedMember();
      memberExcution = (InternalExecution)FunctionService.onMember(ds,localmember);
    }
    else if(noOfMembers.intValue() == 5){
      memberExcution = (InternalExecution)FunctionService.onMembers(ds);
    }
    else{
      Set memberSet = new HashSet(ds.getDistributionManager().getNormalDistributionManagerIds());
      InternalDistributedMember localVM = ds.getDistributionManager().getDistributionManagerId();
      memberSet.remove(localVM);
      Iterator iter = memberSet.iterator();
      while(iter.hasNext()){
        InternalDistributedMember member = (InternalDistributedMember)iter.next();
      }      
      memberExcution = (InternalExecution)FunctionService.onMembers(ds,memberSet);
    }
    try {
      ResultCollector rc = memberExcution.withArgs(Boolean.TRUE).execute(function);
      List li = (ArrayList)rc.getResult();
      LogWriterUtils.getLogWriter().info(
          "MemberFunctionExecutionDUnitTest#excuteOnMembers: Result : " + li);
      assertEquals(noOfMembers.intValue(), li.size());
      for (Object obj : li) {
        assertTrue(obj instanceof MyFunctionExecutionException );
      }
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception Occured : "+ e.getMessage());
      e.printStackTrace();
      Assert.fail("Test failed",e);
    }
  }
  
  public static void excuteOnMembers_NoLastResult(Integer noOfMembers) {
    assertNotNull(ds);
    Function function = new TestFunction(true,TestFunction.TEST_FUNCTION_NO_LASTRESULT);
    InternalExecution memberExcution = null;
    Execution executor = null;
    Map memArgs = new HashMap();
    if(noOfMembers.intValue() == 1){  //Local VM
      DistributedMember localmember = ds.getDistributedMember();
      memberExcution = (InternalExecution)FunctionService.onMember(ds,localmember);
      memArgs.put(localmember.getId(), localmember.getId());
      MemberMappedArgument args = new MemberMappedArgument("Key", memArgs);
      executor = memberExcution.withMemberMappedArgument(args);
    }
    else if(noOfMembers.intValue() == 5){
      memberExcution = (InternalExecution)FunctionService.onMembers(ds);
      Set memberSet = new HashSet(ds.getDistributionManager().getNormalDistributionManagerIds());
      Iterator iter = memberSet.iterator();
      while(iter.hasNext()){
        InternalDistributedMember member = (InternalDistributedMember)iter.next();
        memArgs.put(member.getId(), member.getId());
      }      
      MemberMappedArgument args = new MemberMappedArgument("Key", memArgs);
      executor = memberExcution.withMemberMappedArgument(args);
    }else{
      Set memberSet = new HashSet(ds.getDistributionManager().getNormalDistributionManagerIds());
      InternalDistributedMember localVM = ds.getDistributionManager().getDistributionManagerId();
      memberSet.remove(localVM);
      Iterator iter = memberSet.iterator();
      while(iter.hasNext()){
        InternalDistributedMember member = (InternalDistributedMember)iter.next();
        memArgs.put(member.getId(), member.getId());
      }      
      MemberMappedArgument args = new MemberMappedArgument("Key", memArgs);
      memberExcution = (InternalExecution)FunctionService.onMembers(ds,memberSet);
      executor = memberExcution.withMemberMappedArgument(args);
    }    
    try {
      ResultCollector rc = executor.execute(function.getId());
      rc.getResult();
      fail("Expcted FunctionException : Function did not send last result");
    }
    catch (Exception ex) {
      assertTrue(ex.getMessage().contains(
          "did not send last result") || ex.getCause().getMessage().contains(
          "did not send last result") );
    }
  }
  /*
   * Execute Function
   */
  public static void excuteOnMembers_InlineFunction(Integer noOfMembers) {
    assertNotNull(ds);
    Execution memberExcution = null;
    if(noOfMembers.intValue() == 1){  //Local VM
      DistributedMember localmember = ds.getDistributedMember();
      memberExcution = FunctionService.onMember(ds,localmember);  
    }
    else if(noOfMembers.intValue() == 5){
      memberExcution = FunctionService.onMembers(ds);
    }else{
      Set memberSet = new HashSet(ds.getDistributionManager().getNormalDistributionManagerIds());
      InternalDistributedMember localVM = ds.getDistributionManager().getDistributionManagerId();
      memberSet.remove(localVM);
      memberExcution = FunctionService.onMembers(ds,memberSet);
    }
    Execution executor = memberExcution.withArgs("Key");
    try {
      ResultCollector rc = executor.execute(new FunctionAdapter(){
        @Override
        public void execute(FunctionContext context) {
          if (context.getArguments() instanceof String) {
            context.getResultSender().lastResult("Success");
          }else{
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
      List li = (ArrayList)rc.getResult();
      LogWriterUtils.getLogWriter().info(
          "MemberFunctionExecutionDUnitTest#excuteOnMembers: Result : " + li);
      assertEquals(li.size(), noOfMembers.intValue());
      for (Object obj : li) {
        assertEquals(obj, "Success");
      }
    }
    catch (Exception e) {
      LogWriterUtils.getLogWriter().info("Exception Occured : "+ e.getMessage());
      e.printStackTrace();
      Assert.fail("Test failed",e);
    }
  }
  
  /*
   * Execute Function
   */
  public static void excuteOnMembersNoResult() {
    assertNotNull(ds);
    Function function = new TestFunction(false,TEST_FUNCTION6);
    Execution memberExcution = FunctionService.onMembers(ds);
    Execution executor = memberExcution.withArgs("Key");
    try {
      ResultCollector rc = executor.execute(function.getId());
      rc.getResult();
      fail("Test Failed");
    }
    catch (Exception expected) {
      LogWriterUtils.getLogWriter().info("Exception Occured : "+ expected.getMessage());
//      boolean check = expected.getMessage().equals("Cannot return any result, as Function.hasResult() is false");
      assertTrue(expected.getMessage().equals(LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
          .toLocalizedString("return any")));
    }
  }
  
  /*
   * Create Disturbued System and Register the Function 
   */
  private void createDistributedSystemAndRegisterFunction() {
    Properties props = new Properties();
    connectToDistributedSystem(props);
    List<VM> members = new ArrayList<VM>(4);
    members.add(member1); members.add(member2); members.add(member3); members.add(member4);
    for (VM member: members) {
      member.invoke(MemberFunctionExecutionDUnitTest.class, "connectToDistributedSystem",
        new Object[] { props });
      member.invoke(MemberFunctionExecutionDUnitTest.class, "registerExpectedExceptions",
          new Object[] { Boolean.TRUE });
    }
  }
  
  public static void registerExpectedExceptions(boolean add) {
    final String action = add? "add" : "remove";
    LogWriterI18n log = InternalDistributedSystem.getLoggerI18n();
    if (log != null) {
      log.convertToLogWriter().info(
       "<ExpectedException action=" + action + ">ClassNotFoundException</ExpectedException>");
    }
  }
  
  public static void connectToDistributedSystem(Properties props) {
    new MemberFunctionExecutionDUnitTest("temp").createSystem(props);
  }
  
  private InternalDistributedSystem createSystem(Properties props){
    try {
      ds = getSystem(props);
      assertNotNull(ds);
      FunctionService.registerFunction(new TestFunction(true,TEST_FUNCTION5));
      FunctionService.registerFunction(new TestFunction(false,TEST_FUNCTION6));
      FunctionService.registerFunction(new TestFunction(true,TestFunction.MEMBER_FUNCTION));
      FunctionService.registerFunction(new TestFunction(true,TestFunction.TEST_FUNCTION_NO_LASTRESULT));
    }
    catch (Exception e) {
      Assert.fail("Failed while creating the Distribued System", e);
    }
    return ds;
  }
  
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    List<VM> members = new ArrayList<VM>(4);
    members.add(member1); members.add(member2); members.add(member3); members.add(member4);
    for (VM member: members) {
      member.invoke(MemberFunctionExecutionDUnitTest.class, "registerExpectedExceptions",
          new Object[] { Boolean.FALSE });
    }
  }
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }
}
