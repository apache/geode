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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;

public class PRClientServerFunctionExecutionNoAckDUnitTest extends PRClientServerTestBase{
  /**
   * 
   */
  private static final String TEST_FUNCTION1 = TestFunction.TEST_FUNCTION1;

  static Boolean isByName = null;

  static Function functionNoAck = null;
  static Function functionAck = null;
  static Boolean toRegister = null;
  
  private static final int NUM_ITERATION=1;
  
  public PRClientServerFunctionExecutionNoAckDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }
  /*
   * Execution of the function on server using the name of the function
   */
  public void testServerFunctionExecution_NoAck() {
    createScenario();

    functionNoAck = new TestFunction(false,TEST_FUNCTION1, true);
    functionAck = new TestFunction(true,TEST_FUNCTION1, true);
    registerFunctionAtServer(functionNoAck);
    registerFunctionAtServer(functionAck);
    
    isByName = new Boolean(true);
    toRegister = new Boolean(true);
    LogWriterUtils.getLogWriter()
        .info(
            "PRClientServerFunctionExecutionNoAckDUnitTest#testServerFunctionExecution_NoAck : Starting test");
    client.invoke(PRClientServerFunctionExecutionNoAckDUnitTest.class,
        "serverExecution", new Object[] { isByName, functionNoAck, functionAck , toRegister});
    client.invoke(PRClientServerFunctionExecutionNoAckDUnitTest.class,
        "allServerExecution", new Object[] { isByName, functionNoAck , toRegister});
  }

  public void testServerFunctionExecution_NoAck_WithoutRegister() {
    createScenario();

    functionNoAck = new TestFunction(false,TEST_FUNCTION1, true);
    functionAck = new TestFunction(true,TEST_FUNCTION1, true);
    registerFunctionAtServer(functionNoAck);
    registerFunctionAtServer(functionAck);
    toRegister = new Boolean(false);
    isByName = new Boolean(true);
    LogWriterUtils.getLogWriter()
        .info(
            "PRClientServerFunctionExecutionNoAckDUnitTest#testServerFunctionExecution_NoAck : Starting test");
    client.invoke(PRClientServerFunctionExecutionNoAckDUnitTest.class,
        "serverExecution", new Object[] { isByName, functionNoAck, functionAck, toRegister });
    client.invoke(PRClientServerFunctionExecutionNoAckDUnitTest.class,
        "allServerExecution", new Object[] { isByName, functionNoAck, toRegister });
  }
  
  private void createScenario() {
    LogWriterUtils.getLogWriter()
        .info(
            "PRClientServerFFunctionExecutionDUnitTest#createScenario : creating scenario");
    createClientServerScenarionWithoutRegion();
  }

  public static void serverExecution(Boolean isByName, Function functionNoAck, Function functionAck, Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);
    if(toRegister.booleanValue()){
      FunctionService.registerFunction(functionNoAck);
    }
    else {
      assertNull(FunctionService.getFunction(functionNoAck.getId()));
    }
    Execution member = FunctionService.onServer(pool);

    try {
      TimeKeeper t = new TimeKeeper();
      t.start();
      for(int i=0;i< NUM_ITERATION;i++)
        execute(member, Boolean.TRUE, functionNoAck, isByName, toRegister);
      t.stop();
      LogWriterUtils.getLogWriter().info("Time taken to execute boolean based" + NUM_ITERATION + "NoAck functions :" + t.getTimeInMs());      
    }
    catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }
        
    try {
      final HashSet testKeysSet = new HashSet();
      for (int i = 0; i < 2; i++) {
        testKeysSet.add("execKey-" + i);
      }
      TimeKeeper t = new TimeKeeper();
      t.start();
      for(int i=0;i< NUM_ITERATION;i++)
        execute(member, testKeysSet, functionNoAck, isByName, toRegister);
      t.stop();
      LogWriterUtils.getLogWriter().info("Time taken to execute setbased" + NUM_ITERATION + "NoAck functions :" + t.getTimeInMs());     
    }
    catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operationssssss");
    }
    if(toRegister.booleanValue()){
      FunctionService.registerFunction(functionAck);
    }
    else {
      assertNull(FunctionService.getFunction(functionAck.getId()));
    }
    try {
      TimeKeeper t = new TimeKeeper();
      long timeinms = 0;
      t.start();
      for(int i=0;i< NUM_ITERATION;i++) {
        ResultCollector rc = execute(member, Boolean.TRUE, functionAck, isByName, toRegister);
        t.stop();
        timeinms += t.getTimeInMs();
        assertEquals(Boolean.TRUE, ((List)rc.getResult()).get(0));
      }
      LogWriterUtils.getLogWriter().info("Time taken to execute boolean based" + NUM_ITERATION + "haveResults functions :" + timeinms);      
    }
    catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }
        
    try {      
      final HashSet testKeysSet = new HashSet();
      for (int i = 0; i < 20; i++) {
        testKeysSet.add("execKey-" + i);
      }
      TimeKeeper t = new TimeKeeper();
      long timeinms = 0;
      t.start();
      for(int i=0;i< NUM_ITERATION;i++){
        ResultCollector rc = execute(member, testKeysSet, functionAck, isByName, toRegister);
        t.stop();
        timeinms += t.getTimeInMs();
        List resultList = (List)rc.getResult();
        for(int j=0; j<20; j++) {
          assertEquals(true, ((List)(resultList).get(0)).contains("execKey-" + j));
        }
        
      }
      LogWriterUtils.getLogWriter().info("Time taken to execute setbased" + NUM_ITERATION + "haveResults functions :" + timeinms);     
    }
    catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operationssssss");
    }    
  }

  public static void allServerExecution(Boolean isByName,
      Function function, Boolean toRegister) {

    DistributedSystem.setThreadsSocketPolicy(false);
    if(toRegister.booleanValue()){
      FunctionService.registerFunction(function);
    }
    else {
      FunctionService.unregisterFunction(function.getId());
      assertNull(FunctionService.getFunction(function.getId()));
    }
    Execution member = FunctionService.onServers(pool);

    try {      
      execute(member, Boolean.TRUE, function, isByName, toRegister);
    }
    catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation allserver   ");
    }

    try {
      final HashSet testKeysSet = new HashSet();
      for (int i = 0; i < 20; i++) {
        testKeysSet.add("execKey-" + i);
      }
      execute(member, testKeysSet, function, isByName, toRegister);
    }
    catch (Exception ex) {
      ex.printStackTrace();
      LogWriterUtils.getLogWriter().info("Exception : ", ex);
      fail("Test failed after the execute operation");
    }
  }

  private static ResultCollector execute(Execution member,
      Serializable args, Function function, Boolean isByName, Boolean toRegister)
      throws Exception {
    if (isByName.booleanValue()) {// by name
      if(toRegister.booleanValue()){
        LogWriterUtils.getLogWriter().info("The function name to execute : " + function.getId());
        Execution me = member.withArgs(args);
        LogWriterUtils.getLogWriter().info("The args passed  : " + args);
        return me.execute(function.getId());
      }
      else {
        LogWriterUtils.getLogWriter().info("The function name to execute : (without Register) " + function.getId());
        Execution me = member.withArgs(args);
        LogWriterUtils.getLogWriter().info("The args passed  : " + args);
        return me.execute(function.getId(), function.hasResult(),function.isHA(),function.optimizeForWrite());
      }
    }
    else { // By Instance
      return member.withArgs(args).execute(function);
    }
  }
}
