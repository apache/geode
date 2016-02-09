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
package com.gemstone.gemfire.internal.cache.functions;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionInvocationTargetException;
import com.gemstone.gemfire.internal.cache.execute.MyFunctionExecutionException;
import com.gemstone.gemfire.internal.cache.execute.RegionFunctionContextImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;

public class TestFunction extends FunctionAdapter implements Declarable2 {
  public static final String TEST_FUNCTION10 = "TestFunction10";
  public static final String TEST_FUNCTION9 = "TestFunction9";
  public static final String TEST_FUNCTION8 = "TestFunction8";
  public static final String TEST_FUNCTION6 = "TestFunction6";
  public static final String TEST_FUNCTION5 = "TestFunction5";
  public static final String TEST_FUNCTION7 = "TestFunction7";
  public static final String TEST_FUNCTION4 = "TestFunction4";
  public static final String TEST_FUNCTION3 = "TestFunction3";
  public static final String TEST_FUNCTION2 = "TestFunction2";
  public static final String TEST_FUNCTION1 = "TestFunction1";
  public static final String TEST_FUNCTION_EXCEPTION = "TestFunctionException";
  public static final String TEST_FUNCTION_RESULT_SENDER = "TestFunctionResultSender";
  public static final String MEMBER_FUNCTION = "MemberFunction";
  public static final String TEST_FUNCTION_SOCKET_TIMEOUT = "SocketTimeOutFunction";
  public static final String TEST_FUNCTION_TIMEOUT = "executeTimeOut";
  public static final String TEST_FUNCTION_HA = "executeFunctionHA";
  public static final String TEST_FUNCTION_HA_SERVER = "executeFunctionHAOnServer";
  public static final String TEST_FUNCTION_NONHA_SERVER = "executeFunctionNonHAOnServer";
  public static final String TEST_FUNCTION_NONHA_REGION = "executeFunctionNonHAOnRegion";
  public static final String TEST_FUNCTION_HA_REGION = "executeFunctionHAOnRegion";
  public static final String TEST_FUNCTION_REEXECUTE_EXCEPTION = "executeFunctionReexecuteException";
  public static final String TEST_FUNCTION_ONSERVER_REEXECUTE_EXCEPTION = "executeFunctionReexecuteExceptionOnServer";
  public static final String TEST_FUNCTION_NO_LASTRESULT= "executeFunctionWithoutLastResult";
  public static final String TEST_FUNCTION_LASTRESULT= "executeFunctionWithLastResult";
  public static final String TEST_FUNCTION_SEND_EXCEPTION= "executeFunction_SendException";
  public static final String TEST_FUNCTION_THROW_EXCEPTION= "executeFunction_ThrowException";
  public static final String TEST_FUNCTION_RETURN_ARGS= "executeFunctionToReturnArgs";
  public static final String TEST_FUNCTION_RUNNING_FOR_LONG_TIME= "executeFunctionRunningForLongTime";
  public static final String TEST_FUNCTION_BUCKET_FILTER= "TestFunctionBucketFilter";
  public static final String TEST_FUNCTION_NONHA_NOP = "executeFunctionNoHANop";
  private static final String ID = "id";
  private static final String HAVE_RESULTS = "haveResults";
  private final Properties props;
  private static final String NOACKTEST = "NoAckTest";
  private static int retryCount = 0;
  private static int firstExecutionCount = 0;

  // Default constructor for Declarable purposes
  public TestFunction() {
    super();
    this.props = new Properties();
  }

  public TestFunction(boolean haveResults, String id) {
    this.props = new Properties();
    this.props.setProperty(HAVE_RESULTS, Boolean.toString(haveResults));
    this.props.setProperty(ID, id);
  }
  
  public TestFunction(boolean haveResults, String id, boolean hashCodeId) {
    this.props = new Properties();
    this.props.setProperty(HAVE_RESULTS, Boolean.toString(haveResults));
    id = id + hashCode();
    this.props.setProperty(ID, id);
    this.props.setProperty(NOACKTEST, Boolean.toString(hashCodeId));
    
  }
  
  /**
   * Application execution implementation
   * 
   * @since 5.8Beta
   */
  @Override
  public void execute(FunctionContext context) {
    String id = this.props.getProperty(ID); 
    String noAckTest = this.props.getProperty(NOACKTEST);
    
    if (id.equals(TEST_FUNCTION1)) {
      execute1(context);
    }
    else if (id.equals(TEST_FUNCTION2)) {
      execute2(context);
    }
    else if (id.equals(TEST_FUNCTION3)) {
      execute3(context);
    }
    else if ((id.equals(TEST_FUNCTION4)) || (id.equals(TEST_FUNCTION7))) {
      execute4(context);
    }
    else if ((id.equals(TEST_FUNCTION5)) || (id.equals(TEST_FUNCTION6))) {
      execute5(context);
    }
    else if (id.equals(TEST_FUNCTION8)) {
      execute8(context);
    }
    else if (id.equals(TEST_FUNCTION9)) {
      execute9(context);
    }
    else if (id.equals(TEST_FUNCTION_EXCEPTION)) {
      executeException(context);
    }
    else if (id.equals(TEST_FUNCTION_REEXECUTE_EXCEPTION)) {
      executeFunctionReexecuteException(context);
    }
    else if (id.equals(TEST_FUNCTION_RESULT_SENDER)) {
      executeResultSender(context);
    }
    else if(id.equals(MEMBER_FUNCTION)) {
      executeMemberFunction(context);
    }
    else if (id.equals(TEST_FUNCTION_TIMEOUT)) {
      executeTimeOut(context);
    }
    else if(id.equals(TEST_FUNCTION_SOCKET_TIMEOUT)){
      executeSocketTimeOut(context);
    }
    else if(id.equals(TEST_FUNCTION_HA)){
      executeHA(context);
    }
    else if(id.equals(TEST_FUNCTION_HA_SERVER) || id.equals(TEST_FUNCTION_NONHA_SERVER)){
      executeHAAndNonHAOnServer(context);
    }
    else if (id.equals(TEST_FUNCTION_NONHA_REGION) || id.equals(TEST_FUNCTION_HA_REGION)) {
      executeHAAndNonHAOnRegion(context);
    }
    else if(id.equals(TEST_FUNCTION_ONSERVER_REEXECUTE_EXCEPTION)){
      executeFunctionReexecuteExceptionOnServer(context);
    }
    else if (id.equals(TEST_FUNCTION_NO_LASTRESULT)) {
      executeWithNoLastResult(context);
    }
    else if (id.equals(TEST_FUNCTION_SEND_EXCEPTION)) {
      executeWithSendException(context);
    }
    else if (id.equals(TEST_FUNCTION_THROW_EXCEPTION)) {
      executeWithThrowException(context);
    }
    else if (id.equals(TEST_FUNCTION_LASTRESULT)) {
      executeWithLastResult(context);
    }
    else if (id.equals(TEST_FUNCTION_RETURN_ARGS)){
      executeFunctionReturningArgs(context);
    }
    else if(id.equals(TEST_FUNCTION_RUNNING_FOR_LONG_TIME)){
      executeFunctionRunningForLongTime(context);
    }
    else if(id.equals(TEST_FUNCTION_BUCKET_FILTER)){
      executeFunctionBucketFilter(context);
    }
    else if(id.equals(TEST_FUNCTION_NONHA_NOP)) {
      execute1(context);
    }
    else if(noAckTest.equals("true")){
      execute1(context);
    } 
  }
  
  
  public void executeFunctionRunningForLongTime(FunctionContext context){
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();    
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {     
      logger.info("Exception in executeFunctionRunningForLongTime");
    }        
    context.getResultSender().lastResult("Ran executeFunctionRunningForLongTime for 10000000");    
    
  }
  
  public void executeFunctionBucketFilter(FunctionContext context) {
    //int bucketIDAsFilter = ((Integer)context.getArguments()).intValue();
    // check if the node contains the bucket passed as filter
    RegionFunctionContextImpl rfc = (RegionFunctionContextImpl)context;
    PartitionedRegion pr = (PartitionedRegion) rfc.getDataSet();
    Set<Integer> bucketIDs = rfc.getLocalBucketSet(pr);   
    pr.getGemFireCache().getLogger().fine("LOCAL BUCKETSET ="+ bucketIDs);
    ResultSender<Integer> rs = context.<Integer>getResultSender();
    if(!pr.getDataStore().areAllBucketsHosted(bucketIDs)) {
      throw new AssertionError("bucket IDs ="+ bucketIDs + " not all hosted locally");
    }else {
      Integer[] bucketIds = bucketIDs.toArray(new Integer[0]);
      for(int i = 0 ; i < bucketIds.length -1; ++i) {
        rs.sendResult(bucketIds[i]);
      }
      rs.lastResult(bucketIds[bucketIds.length-1]);
    }
  }
  
  
  public void executeFunctionReturningArgs(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    logger.info(
        "Executing executeFunctionReturningArgs in TestFunction on Member : "
            + ds.getDistributedMember()+ "with Context : " + context);
    if (!hasResult()) {
      return;
    }
    Object[] args = (Object[])context.getArguments();
    if(args != null){
      context.getResultSender().lastResult(args[0]);
    }else{
      context.getResultSender().lastResult(Boolean.FALSE);      
    }
    

  }
  
  public void execute1(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    logger.info(
        "Executing execute1 in TestFunction on Member : "
            + ds.getDistributedMember()+ "with Context : " + context);
    if (!hasResult()) {
      return;
    }
    if (context.getArguments() instanceof Boolean) {      
      //context.getResultSender().sendResult();
      context.getResultSender().lastResult((Serializable) context.getArguments());
    }
    else if (context.getArguments() instanceof String) {
/*      String key = (String)context.getArguments();
      return key;*/
      
      context.getResultSender().lastResult((Serializable) context.getArguments());
    }
    else if (context.getArguments() instanceof Set) {
      Set origKeys = (Set)context.getArguments();
      ArrayList vals = new ArrayList();
      for (Iterator i = origKeys.iterator(); i.hasNext();) {
        Object val = i.next();
        if (val != null) {
          vals.add(val);
        }
      }
      /*return vals;*/
      context.getResultSender().lastResult(vals);
    }
    else {
     /* return Boolean.FALSE;*/
      context.getResultSender().lastResult(Boolean.FALSE);
    }
  }

  public void execute2(FunctionContext context) {
  if (context instanceof RegionFunctionContext) {
      RegionFunctionContext rfContext = (RegionFunctionContext)context;
      rfContext.getDataSet().getCache().getLogger().info(
          "Executing function :  TestFunction2.execute " + rfContext);
      if (rfContext.getArguments() instanceof Boolean) {
        /*return rfContext.getArguments();*/
        if (hasResult()) {
          rfContext.getResultSender().lastResult((Serializable) rfContext.getArguments());
        }
        else{
          rfContext.getDataSet().getCache().getLogger().info(
              "Executing function :  TestFunction2.execute " + rfContext);
          while(true && !rfContext.getDataSet().isDestroyed()){
            rfContext.getDataSet().getCache().getLogger().info(
                "For Bug43513 ");
            try {
              Thread.sleep(100);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        }
      }
      else if (rfContext.getArguments() instanceof String) {
        String key = (String)rfContext.getArguments();
        if(key.equals("TestingTimeOut")){  // for test PRFunctionExecutionDUnitTest#testRemoteMultiKeyExecution_timeout
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            rfContext.getDataSet().getCache().getLogger().warning(
                "Got Exception : Thread Interrupted" + e);
          }
        }
        if (PartitionRegionHelper.isPartitionedRegion(rfContext.getDataSet())) {
          /*return (Serializable)PartitionRegionHelper.getLocalDataForContext(rfContext).get(key);*/
          rfContext.getResultSender().lastResult((Serializable)PartitionRegionHelper.getLocalDataForContext(rfContext).get(key));
        }else{
          rfContext.getResultSender().lastResult((Serializable)rfContext.getDataSet().get(key));
        }
        /*return (Serializable)rfContext.getDataSet().get(key);*/
      }
      else if (rfContext.getArguments() instanceof Set) {
        Set origKeys = (Set)rfContext.getArguments();
        ArrayList vals = new ArrayList();
        for (Object key : origKeys) {
          Object val = PartitionRegionHelper.getLocalDataForContext(rfContext).get(key);
          if (val != null) {
            vals.add(val);
          }
        }
        rfContext.getResultSender().lastResult(vals);
        /*return vals;*/
      }
      else if (rfContext.getArguments() instanceof HashMap) {
        HashMap putData = (HashMap)rfContext.getArguments();
        for (Iterator i = putData.entrySet().iterator(); i.hasNext();) {
          Map.Entry me = (Map.Entry)i.next();
          rfContext.getDataSet().put(me.getKey(), me.getValue());
        }
        rfContext.getResultSender().lastResult(Boolean.TRUE);
      }
      else {
        rfContext.getResultSender().lastResult(Boolean.FALSE);
      }
    }
    else {
      if (hasResult()) {
        context.getResultSender().lastResult(Boolean.FALSE);
      }
      else {
        DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
        LogWriter logger = ds.getLogWriter();
        logger.info("Executing in TestFunction on Server : "
            + ds.getDistributedMember() + "with Context : " + context);
        while (ds.isConnected()) {
          logger.fine("Just executing function in infinite loop for Bug43513");
          try { Thread.sleep(250); } catch (InterruptedException e) { return; }
        }
      }
    }
    
  }

  public void execute3(FunctionContext context) {

    if (context instanceof RegionFunctionContext) { 
      RegionFunctionContext prContext = (RegionFunctionContext)context;
      prContext.getDataSet().getCache().getLogger().info(
          "Executing function : TestFunction3.execute " + prContext);
      if (prContext.getArguments() instanceof Set) {
        Set origKeys = (Set)prContext.getArguments();
        ArrayList vals = new ArrayList();
        for (Iterator i = origKeys.iterator(); i.hasNext();) {
          Object val = 
            PartitionRegionHelper.getLocalDataForContext(prContext).get(i.next());
          if (val != null) {
            vals.add(val);
          }
        }
        prContext.getResultSender().lastResult(vals);
       /* return vals;*/
      }
      else if (prContext.getFilter() != null) {
        Set origKeys = prContext.getFilter();
        ArrayList vals = new ArrayList();
        for (Iterator i = origKeys.iterator(); i.hasNext();) {
          Object val = 
            PartitionRegionHelper.getLocalDataForContext(prContext).get(i.next());
          if (val != null) {
            vals.add(val);
          }
        }
        /*return vals;*/
        //prContext.getResultSender().sendResult(vals);
        prContext.getResultSender().lastResult(vals);
      }
      else {
        /*return Boolean.FALSE;*/
        //prContext.getResultSender().sendResult(Boolean.FALSE);
        prContext.getResultSender().lastResult(Boolean.FALSE);
      }
    }
    else {
      /*return Boolean.FALSE;*/
      //context.getResultSender().sendResult(Boolean.FALSE);
      context.getResultSender().lastResult(Boolean.FALSE);
    }
  
  }

  public void execute4(FunctionContext context) {

    if (context instanceof RegionFunctionContext) {
      RegionFunctionContext prContext = (RegionFunctionContext)context;
      prContext.getDataSet().getCache().getLogger().info(
          "Executing function :  TestFunction4-7.execute " + prContext);
      if (prContext.getArguments() instanceof Boolean) {
        /*return prContext.getArguments();*/
        if(hasResult()) prContext.getResultSender().lastResult((Serializable) prContext.getArguments());
      }
      else if (prContext.getArguments() instanceof String) {
        String key = (String)prContext.getArguments();
        /*return (Serializable)PartitionRegionHelper.getLocalDataForContext(prContext).get(key);*/
        prContext.getResultSender().lastResult((Serializable)PartitionRegionHelper.getLocalDataForContext(prContext).get(key));
      }
      else if (prContext.getArguments() instanceof Set) {
        Set origKeys = (Set)prContext.getArguments();
        ArrayList vals = new ArrayList();
        for (Iterator i = origKeys.iterator(); i.hasNext();) {
          Object val = PartitionRegionHelper.getLocalDataForContext(prContext).get(i.next());
          if (val != null) {
            vals.add(val);
          }
        }
        /*return vals;*/
        //prContext.getResultSender().sendResult(vals);
        if(hasResult()) prContext.getResultSender().lastResult(vals);
      }
      else if (prContext.getArguments() instanceof HashMap) {
        HashMap putData = (HashMap)prContext.getArguments();
        for (Iterator i = putData.entrySet().iterator(); i.hasNext();) {
          Map.Entry me = (Map.Entry)i.next();
          prContext.getDataSet().put(me.getKey(), me.getValue());
        }
        /*return Boolean.TRUE;*/
        //prContext.getResultSender().sendResult(Boolean.TRUE);
        if(hasResult()) prContext.getResultSender().lastResult(Boolean.TRUE);
      }
      else {
        /*return Boolean.FALSE;*/
        //prContext.getResultSender().sendResult(Boolean.FALSE);
        if(hasResult()) prContext.getResultSender().lastResult(Boolean.FALSE);
      }
    }else{
      /*return Boolean.FALSE;*/
      //context.getResultSender().sendResult(Boolean.FALSE);
      if(hasResult()) context.getResultSender().lastResult(Boolean.FALSE);
    }
  
  }

  public void execute5(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    logger.info(
        "Executing executeException in TestFunction on Member : "
            + ds.getDistributedMember()+ "with Context : " + context);
    if (this.hasResult()) {
      if (context.getArguments() instanceof String) {
        context.getResultSender().lastResult("Success");
      }
      else {
        context.getResultSender().lastResult("Failure");
      }
    }
  }
  
 
  
  public void executeMemberFunction(FunctionContext context) {
    if (this.hasResult()) {
      if (context.getArguments() instanceof String) {
        String args = (String)context.getArguments();
        if (!args.equalsIgnoreCase("Key")) {
          Assert.assertTrue(args.equals(InternalDistributedSystem
              .getAnyInstance().getDistributedMember().getId()),"Args was supposed to be ["+InternalDistributedSystem
              .getAnyInstance().getDistributedMember().getId()+"] but was:["+args+"]");
        }
        context.getResultSender().lastResult("Success");
      }
      else {
        context.getResultSender().lastResult("Failure");
      }
    }
  }

  public void execute8(FunctionContext context) {
    if (context instanceof RegionFunctionContext) { 
      RegionFunctionContext rfContext = (RegionFunctionContext)context;
      rfContext.getDataSet().getCache().getLogger().info(
          "Executing function :  TestFunction8.execute "
              + rfContext);

      if (rfContext.getArguments() instanceof Boolean) {
        /*return rfContext.getArguments();*/
        //rfContext.getResultSender().sendResult(rfContext.getArguments());
        rfContext.getResultSender().lastResult((Serializable) rfContext.getArguments());
      }
      else if (rfContext.getArguments() instanceof String) {
        String op = (String)rfContext.getArguments();
        if (op.equals("DELETE")) {
          Region r = rfContext.getDataSet();
          Set s = rfContext.getFilter();
          if(s == null){
           /* return Boolean.FALSE;*/
            //rfContext.getResultSender().sendResult(Boolean.FALSE);
            rfContext.getResultSender().lastResult(Boolean.FALSE);
          }
          Iterator itr = s.iterator();
          while (itr.hasNext()) {
            r.destroy(itr.next());
          }
          /*return Boolean.TRUE;*/
          //rfContext.getResultSender().sendResult(Boolean.TRUE);
          rfContext.getResultSender().lastResult(Boolean.TRUE);
        }
        else if (op.equals("GET")) {
          Region r = rfContext.getDataSet();
          Set s = rfContext.getFilter();
          if(s == null){
            /*return Boolean.FALSE;*/
            rfContext.getResultSender().lastResult(Boolean.FALSE);
          }
          Iterator itr = s.iterator();
          ArrayList vals = new ArrayList();
          while (itr.hasNext()) {
            vals.add(r.get(itr.next()));
          }
          /*return vals;*/
          rfContext.getResultSender().lastResult(vals);
        }
        /*return Boolean.FALSE;*/
        rfContext.getResultSender().lastResult(Boolean.FALSE);
      }
      else {
        /*return Boolean.FALSE;*/
        rfContext.getResultSender().lastResult(Boolean.FALSE);
      }
    }
    else {
      /*return Boolean.FALSE;*/
      context.getResultSender().lastResult(Boolean.FALSE);
    }
  }  
  
  public void execute9(FunctionContext context) {
    if (context instanceof RegionFunctionContext) { 
      RegionFunctionContext rfContext = (RegionFunctionContext)context;
      rfContext.getDataSet().getCache().getLogger().info(
          "Executing function :  TestFunction9.execute " + rfContext);
      if (rfContext.getArguments() instanceof Boolean) {
        rfContext.getResultSender().lastResult((Serializable) rfContext.getArguments());
      }
      else if (rfContext.getArguments() instanceof String) {
        String key = (String)rfContext.getArguments();
        if(key.equals("TestingTimeOut")){  // for test PRFunctionExecutionDUnitTest#testRemoteMultiKeyExecution_timeout
          try{
            synchronized (this) {
              this.wait(2000);
            }
          }catch(InterruptedException e){
            rfContext.getDataSet().getCache().getLogger().warning(
                "Got Exception : Thread Interrupted" + e);
          }
        }
        if(context instanceof RegionFunctionContext){
          RegionFunctionContext prContext = (RegionFunctionContext)context;
          if (PartitionRegionHelper.isPartitionedRegion(prContext.getDataSet())) {
            rfContext.getResultSender().lastResult(
                (Serializable)PartitionRegionHelper.getLocalDataForContext(
                    prContext).get(key));
          }
        }
      }
      else if (rfContext.getArguments() instanceof Set) {
        Set origKeys = (Set)rfContext.getArguments();
        ArrayList vals = new ArrayList();
        for (Iterator i = origKeys.iterator(); i.hasNext();) {
          Object val = null;
          if(context instanceof RegionFunctionContext){
            RegionFunctionContext prContext = (RegionFunctionContext)context;
            val = PartitionRegionHelper.getLocalDataForContext(prContext).get(i.next());
          }else{
            val = rfContext.getDataSet().get(i.next());
          }

          if (i.hasNext())
            rfContext.getResultSender().sendResult((Serializable)val);
          else
            rfContext.getResultSender().lastResult((Serializable)val);
          
          if (val != null) {
            vals.add(val);
          }
        }
      }
      else if (rfContext.getArguments() instanceof HashMap) {
        HashMap putData = (HashMap)rfContext.getArguments();
        for (Iterator i = putData.entrySet().iterator(); i.hasNext();) {
          Map.Entry me = (Map.Entry)i.next();
          rfContext.getDataSet().put(me.getKey(), me.getValue());
        }
        rfContext.getResultSender().lastResult(Boolean.TRUE);
      }
      else {
        rfContext.getResultSender().lastResult(Boolean.TRUE);
      }
    }else{
      
    }
    context.getResultSender().lastResult("ABCD");
  }
  
  private void executeException(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    logger.fine(
        "Executing executeException in TestFunction on Member : "
            + ds.getDistributedMember()+ "with Context : " + context);
    if (context.getArguments() instanceof Boolean) {
      logger.fine("MyFunctionExecutionException Exception is intentionally thrown");
      throw new MyFunctionExecutionException("I have been thrown from TestFunction");
    }
    else if (context.getArguments() instanceof String) {
      String key = (String)context.getArguments();
      logger.fine("Result sent back :"  + key);
      context.getResultSender().lastResult(key);
    }
    else if (context.getArguments() instanceof Set) {
      Set origKeys = (Set)context.getArguments();
      ArrayList vals = new ArrayList();
      for (Iterator i = origKeys.iterator(); i.hasNext();) {
        Object val = i.next();
        if (val != null) {
          vals.add(val);
        }
      }
      logger.fine("Result sent back :"  + vals);
      context.getResultSender().lastResult(vals);
    }
    else {
      logger.fine("Result sent back :"  + Boolean.FALSE);
      context.getResultSender().lastResult(Boolean.FALSE);
    }
  }
  
 
  private void executeWithSendException(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    logger.fine(
        "Executing executeWithSendException in TestFunction on Member : "
            + ds.getDistributedMember()+ "with Context : " + context);
    if (context.getArguments() instanceof Boolean) {
      context.getResultSender().sendException(new MyFunctionExecutionException("I have been send from TestFunction"));
    }
    else if (context.getArguments() instanceof String) {
      String arg = (String)context.getArguments();
      if (arg.equals("Multiple")) {
        logger.fine("Sending Exception First time");
        context.getResultSender().sendException(
            new MyFunctionExecutionException("I have been send from TestFunction"));
        logger.fine("Sending Exception Second time");
        context.getResultSender().sendException(
            new MyFunctionExecutionException("I have been send from TestFunction"));
      }
    }
    else if (context.getArguments() instanceof Set) {
      Set args = (Set)context.getArguments();
      for(int i = 0 ;i < args.size() ; i++){
        context.getResultSender().sendResult(new Integer(i));
      }
      context.getResultSender().sendException(
          new MyFunctionExecutionException("I have been thrown from TestFunction with set"));
    }
    else {
      logger.fine("Result sent back :"  + Boolean.FALSE);
      context.getResultSender().lastResult(Boolean.FALSE);
    }
  }
  
  private void executeWithThrowException(FunctionContext context) {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    RegionFunctionContext rfContext = (RegionFunctionContext)context;
    LogWriter logger = ds.getLogWriter();
    logger.fine(
        "Executing executeWithThrowException in TestFunction on Member : "
            + ds.getDistributedMember()+ "with Context : " + context);
    if (context.getArguments() instanceof Boolean) {
      logger.fine("MyFunctionExecutionException Exception is intentionally thrown");
      throw new MyFunctionExecutionException("I have been thrown from TestFunction");
    }
    else if (rfContext.getArguments() instanceof Set) {
      Set origKeys = (Set)rfContext.getArguments();
      for (Iterator i = origKeys.iterator(); i.hasNext();) {
        Region r = PartitionRegionHelper.getLocalDataForContext(rfContext);
        Object key = i.next();
        Object val = r.get(key);
        if (val != null) {
          throw new MyFunctionExecutionException("I have been thrown from TestFunction");
        }
      }
    }
    else {
      logger.fine("Result sent back :"  + Boolean.FALSE);
      rfContext.getResultSender().lastResult(Boolean.FALSE);
    }
  }
  
  private void executeFunctionReexecuteException(FunctionContext context) {
    retryCount++;
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    logger.fine("Executing executeException in TestFunction on Member : "
        + ds.getDistributedMember() + "with Context : " + context);
    if (retryCount >= 5) {
      logger
          .fine("Tried Function Execution 5 times. Now Returning after 5 attempts");
      context.getResultSender().lastResult(new Integer(retryCount));
      retryCount = 0;
      return;
    }
    if (context.getArguments() instanceof Boolean) {
      logger.fine("MyFunctionExecutionException is intentionally thrown");
      throw new FunctionInvocationTargetException(new MyFunctionExecutionException(
          "I have been thrown from TestFunction"));
    }
  }
   
  private void executeResultSender(FunctionContext context) {
    ResultSender resultSender = context.getResultSender();
    if (context instanceof RegionFunctionContext) { 
      RegionFunctionContext rfContext = (RegionFunctionContext)context;
      rfContext.getDataSet().getCache().getLogger().info(
          "Executing function :  TestFunctionexecuteResultSender.execute " + rfContext);
      if (rfContext.getArguments() instanceof Boolean) {
        if (this.hasResult()) {
          resultSender.lastResult((Serializable) rfContext.getArguments());
        }
      }
      else if (rfContext.getArguments() instanceof Set) {
        Set origKeys = (Set)rfContext.getArguments();
        Object[] objectArray = origKeys.toArray();
        int size = objectArray.length;
        int i = 0;
        for( ;i < (size-1) ;i++){
          Object val = PartitionRegionHelper.getLocalDataForContext(rfContext).get(objectArray[i]);
          if (val != null) {
            resultSender.sendResult((Serializable)val);
          }
        }
        resultSender.lastResult((Serializable)objectArray[i]);
      }
      else {
        resultSender.lastResult(Boolean.FALSE);
      }
    }else{
      resultSender.lastResult(Boolean.FALSE);
    }
  }
  
  public void executeSocketTimeOut(FunctionContext context) {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        return false;
      }

      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, 15000, 1000, false);
    if (context.getArguments() instanceof Boolean) {
      context.getResultSender().lastResult((Serializable) context.getArguments());
    }
    else if (context.getArguments() instanceof String) {
      context.getResultSender().lastResult((Serializable) context.getArguments());
    }
    else if (context.getArguments() instanceof Set) {
      Set origKeys = (Set)context.getArguments();
      ArrayList vals = new ArrayList();
      for (Iterator i = origKeys.iterator(); i.hasNext();) {
        Object val = i.next();
        if (val != null) {
          vals.add(val);
        }
      }  
      /* return vals; */
      context.getResultSender().lastResult(vals);
    }
    else {
      /* return Boolean.FALSE; */
      context.getResultSender().lastResult(Boolean.FALSE);
    }
  }

  public void executeTimeOut(FunctionContext context) {
    
    try{
      synchronized (this) {
        this.wait(2000);
      }
    }catch(InterruptedException e){
     
    }
    if (context instanceof RegionFunctionContext) { 
      RegionFunctionContext rfContext = (RegionFunctionContext)context;
      rfContext.getDataSet().getCache().getLogger().info(
          "Executing function :  TestFunction.execute " + rfContext);
      if (rfContext.getArguments() instanceof Boolean) {
        /*return rfContext.getArguments();*/
        rfContext.getResultSender().lastResult((Serializable) rfContext.getArguments());
      }
      else if (rfContext.getArguments() instanceof String) {
        String key = (String)rfContext.getArguments();
        if(key.equals("TestingTimeOut")){  // for test PRFunctionExecutionDUnitTest#testRemoteMultiKeyExecution_timeout
          try{
            synchronized (this) {
              this.wait(2000);
            }
          }catch(InterruptedException e){
            rfContext.getDataSet().getCache().getLogger().warning(
                "Got Exception : Thread Interrupted" + e);
          }
        }
        try{
          synchronized (this) {
            this.wait(2000);
          }
        }catch(InterruptedException e){
          rfContext.getDataSet().getCache().getLogger().warning(
              "Got Exception : Thread Interrupted" + e);
        }
        if (PartitionRegionHelper.isPartitionedRegion(rfContext.getDataSet())) {
          /*return (Serializable)PartitionRegionHelper.getLocalDataForContext(rfContext).get(key);*/
          rfContext.getResultSender().lastResult((Serializable)PartitionRegionHelper.getLocalDataForContext(rfContext).get(key));
        }else{
          rfContext.getResultSender().lastResult((Serializable)rfContext.getDataSet().get(key));
        }
        /*return (Serializable)rfContext.getDataSet().get(key);*/
      }
      else if (rfContext.getArguments() instanceof Set) {
        Set origKeys = (Set)rfContext.getArguments();
        ArrayList vals = new ArrayList();
        for (Iterator i = origKeys.iterator(); i.hasNext();) {
          Object val = PartitionRegionHelper.getLocalDataForContext(rfContext).get(i.next());
          if (val != null) {
            vals.add(val);
          }
        }
        rfContext.getResultSender().lastResult(vals);
        /*return vals;*/
      }
      else if (rfContext.getArguments() instanceof HashMap) {
        HashMap putData = (HashMap)rfContext.getArguments();
        for (Iterator i = putData.entrySet().iterator(); i.hasNext();) {
          Map.Entry me = (Map.Entry)i.next();
          rfContext.getDataSet().put(me.getKey(), me.getValue());
        }
        rfContext.getResultSender().lastResult(Boolean.TRUE);
      }
      else {
        rfContext.getResultSender().lastResult(Boolean.FALSE);
      }
    }else{
      context.getResultSender().lastResult(Boolean.FALSE);
    }
    
  }
  
  public void executeHA(FunctionContext context) {
    RegionFunctionContext rcontext = (RegionFunctionContext)context;
    Region region = rcontext.getDataSet();
    region.getCache().getLogger().fine("executeHA#execute( " + rcontext + " )");
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        return false;
      }

      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, 10000, 500, false);
    rcontext.getResultSender().lastResult((Serializable) rcontext.getArguments());
  }
  
  public void executeHAAndNonHAOnServer(FunctionContext context) {
    List<CacheServer> servers = CacheFactory.getAnyInstance().getCacheServers();
    ArrayList<String> args = (ArrayList<String>)context.getArguments();
    Region r = CacheFactory.getAnyInstance().getRegion(args.get(0));
    String testName = args.get(1);
    Integer numTimesStopped = (Integer)r.get("stopped");
    Integer numTimesSentResult = (Integer)r.get("sentresult");
    
    if (context.isPossibleDuplicate()) {
      if (testName.equals("serverExecutionHATwoServerDown")) {
        if ((Integer)r.get("stopped") == 2) {
          if (numTimesSentResult == null) {
            r.put("sentresult", 1);
          } else {
            r.put("sentresult", ++numTimesSentResult);
          }
          context.getResultSender().lastResult(args.get(0));
        } else {
          r.put("stopped", ++numTimesStopped);
          for (CacheServer s : servers) {
            if (((CacheServerImpl)s).getSystem().getDistributedMember().equals(
                ((GemFireCacheImpl)CacheFactory.getAnyInstance()).getMyId())) {
              s.stop();
              DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
              ds.disconnect();
            }
          }
        }
      } else if (testName.equals("serverExecutionHAOneServerDown")) {
        if (numTimesSentResult == null) {
          r.put("sentresult", 1);
        } else {
          r.put("sentresult", ++numTimesSentResult);
        }
        context.getResultSender().lastResult(args.get(0));
      }
      else{
        context.getResultSender().lastResult(args.get(0));
      }
    } else {
      if (numTimesStopped == null) {
        r.put("stopped", 1);
      } else {
        r.put("stopped", ++numTimesStopped);
      }
      
      for (CacheServer s : servers) {
        if (((CacheServerImpl)s).getSystem().getDistributedMember().equals(
            ((GemFireCacheImpl)CacheFactory.getAnyInstance()).getMyId())) {
          s.stop();
          DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
          ds.disconnect();
        }
      }
      
    }
  }
  
  public void executeHAAndNonHAOnRegion(FunctionContext context) {
    List<CacheServer> servers = CacheFactory.getAnyInstance().getCacheServers();
    ArrayList<String> args = (ArrayList<String>)context.getArguments();

    RegionFunctionContext rfContext = (RegionFunctionContext)context;
    rfContext.getDataSet().getCache().getLogger()
        .info("Executing function :  executeHAAndNonHAOnRegion " + rfContext);

    Region r = CacheFactory.getAnyInstance().getRegion(args.get(0));
    String testName = args.get(1);
    Integer numTimesStopped = (Integer)r.get("stopped");
    Integer numTimesSentResult = (Integer)r.get("sentresult");

    if (context.isPossibleDuplicate()) {
      if (testName.equals("regionExecutionHATwoServerDown")) {
        if ((Integer)r.get("stopped") == 2) {
          if (numTimesSentResult == null) {
            r.put("sentresult", 1);
          } else {
            r.put("sentresult", ++numTimesSentResult);
          }
          context.getResultSender().lastResult(args.get(0));
        } else {
          r.put("stopped", ++numTimesStopped);
          for (CacheServer s : servers) {
            if (((CacheServerImpl)s).getSystem().getDistributedMember().equals(
                ((GemFireCacheImpl)CacheFactory.getAnyInstance()).getMyId())) {
              s.stop();
              DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
              ds.disconnect();
            }
          }
        }
      } else if (testName.equals("regionExecutionHAOneServerDown")) {
        if (numTimesSentResult == null) {
          r.put("sentresult", 1);
        } else {
          r.put("sentresult", ++numTimesSentResult);
        }
        context.getResultSender().lastResult(args.get(0));
      } else {
        context.getResultSender().lastResult(args.get(0));
      }
    } else {
      if (numTimesStopped == null) {
        r.put("stopped", 1);
      } else {
        r.put("stopped", ++numTimesStopped);
      }
      
      for (CacheServer s : servers) {
        if (((CacheServerImpl)s).getSystem().getDistributedMember().equals(
            ((GemFireCacheImpl)CacheFactory.getAnyInstance()).getMyId())) {
          s.stop();
          DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
          ds.disconnect();
        }
      }
      context.getResultSender().lastResult(args.get(0));
    }
  }
  
  private void executeFunctionReexecuteExceptionOnServer(FunctionContext context) {
    if(context.isPossibleDuplicate()) {
      retryCount++;
    }
    else {
      firstExecutionCount++;  
    }
    
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    logger.fine("Executing executeException in TestFunction on Member : "
        + ds.getDistributedMember() + "with Context : " + context);
    if (retryCount >= 5) {
      logger
          .fine("Tried Function Execution 5 times. Now Returning after 5 attempts");
      context.getResultSender().sendResult(new Integer(firstExecutionCount));
      context.getResultSender().lastResult(new Integer(retryCount));
      firstExecutionCount = 0;
      retryCount = 0;
      return;
    }
    if (context.getArguments() instanceof Boolean) {
      logger.fine("MyFunctionExecutionException Exception is intentionally thrown");
      throw new InternalFunctionInvocationTargetException(new MyFunctionExecutionException(
          "I have been thrown from TestFunction"));
    }
  }
  
  public void executeWithNoLastResult(FunctionContext context) {
    // add expected exception
    InternalDistributedSystem.getConnectedInstance().getLogWriter().info(
        "<ExpectedException action=add>did not send last result"
        + "</ExpectedException>");
    context.getResultSender().sendResult((Serializable) context.getArguments());
  }
  
  private void executeWithLastResult(FunctionContext context) {
    RegionFunctionContext rfContext = (RegionFunctionContext)context;
    final PartitionedRegion pr = (PartitionedRegion)rfContext.getDataSet();
    
    ResourceManager resMan = pr.getCache().getResourceManager();
    RebalanceFactory factory = resMan.createRebalanceFactory();
    RebalanceOperation rebalanceOp = factory.start();
    try {
      RebalanceResults rebalanceResults = rebalanceOp.getResults();
    }
    catch (CancellationException e) {
      e.printStackTrace();
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
    context.getResultSender().lastResult((Serializable) context.getArguments());
  }

  /**
   * Get the function identifier, used by clients to invoke this function
   * 
   * @return an object identifying this function
   * @since 5.8Beta
   */
  @Override
  public String getId() {
    return this.props.getProperty(ID);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof TestFunction)) {
      return false;      
    }
    TestFunction function = (TestFunction)obj;
    if (!this.props.equals(function.getConfig())) {
      return false;
    }
    return true;
  }

  @Override
  public boolean hasResult() {
    return Boolean.valueOf(this.props.getProperty(HAVE_RESULTS)).booleanValue();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.xmlcache.Declarable2#getConfig()
   */
  public Properties getConfig() {
    return this.props;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    this.props.putAll(props);
  }
  
  @Override
  public boolean isHA() {
    if (getId().equals(TEST_FUNCTION10)) {
      return true;
    }
    if (getId().equals(TEST_FUNCTION_NONHA_SERVER) || getId().equals(TEST_FUNCTION_NONHA_REGION) || getId().equals(TEST_FUNCTION_NONHA_NOP)) {
      return false;
    }
    return Boolean.valueOf(this.props.getProperty(HAVE_RESULTS)).booleanValue();
  }
  
}
