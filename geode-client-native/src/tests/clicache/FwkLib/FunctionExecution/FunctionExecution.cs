//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;

  public class FunctionExecution : FwkTest
  {
    protected Region GetRegion()
    {
      return GetRegion(null);
    }

    protected Region GetRegion(string regionName)
    {
      Region region;
      if (regionName == null)
      {
        region = GetRootRegion();
        if (region == null)
        {
          Region[] rootRegions = CacheHelper.DCache.RootRegions();
          if (rootRegions != null && rootRegions.Length > 0)
          {
            region = rootRegions[Util.Rand(rootRegions.Length)];
          }
        }
      }
      else
      {
        region = CacheHelper.GetRegion(regionName);
      }
      return region;
    }
    public virtual void DoCreateRegion()
    {
      FwkInfo("In DoCreateRegion()");
      try
      {
        Region region = CreateRootRegion();
        if (region == null)
        {
          FwkException("DoCreateRegion()  could not create region.");
        }
        FwkInfo("DoCreateRegion()  Created region '{0}'", region.Name);
      }
      catch (Exception ex)
      {
        FwkException("DoCreateRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("DoCreateRegion() complete.");
    }
    public void DoCloseCache()
    {
      FwkInfo("DoCloseCache()  Closing cache and disconnecting from" +
        " distributed system.");
      CacheHelper.Close();
    }
    public void DoLoadRegion()
    {
      FwkInfo("In DoLoadRegion()");
      try
      {
        Region region = GetRegion();
        ResetKey("distinctKeys");
        int numKeys = GetUIntValue("distinctKeys");
        bool isUpdate = GetBoolValue("update");
        string key = null;
        string value = null;
        for (int j = 1; j < numKeys; j++)
        {
          key = "key-" + j;
          if (isUpdate)
            value = "valueUpdate-" + j;
          else
            value = "valueCreate-" + j;
          region.Put(key, value);
        }
      }
      catch (Exception ex)
      {
        FwkException("DoLoadRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("DoLoadRegion() complete.");
    }
    public void DoAddDestroyNewKeysFunction()
    {
      FwkInfo("In DoAddDestroyNewKeysFunction()");
      Region region = GetRegion();
      ResetKey("distinctKeys");
      Int32 numKeys = GetUIntValue("distinctKeys");
      int clientNum = Util.ClientNum;
      IGFSerializable[] filterObj = new IGFSerializable[numKeys];
      try
      {
        for(int j = 0; j < numKeys; j++)
        {
          filterObj[j] = new CacheableString("KEY--" + clientNum + "--" + j);
        }
        string opcode = GetStringValue( "entryOps" );
        if(opcode == "destroy")
          ExecuteFunction(filterObj,"destroy");
        else
          ExecuteFunction(filterObj,"addKey");
      }
      catch (Exception ex)
      {
        FwkException("DoAddDestroyNewKeysFunction() Caught Exception: {0}", ex);
      }
      FwkInfo("DoAddDestroyNewKeysFunction() complete.");
    }
    private void ExecuteFunction(IGFSerializable[] filterObj, string ops)
    {
      FwkInfo("In ExecuteFunction()");
      try
      {
        ResetKey( "getResult" );
        Boolean getresult = GetBoolValue("getResult");
        ResetKey( "replicated" );
        bool isReplicate = GetBoolValue("replicated");
        ResetKey( "distinctKeys" );
        Int32 numKeys = GetUIntValue( "distinctKeys" );
        CacheableVector args = new CacheableVector();
        if (filterObj == null)
        {
          int clntId = Util.ClientNum;
          filterObj = new IGFSerializable[1];
          Random rnd = new Random();
          filterObj[0] = new CacheableString("KEY--" + clntId + "--" + rnd.Next(numKeys));
          args.Add(filterObj[0]);
        }
        else
        {
          for (int i = 0; i < filterObj.Length; i++)
          {
            args.Add(filterObj[i]);
          }
        }
        Execution exc = null;
        Pool pptr = null;
        Region region = GetRegion();
        ResetKey("executionMode");
        string executionMode = GetStringValue( "executionMode" );
        ResetKey("poolName");
        string poolname = GetStringValue( "poolName" );
        string funcName = null;
        if(executionMode == "onServers" || executionMode  == "onServer"){
          pptr = PoolManager.Find(poolname);
          if(getresult)
            funcName = "ServerOperationsFunction";
          else
            funcName = "ServerOperationsWithOutResultFunction";
        }
        if ( executionMode == "onServers") {
          exc = FunctionService.OnServers(pptr);
        } if ( executionMode == "onServer"){
          exc = FunctionService.OnServer(pptr);
        }else if( executionMode == "onRegion"){
          exc = FunctionService.OnRegion(region);
          if(getresult)
            funcName = "RegionOperationsFunction";
          else
            funcName = "RegionOperationsWithOutResultFunction";
        }
        IGFSerializable[] executeFunctionResult = null;
        if(!isReplicate){
          if(getresult == true){
            if(executionMode == "onRegion"){
              executeFunctionResult = exc.WithArgs(new CacheableString(ops)).WithFilter(filterObj).Execute(funcName, getresult, 15, true, true).GetResult();
            }else{
              args.Add(new CacheableString(ops));
              executeFunctionResult = exc.WithArgs(args).Execute(funcName, getresult, 15, true, true).GetResult();
            }
          }else {
            if(executionMode == "onRegion"){
              exc.WithArgs(new CacheableString(ops)).WithFilter(filterObj).Execute(funcName, getresult, 15, false, true);
            } else {
              args.Add(new CacheableString(ops));
              exc.WithArgs(args).Execute(funcName, getresult, 15, false, true);
            }
          }
        } else {
          args.Add(new CacheableString(ops));
          if (getresult)
            executeFunctionResult = exc.WithArgs(args).Execute(funcName, getresult,15, true, true).GetResult();
          else
            executeFunctionResult = exc.WithArgs(args).Execute(funcName, getresult,15, false, true).GetResult();
        }
        Thread.Sleep(30000);
        if (ops == "addKey")
          VerifyAddNewResult(executeFunctionResult, filterObj);
        else {
          VerifyResult(executeFunctionResult, filterObj, ops);
        }
      }
      catch (Exception ex)
      {
        FwkException("ExecuteFunction() Caught Exception: {0}", ex);
      }
      FwkInfo("ExecuteFunction() complete.");
    }
    public void VerifyAddNewResult(IGFSerializable[] exefuncResult, IGFSerializable[] filterObj)
    {
      FwkInfo("In VerifyAddNewResult()");
      try
      {
        Thread.Sleep(30000);
        Region region = GetRegion();
        CacheableString value = null;
        if (filterObj != null)
        {
          for (int i = 0; i < filterObj.Length; i++)
          {
            CacheableKey key = filterObj[i] as CacheableKey;
            for (int cnt = 0; cnt < 100; cnt++)
            {
              value = region.Get(key) as CacheableString;
            }
            CacheableString expectedVal = filterObj[i] as CacheableString;
            if (!(expectedVal.Value.Equals(value.Value)))
            {
              FwkException("VerifyAddNewResult Failed: expected value is {0} and found {1} for key {2}", expectedVal.Value, value.Value, key.ToString());
            }
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("VerifyAddNewResult() Caught Exception: {0}", ex);
      }
      FwkInfo("VerifyAddNewResult() complete.");
    }
    public void VerifyResult(IGFSerializable[] exefuncResult, IGFSerializable[] filterObj, string ops)
    {
      FwkInfo("In VerifyResult()");
      try
      {
        Region region = GetRegion();
        string buf = null;
        if(exefuncResult != null)
        {
          CacheableBoolean lastResult = exefuncResult[0] as CacheableBoolean;
          if (lastResult != null && lastResult.Value != true)
            FwkException("FunctionExecution::VerifyResult failed, last result is not true");
        }
        CacheableString value = null;
        if(filterObj != null )
        {
            CacheableKey key = filterObj[0] as CacheableKey;
            for (int i = 0; i<50;i++){
              value = region.Get(key) as CacheableString;
            }
            if(ops == "update" || ops == "get"){
              if(value.Value.IndexOf("update_") == 0)
                buf = "update_" + key.ToString();
              else
                buf = key.ToString();
              if(!(buf.Equals(value.Value)))
                 FwkException("VerifyResult Failed: expected value is {0} and found {1} for key {2} for operation {3}" ,buf ,value.Value,key.ToString(),ops);
            } else if(ops == "destroy"){
                if(value == null){
                  ExecuteFunction(filterObj,"addKey");
                }else{
                  FwkException("FunctionExecution::VerifyResult failed to destroy key {0}",key.ToString());
                }
            } else if(ops == "invalidate"){
              if(value == null)
              {
                ExecuteFunction(filterObj,"update");
              }else {
                FwkException("FunctionExecution::VerifyResult Failed for invalidate key {0}" ,key.ToString());
              }
            }
        }
      }
      catch (Exception ex)
      {
        FwkException("VerifyResult() Caught Exception: {0}", ex);
      }
      FwkInfo("VerifyResult() complete.");
    }
    public void DoExecuteFunctions()
    {
      FwkInfo("In DoExecuteFunctions()");
      int secondsToRun = GetTimeValue("workTime");
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;
      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);
      string opcode = null;
      while (now < end)
      {
        try
        {
          opcode = GetStringValue("entryOps");
          ExecuteFunction(null,opcode);
        }
        catch (Exception ex)
        {
          FwkException("DoExecuteFunctions() Caught Exception: {0}", ex);
        }
        now = DateTime.Now;
      }
      FwkInfo("DoExecuteFunctions() complete.");
    }
    public void DoExecuteExceptionHandling()
    {
      FwkInfo("In DoExecuteExceptionHandling()");
      int secondsToRun = GetTimeValue("workTime");
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;
      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);
      string opcode = null;
      while (now < end)
      {
        try
        {
          opcode = GetStringValue("entryOps");
          if(opcode == "ParitionedRegionFunctionExecution")
          {
            DoParitionedRegionFunctionExecution();
          }
          else if (opcode == "ReplicatedRegionFunctionExecution")
          {
            DoReplicatedRegionFunctionExecution();
          }
          else if (opcode == "FireAndForgetFunctionExecution")
          {
            DoFireAndForgetFunctionExecution();
          }
          else if (opcode == "OnServersFunctionExcecution")
          {
            DoOnServersFunctionExcecution();
          }
          
        }
        catch (Exception ex)
        {
          FwkException( "Caught unexpected {0} during exception handling for {1} operation: {2} : exiting task." ,ex.ToString(), opcode,ex.Message);

        }
        now = DateTime.Now;
      }
      FwkInfo("DoExecuteExceptionHandling() complete.");
    }
    public void DoParitionedRegionFunctionExecution()
    {
      FwkInfo("In DoParitionedRegionFunctionExecution()");
      Execution exc = null;
      Region region = GetRegion("partitionedRegion");
      try
      {
         exc = FunctionService.OnRegion(region);
         MyResultCollector myRC = new MyResultCollector();
         Random rnd = new Random();
         if(rnd.Next(100) % 2 == 0)
         {
           //Execution on partitionedRegion with no filter
           exc = exc.WithCollector(myRC);
         }
         else
         {
           //Execution on partitionedRegion with filter
           ICacheableKey[] keys = region.GetKeys();
           IGFSerializable[] filterObj = new IGFSerializable[keys.Length];
           for (int i = 0; i < keys.Length; i++)
           {
             filterObj[i] = keys[i];
           }
           exc = exc.WithFilter(filterObj).WithCollector(myRC);
         }
         // execute function
         IGFSerializable[] executeFunctionResult = exc.Execute("ExceptionHandlingFunction", true,30, true, true).GetResult();
      }
      catch (FunctionExecutionException)
      {
        //expected exception
      }
      catch (GemFire.Cache.TimeoutException)
      {
        //expected exception
      }
      FwkInfo("DoParitionedRegionFunctionExecution() complete.");
    }
    public void DoReplicatedRegionFunctionExecution()
    {
      FwkInfo("In DoReplicatedRegionFunctionExecution()");
      Execution exc = null;
      Region region = GetRegion("replicatedRegion");
      try
      {
        exc = FunctionService.OnRegion(region);
        MyResultCollector myRC = new MyResultCollector();
        exc = exc.WithCollector(myRC);
        // execute function
        IGFSerializable[] executeFunctionResult = exc.Execute("ExceptionHandlingFunction", true,30, true, true).GetResult();
      }
      catch (FunctionExecutionException)
      {
        //expected exception
      }
      catch (GemFire.Cache.TimeoutException)
      {
        //expected exception
      }
      FwkInfo("DoReplicatedRegionFunctionExecution() complete.");
    }
    public void DoFireAndForgetFunctionExecution()
    {
      FwkInfo("In DoFireAndForgetFunctionExecution()");
      string name = null;
      Random rnd = new Random();
      if(rnd.Next(100) % 2 == 0){
        //Execution Fire and forget on partitioned region
        name = "partitionedRegion";
      }
      else
      {
        //Execution Fire and forget on replicated region
        name = "replicatedRegion";
      }
      Region region = GetRegion(name);
      try
      {
        MyResultCollector myRC = new MyResultCollector();
        Execution exc = FunctionService.OnRegion(region).WithCollector(myRC);
        // execute function
        IResultCollector rc = exc.Execute("FireNForget", false, 30,false);
      }
      catch (FunctionExecutionException)
      {
        //expected exception
      }
      catch (GemFire.Cache.TimeoutException)
      {
        //expected exception
      }
      FwkInfo("DoFireAndForgetFunctionExecution() complete.");
    }
    public void DoOnServersFunctionExcecution()
    {
      FwkInfo("In DoOnServersFunctionExcecution()");
      ResetKey("poolName");
      string poolname = GetStringValue("poolName");
      Execution exc = null;
      try
      {
        Pool pptr = PoolManager.Find(poolname);
        MyResultCollector myRC = new MyResultCollector();
        exc = FunctionService.OnServers(pptr).WithCollector(myRC);
        // execute function
        IGFSerializable[] executeFunctionResult = exc.Execute("ExceptionHandlingFunction", true, 30, true, true).GetResult();
      }
      catch (FunctionExecutionException)
      {
        //expected exception
      }
      catch (GemFire.Cache.TimeoutException)
      {
        //expected exception
      }
      FwkInfo("DoOnServersFunctionExcecution() complete.");
    }

    public void DoExecuteFunctionsHA()
    {
      ResetKey("getResult");
      bool getresult = GetBoolValue("getResult");
      ResetKey("distinctKeys");
      int numKeys = GetUIntValue("distinctKeys");
      Region region = GetRegion();
      IGFSerializable[] executeFunctionResult = null;
      string funcName = "GetFunctionExeHA";
      Execution exc = FunctionService.OnRegion(region);
      MyResultCollectorHA myRC = new MyResultCollectorHA();
      exc = exc.WithCollector(myRC);
      executeFunctionResult = exc.Execute(funcName, getresult, 120, true, true).GetResult();
      if (executeFunctionResult != null)
      {
        /*
        CacheableBoolean lastResult = executeFunctionResult[0] as CacheableBoolean;
        if (lastResult != null && lastResult.Value != true)
          FwkException("FunctionExecution::DoExecuteFunctionHA failed, last result is not true");
         */
        
        IGFSerializable[] resultList = myRC.GetResult(60);
        //FwkInfo("FunctionExecution::DoExecuteFunctionHA GetClearResultCount {0} GetGetResultCount {1} GetAddResultCount {2}", myRC.GetClearResultCount(), myRC.GetGetResultCount(), myRC.GetAddResultCount());
        if (resultList != null)
        {
          if (numKeys == resultList.Length)
          {
            for (int i = 1; i < numKeys; i++)
            {
              int count = 0;
              string key = "key-" + i;
              for (int j = 0; j < resultList.Length; j++)
              {
                if ((key.Equals(resultList[j])))
                {
                  count++;
                  if (count > 1)
                  {
                    FwkException("FunctionExecution::DoExecuteFunctionHA: duplicate entry found for key {0} ", key);
                  }
                } 
              }
              if(count == 0) {
                  FwkException("FunctionExecution::DoExecuteFunctionHA failed: key is missing in result list {0}", key);
                }
              }
            }
          }
          else
          {
            FwkException("FunctionExecution::DoExecuteFunctionHA failed: result size {0} doesn't match with number of keys {1}", resultList.Length, numKeys);
          }
        }
        else {
          FwkException("FunctionExecution::DoExecuteFunctionHA executeFunctionResult is null");
        }
        
      }
    }
  }
