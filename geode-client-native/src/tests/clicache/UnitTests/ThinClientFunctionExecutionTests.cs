//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;
  public class MyResultCollector : IResultCollector
  {
    #region Private members
    private bool m_resultReady = false;
    private CacheableVector m_results = null;
    private int m_addResultCount = 0;
    private int m_getResultCount = 0;
    private int m_endResultCount = 0;
    #endregion
    public int GetAddResultCount()
    {
      return m_addResultCount ;
    }
    public int GetGetResultCount()
    {
      return m_getResultCount ;
    }
    public int GetEndResultCount()
    {
      return m_endResultCount ;
    }
    public MyResultCollector()
    {
      m_results = new CacheableVector();
    }
    public void AddResult(IGFSerializable result)
    {
      Util.Log("MyResultCollector AddResult m_addResultCount = {0} ", m_addResultCount);
      m_addResultCount++;
      Util.Log("MyResultCollector AddResult CacheableArrayList cast");
      CacheableArrayList rs = result as CacheableArrayList;
      if (rs != null) {
        Util.Log("MyResultCollector AddResult CacheableArrayList cast done ");
        for (int i = 0; i < rs.Count; i++) {
          Util.Log("MyResultCollector AddResult i = {0} , rs[i] = {1} ", i, rs[i]);
          m_results.Add(rs[i]);
        }
      }
      else {
        Util.Log("MyResultCollector AddResult UserFunctionExecutionException cast");
        UserFunctionExecutionException UserRs = result as UserFunctionExecutionException;
        Util.Log("MyResultCollector AddResult UserFunctionExecutionException cast done");
        m_results.Add(UserRs);
      }
    }
    public IGFSerializable[] GetResult()
    {
      return GetResult(50);
    }
    public IGFSerializable[] GetResult(UInt32 timeout)
    {
      m_getResultCount++;
      if(m_resultReady == true)
      {
	return m_results.ToArray();
      }
      else 
      {
	for(int i=0; i < timeout; i++)
	{
	  Thread.Sleep(1000);
          if(m_resultReady == true)
          {
	    return m_results.ToArray();
          }
	}
	throw new FunctionExecutionException(
	           "Result is not ready, endResults callback is called before invoking getResult() method");
	
      }
    }
    public void EndResults()
    {
       m_endResultCount++;
       m_resultReady = true;
    }
    public void ClearResults()
    {
      m_results.Clear();
    }
  }


  [TestFixture]
  [Category("group3")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientFunctionExecutionTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1;
    private UnitProcess m_client2;
    private static string[] FunctionExecutionRegionNames = { "partition_region", "partition_region1"};
    private static string poolName = "__TEST_POOL1__";
    private static string  serverGroup = "ServerGroup1";
    private static string QERegionName = "partition_region";
    private static string getFuncName = "MultiGetFunction";
    private static string getFuncIName = "MultiGetFunctionI";
    private static string OnServerHAExceptionFunction = "OnServerHAExceptionFunction";
    private static string OnServerHAShutdownFunction = "OnServerHAShutdownFunction";
    private static string RegionOperationsHAFunction = "RegionOperationsHAFunction";
    private static string exFuncNameSendException = "executeFunction_SendException";

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
    }
    [TearDown]
    public override void EndTest()
    {
      try
      {
        m_client1.Call(CacheHelper.Close);
        m_client2.Call(CacheHelper.Close);
        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
      finally
      {
        CacheHelper.StopJavaServers();
        CacheHelper.StopJavaLocators();
      }
      base.EndTest();
    }

    public void InitClient(string endpoints, int redundancyLevel)
    {
      if(endpoints != null)
      {
         CacheHelper.InitConfig(endpoints, redundancyLevel);
      }
      else
         CacheHelper.Init();
    }

    public void createRegionAndAttachPool(string regionName, string poolName)
    {
      CacheHelper.CreateTCRegion_Pool(regionName, true, true, null, null, poolName, false, serverGroup);
    }
    public void createPool(string name, string endpoints, string locators, string serverGroup,
      int redundancy, bool subscription)
    {
      CacheHelper.CreatePool(name, endpoints, locators, serverGroup, redundancy, subscription);
    }

    public void StepOne(string endpoints)
    {

      CacheHelper.CreateTCRegion(QERegionName, true, true,
        null, endpoints, true);

      Region region = CacheHelper.GetVerifyRegion(QERegionName);
      for(int i=0; i < 34; i++)
      {
          region.Put("KEY--"+i, "VALUE--"+i);
      }
      
      IGFSerializable[] routingObj = new IGFSerializable[17];
      int j=0;
      for(int i=0; i < 34; i++)
      {
          if(i%2==0) continue;
          routingObj[j] = new CacheableString("KEY--"+i);
	  j++;
      }
      Util.Log("routingObj count= {0}.", routingObj.Length);
    
      IGFSerializable args = new CacheableBoolean(true);
      Boolean getResult = true;
      //test data dependant function execution
      //     test get function with result
      Execution exc = FunctionService.OnRegion(region);
      IResultCollector rc =  exc.WithArgs(args).WithFilter(routingObj).Execute(
	  getFuncName, getResult);
      IGFSerializable[] executeFunctionResult = rc.GetResult();
      List<IGFSerializable> resultList = new List<IGFSerializable>();
      //resultList.Clear();
      for (int pos = 0; pos < executeFunctionResult.Length; pos++)
      {
        CacheableArrayList resultItem = executeFunctionResult[pos] as CacheableArrayList;
        foreach (IGFSerializable item in resultItem)
        {
          resultList.Add(item);
        }
      }
      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 17, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on region:get:result[{0}]={1}.", i, (resultList[i] as CacheableString).Value);
      }
      // Bring down the region
      region.LocalDestroyRegion();
    }
    public void PoolStepOne(string locators)
    {
      Region region = CacheHelper.GetVerifyRegion(QERegionName);
      for (int i = 0; i < 34; i++) {
        region.Put("KEY--" + i, "VALUE--" + i);
      }

      IGFSerializable[] routingObj = new IGFSerializable[17];
      int j = 0;
      for (int i = 0; i < 34; i++) {
        if (i % 2 == 0) continue;
        routingObj[j] = new CacheableString("KEY--" + i);
        j++;
      }
      Util.Log("routingObj count= {0}.", routingObj.Length);

      IGFSerializable args = new CacheableBoolean(true);
      Boolean getResult = true;
      //test data dependant function execution
      //     test get function with result
      Execution exc = FunctionService.OnRegion(region);
      IResultCollector rc = exc.WithArgs(args).WithFilter(routingObj).Execute(getFuncName, getResult);
      IGFSerializable[] executeFunctionResult = rc.GetResult();
      List<IGFSerializable> resultList = new List<IGFSerializable>();
      //resultList.Clear();
      for (int pos = 0; pos < executeFunctionResult.Length; pos++)
      {
        CacheableArrayList resultItem = executeFunctionResult[pos] as CacheableArrayList;
        foreach (IGFSerializable item in resultItem)
        {
          resultList.Add(item);
        }
      }
      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 34, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on region:get:result[{0}]={1}.", i, (resultList[i] as CacheableString).Value);
      }

      //---------------------Test for function execution with sendException------------------------//
      for (int i = 1; i <= 200; i++) {
        region.Put("execKey-" + i, i);
      }
      Util.Log("Put on region complete for execKeys");
      CacheableArrayList arrList = new CacheableArrayList();
      for (int i = 100; i < 120; i++) {
        arrList.Add(CacheableString.Create("execKey-" + i));
      }

      IGFSerializable[] filter = new IGFSerializable[20];
      j = 0;
      for (int i = 100; i < 120; i++) {
        filter[j] = new CacheableString("execKey-" + i);
        j++;
      }
      Util.Log("filter count= {0}.", filter.Length);

      args = new CacheableBoolean(true);
      getResult = true;

      exc = FunctionService.OnRegion(region);
      rc = exc.WithArgs(args).WithFilter(filter).Execute(exFuncNameSendException, getResult);
      //Util.Log("Executing exFuncNameSendException on region for execKeys for bool arguement execute done");
      executeFunctionResult = rc.GetResult();
      //Util.Log("Executing exFuncNameSendException on region for execKeys for bool arguement called GetResult");

      Assert.IsTrue(executeFunctionResult.Length == 1, "executeFunctionResult count check failed");

      UserFunctionExecutionException uFEE = executeFunctionResult[0] as UserFunctionExecutionException;
      Assert.IsTrue(uFEE != null, "string exception is NULL");
      Util.Log("Read expected UserFunctionExecutionException for bool arguement is {0} ", uFEE.Message);

      Util.Log("Executing exFuncNameSendException on region for execKeys for bool arguement done.");

      rc = exc.WithArgs(arrList).WithFilter(filter).Execute(exFuncNameSendException, getResult);
      executeFunctionResult = rc.GetResult();
      //Util.Log("Executing exFuncNameSendException on region for execKeys for arrList arguement");

      Util.Log("exFuncNameSendException for arrList result->size() = {0} ", executeFunctionResult.Length);

      Assert.IsTrue(executeFunctionResult.Length == arrList.Count + 1, "region get: resultList count is not as arrList count + exception");

      for (int i = 0; i < executeFunctionResult.Length; i++) {
        UserFunctionExecutionException csp = executeFunctionResult[i] as UserFunctionExecutionException;
        if (csp != null) {
          Util.Log("Read expected string exception for arrList arguement is {0} ", csp.Message);
        }
        else {
          CacheableInt32 arrListNo = executeFunctionResult[i] as CacheableInt32;
          Util.Log("intNo for arrListNo arguement is {0} ", arrListNo);
        }
      }

      MyResultCollector myRC1 = new MyResultCollector();
      rc = exc.WithArgs(args).WithCollector(myRC1).Execute(exFuncNameSendException, getResult);
      executeFunctionResult = myRC1.GetResult();
      Util.Log("add result count= {0}.", myRC1.GetAddResultCount());
      Util.Log("get result count= {0}.", myRC1.GetGetResultCount());
      Util.Log("end result count= {0}.", myRC1.GetEndResultCount());
      Assert.IsTrue(myRC1.GetAddResultCount() == 1, "add result count check failed");
      Assert.IsTrue(myRC1.GetGetResultCount() == 1, "get result count check failed");
      Assert.IsTrue(myRC1.GetEndResultCount() == 1, "end result count check failed");
      Util.Log("on Region with collector: result count= {0}.", executeFunctionResult.Length);
      Assert.IsTrue(executeFunctionResult.Length == 1, "result count check failed");
      for (int i = 0; i < executeFunctionResult.Length; i++) {
        Util.Log("on Region with collector: get:result[{0}]={1}.", i, (executeFunctionResult[i] as UserFunctionExecutionException).Message);
      }

      //---------------------Test for function execution with sendException Done-----------------------//

      Pool pl = PoolManager.Find(poolName);
      getResult = true;
      //test date independant fucntion execution on one server
      //     test get function with result
      exc = FunctionService.OnServer(pl);
      CacheableVector args1 = new CacheableVector();
      for (int i = 0; i < routingObj.Length; i++) {
        Util.Log("routingObj[{0}]={1}.", i, (routingObj[i] as CacheableString).Value);
        args1.Add(routingObj[i]);
      }
      rc = exc.WithArgs(args1).Execute(
      getFuncIName, getResult);
      executeFunctionResult = rc.GetResult();

      resultList.Clear();
      for (int pos = 0; pos < executeFunctionResult.Length; pos++) {
        CacheableArrayList resultItem = executeFunctionResult[pos] as CacheableArrayList;
        foreach (IGFSerializable item in resultItem) {
          resultList.Add(item);
        }
      }
      Util.Log("on one server: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 17, "result count check failed");
      for (int i = 0; i < resultList.Count; i++) {
        Util.Log("on one server: get:result[{0}]={1}.", i, (resultList[i] as CacheableString).Value);
      }

      //test data independant fucntion execution on all servers
      //     test get function with result
      exc = FunctionService.OnServers(pl);
      rc = exc.WithArgs(args1).Execute(getFuncIName, getResult);
      executeFunctionResult = rc.GetResult();
      resultList.Clear();
      for (int pos = 0; pos < executeFunctionResult.Length; pos++) {
        CacheableArrayList resultItem = executeFunctionResult[pos] as CacheableArrayList;
        foreach (IGFSerializable item in resultItem) {
          resultList.Add(item);
        }
      }
      Util.Log("on all servers: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 34, "result count check failed");
      for (int i = 0; i < resultList.Count; i++) {
        Util.Log("on all servers: get:result[{0}]={1}.", i, (resultList[i] as CacheableString).Value);
      }
      //test withCollector
      MyResultCollector myRC = new MyResultCollector();
      rc = exc.WithArgs(args1).WithCollector(myRC).Execute(getFuncIName, getResult);
      executeFunctionResult = myRC.GetResult();
      Util.Log("add result count= {0}.", myRC.GetAddResultCount());
      Util.Log("get result count= {0}.", myRC.GetGetResultCount());
      Util.Log("end result count= {0}.", myRC.GetEndResultCount());
      Util.Log("on all servers with collector: result count= {0}.", executeFunctionResult.Length);      
      //Assert.IsTrue(executeFunctionResult.Length == 34, "result count check failed");
      for (int i = 0; i < executeFunctionResult.Length; i++) {
        Util.Log("on all servers with collector: get:result[{0}]={1}.", i, (executeFunctionResult[i] as CacheableString).Value);
      }

      Util.Log("exFuncNameSendException with onServers with custom result collector.");
      MyResultCollector myRC2 = new MyResultCollector();
      rc = exc.WithArgs(args).WithCollector(myRC2).Execute(exFuncNameSendException, getResult);
      executeFunctionResult = myRC2.GetResult();
      Util.Log("add result count= {0}.", myRC2.GetAddResultCount());
      Util.Log("get result count= {0}.", myRC2.GetGetResultCount());
      Util.Log("end result count= {0}.", myRC2.GetEndResultCount());
      Assert.IsTrue(myRC2.GetAddResultCount() == 2, "add result count check failed");
      Assert.IsTrue(myRC2.GetGetResultCount() == 1, "get result count check failed");
      Assert.IsTrue(myRC2.GetEndResultCount() == 1, "end result count check failed");
      Util.Log("on Region with collector: result count= {0}.", executeFunctionResult.Length);
      Assert.IsTrue(executeFunctionResult.Length == 2, "result count check failed");
      for (int i = 0; i < executeFunctionResult.Length; i++) {
        Util.Log("on Region with collector: get:result[{0}]={1}.", i, (executeFunctionResult[i] as UserFunctionExecutionException).Message);
      }
      Util.Log("exFuncNameSendException with onServers with custom result collector done.");

      // Bring down the region
      region.LocalDestroyRegion();
    }

    public void HAStepOne(string endpoints)
    {

      Region region = CacheHelper.GetVerifyRegion(QERegionName);
      for(int i=0; i < 34; i++)
      {
          region.Put("KEY--"+i, "VALUE--"+i);
      }
      
      IGFSerializable[] routingObj = new IGFSerializable[17];
      int j=0;
      for(int i=0; i < 34; i++)
      {
          if(i%2==0) continue;
          routingObj[j] = new CacheableString("KEY--"+i);
	  j++;
      }
      Util.Log("routingObj count= {0}.", routingObj.Length);
    
      IGFSerializable args = new CacheableBoolean(true);
      Boolean getResult = true;
      //test data dependant function execution
      //     test get function with result
      Execution exc = FunctionService.OnRegion(region);
      IResultCollector rc =  exc.WithArgs(args).WithFilter(routingObj).Execute(
	  getFuncName, getResult);
      IGFSerializable[] executeFunctionResult = rc.GetResult();
      List<IGFSerializable> resultList = new List<IGFSerializable>();
      //resultList.Clear();
      for (int pos = 0; pos < executeFunctionResult.Length; pos++)
      {
        CacheableArrayList resultItem = executeFunctionResult[pos] as CacheableArrayList;
        foreach (IGFSerializable item in resultItem)
        {
          resultList.Add(item);
        }
      }
      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 34, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on region:get:result[{0}]={1}.", i, (resultList[i] as CacheableString).Value);
      }

      // Bring down the region
      region.LocalDestroyRegion();
    }

    public void OnRegionHAStepOne()
    {
      Region region = CacheHelper.GetVerifyRegion(QERegionName);
      for (int i = 0; i < 34; i++) {
        region.Put("KEY--" + i, "VALUE--" + i);
      }

      IGFSerializable[] routingObj = new IGFSerializable[17];
      CacheableVector args1 = new CacheableVector();
      int j = 0;
      for (int i = 0; i < 34; i++) {
        if (i % 2 == 0) continue;
        routingObj[j] = new CacheableString("KEY--" + i);
        j++;
      }
      Util.Log("routingObj count= {0}.", routingObj.Length);

      for (int i = 0; i < routingObj.Length; i++) {
        Console.WriteLine("routingObj[{0}]={1}.", i, (routingObj[i] as CacheableString).Value);
        args1.Add(routingObj[i]);
      }

      //IGFSerializable args = new CacheableBoolean(true);
      //args = routingObj;
      Boolean getResult = true;
      //test data independant function execution with result onServer
      Execution exc = FunctionService.OnRegion(region);

      Assert.IsTrue(exc != null, "onRegion Returned NULL");

      IResultCollector rc = exc.WithArgs(args1).Execute(RegionOperationsHAFunction, getResult, 15, true, true);

      IGFSerializable[] executeFunctionResult = rc.GetResult();
      List<IGFSerializable> resultList = new List<IGFSerializable>();
      for (int pos = 0; pos < executeFunctionResult.Length; pos++) {
        CacheableArrayList resultItem = executeFunctionResult[pos] as CacheableArrayList;
        foreach (IGFSerializable item in resultItem) {
          resultList.Add(item);
        }
      }
      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 17, "result count check failed");
      for (int i = 0; i < resultList.Count; i++) {
        Util.Log("on region:get:result[{0}]={1}.", i, (resultList[i] as CacheableString).Value);
        Assert.IsTrue((resultList[i] as CacheableString).Value != null, "onServer Returned NULL");
      }

    }

    public void OnServerHAStepOne()
    {

      Region region = CacheHelper.GetVerifyRegion(QERegionName);
      for(int i=0; i < 34; i++)
      {
          region.Put("KEY--"+i, "VALUE--"+i);
      }
      
      IGFSerializable[] routingObj = new IGFSerializable[17];
      CacheableVector args1 = new CacheableVector();
      int j=0;
      for(int i=0; i < 34; i++)
      {
          if(i%2==0) continue;
          routingObj[j] = new CacheableString("KEY--"+i);
	  j++;
      }
      Util.Log("routingObj count= {0}.", routingObj.Length);

      for (int i = 0; i < routingObj.Length; i++) {
        Console.WriteLine("routingObj[{0}]={1}.", i, (routingObj[i] as CacheableString).Value);
        args1.Add(routingObj[i]);
      }
    
      //IGFSerializable args = new CacheableBoolean(true);
      //args = routingObj;
      Boolean getResult = true;
      //test data independant function execution with result onServer
      Pool pool = PoolManager.Find(poolName);
      Execution exc = FunctionService.OnServer(pool);
      Assert.IsTrue(exc!=null, "onServer Returned NULL");

      IResultCollector rc = exc.WithArgs(args1).Execute(OnServerHAExceptionFunction, getResult, 15, true, false);
	  
      IGFSerializable[] executeFunctionResult = rc.GetResult();
      List<IGFSerializable> resultList = new List<IGFSerializable>();
      for (int pos = 0; pos < executeFunctionResult.Length; pos++)
      {
        CacheableArrayList resultItem = executeFunctionResult[pos] as CacheableArrayList;
        foreach (IGFSerializable item in resultItem)
        {
          resultList.Add(item);
        }
      }
      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 17, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on region:get:result[{0}]={1}.", i, (resultList[i] as CacheableString).Value);
        Assert.IsTrue((resultList[i] as CacheableString).Value != null, "onServer Returned NULL");
      }

      rc = exc.WithArgs(args1).Execute(OnServerHAShutdownFunction, getResult, 15, true, false);

      IGFSerializable[] executeFunctionResult1 = rc.GetResult();
      List<IGFSerializable> resultList1 = new List<IGFSerializable>();
      for (int pos = 0; pos < executeFunctionResult1.Length; pos++) {
        CacheableArrayList resultItem = executeFunctionResult1[pos] as CacheableArrayList;
        foreach (IGFSerializable item in resultItem) {
          resultList1.Add(item);
        }
      }
      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList1.Count == 17, "result count check failed");
      for (int i = 0; i < resultList1.Count; i++) {
        Util.Log("on region:get:result[{0}]={1}.", i, (resultList1[i] as CacheableString).Value);        
        Assert.IsTrue((resultList1[i] as CacheableString).Value !=null, "onServer Returned NULL");
      }

      // Bring down the region
      //region.LocalDestroyRegion();
    }

    void runOnServerHAExecuteFunction(bool pool, bool locator)
    { 
      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "func_cacheserver1_pool.xml",
        "func_cacheserver2_pool.xml", "func_cacheserver3_pool.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
        Util.Log("Cacheserver 3 started.");
      }
      else {
        CacheHelper.SetupJavaServers(false, "func_cacheserver1_pool.xml",
        "func_cacheserver2_pool.xml", "func_cacheserver3_pool.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StartJavaServer(3, "GFECS3");
        Util.Log("Cacheserver 3 started.");
      }      

      if (pool) {
        if (locator) {
          m_client1.Call(createPool, poolName, (string)null, CacheHelper.Locators, serverGroup, 1, true);
          m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);         
          Util.Log("Client 1 (pool locator) regions created");
        }
        else {
          m_client1.Call(createPool, poolName, CacheHelper.Endpoints, (string)null, serverGroup, 1, true);
          m_client1.Call(createRegionAndAttachPool, QERegionName, poolName); 
          Util.Log("Client 1 (pool endpoints) regions created");          
        }
      }

      m_client1.Call(OnServerHAStepOne);      
    
      m_client1.Call(Close);
      Util.Log("Client 1 closed");     

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runOnRegionHAExecuteFunction(bool pool, bool locator)
    {
      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "func_cacheserver1_pool.xml",
        "func_cacheserver2_pool.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");        
      }
      else {
        CacheHelper.SetupJavaServers(false, "func_cacheserver1_pool.xml",
        "func_cacheserver2_pool.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");        
      }

      if (pool) {
        if (locator) {
          m_client1.Call(createPool, poolName, (string)null, CacheHelper.Locators, serverGroup, 1, true);
          m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);
          Util.Log("Client 1 (pool locator) regions created");
        }
        else {
          m_client1.Call(createPool, poolName, CacheHelper.Endpoints, (string)null, serverGroup, 1, true);
          m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);
          Util.Log("Client 1 (pool endpoints) regions created");
        }
      }

      m_client1.Call(OnRegionHAStepOne);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");      

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }
    

    [Test]
    public void PoolExecuteFunctionTest()
    {
        CacheHelper.SetupJavaServers(true, "func_cacheserver1_pool.xml",
          "func_cacheserver2_pool.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
      m_client1.Call(createPool, poolName, (string)null, CacheHelper.Locators, serverGroup, 0, true);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);

        m_client1.Call(PoolStepOne, CacheHelper.Locators);
        Util.Log("PoolStepOne complete.");
      m_client1.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");
    }

    [Test]
    public void HAExecuteFunctionTest()
    {
        CacheHelper.SetupJavaServers(true, "func_cacheserver1_pool.xml",
          "func_cacheserver2_pool.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
      m_client1.Call(createPool, poolName, (string)null, CacheHelper.Locators, serverGroup, 1, true);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);

      m_client1.Call(HAStepOne, CacheHelper.Endpoints);
      Util.Log("HAStepOne complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");
           

    }

    [Test]
    public void OnServerHAExecuteFunctionTest()
    {
      runOnServerHAExecuteFunction(true, false); // pool with server endpoints
      runOnServerHAExecuteFunction(true, true);  // pool with locators
    }

    [Test]
    public void OnRegionHAExecuteFunctionTest()
    {
      runOnRegionHAExecuteFunction(true, false); // pool with server endpoints
      runOnRegionHAExecuteFunction(true, true);  // pool with locators
    }   

  }
}
