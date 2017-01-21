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

using System;
using System.Collections.Generic;
using System.Collections;
using System.Threading;

namespace Apache.Geode.Client.UnitTests
{
  using NUnit.Framework;
  using Apache.Geode.DUnitFramework;

  using Apache.Geode.Client;
  using Region = Apache.Geode.Client.IRegion<Object, Object>;

  public class MyResultCollector<TResult> : Client.IResultCollector<TResult>
  {
    #region Private members
    private bool m_resultReady = false;
    ICollection<TResult> m_results = null;
    private int m_addResultCount = 0;
    private int m_getResultCount = 0;
    private int m_endResultCount = 0;
    #endregion
    public int GetAddResultCount()
    {
      return m_addResultCount;
    }
    public int GetGetResultCount()
    {
      return m_getResultCount;
    }
    public int GetEndResultCount()
    {
      return m_endResultCount;
    }
    public MyResultCollector()
    {
      m_results = new List<TResult>();
    }
    public void AddResult(TResult result)
    {
      Util.Log(" in MyResultCollector " + result + " :  " + result.GetType());
      m_addResultCount++;
      m_results.Add(result);
    }
    public ICollection<TResult> GetResult()
    {
      return GetResult(50);
    }

    public ICollection<TResult> GetResult(UInt32 timeout) 
    {
      m_getResultCount++;
      if (m_resultReady == true)
      {
        return m_results;
      }
      else
      {
        for (int i = 0; i < timeout; i++)
        {
          Thread.Sleep(1000);
          if (m_resultReady == true)
          {
            return m_results;
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
    public void ClearResults(/*bool unused*/)
    {
      m_results.Clear();
      m_addResultCount = 0;
      m_getResultCount = 0;
      m_endResultCount = 0;
    }
  }


  [TestFixture]
  [Category("group3")]
  [Category("unicast_only")]
  [Category("generics")]
  public class ThinClientFunctionExecutionTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1;
    private UnitProcess m_client2;
    private static string[] FunctionExecutionRegionNames = { "partition_region", "partition_region1" };
    private static string poolName = "__TEST_POOL1__";
    private static string serverGroup = "ServerGroup1";
    private static string QERegionName = "partition_region";
    private static string getFuncName = "MultiGetFunction";
    private static string getFuncIName = "MultiGetFunctionI";
    private static string OnServerHAExceptionFunction = "OnServerHAExceptionFunction";
    private static string OnServerHAShutdownFunction = "OnServerHAShutdownFunction";
    private static string RegionOperationsHAFunction = "RegionOperationsHAFunction";
    private static string RegionOperationsHAFunctionPrSHOP = "RegionOperationsHAFunctionPrSHOP";
    private static string exFuncNameSendException = "executeFunction_SendException";
    private static string FEOnRegionPrSHOP = "FEOnRegionPrSHOP";
    private static string FEOnRegionPrSHOP_OptimizeForWrite = "FEOnRegionPrSHOP_OptimizeForWrite";
    private static string putFuncName = "MultiPutFunction";
    private static string putFuncIName = "MultiPutFunctionI";
    private static string FuncTimeOutName = "FunctionExecutionTimeOut";

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

    public void InitClient()
    {
      CacheHelper.Init();
    }

    public void createRegionAndAttachPool(string regionName, string poolName)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(regionName, true, true, null, null, poolName, false, serverGroup);
    }
    public void createPool(string name, string locators, string serverGroup,
      int redundancy, bool subscription, bool prSingleHop, bool threadLocal = false)
    {
      CacheHelper.CreatePool<object, object>(name, locators, serverGroup, redundancy, subscription, prSingleHop, threadLocal);
    }

    //public void StepOne(string endpoints)
    //{

    //  CacheHelper.CreateTCRegion(QERegionName, true, true,
    //    null, endpoints, true);

    //  Region region = CacheHelper.GetVerifyRegion(QERegionName);
    //  for (int i = 0; i < 34; i++)
    //  {
    //    region["KEY--" + i] = "VALUE--" + i;
    //  }

    //  string[] routingObj = new string[17];
    //  int j = 0;
    //  for (int i = 0; i < 34; i++)
    //  {
    //    if (i % 2 == 0) continue;
    //    routingObj[j] = "KEY--" + i;
    //    j++;
    //  }
    //  Util.Log("routingObj count= {0}.", routingObj.Length);

    //  bool args = true;
    //  Boolean getResult = true;
    //  //test data dependant function execution
    //  //     test get function with result
    //  Execution<object> exc = FunctionService<object>.OnRegion<object, object> (region);
    //  IResultCollector<object> rc = exc.WithArgs<bool>(args).WithFilter<string>(routingObj).Execute(
    //getFuncName, getResult);
    //  ICollection<object> executeFunctionResult = rc.GetResult();
    //  List<object> resultList = new List<object>();
    //  //resultList.Clear();
      
    //  foreach (List<object> item in executeFunctionResult)
    //  {

    //    foreach (object item2 in item)
    //    {
    //      resultList.Add(item2);
    //    }
    //  }
    //  Util.Log("on region: result count= {0}.", resultList.Count);
    //  Assert.IsTrue(resultList.Count == 17, "result count check failed");
    //  for (int i = 0; i < resultList.Count; i++)
    //  {
    //    Util.Log("on region:get:result[{0}]={1}.", i, (string)resultList[i]);
    //  }
    //  // Bring down the region
    //  region.GetLocalView().DestroyRegion();
    //}

    public void PoolStepOne(string locators)
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      for (int i = 0; i < 230; i++)
      {
        region["KEY--" + i] = "VALUE--" + i;
      }

      Object[] routingObj = new Object[17];
      int j = 0;
      for (int i = 0; i < 34; i++)
      {
        if (i % 2 == 0) continue;
        routingObj[j] = "KEY--" + i;
        j++;
      }
      Util.Log("routingObj count= {0}.", routingObj.Length);

      object args = true;
      //test data dependant function execution
      //     test get function with result
      
      Apache.Geode.Client.Execution<object> exc = Client.FunctionService<object>.OnRegion<object, object>(region);
      Client.IResultCollector<object> rc = exc.WithArgs<object>(args).WithFilter<object>(routingObj).Execute(getFuncName);
      ICollection<object> executeFunctionResult = rc.GetResult();
      List<object> resultList = new List<object>();
      //resultList.Clear();
      
      foreach (List<object> item in executeFunctionResult)
      { 
        foreach (object item2 in item)
        {
          resultList.Add(item2);
        }
      }
      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 34, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on region:get:result[{0}]={1}.", i, (string)resultList[i]);
      }

      //---------------------Test for function execution with sendException------------------------//
      for (int i = 1; i <= 230; i++) {
        region["execKey-" + i] = i;        
      }
      Util.Log("Put on region complete for execKeys");

      List<Object> arrList = new List<Object>(20);      
      for (int i = 100; i < 120; i++) {       
        arrList.Add("execKey-" + i);
      }

      Object[] filter = new Object[20];
      j = 0;
      for (int i = 100; i < 120; i++) {
        filter[j] = "execKey-" + i;
        j++;
      }
      Util.Log("filter count= {0}.", filter.Length);

      args = true;

      exc = Client.FunctionService<object>.OnRegion<object, object>(region);

      rc = exc.WithArgs<object>(args).WithFilter<object>(filter).Execute(exFuncNameSendException);

      executeFunctionResult = rc.GetResult();      

      Assert.IsTrue(executeFunctionResult.Count == 1, "executeFunctionResult count check failed");

      foreach (UserFunctionExecutionException item in executeFunctionResult) {                  
          Util.Log("Item returned for bool arguement is {0} ", item.Message);        
      }

      Util.Log("Executing exFuncNameSendException on region for execKeys for bool arguement done.");      

      rc = exc.WithArgs<object>(arrList).WithFilter<object>(filter).Execute(exFuncNameSendException);
      executeFunctionResult = rc.GetResult();      

      Util.Log("exFuncNameSendException for arrList result->size() = {0} ", executeFunctionResult.Count);

      Assert.IsTrue(executeFunctionResult.Count == arrList.Count + 1, "region get: resultList count is not as arrList count + exception");

      foreach (object item in executeFunctionResult) {                  
          Util.Log("Items returned for arrList arguement is {0} ", item);        
      }

      Util.Log("Executing exFuncNameSendException on region for execKeys for arrList arguement done.");

      MyResultCollector<object> myRC1 = new MyResultCollector<object>();
      rc = exc.WithArgs<object>(args).WithCollector(myRC1).Execute(exFuncNameSendException);
      executeFunctionResult = rc.GetResult();
      Util.Log("add result count= {0}.", myRC1.GetAddResultCount());
      Util.Log("get result count= {0}.", myRC1.GetGetResultCount());
      Util.Log("end result count= {0}.", myRC1.GetEndResultCount());
      Assert.IsTrue(myRC1.GetAddResultCount() == 1, "add result count check failed");
      Assert.IsTrue(myRC1.GetGetResultCount() == 1, "get result count check failed");
      Assert.IsTrue(myRC1.GetEndResultCount() == 1, "end result count check failed");
      Util.Log("on Region with collector: result count= {0}.", executeFunctionResult.Count);
      Assert.IsTrue(executeFunctionResult.Count == 1, "result count check failed");
      foreach (object item in executeFunctionResult) {
        Util.Log("on Region with collector: get:result {0}", (item as UserFunctionExecutionException).Message);
      }      

      //---------------------Test for function execution with sendException Done-----------------------//

      Pool/*<object, object>*/ pl = PoolManager/*<object, object>*/.Find(poolName);
      //test date independant fucntion execution on one server
      //     test get function with result
      
      exc = Client.FunctionService<object>.OnServer(pl);
      ArrayList args1 = new ArrayList();
      for (int i = 0; i < routingObj.Length; i++)
      {
        Util.Log("routingObj[{0}]={1}.", i, (string)routingObj[i]);
        args1.Add(routingObj[i]);
      }
      rc = exc.WithArgs<ArrayList>(args1).Execute(getFuncIName);
      executeFunctionResult = rc.GetResult();

      resultList.Clear();
      foreach (List<object> item in executeFunctionResult)
      {

        foreach (object item2 in item)
        {
          resultList.Add(item2);
        }
      }
      Util.Log("on one server: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 17, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on one server: get:result[{0}]={1}.", i, (string)resultList[i]);
      }

      //test data independant fucntion execution on all servers
      //     test get function with result
      
      exc = Client.FunctionService<object>.OnServers(pl);
      rc = exc.WithArgs<ArrayList>(args1).Execute(getFuncIName);
      executeFunctionResult = rc.GetResult();
      resultList.Clear();
      
      foreach (List<object> item in executeFunctionResult)
      {
        foreach (object item2 in item)
        {
          resultList.Add(item2);
        }
      }
      Util.Log("on all servers: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 34, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on all servers: get:result[{0}]={1}.", i, (string)resultList[i]);
      }
      //TODO::enable it once the StringArray conversion is fixed.
      //test withCollector
      MyResultCollector<object> myRC = new MyResultCollector<object>();
      rc = exc.WithArgs<ArrayList>(args1).WithCollector(myRC).Execute(getFuncIName);
      //executeFunctionResult = rc.GetResult();
      Util.Log("add result count= {0}.", myRC.GetAddResultCount());
      Util.Log("get result count= {0}.", myRC.GetGetResultCount());
      Util.Log("end result count= {0}.", myRC.GetEndResultCount());
      Util.Log("on all servers with collector: result count= {0}.", executeFunctionResult.Count);
      Assert.IsTrue(myRC.GetResult().Count == 2, "result count check failed");

      IList res = (IList)myRC.GetResult();

      foreach (object o in res)
      {
        IList resList = (IList)o;
        Util.Log("results " + resList.Count);

        Assert.AreEqual(17, resList.Count);
      }

      MyResultCollector<object> myRC2 = new MyResultCollector<object>();
      rc = exc.WithArgs<object>(args).WithCollector(myRC2).Execute(exFuncNameSendException);
      executeFunctionResult = rc.GetResult();
      Util.Log("add result count= {0}.", myRC2.GetAddResultCount());
      Util.Log("get result count= {0}.", myRC2.GetGetResultCount());
      Util.Log("end result count= {0}.", myRC2.GetEndResultCount());
      Assert.IsTrue(myRC2.GetAddResultCount() == 2, "add result count check failed");
      Assert.IsTrue(myRC2.GetGetResultCount() == 1, "get result count check failed");
      Assert.IsTrue(myRC2.GetEndResultCount() == 1, "end result count check failed");
      Util.Log("on Region with collector: result count= {0}.", executeFunctionResult.Count);
      Assert.IsTrue(executeFunctionResult.Count == 2, "result count check failed");
      foreach (object item in executeFunctionResult) {
        Util.Log("on Region with collector: get:result {0}", (item as UserFunctionExecutionException).Message);
      }     

    }

    public void genericFEResultIntTest(string locators)
    {
      IRegion<int, int> region = CacheHelper.GetVerifyRegion<int, int>(QERegionName);
      Pool pl = PoolManager.Find(poolName);

      for (int n = 0; n < 34; n++)
      {
        region[n] = n;
      }

      ICollection<int> routingObj = new List<int>();

      int j = 0;
      for (int i = 0; i < 34; i++)
      {
        if (i % 2 == 0) continue;
        routingObj.Add(i);
        j++;
      }
      Console.WriteLine("routingObj count= {0}.", routingObj.Count);

      int args1 = 0;
     
      MyResultCollector<int> myRC = new MyResultCollector<int>();
      Apache.Geode.Client.Execution<int> exc = Client.FunctionService<int>.OnServers/*OnRegion<string, string>*/(pl/*region*/);
      Client.IResultCollector<int> rc = exc.WithArgs<int>(args1).WithCollector(myRC).Execute("SingleStrGetFunction");
      
      Util.Log("add result count= {0}.", myRC.GetAddResultCount());
      Util.Log("get result count= {0}.", myRC.GetGetResultCount());
      Util.Log("end result count= {0}.", myRC.GetEndResultCount());
      
      Assert.IsTrue(myRC.GetResult().Count == 2, "result count check failed");

      ICollection<int> res = myRC.GetResult();
      Assert.AreEqual(2, res.Count);

      foreach (int o in res)
      {
        Util.Log("results " + o);
      }
      
    }

    public void genericFEResultStringTest(string locators)
    {
      IRegion<string, string> region = CacheHelper.GetVerifyRegion<string, string>(QERegionName);
      Pool pl = PoolManager.Find(poolName);

      for (int n = 0; n < 34; n++)
      {
        region["KEY--" + n] = "VALUE--" + n;
      }

      ICollection<string> routingObj = new List<string>();

      int j = 0;
      for (int i = 0; i < 34; i++)
      {
        if (i % 2 == 0) continue;
        routingObj.Add("KEY--" + i);
        j++;
      }
      Console.WriteLine("routingObj count= {0}.", routingObj.Count);

      ArrayList args1 = new ArrayList();
      
      int count = 0;
      foreach (string str in routingObj)
      {
        Util.Log("string at index {0} = {1}.", count, str);
        args1.Add(str);
        count++;
      }

      MyResultCollector<string> myRC = new MyResultCollector<string>();
      Apache.Geode.Client.Execution<string> exc = Client.FunctionService<string>.OnServers/*OnRegion<string, string>*/(pl/*region*/);
      Client.IResultCollector<string> rc = exc.WithArgs<ArrayList>(args1).WithCollector(myRC).Execute("SingleStrGetFunction");
      
      Util.Log("add result count= {0}.", myRC.GetAddResultCount());
      Util.Log("get result count= {0}.", myRC.GetGetResultCount());
      Util.Log("end result count= {0}.", myRC.GetEndResultCount());
      
      Assert.IsTrue(myRC.GetResult().Count == 2, "result count check failed");

      ICollection<string> res = myRC.GetResult();
      Assert.AreEqual(2, res.Count);
      foreach (string o in res)
      {
        Util.Log("results " + o);
      }
      
    }

    public void genericFEResultDCStringTest(string locators)
    {
      IRegion<string, string> region = CacheHelper.GetVerifyRegion<string, string>(QERegionName);
      Pool pl = PoolManager.Find(poolName);

      for (int n = 0; n < 34; n++)
      {
        region["KEY--" + n] = "VALUE--" + n;
      }

      ICollection<string> routingObj = new List<string>();

      int j = 0;
      for (int i = 0; i < 34; i++)
      {
        if (i % 2 == 0) continue;
        routingObj.Add("KEY--" + i);
        j++;
      }
      Console.WriteLine("routingObj count= {0}.", routingObj.Count);

      ArrayList args1 = new ArrayList();

      int count = 0;
      foreach (string str in routingObj)
      {
        Util.Log("string at index {0} = {1}.", count, str);
        args1.Add(str);
        count++;
      }

      Apache.Geode.Client.Execution<string> exc = Client.FunctionService<string>.OnServers/*OnRegion<string, string>*/(pl/*region*/);
      Client.IResultCollector<string> rc = exc.WithArgs<ArrayList>(args1).Execute("SingleStrGetFunction");

      Assert.IsTrue(rc.GetResult().Count == 2, "result count check failed");

      ICollection<string> res = rc.GetResult();
      Assert.AreEqual(2, res.Count);
      foreach (string o in res)
      {
        Util.Log("results " + o);
      }

    }

    public void genericFEResultDCPdxTest(string locators)
    {
      Serializable.RegisterPdxType(PdxTests.PdxTypes1.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTests.PdxTypes8.CreateDeserializable);
      IRegion<string, IPdxSerializable> region = CacheHelper.GetVerifyRegion<string, IPdxSerializable>(QERegionName);
      Pool pl = PoolManager.Find(poolName);

      for (int n = 0; n < 34; n++)
      {
        region["KEY--pdx" + n] = new PdxTests.PdxTypes8();
      }

      ICollection<string> routingObj = new List<string>();

      int j = 0;
      for (int i = 0; i < 34; i++)
      {
        if (i % 2 == 0) continue;
        routingObj.Add("KEY--pdx" + i);
        j++;
      }
      Console.WriteLine("routingObj count= {0}.", routingObj.Count);

      ArrayList args1 = new ArrayList();

      int count = 0;
      foreach (string str in routingObj)
      {
        Util.Log("string at index {0} = {1}.", count, str);
        args1.Add(str);
        count++;
      }

      Apache.Geode.Client.Execution<object> exc = Client.FunctionService<object>.OnServers/*OnRegion<string, string>*/(pl/*region*/);
      Client.IResultCollector<object> rc = exc.WithArgs<ArrayList>(args1).Execute("PdxFunctionTest");

      Assert.IsTrue(rc.GetResult().Count == 2, "result count check failed");

      ICollection<object> res = rc.GetResult();
      Assert.AreEqual(2, res.Count);
      foreach (ICollection o in res)
      {
        Util.Log("results " + o);
        Assert.IsTrue(o.Count == 1);
        foreach (IPdxSerializable ip in o)
        {
          Assert.IsNotNull(ip);
        }
      }

    }

    public void HAStepOne()
    {

      Region region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      for (int i = 0; i < 226; i++)
      {
        region["KEY--" + i] = "VALUE--" + i;
      }

      object[] routingObj = new object[17];
      int j = 0;
      for (int i = 0; i < 34; i++)
      {
        if (i % 2 == 0) continue;
        routingObj[j] = "KEY--" + i;
        j++;
      }
      Util.Log("routingObj count= {0}.", routingObj.Length);

      bool args = true;
      //test data dependant function execution
      //     test get function with result
      Apache.Geode.Client.Execution<object> exc = Client.FunctionService<object>.OnRegion<object, object>(region);
      Client.IResultCollector<object> rc = exc.WithArgs<bool>(args).WithFilter<object>(routingObj).Execute(getFuncName);
      ICollection<object> executeFunctionResult = rc.GetResult();
      List<object> resultList = new List<object>();
      //resultList.Clear();

      Util.Log("executeFunctionResult Length = {0}", executeFunctionResult.Count);

      foreach (List<object> item in executeFunctionResult)
      {

        foreach (object item2 in item)
        {
          resultList.Add(item2);
        }
      }

      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 34, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on region:get:result[{0}]={1}.", i, (string)resultList[i]);
      }

      // Bring down the region
      region.GetLocalView().DestroyRegion();
    }

    public void OnRegionHA()
    {
      Region region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      for (int i = 0; i < 230; i++)
      {
        region[i] = "VALUE--" + i;
      }

      Object[] routingObj = new Object[17];
      ArrayList args1 = new ArrayList();
      int j = 0;
      for (int i = 0; i < 34; i++)
      {
        if (i % 2 == 0) continue;
        routingObj[j] = i;
        j++;
      }
      Util.Log("routingObj count= {0}.", routingObj.Length);

      for (int i = 0; i < routingObj.Length; i++)
      {
        Util.Log("routingObj[{0}]={1}.", i, (int)routingObj[i]);
        args1.Add(routingObj[i]);
      }

      //test data independant function execution with result OnRegion

      Apache.Geode.Client.Execution<object> exc = Client.FunctionService<object>.OnRegion<object, object>(region);

      Assert.IsTrue(exc != null, "onRegion Returned NULL");

      Client.IResultCollector<object> rc = exc.WithArgs<ArrayList>(args1).Execute(RegionOperationsHAFunctionPrSHOP, 15);

      ICollection<object> executeFunctionResult = rc.GetResult();
      List<Object> resultList = new List<Object>();
      Console.WriteLine("executeFunctionResult.Length = {0}", executeFunctionResult.Count);

      foreach (List<object> item in executeFunctionResult)
      {
        foreach (object item2 in item)
        {
          resultList.Add(item2);
        }
      }

      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 17, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on region:get:result[{0}]={1}.", i, (string)resultList[i]);
        Assert.IsTrue((string)resultList[i] != null, "onRegion Returned NULL");
      }
    }


    public void OnRegionHAStepOne()
    {
      Region region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      for (int i = 0; i < 226; i++)
      {
        region["KEY--" + i] = "VALUE--" + i;
      }

      Object[] routingObj = new Object[17];
      ArrayList args1 = new ArrayList();
      int j = 0;
      for (int i = 0; i < 34; i++)
      {
        if (i % 2 == 0) continue;
        routingObj[j] = "KEY--" + i;
        j++;
      }
      Util.Log("routingObj count= {0}.", routingObj.Length);

      for (int i = 0; i < routingObj.Length; i++)
      {
        Console.WriteLine("routingObj[{0}]={1}.", i, (string)routingObj[i]);
        args1.Add(routingObj[i]);
      }

      //test data independant function execution with result onServer
      
      Apache.Geode.Client.Execution<object> exc = Client.FunctionService<object>.OnRegion<object, object>(region);

      Assert.IsTrue(exc != null, "onRegion Returned NULL");

      Client.IResultCollector<object> rc = exc.WithArgs<ArrayList>(args1).Execute(RegionOperationsHAFunction, 15);

      ICollection<object> executeFunctionResult = rc.GetResult();
      List<Object> resultList = new List<Object>();
      Console.WriteLine("executeFunctionResult.Length = {0}", executeFunctionResult.Count);
     
      foreach (List<object> item in executeFunctionResult)
      {
        foreach (object item2 in item)
        {
          resultList.Add(item2);
        }
      }      

      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 17, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on region:get:result[{0}]={1}.", i, (string)resultList[i]);
        Assert.IsTrue((string)resultList[i] != null, "onServer Returned NULL");
      }

      ///////////////OnRegion with Single filter Key ////////////////

      Object[] filter = new Object[1];
      filter[0] = "KEY--" + 10;
      rc = exc.WithArgs<ArrayList>(args1).WithFilter<Object>(filter).Execute(RegionOperationsHAFunction, 15);

      executeFunctionResult = rc.GetResult();
      resultList = new List<Object>();
      Console.WriteLine("executeFunctionResult.Length = {0}", executeFunctionResult.Count);

      foreach (List<object> item in executeFunctionResult)
      {
        foreach (object item2 in item)
        {
          resultList.Add(item2);
        }
      }

      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 17, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on region:get:result[{0}]={1}.", i, (string)resultList[i]);
        Assert.IsTrue((string)resultList[i] != null, "onServer Returned NULL");
      }

      ///////////////OnRegion with Single filter Key Done////////////////
    }


    public void OnRegionPrSHOPSingleFilterKey()
    {
      Region region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);

      for (int i = 0; i < 230; i++)
      {
        region[i] = "VALUE--" + i;
      }
      Util.Log("Put on region complete ");
      ///////////////////// OnRegion with single filter key /////////////////////////////
      Object[] filter = new Object[1];
      for (int i = 0; i < 230; i++)
      {
        filter[0] = i;
        Util.Log("filter count= {0}.", filter.Length);


        Execution<object> exc = Client.FunctionService<object>.OnRegion<object, object>(region);        
        IResultCollector<object> rc = exc.WithFilter<object>(filter).Execute(FEOnRegionPrSHOP);
        ICollection<object> executeFunctionResult = rc.GetResult();
        Util.Log("OnRegionPrSHOPSingleFilterKey for filter executeFunctionResult.Count = {0} ", executeFunctionResult.Count);
        Assert.AreEqual(1, executeFunctionResult.Count, "executeFunctionResult count check failed");
        foreach (Boolean item in executeFunctionResult)
        {
          Util.Log("on region:FEOnRegionPrSHOP:= {0}.", item);
          Assert.AreEqual(true, item, "FEOnRegionPrSHOP item not true");
        }
        Util.Log("FEOnRegionPrSHOP done");

        rc = exc.WithFilter<object>(filter).Execute(FEOnRegionPrSHOP_OptimizeForWrite);
        executeFunctionResult = rc.GetResult();
        Util.Log("OnRegionPrSHOPSingleFilterKey for FEOnRegionPrSHOP_OptimizeForWrite executeFunctionResult.Count = {0} ", executeFunctionResult.Count);
        Assert.AreEqual(1, executeFunctionResult.Count, "executeFunctionResult.Count check failed");
        foreach (Boolean item in executeFunctionResult)
        {
          Util.Log("on region:FEOnRegionPrSHOP_OptimizeForWrite:= {0}.", item);
          Assert.AreEqual(true, item, "FEOnRegionPrSHOP_OptimizeForWrite item not true");
        }
        Util.Log("FEOnRegionPrSHOP_OptimizeForWrite done");
      }              
      ///////////////////// OnRegion with single filter key done/////////////////////////////
    }

    public void FEOnRegionTx()
    {
      Region region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      CacheTransactionManager csTx = CacheHelper.CSTXManager;
      csTx.Begin();
      Util.Log("Transaction begun.");

      for (int i = 0; i < 230; i++)
      {
        region["KEY--" + i] = "VALUE--" + i;
      }
      Util.Log("Put on region complete ");

      int j = 0;
      Object[] filter = new Object[20];
      for (int i = 0; i < 20; i++)
      {
        filter[j] = "KEY--" + i;
        j++;
      }
      Util.Log("filter count= {0}.", filter.Length);


      Execution<object> exc = Client.FunctionService<object>.OnRegion<object, object>(region);

      IResultCollector<object> rc = exc.WithFilter<object>(filter).Execute(putFuncName);

      Util.Log("Executing ExecuteFunctionOnRegion on region for execKeys for arrList arguement done.");

      csTx.Commit();
      Util.Log("Transaction commited");

      for (int i = 0; i < filter.Length; i++)
      {
        String str = (String)filter[i];
        String val = (String)region[str];
        Util.Log("Filter Key = {0}, get Value = {1} ", str, val);
        if (!str.Equals(val))
          Assert.Fail("Value after function execution and transaction is incorrect");
      }      
    }

    public void ExecuteFEOnRegionMultiFilterKeyStepOne()
    {
      Region region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);      

      for (int k = 0; k < 210; k++)
      {
        int j = 0;
        Object[] filter = new Object[20];
        for (int i = k; i < k + 20; i++)
        {
          filter[j] = i;
          j++;
        }
        Util.Log("filter count= {0}.", filter.Length);

        object args = true;

        Execution<object> exc = Client.FunctionService<object>.OnRegion<object, object>(region);

        IResultCollector<object> rc = exc.WithFilter<object>(filter).Execute(getFuncName);
        ICollection<object> executeFunctionResult = rc.GetResult();

        Util.Log("ExecuteFEOnRegionMultiFilterKeyStepOne for filter executeFunctionResult.Count = {0} ", executeFunctionResult.Count);

        List<object> resultList = new List<object>();
        foreach (List<object> item in executeFunctionResult)
        {
          foreach (object item2 in item)
          {
            resultList.Add(item2);
          }
        }

        Util.Log("on region: result count= {0}.", resultList.Count);
        Assert.AreEqual(40, resultList.Count, "result count check failed");
        for (int i = 0; i < resultList.Count; i++)
        {
          Util.Log("on region:get:= {0}.", resultList[i]);
        }

        Util.Log("Executing ExecuteFEOnRegionMultiFilterKeyStepOne on region for execKeys for arrList arguement done.");

        MyResultCollector<object> myRC1 = new MyResultCollector<object>();
        rc = exc.WithFilter<object>(filter).WithCollector(myRC1).Execute(getFuncName);
        executeFunctionResult = rc.GetResult();
        Util.Log("add result count= {0}.", myRC1.GetAddResultCount());
        Util.Log("get result count= {0}.", myRC1.GetGetResultCount());
        Util.Log("end result count= {0}.", myRC1.GetEndResultCount());
        Assert.AreEqual(4, myRC1.GetAddResultCount(), "add result count check failed");
        Assert.AreEqual(1, myRC1.GetGetResultCount(), "get result count check failed");
        Assert.AreEqual(1, myRC1.GetEndResultCount(), "end result count check failed");
        Util.Log("on Region with collector: result count= {0}.", executeFunctionResult.Count);

        resultList.Clear();
        foreach (List<object> item in executeFunctionResult)
        {
          foreach (object item2 in item)
          {
            resultList.Add(item2);
          }
        }

        Util.Log("on region: result count with custom ResultCollector = {0}.", resultList.Count);
        Assert.AreEqual(40, resultList.Count, "result count check failed");
        for (int i = 0; i < resultList.Count; i++)
        {
          Util.Log("on region:get:= {0}.", resultList[i]);
        }
      }
    }

    public void ExecuteFETimeOut()
    {
      Region region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      for (int i = 0; i < 230; i++)
      {
        region[i] = "VALUE--" + i;
      }

      object args = 5000;

      for (int k = 0; k < 210; k++)
      {
        int j = 0;
        Object[] filter = new Object[20];
        for (int i = k; i < k + 20; i++)
        {
          filter[j] = i;
          j++;
        }
        Util.Log("filter count= {0}.", filter.Length);

        Execution<object> exc = Client.FunctionService<object>.OnRegion<object, object>(region);
        IResultCollector<object> rc = exc.WithArgs<Object>(args).WithFilter<object>(filter).Execute(FuncTimeOutName, 5000);
        ICollection<object> FunctionResult = rc.GetResult();
        Util.Log("ExecuteFETimeOut onRegion FunctionResult.Count = {0} ", FunctionResult.Count);        
        foreach (Boolean item in FunctionResult)
        {
          Util.Log("on region:ExecuteFETimeOut:= {0}.", item);
          Assert.AreEqual(true, item, "ExecuteFETimeOut item not true");
          break;
        }
        Util.Log("ExecuteFETimeOut onRegion Done");
      }

      Pool pool = PoolManager.Find(poolName);
      Execution<object> excs = Client.FunctionService<object>.OnServer(pool);
      IResultCollector<object> rcs = excs.WithArgs<Object>(args).Execute(FuncTimeOutName, 5000);
      ICollection<object> ServerFunctionResult = rcs.GetResult();
      Util.Log("ExecuteFETimeOut onServer FunctionResult.Count = {0} ", ServerFunctionResult.Count);
      foreach (Boolean item in ServerFunctionResult)
      {
        Util.Log("on server:ExecuteFETimeOut:= {0}.", item);
        Assert.AreEqual(true, item, "ExecuteFETimeOut item not true");
      }
      Util.Log("ExecuteFETimeOut onServer Done");


      Execution<object> excss = Client.FunctionService<object>.OnServers(pool);
      IResultCollector<object> rcss = excss.WithArgs<Object>(args).Execute(FuncTimeOutName, 5000);
      ICollection<object> ServerFunctionResults = rcss.GetResult();
      Util.Log("ExecuteFETimeOut onServer FunctionResult.Count = {0} ", ServerFunctionResults.Count);
      foreach (Boolean item in ServerFunctionResults)
      {
        Util.Log("on servers:ExecuteFETimeOut:= {0}.", item);
        Assert.AreEqual(true, item, "ExecuteFETimeOut item not true");
      }
      Util.Log("ExecuteFETimeOut onServers Done");        
    }

    public void ExecuteFEOnRegionMultiFilterKeyStepTwo()
    {
      Region region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);

      for (int i = 0; i < 230; i++)
      {
        region[i] = "VALUE--" + i;
      }
      Util.Log("Put on region complete ");
            
      for (int k = 0; k < 210; k++)
      {
        int j = 0;
        Object[] filter = new Object[20];
        for (int i = k; i < k + 20; i++)
        {
          filter[j] = i;
          j++;
        }
        Util.Log("filter count= {0}.", filter.Length);

        object args = true;

        Execution<object> exc = Client.FunctionService<object>.OnRegion<object, object>(region);

        IResultCollector<object> rc = exc.WithFilter<object>(filter).Execute(getFuncName);
        ICollection<object> executeFunctionResult = rc.GetResult();

        Util.Log("ExecuteFEOnRegionMultiFilterKeyStepTwo for filter executeFunctionResult.Count = {0} ", executeFunctionResult.Count);

        List<object> resultList = new List<object>();
        foreach (List<object> item in executeFunctionResult)
        {
          foreach (object item2 in item)
          {
            resultList.Add(item2);
          }
        }

        Util.Log("on region: result count= {0}.", resultList.Count);
        Assert.AreEqual(40, resultList.Count, "result count check failed");
        for (int i = 0; i < resultList.Count; i++)
        {
          Util.Log("on region:get:= {0}.", resultList[i]);
        }

        Util.Log("Executing ExecuteFEOnRegionMultiFilterKeyStepTwo on region for execKeys for arrList arguement done.");

        MyResultCollector<object> myRC1 = new MyResultCollector<object>();
        rc = exc.WithFilter<object>(filter).WithCollector(myRC1).Execute(getFuncName);
        executeFunctionResult = rc.GetResult();
        Util.Log("add result count= {0}.", myRC1.GetAddResultCount());
        Util.Log("get result count= {0}.", myRC1.GetGetResultCount());
        Util.Log("end result count= {0}.", myRC1.GetEndResultCount());
        Assert.AreEqual(4, myRC1.GetAddResultCount(), "add result count check failed");
        Assert.AreEqual(1, myRC1.GetGetResultCount(), "get result count check failed");
        Assert.AreEqual(1, myRC1.GetEndResultCount(), "end result count check failed");
        Util.Log("on Region with collector: result count= {0}.", executeFunctionResult.Count);

        resultList.Clear();
        foreach (List<object> item in executeFunctionResult)
        {
          foreach (object item2 in item)
          {
            resultList.Add(item2);
          }
        }

        Util.Log("on region: result count with custom ResultCollector = {0}.", resultList.Count);
        Assert.AreEqual(40, resultList.Count, "result count check failed");
        for (int i = 0; i < resultList.Count; i++)
        {
          Util.Log("on region:get:= {0}.", resultList[i]);
        }
      }      
    }

    public void OnRegionMultiFilterKeyPrSHOP()
    {
      Region region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      region.GetSubscriptionService().RegisterAllKeys();
      for (int i = 0; i < 230; i++)
      {
        region[i] = "VALUE--" + i;
      }
      Util.Log("Put on region complete ");

      for (int k = 0; k < 210; k++)
      {
        Object[] filter = new Object[20];
        int j = 0;
        for (int i = k; i < k + 20; i++)
        {
          filter[j] = i;
          j++;
        }
        Util.Log("filter count= {0}.", filter.Length);

        object args = true;

        Execution<object> exc = Client.FunctionService<object>.OnRegion<object, object>(region);

        IResultCollector<object> rc = exc.WithFilter<object>(filter).Execute(FEOnRegionPrSHOP);
        ICollection<object> executeFunctionResult = rc.GetResult();
        Util.Log("OnRegionMultiFilterKeyPrSHOP for filter executeFunctionResult.Count = {0} ", executeFunctionResult.Count);
        Assert.AreEqual(2, executeFunctionResult.Count, "executeFunctionResult count check failed");
        foreach (Boolean item in executeFunctionResult)
        {
          Util.Log("on region:OnRegionMultiFilterKeyPrSHOP:= {0}.", item);
          Assert.AreEqual(true, item, "FEOnRegionPrSHOP item not true");
        }
        Util.Log("OnRegionMultiFilterKeyPrSHOP done");

        rc = exc.WithFilter<object>(filter).Execute(FEOnRegionPrSHOP_OptimizeForWrite);
        executeFunctionResult = rc.GetResult();
        Util.Log("OnRegionMultiFilterKeyPrSHOP for FEOnRegionPrSHOP_OptimizeForWrite executeFunctionResult.Count = {0} ", executeFunctionResult.Count);
        Assert.AreEqual(3, executeFunctionResult.Count, "executeFunctionResult.Count check failed");
        foreach (Boolean item in executeFunctionResult)
        {
          Util.Log("on region:FEOnRegionPrSHOP_OptimizeForWrite:= {0}.", item);
          Assert.AreEqual(true, item, "FEOnRegionPrSHOP_OptimizeForWrite item not true");
        }
        Util.Log("FEOnRegionPrSHOP_OptimizeForWrite done");

        MyResultCollector<object> myRC1 = new MyResultCollector<object>();
        rc = exc.WithFilter<object>(filter).WithCollector(myRC1).Execute(FEOnRegionPrSHOP);
        executeFunctionResult = rc.GetResult();
        Util.Log("add result count= {0}.", myRC1.GetAddResultCount());
        Util.Log("get result count= {0}.", myRC1.GetGetResultCount());
        Util.Log("end result count= {0}.", myRC1.GetEndResultCount());
        Assert.AreEqual(2, myRC1.GetAddResultCount(), "add result count check failed");
        Assert.AreEqual(1, myRC1.GetGetResultCount(), "get result count check failed");
        Assert.AreEqual(1, myRC1.GetEndResultCount(), "end result count check failed");
        executeFunctionResult = rc.GetResult();
        Util.Log("OnRegionMultiFilterKeyPrSHOP for filter executeFunctionResult.Count = {0} ", executeFunctionResult.Count);
        Assert.AreEqual(2, executeFunctionResult.Count, "executeFunctionResult count check failed");
        foreach (Boolean item in executeFunctionResult)
        {
          Util.Log("on region:FEOnRegionPrSHOP:= {0}.", item);
          Assert.AreEqual(true, item, "FEOnRegionPrSHOP item not true");
        }
        Util.Log("FEOnRegionPrSHOP with ResultCollector done");

        MyResultCollector<object> myRC2 = new MyResultCollector<object>();
        rc = exc.WithFilter<object>(filter).WithCollector(myRC2).Execute(FEOnRegionPrSHOP_OptimizeForWrite);
        executeFunctionResult = rc.GetResult();
        Util.Log("add result count= {0}.", myRC2.GetAddResultCount());
        Util.Log("get result count= {0}.", myRC2.GetGetResultCount());
        Util.Log("end result count= {0}.", myRC2.GetEndResultCount());
        Assert.AreEqual(3, myRC2.GetAddResultCount(), "add result count check failed");
        Assert.AreEqual(1, myRC2.GetGetResultCount(), "get result count check failed");
        Assert.AreEqual(1, myRC2.GetEndResultCount(), "end result count check failed");
        executeFunctionResult = rc.GetResult();
        Util.Log("OnRegionMultiFilterKeyPrSHOP for FEOnRegionPrSHOP_OptimizeForWrite executeFunctionResult.Count = {0} ", executeFunctionResult.Count);
        Assert.AreEqual(3, executeFunctionResult.Count, "executeFunctionResult.Count check failed");
        foreach (Boolean item in executeFunctionResult)
        {
          Util.Log("on region:FEOnRegionPrSHOP_OptimizeForWrite:= {0}.", item);
          Assert.AreEqual(true, item, "FEOnRegionPrSHOP_OptimizeForWrite item not true");
        }
        Util.Log("FEOnRegionPrSHOP_OptimizeForWrite with ResultCollector done");
      }
      
      Execution<object> exe = Client.FunctionService<object>.OnRegion<object, object>(region);      

      //w/o filter      
      IResultCollector<object> collector = exe.Execute(FEOnRegionPrSHOP);
      ICollection<Object> FunctionResult = collector.GetResult();
      Util.Log("OnRegionMultiFilterKeyPrSHOP for filter FunctionResult.Count = {0} ", FunctionResult.Count);
      Assert.AreEqual(2, FunctionResult.Count, "FunctionResult count check failed");
      foreach (Boolean item in FunctionResult)
      {
        Util.Log("on region:FEOnRegionPrSHOP:= {0}.", item);
        Assert.AreEqual(true, item, "FEOnRegionPrSHOP item not true");
      }
      collector.ClearResults();
      Util.Log("FEOnRegionPrSHOP without filter done");

      // w/o filter
      MyResultCollector<object> rC = new MyResultCollector<object>();
      IResultCollector<Object> Rcollector = exe.WithCollector(rC).Execute(FEOnRegionPrSHOP);
      FunctionResult = Rcollector.GetResult();
      Util.Log("add result count= {0}.", rC.GetAddResultCount());
      Util.Log("get result count= {0}.", rC.GetGetResultCount());
      Util.Log("end result count= {0}.", rC.GetEndResultCount());
      Assert.AreEqual(2, rC.GetAddResultCount(), "add result count check failed");
      Assert.AreEqual(1, rC.GetGetResultCount(), "get result count check failed");
      Assert.AreEqual(1, rC.GetEndResultCount(), "end result count check failed");
      FunctionResult = Rcollector.GetResult();
      Util.Log("OnRegionMultiFilterKeyPrSHOP for filter FunctionResult.Count = {0} ", FunctionResult.Count);
      Assert.AreEqual(2, FunctionResult.Count, "executeFunctionResult count check failed");
      foreach (Boolean item in FunctionResult)
      {
        Util.Log("on region:FEOnRegionPrSHOP:= {0}.", item);
        Assert.AreEqual(true, item, "FEOnRegionPrSHOP item not true");
      }
      Util.Log("FEOnRegionPrSHOP with ResultCollector without filter done");

      //w/o filter
      collector.ClearResults();
      collector = exe.Execute(FEOnRegionPrSHOP_OptimizeForWrite);
      Util.Log("OnRegionMultiFilterKeyPrSHOP for FEOnRegionPrSHOP_OptimizeForWrite executeFunctionResult.Count = {0} ", collector.GetResult().Count);
      Assert.AreEqual(3, collector.GetResult().Count, "executeFunctionResult.Count check failed");
      foreach (Boolean item in collector.GetResult())
      {
        Util.Log("on region:FEOnRegionPrSHOP_OptimizeForWrite:= {0}.", item);
        Assert.AreEqual(true, item, "FEOnRegionPrSHOP_OptimizeForWrite item not true");
      }
      Util.Log("FEOnRegionPrSHOP_OptimizeForWrite done w/o filter");      

      //w/o filter
      MyResultCollector<object> rC2 = new MyResultCollector<object>();
      Rcollector = exe.WithCollector(rC2).Execute(FEOnRegionPrSHOP_OptimizeForWrite);
      FunctionResult = Rcollector.GetResult();
      Util.Log("add result count= {0}.", rC2.GetAddResultCount());
      Util.Log("get result count= {0}.", rC2.GetGetResultCount());
      Util.Log("end result count= {0}.", rC2.GetEndResultCount());
      Assert.AreEqual(3, rC2.GetAddResultCount(), "add result count check failed");
      Assert.AreEqual(1, rC2.GetGetResultCount(), "get result count check failed");
      Assert.AreEqual(1, rC2.GetEndResultCount(), "end result count check failed");
      Util.Log("OnRegionMultiFilterKeyPrSHOP for FEOnRegionPrSHOP_OptimizeForWrite FunctionResult.Count = {0} ", FunctionResult.Count);
      Assert.AreEqual(3, FunctionResult.Count, "executeFunctionResult.Count check failed");
      foreach (Boolean item in FunctionResult)
      {
        Util.Log("on region:FEOnRegionPrSHOP_OptimizeForWrite:= {0}.", item);
        Assert.AreEqual(true, item, "FEOnRegionPrSHOP_OptimizeForWrite item not true");
      }
      Util.Log("FEOnRegionPrSHOP_OptimizeForWrite with ResultCollector w/o filter done");

      for (int i = 0; i < 500; i++)
      {
        region["KEY--" + i] = "VALUE--" + i;
      }
      Util.Log("Put on region complete ");

      Object[] fil = new Object[500];
      int x = 0;
      for (int i = 0; i < 500; i++)
      {
        fil[x] = "KEY--" + i;
        x++;
      }
      Util.Log("filter count= {0}.", fil.Length);

      // Fire N Forget with filter keys
      exe = Client.FunctionService<object>.OnRegion<object, object>(region);
      exe.WithFilter<object>(fil).Execute(putFuncName);
      Util.Log("Executing ExecuteFunctionOnRegion on region for execKeys for arrList arguement done.");
      Thread.Sleep(4000); //wait for results to be updated

      for (int i = 0; i < fil.Length; i++)
      {
        String str = (String)fil[i];
        String val = (String)region[str];
        Util.Log("Filter Key = {0}, get Value = {1} ", str, val);
        if (!str.Equals(val))
          Assert.Fail("Value after function execution is incorrect");
      }

      // Fire N Forget without filter keys
      ArrayList args1 = new ArrayList();
      for (int i = 10; i < 200; i++)
      {
        args1.Add("KEY--" + i);
      }
      exe = Client.FunctionService<object>.OnRegion<object, object>(region);
      exe.WithArgs<ArrayList>(args1).Execute(putFuncIName);
      Util.Log("Executing ExecuteFunctionOnRegion on region for execKeys for arrList arguement done.");
      Thread.Sleep(4000); ////wait for results to be updated

      for (int i = 0; i < args1.Count; i++)
      {
        String str = (String)args1[i];
        String val = (String)region[str];
        Util.Log("Arg Key = {0}, get Value = {1} ", str, val);
        if (!str.Equals(val))
          Assert.Fail("Value after function execution is incorrect");
      }
    }

    public void OnServerHAStepOne()
    {

      Region region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      for (int i = 0; i < 34; i++)
      {
        region["KEY--" + i] = "VALUE--" + i;
      }

      object[] routingObj = new object[17];

      ArrayList args1 = new ArrayList();

      int j = 0;
      for (int i = 0; i < 34; i++)
      {
        if (i % 2 == 0) continue;
        routingObj[j] = "KEY--" + i;
        j++;
      }
      Util.Log("routingObj count= {0}.", routingObj.Length);

      for (int i = 0; i < routingObj.Length; i++)
      {
        Console.WriteLine("routingObj[{0}]={1}.", i, (string)routingObj[i]);
        args1.Add(routingObj[i]);
      }

      //test data independant function execution with result onServer
      Pool/*<TKey, TValue>*/ pool = PoolManager/*<TKey, TValue>*/.Find(poolName);
      
      Apache.Geode.Client.Execution<object> exc = Client.FunctionService<object>.OnServer(pool);
      Assert.IsTrue(exc != null, "onServer Returned NULL");

      Client.IResultCollector<object> rc = exc.WithArgs<ArrayList>(args1).Execute(OnServerHAExceptionFunction, 15);

      ICollection<object> executeFunctionResult = rc.GetResult();

      List<object> resultList = new List<object>();

      Console.WriteLine("executeFunctionResult.Length = {0}", executeFunctionResult.Count);
      
      foreach (List<object> item in executeFunctionResult)
      {
        foreach (object item2 in item)
        {
          resultList.Add(item2);
        }
      }

      Util.Log("on region: result count= {0}.", resultList.Count);
      Assert.IsTrue(resultList.Count == 17, "result count check failed");
      for (int i = 0; i < resultList.Count; i++)
      {
        Util.Log("on region:get:result[{0}]={1}.", i, (string)resultList[i]);
        Assert.IsTrue(((string)resultList[i]) != null, "onServer Returned NULL");
      }

      rc = exc.WithArgs<ArrayList>(args1).Execute(OnServerHAShutdownFunction, 15);

      ICollection<object> executeFunctionResult1 = rc.GetResult();

      List<object> resultList1 = new List<object>();
     
      foreach (List<object> item in executeFunctionResult1)
      {
        foreach (object item2 in item)
        {
          resultList1.Add(item2);
        }
      }

      Util.Log("on region: result count= {0}.", resultList1.Count);

      Console.WriteLine("resultList1.Count = {0}", resultList1.Count);

      Assert.IsTrue(resultList1.Count == 17, "result count check failed");
      for (int i = 0; i < resultList1.Count; i++)
      {
        Util.Log("on region:get:result[{0}]={1}.", i, (string)resultList1[i]);
        Assert.IsTrue(((string)resultList1[i]) != null, "onServer Returned NULL");
      }

      // Bring down the region
      //region.LocalDestroyRegion();
    }

    void runOnServerHAExecuteFunction()
    {
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

      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 1, true, true, /*threadLocal*/true);
          m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);
          Util.Log("Client 1 (pool locator) regions created");

      m_client1.Call(OnServerHAStepOne);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFEOnRegionPrSHOPSingleFilterKey()
    {
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
      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 1, true, true, true);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);
      Util.Log("Client 1 (pool locator) regions created");

      m_client1.Call(OnRegionPrSHOPSingleFilterKey);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runOnRegionHAExecuteFunction()
    {
      CacheHelper.SetupJavaServers(true, "func_cacheserver1_pool.xml",
      "func_cacheserver2_pool.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");

      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 1, true, /*singleHop*/false, /*threadLocal*/false);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);
      Util.Log("Client 1 (pool locator) regions created");

      m_client1.Call(OnRegionHAStepOne);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runOnRegionHAExecuteFunctionPrSHOP()
    {
      CacheHelper.SetupJavaServers(true, "func_cacheserver1_pool.xml",
      "func_cacheserver2_pool.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");

      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 1, true, /*singleHop*/true, /*threadLocal*/true);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);
      Util.Log("Client 1 (pool locator) regions created");

      m_client1.Call(OnRegionHA);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runExecuteFunctionOnRegionMultiFilterKey(bool singleHop)
    {
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

      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 1, true, singleHop, /*threadLocal*/false);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);
      Util.Log("Client 1 (pool locator) regions created");

      m_client1.Call(ExecuteFEOnRegionMultiFilterKeyStepOne);
      m_client1.Call(ExecuteFEOnRegionMultiFilterKeyStepTwo);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runExecuteFunctionTimeOut(bool singleHop)
    {
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

      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 1, true, singleHop, /*threadLocal*/false);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);
      Util.Log("Client 1 (pool locator) regions created");

      m_client1.Call(ExecuteFETimeOut);      

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFEOnRegionTx(bool singleHop)
    {
      CacheHelper.SetupJavaServers(true, "func_cacheserver1_pool.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");        

      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 1, true, singleHop, /*threadLocal*/true);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);
      Util.Log("Client 1 (pool locator) regions created");

      m_client1.Call(FEOnRegionTx);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");      

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFEOnRegionPrSHOPMultiFilterKey(bool singleHop)
    {
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

      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 1, true, singleHop, /*threadLocal*/true);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);
      Util.Log("Client 1 (pool locator) regions created");

      m_client1.Call(OnRegionMultiFilterKeyPrSHOP);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

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
      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 0, true, /*singleHop*/false, /*threadLocal*/false);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);

      m_client1.Call(PoolStepOne, CacheHelper.Locators);
      m_client1.Call(genericFEResultIntTest, CacheHelper.Locators);
      m_client1.Call(genericFEResultStringTest, CacheHelper.Locators);
      m_client1.Call(genericFEResultDCStringTest, CacheHelper.Locators);
      m_client1.Call(genericFEResultDCPdxTest, CacheHelper.Locators);

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
    public void PoolExecuteFunctionTestPrSHOP()
    {
      CacheHelper.SetupJavaServers(true, "func_cacheserver1_pool.xml",
        "func_cacheserver2_pool.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");
      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 0, true, /*singleHop*/true, /*threadLocal*/true);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);

      m_client1.Call(PoolStepOne, CacheHelper.Locators);
      m_client1.Call(genericFEResultIntTest, CacheHelper.Locators);
      m_client1.Call(genericFEResultStringTest, CacheHelper.Locators);
      m_client1.Call(genericFEResultDCStringTest, CacheHelper.Locators);
      m_client1.Call(genericFEResultDCPdxTest, CacheHelper.Locators);

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
        "func_cacheserver2_pool.xml", "func_cacheserver3_pool.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");
      CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
      Util.Log("Cacheserver 3 started.");
      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 1, true, /*singleHop*/false, /*threadLocal*/false);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);

      m_client1.Call(HAStepOne);
      Util.Log("HAStepOne complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");
      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");
    }

    [Test]
    public void HAExecuteFunctionTestPrSHOP()
    {
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
      m_client1.Call(createPool, poolName, CacheHelper.Locators, serverGroup, 1, true, /*singleHop*/true, true);
      m_client1.Call(createRegionAndAttachPool, QERegionName, poolName);

      m_client1.Call(HAStepOne);
      Util.Log("HAStepOne complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");
    }

    [Test]
    public void OnServerHAExecuteFunctionTest()
    {
      runOnServerHAExecuteFunction();
    }

    [Test]
    public void OnRegionHAExecuteFunctionTest()
    {
      runOnRegionHAExecuteFunction();
    }

    [Test]
    public void TestFEOnRegionPrSHOPSingleFilterKey()
    {
      runFEOnRegionPrSHOPSingleFilterKey();
    }

    [Test]
    public void OnRegionHAExecuteFunctionTestPrSHOP()
    {
      runOnRegionHAExecuteFunctionPrSHOP();
    }

    [Test]
    public void ExecuteFunctionOnRegionMultiFilterKey()
    {
      runExecuteFunctionOnRegionMultiFilterKey(false);
      runExecuteFunctionOnRegionMultiFilterKey(true);
    }

    [Test]
    public void ExecuteFunctionTimeOut()
    {
      runExecuteFunctionTimeOut(false);
      runExecuteFunctionTimeOut(true);
    }

    [Test]
    public void TestFEOnRegionPrSHOPMultiFilterKey()
    {
      runFEOnRegionPrSHOPMultiFilterKey(true);
    }

    [Test]
    public void TestFEOnRegionTransaction()
    {
      runFEOnRegionTx(true);
      runFEOnRegionTx(false);
    }
  }
}
