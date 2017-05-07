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

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientQueryTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1;
    private UnitProcess m_client2;
    private static string[] QueryRegionNames = { "Portfolios", "Positions", "Portfolios2",
      "Portfolios3" };
    private static string QERegionName = "Portfolios";
    private static string endpoint1;
    private static string endpoint2;
    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
    }

    [TestFixtureSetUp]
    public override void InitTests()
    {
      base.InitTests();
      m_client1.Call(InitClient);
      m_client2.Call(InitClient);
    }

    [TearDown]
    public override void EndTest()
    {
      CacheHelper.StopJavaServers();
      base.EndTest();
    }

    #region Functions invoked by the tests

    public void InitClient()
    {
      CacheHelper.Init();
      try
      {
        Serializable.RegisterType(Portfolio.CreateDeserializable);
        Serializable.RegisterType(Position.CreateDeserializable);
      }
      catch (IllegalStateException)
      {
        // ignore since we run multiple iterations for pool and non pool configs
      }
    }

    public void StepOneQE(string endpoints, string locators, bool pool, bool locator)
    {
      CacheHelper.Init();
      try
      {
        QueryService qsFail = null;
        if (pool)
        {
          qsFail = PoolManager.CreateFactory().Create("_TESTFAILPOOL_").GetQueryService();
        }
        else
        {
          qsFail = CacheHelper.DCache.GetQueryService();
        }
        Query qryFail = qsFail.NewQuery("select distinct * from /" + QERegionName);
        ISelectResults resultsFail = qryFail.Execute();
        Assert.Fail("Since no endpoints defined, so exception expected");
      }
      catch (IllegalStateException ex)
      {
        Util.Log("Got expected exception: {0}", ex);
      }
      
      if (pool)
      {
        if (locator)
        {
          CacheHelper.CreateTCRegion_Pool(QERegionName, true, true,
            null, (string)null, locators, "__TESTPOOL1_", true);
        }
        else
        {
          CacheHelper.CreateTCRegion_Pool(QERegionName, true, true,
            null, endpoints, (string)null, "__TESTPOOL1_", true);
        }
      }
      else
      {
        CacheHelper.CreateTCRegion(QERegionName, true, true,
          null, endpoints, true);
      }
      Region region = CacheHelper.GetVerifyRegion(QERegionName);
      Portfolio p1 = new Portfolio(1, 100);
      Portfolio p2 = new Portfolio(2, 100);
      Portfolio p3 = new Portfolio(3, 100);
      Portfolio p4 = new Portfolio(4, 100);

      region.Put("1", p1);
      region.Put("2", p2);
      region.Put("3", p3);
      region.Put("4", p4);

      QueryService qs = null;
      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }
      Query qry = qs.NewQuery("select distinct * from /" + QERegionName);
      ISelectResults results = qry.Execute();
      Int32 count = results.Size;
      Assert.AreEqual(4, count, "Expected 4 as number of portfolio objects.");

      // Bring down the region
      region.LocalDestroyRegion();
    }

    public void StepTwoQE(bool pool)
    {
      QueryService qs = null;
      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }
      Util.Log("Going to execute the query");
      Query qry = qs.NewQuery("select distinct * from /" + QERegionName);
      ISelectResults results = qry.Execute();
      Int32 count = results.Size;
      Assert.AreEqual(4, count, "Expected 4 as number of portfolio objects.");
    }

    public void StepOne(string endpoints, string locators, bool pool, bool locator)
    {
      if (pool)
      {
        if (locator)
        {
          CacheHelper.CreateTCRegion_Pool(QueryRegionNames[0], true, true,
          null, (string)null, locators, "__TESTPOOL1_", true);
          CacheHelper.CreateTCRegion_Pool(QueryRegionNames[1], true, true,
            null, (string)null, locators, "__TESTPOOL1_", true);
          CacheHelper.CreateTCRegion_Pool(QueryRegionNames[2], true, true,
            null, (string)null, locators, "__TESTPOOL1_", true);
          CacheHelper.CreateTCRegion_Pool(QueryRegionNames[3], true, true,
            null, (string)null, locators, "__TESTPOOL1_", true);
        }
        else
        {
          CacheHelper.CreateTCRegion_Pool(QueryRegionNames[0], true, true,
          null, endpoints, (string)null, "__TESTPOOL1_", true);
          CacheHelper.CreateTCRegion_Pool(QueryRegionNames[1], true, true,
            null, endpoints, (string)null, "__TESTPOOL1_", true);
          CacheHelper.CreateTCRegion_Pool(QueryRegionNames[2], true, true,
            null, endpoints, (string)null, "__TESTPOOL1_", true);
          CacheHelper.CreateTCRegion_Pool(QueryRegionNames[3], true, true,
            null, endpoints, (string)null, "__TESTPOOL1_", true);
        }
      }
      else
      {
        CacheHelper.CreateTCRegion(QueryRegionNames[0], true, true,
          null, endpoints, true);
        CacheHelper.CreateTCRegion(QueryRegionNames[1], true, true,
          null, endpoints, true);
        CacheHelper.CreateTCRegion(QueryRegionNames[2], true, true,
          null, endpoints, true);
        CacheHelper.CreateTCRegion(QueryRegionNames[3], true, true,
          null, endpoints, true);
      }

      Region region = CacheHelper.GetRegion(QueryRegionNames[0]);
      RegionAttributes regattrs = region.Attributes;
      region.CreateSubRegion(QueryRegionNames[1], regattrs);
    }

    public void StepTwo()
    {
      Region region0 = CacheHelper.GetRegion(QueryRegionNames[0]);
      Region subRegion0 = region0.GetSubRegion(QueryRegionNames[1]);
      Region region1 = CacheHelper.GetRegion(QueryRegionNames[1]);
      Region region2 = CacheHelper.GetRegion(QueryRegionNames[2]);
      Region region3 = CacheHelper.GetRegion(QueryRegionNames[3]);

      QueryHelper qh = QueryHelper.GetHelper();
      Util.Log("SetSize {0}, NumSets {1}.", qh.PortfolioSetSize,
        qh.PortfolioNumSets);

      qh.PopulatePortfolioData(region0, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
      qh.PopulatePositionData(subRegion0, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
      qh.PopulatePositionData(region1, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
      qh.PopulatePortfolioData(region2, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
      qh.PopulatePortfolioData(region3, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
    }

    public void StepTwoQT()
    {
      Region region0 = CacheHelper.GetRegion(QueryRegionNames[0]);
      Region subRegion0 = region0.GetSubRegion(QueryRegionNames[1]);

      QueryHelper qh = QueryHelper.GetHelper();

      qh.PopulatePortfolioData(region0, 100, 20, 100);
      qh.PopulatePositionData(subRegion0, 100, 20);
    }

    public void StepThreeRS(bool pool)
    {
      bool ErrorOccurred = false;

      QueryHelper qh = QueryHelper.GetHelper();

      QueryService qs = null;

      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.ResultSetQueries)
      {
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}.", qryIdx);

        Query query = qs.NewQuery(qrystr.Query);

        ISelectResults results = query.Execute();

        int expectedRowCount = qh.IsExpectedRowsConstantRS(qryIdx) ?
          QueryStatics.ResultSetRowCounts[qryIdx] : QueryStatics.ResultSetRowCounts[qryIdx] * qh.PortfolioNumSets;

        if (!qh.VerifyRS(results, expectedRowCount))
        {
          ErrorOccurred = true;
          Util.Log("Query verify failed for query index {0}.", qryIdx);
          qryIdx++;
          continue;
        }

        ResultSet rs = results as ResultSet;

        foreach (IGFSerializable item in rs)
        {
          Portfolio port = item as Portfolio;
          if (port == null)
          {
            Position pos = item as Position;
            if (pos == null)
            {
              CacheableString cs = item as CacheableString;
              if (cs == null)
              {
                Util.Log("Query got other/unknown object.");
              }
              else
              {
                Util.Log("Query got string : {0}.", cs.Value);
              }
            }
            else
            {
              Util.Log("Query got Position object with secId {0}, shares {1}.", pos.SecId, pos.SharesOutstanding);
            }
          }
          else
          {
            Util.Log("Query got Portfolio object with ID {0}, pkid {1}.", port.ID, port.Pkid);
          }
        }

        qryIdx++;
      }

      Assert.IsFalse(ErrorOccurred, "One or more query validation errors occurred.");
    }

    public void StepThreePQRS(bool pool)
    {
      bool ErrorOccurred = false;

      QueryHelper qh = QueryHelper.GetHelper();

      QueryService qs = null;

      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }

      int qryIdx = 0;

      foreach (QueryStrings paramqrystr in QueryStatics.ResultSetParamQueries)
      {
        if (paramqrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}.", qryIdx);

        Query query = qs.NewQuery(paramqrystr.Query);

        //Populate the parameter list (paramList) for the query.
        IGFSerializable[] paramList = new IGFSerializable[QueryStatics.NoOfQueryParam[qryIdx]];
        Int32 numVal = 0;
        for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParam[qryIdx]; ind++)
        {
          //Util.Log("NIL::PQRS:: QueryStatics.QueryParamSet[{0},{1}] = {2}", qryIdx, ind, QueryStatics.QueryParamSet[qryIdx][ind]);

          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSet[qryIdx][ind]);
            paramList[ind] = new CacheableInt32(numVal);
            //Util.Log("NIL::PQRS::361 Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = new CacheableString((System.String)QueryStatics.QueryParamSet[qryIdx][ind]);
            //Util.Log("NIL::PQRS:: Interger Args:: routingObj[0] = {1}", ind, routingObj[ind].ToString());
          }
        }

        ISelectResults results = query.Execute(paramList);

        //Varify the result
        int expectedRowCount = qh.IsExpectedRowsConstantPQRS(qryIdx) ?
        QueryStatics.ResultSetPQRowCounts[qryIdx] : QueryStatics.ResultSetPQRowCounts[qryIdx] * qh.PortfolioNumSets;

        if (!qh.VerifyRS(results, expectedRowCount))
        {
          ErrorOccurred = true;
          Util.Log("Query verify failed for query index {0}.", qryIdx);
          qryIdx++;
          continue;
        }

        ResultSet rs = results as ResultSet;

        foreach (IGFSerializable item in rs)
        {
          Portfolio port = item as Portfolio;
          if (port == null)
          {
            Position pos = item as Position;
            if (pos == null)
            {
              CacheableString cs = item as CacheableString;
              if (cs == null)
              {
                Util.Log("Query got other/unknown object.");
              }
              else
              {
                Util.Log("Query got string : {0}.", cs.Value);
              }
            }
            else
            {
              Util.Log("Query got Position object with secId {0}, shares {1}.", pos.SecId, pos.SharesOutstanding);
            }
          }
          else
          {
            Util.Log("Query got Portfolio object with ID {0}, pkid {1}.", port.ID, port.Pkid);
          }
        }

        qryIdx++;
      }

      Assert.IsFalse(ErrorOccurred, "One or more query validation errors occurred.");
    }

    public void StepFourRS(bool pool)
    {
      bool ErrorOccurred = false;

      QueryHelper qh = QueryHelper.GetHelper();

      QueryService qs = null;

      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.ResultSetQueries)
      {
        if (qrystr.Category != QueryCategory.Unsupported)
        {
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating unsupported query index {0}.", qryIdx);

        Query query = qs.NewQuery(qrystr.Query);

        try
        {
          ISelectResults results = query.Execute();

          Util.Log("Query exception did not occur for index {0}.", qryIdx);
          ErrorOccurred = true;
          qryIdx++;
        }
        catch (GemFireException)
        {
          // ok, exception expected, do nothing.
          qryIdx++;
        }
        catch (Exception)
        {
          Util.Log("Query unexpected exception occurred for index {0}.", qryIdx);
          ErrorOccurred = true;
          qryIdx++;
        }
      }

      Assert.IsFalse(ErrorOccurred, "Query expected exceptions did not occur.");
    }

    public void StepFourPQRS(bool pool)
    {
      bool ErrorOccurred = false;

      QueryHelper qh = QueryHelper.GetHelper();

      QueryService qs = null;

      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.ResultSetParamQueries)
      {
        if (qrystr.Category != QueryCategory.Unsupported)
        {
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating unsupported query index {0}.", qryIdx);

        Query query = qs.NewQuery(qrystr.Query);

        IGFSerializable[] paramList = new IGFSerializable[QueryStatics.NoOfQueryParam[qryIdx]];

        Int32 numVal = 0;
        for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParam[qryIdx]; ind++)
        {
          //Util.Log("NIL::PQRS:: QueryStatics.QueryParamSet[{0},{1}] = {2}", qryIdx, ind, QueryStatics.QueryParamSet[qryIdx, ind]);

          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSet[qryIdx][ind]);
            paramList[ind] = new CacheableInt32(numVal);
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = new CacheableString((System.String)QueryStatics.QueryParamSet[qryIdx][ind]);
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind].ToString());
          }
        }

        try
        {
          ISelectResults results = query.Execute(paramList);

          Util.Log("Query exception did not occur for index {0}.", qryIdx);
          ErrorOccurred = true;
          qryIdx++;
        }
        catch (GemFireException)
        {
          // ok, exception expected, do nothing.
          qryIdx++;
        }
        catch (Exception)
        {
          Util.Log("Query unexpected exception occurred for index {0}.", qryIdx);
          ErrorOccurred = true;
          qryIdx++;
        }
      }

      Assert.IsFalse(ErrorOccurred, "Query expected exceptions did not occur.");
    }

    public void StepThreeSS(bool pool)
    {
      bool ErrorOccurred = false;

      QueryHelper qh = QueryHelper.GetHelper();

      QueryService qs = null;

      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.StructSetQueries)
      {
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}.", qryIdx);

        Query query = qs.NewQuery(qrystr.Query);

        ISelectResults results = query.Execute();

        int expectedRowCount = qh.IsExpectedRowsConstantSS(qryIdx) ?
          QueryStatics.StructSetRowCounts[qryIdx] : QueryStatics.StructSetRowCounts[qryIdx] * qh.PortfolioNumSets;

        if (!qh.VerifySS(results, expectedRowCount, QueryStatics.StructSetFieldCounts[qryIdx]))
        {
          ErrorOccurred = true;
          Util.Log("Query verify failed for query index {0}.", qryIdx);
          qryIdx++;
          continue;
        }

        StructSet ss = results as StructSet;
        if (ss == null)
        {
          Util.Log("Zero records found for query index {0}, continuing.", qryIdx);
          qryIdx++;
          continue;
        }

        uint rows = 0;
        Int32 fields = 0;
        foreach (Struct si in ss)
        {
          rows++;
          fields = (Int32)si.Length;
        }

        Util.Log("Query index {0} has {1} rows and {2} fields.", qryIdx, rows, fields);

        qryIdx++;
      }

      Assert.IsFalse(ErrorOccurred, "One or more query validation errors occurred.");
    }

    public void StepThreePQSS(bool pool)
    {
      bool ErrorOccurred = false;

      QueryHelper qh = QueryHelper.GetHelper();

      QueryService qs = null;

      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.StructSetParamQueries)
      {
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}.", qryIdx);

        Query query = qs.NewQuery(qrystr.Query);

        //Populate the param list, paramList for parameterized query 
        IGFSerializable[] paramList = new IGFSerializable[QueryStatics.NoOfQueryParamSS[qryIdx]];

        Int32 numVal = 0;
        for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[qryIdx]; ind++)
        {
          //Util.Log("NIL::PQRS:: QueryStatics.QueryParamSetSS[{0},{1}] = {2}", qryIdx, ind, QueryStatics.QueryParamSetSS[qryIdx, ind]);

          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[qryIdx][ind]);
            paramList[ind] = new CacheableInt32(numVal);
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = new CacheableString((System.String)QueryStatics.QueryParamSetSS[qryIdx][ind]);
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind].ToString());
          }
        }

        ISelectResults results = query.Execute(paramList);

        int expectedRowCount = qh.IsExpectedRowsConstantPQSS(qryIdx) ?
        QueryStatics.StructSetPQRowCounts[qryIdx] : QueryStatics.StructSetPQRowCounts[qryIdx] * qh.PortfolioNumSets;

        if (!qh.VerifySS(results, expectedRowCount, QueryStatics.StructSetPQFieldCounts[qryIdx]))
        {
          ErrorOccurred = true;
          Util.Log("Query verify failed for query index {0}.", qryIdx);
          qryIdx++;
          continue;
        }

        StructSet ss = results as StructSet;
        if (ss == null)
        {
          Util.Log("Zero records found for query index {0}, continuing.", qryIdx);
          qryIdx++;
          continue;
        }

        uint rows = 0;
        Int32 fields = 0;
        foreach (Struct si in ss)
        {
          rows++;
          fields = (Int32)si.Length;
        }

        Util.Log("Query index {0} has {1} rows and {2} fields.", qryIdx, rows, fields);

        qryIdx++;
      }

      Assert.IsFalse(ErrorOccurred, "One or more query validation errors occurred.");
    }

    public void StepFourSS(bool pool)
    {
      bool ErrorOccurred = false;

      QueryHelper qh = QueryHelper.GetHelper();

      QueryService qs = null;

      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.StructSetQueries)
      {
        if (qrystr.Category != QueryCategory.Unsupported)
        {
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating unsupported query index {0}.", qryIdx);

        Query query = qs.NewQuery(qrystr.Query);

        try
        {
          ISelectResults results = query.Execute();

          Util.Log("Query exception did not occur for index {0}.", qryIdx);
          ErrorOccurred = true;
          qryIdx++;
        }
        catch (GemFireException)
        {
          // ok, exception expected, do nothing.
          qryIdx++;
        }
        catch (Exception)
        {
          Util.Log("Query unexpected exception occurred for index {0}.", qryIdx);
          ErrorOccurred = true;
          qryIdx++;
        }
      }

      Assert.IsFalse(ErrorOccurred, "Query expected exceptions did not occur.");
    }

    public void StepFourPQSS(bool pool)
    {
      bool ErrorOccurred = false;

      QueryHelper qh = QueryHelper.GetHelper();

      QueryService qs = null;

      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.StructSetParamQueries)
      {
        if (qrystr.Category != QueryCategory.Unsupported)
        {
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating unsupported query index {0}.", qryIdx);

        Query query = qs.NewQuery(qrystr.Query);

        //Populate the param list
        IGFSerializable[] paramList = new IGFSerializable[QueryStatics.NoOfQueryParamSS[qryIdx]];

        Int32 numVal = 0;
        for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[qryIdx]; ind++)
        {
          //Util.Log("NIL::PQRS:: QueryStatics.QueryParamSetSS[{0},{1}] = {2}", qryIdx, ind, QueryStatics.QueryParamSetSS[qryIdx, ind]);

          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[qryIdx][ind]);
            paramList[ind] = new CacheableInt32(numVal);
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = new CacheableString((System.String)QueryStatics.QueryParamSetSS[qryIdx][ind]);
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind].ToString());
          }
        }

        try
        {
          ISelectResults results = query.Execute(paramList);

          Util.Log("Query exception did not occur for index {0}.", qryIdx);
          ErrorOccurred = true;
          qryIdx++;
        }
        catch (GemFireException)
        {
          // ok, exception expected, do nothing.
          qryIdx++;
        }
        catch (Exception)
        {
          Util.Log("Query unexpected exception occurred for index {0}.", qryIdx);
          ErrorOccurred = true;
          qryIdx++;
        }
      }

      Assert.IsFalse(ErrorOccurred, "Query expected exceptions did not occur.");
    }

    public void KillServer()
    {
      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
    }

    public delegate void KillServerDelegate();

    public void StepOneFailover(bool pool, bool locator)
    {
      // This is here so that Client1 registers information of the cacheserver
      // that has been already started
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true,
          "cacheserver_remoteoql.xml",
          "cacheserver_remoteoql2.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false,
          "cacheserver_remoteoql.xml",
          "cacheserver_remoteoql2.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      if (pool)
      {
        if (locator)
        {
          CacheHelper.CreateTCRegion_Pool(QueryRegionNames[0], true, true, null,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", true);
        }
        else
        {
          CacheHelper.CreateTCRegion_Pool(QueryRegionNames[0], true, true, null,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", true);
        }
      }
      else
      {
        CacheHelper.CreateTCRegion(QueryRegionNames[0], true, true, null, CacheHelper.Endpoints, true);
      }

      Region region = CacheHelper.GetVerifyRegion(QueryRegionNames[0]);
      Portfolio p1 = new Portfolio(1, 100);
      Portfolio p2 = new Portfolio(2, 200);
      Portfolio p3 = new Portfolio(3, 300);
      Portfolio p4 = new Portfolio(4, 400);

      region.Put("1", p1);
      region.Put("2", p2);
      region.Put("3", p3);
      region.Put("4", p4);
    }

    public void StepTwoFailover(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(2, "GFECS2");
      }
      Util.Log("Cacheserver 2 started.");

      IAsyncResult killRes = null;
      KillServerDelegate ksd = new KillServerDelegate(KillServer);

      QueryService qs = null;

      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }

      for (int i = 0; i < 10000; i++)
      {
        Query qry = qs.NewQuery("select distinct * from /" + QueryRegionNames[0]);

        ISelectResults results = qry.Execute();

        if (i == 10)
        {
          killRes = ksd.BeginInvoke(null, null);
        }

        Int32 resultSize = results.Size;

        if (i % 100 == 0)
        {
          Util.Log("Iteration upto {0} done, result size is {1}", i, resultSize);
        }

        Assert.AreEqual(4, resultSize, "Result size is not 4!");
      }

      killRes.AsyncWaitHandle.WaitOne();
      ksd.EndInvoke(killRes);
    }

    public void StepTwoPQFailover(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(2, "GFECS2");
      }
      Util.Log("Cacheserver 2 started.");

      IAsyncResult killRes = null;
      KillServerDelegate ksd = new KillServerDelegate(KillServer);

      QueryService qs = null;

      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }

      for (int i = 0; i < 10000; i++)
      {
        Query qry = qs.NewQuery("select distinct * from /" + QueryRegionNames[0] + " where ID > $1" );

        //Populate the param list
        IGFSerializable[] paramList = new IGFSerializable[1];
        paramList[0] = new CacheableInt32(1);

        ISelectResults results = qry.Execute(paramList);

        if (i == 10)
        {
          killRes = ksd.BeginInvoke(null, null);
        }

        Int32 resultSize = results.Size;

        if (i % 100 == 0)
        {
          Util.Log("Iteration upto {0} done, result size is {1}", i, resultSize);
        }

        Assert.AreEqual(3, resultSize, "Result size is not 3!");
      }

      killRes.AsyncWaitHandle.WaitOne();
      ksd.EndInvoke(killRes);
    }

    public void StepThreeQT(bool pool)
    {
      QueryHelper qh = QueryHelper.GetHelper();
      QueryService qs = null;
      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }
      Query query = qs.NewQuery(QueryStatics.ResultSetQueries[34].Query);

      try
      {
        Util.Log("EXECUTE 1 START for query: ", query.QueryString);
        ISelectResults results = query.Execute(3);
        Util.Log("EXECUTE 1 STOP");
        Util.Log("Result size is {0}", results.Size);
        Assert.Fail("Didnt get expected timeout exception for first execute");
      }
      catch (GemFireException excp)
      {
        Util.Log("First execute expected exception: {0}", excp.Message);
      }
    }

    public void StepFourQT(bool pool)
    {
      QueryHelper qh = QueryHelper.GetHelper();
      QueryService qs = null;
      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }
      Query query = qs.NewQuery(QueryStatics.ResultSetQueries[34].Query);

      try
      {
        Util.Log("EXECUTE 2 START for query: ", query.QueryString);
        ISelectResults results = query.Execute(850);
        Util.Log("EXECUTE 2 STOP");
        Util.Log("Result size is {0}", results.Size);
      }
      catch (GemFireException excp)
      {
        Assert.Fail("Second execute unwanted exception: {0}", excp.Message);
      }
    }

    public void StepFiveQT(bool pool)
    {
      QueryHelper qh = QueryHelper.GetHelper();
      QueryService qs = null;
      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }
      Query query = qs.NewQuery(QueryStatics.StructSetQueries[17].Query);

      try
      {
        Util.Log("EXECUTE 3 START for query: ", query.QueryString);
        ISelectResults results = query.Execute(2);
        Util.Log("EXECUTE 3 STOP");
        Util.Log("Result size is {0}", results.Size);
        Assert.Fail("Didnt get expected timeout exception for third execute");
      }
      catch (GemFireException excp)
      {
        Util.Log("Third execute expected exception: {0}", excp.Message);
      }
    }

    public void StepSixQT(bool pool)
    {
      QueryHelper qh = QueryHelper.GetHelper();
      QueryService qs = null;
      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }
      Query query = qs.NewQuery(QueryStatics.StructSetQueries[17].Query);

      try
      {
        Util.Log("EXECUTE 4 START for query: ", query.QueryString);
        ISelectResults results = query.Execute(850);
        Util.Log("EXECUTE 4 STOP");
        Util.Log("Result size is {0}", results.Size);
      }
      catch (GemFireException excp)
      {
        Assert.Fail("Fourth execute unwanted exception: {0}", excp.Message);
      }
    }

    public void StepThreePQT(bool pool)
    {
      QueryHelper qh = QueryHelper.GetHelper();
      QueryService qs = null;
      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }
      Query query = qs.NewQuery(QueryStatics.StructSetParamQueries[5].Query);

      try
      {
        Util.Log("EXECUTE 5 START for query: ", query.QueryString);
        //Populate the param list, paramList for parameterized query 
        IGFSerializable[] paramList = new IGFSerializable[QueryStatics.NoOfQueryParamSS[5]];

        Int32 numVal = 0;
        for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[5]; ind++)
        { 
          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[5][ind]);
            paramList[ind] = new CacheableInt32(numVal);
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = new CacheableString((System.String)QueryStatics.QueryParamSetSS[5][ind]);
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind].ToString());
          }
        }
        
        ISelectResults results = query.Execute(paramList, 1);
        Util.Log("EXECUTE 5 STOP");
        Util.Log("Result size is {0}", results.Size);
        Assert.Fail("Didnt get expected timeout exception for Fifth execute");
      }
      catch (GemFireException excp)
      {
        Util.Log("Fifth execute expected exception: {0}", excp.Message);
      }
    }

    public void StepFourPQT(bool pool)
    {
      QueryHelper qh = QueryHelper.GetHelper();
      QueryService qs = null;
      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }
      Query query = qs.NewQuery(QueryStatics.StructSetParamQueries[5].Query);

      try
      {
        Util.Log("EXECUTE 6 START for query: ", query.QueryString);
        //Populate the param list, paramList for parameterized query 
        IGFSerializable[] paramList = new IGFSerializable[QueryStatics.NoOfQueryParamSS[5]];

        Int32 numVal = 0;
        for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[5]; ind++)
        {
          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[5][ind]);
            paramList[ind] = new CacheableInt32(numVal);
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = new CacheableString((System.String)QueryStatics.QueryParamSetSS[5][ind]);
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind].ToString());
          }
        }
        
        ISelectResults results = query.Execute(paramList, 850);
        Util.Log("EXECUTE 6 STOP");
        Util.Log("Result size is {0}", results.Size);
      }
      catch (GemFireException excp)
      {
        Assert.Fail("Sixth execute unwanted exception: {0}", excp.Message);
      }
    }
    
    public void StepThreeRQ()
    {
      bool ErrorOccurred = false;

      Region region = CacheHelper.GetRegion(QueryRegionNames[0]);

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.RegionQueries)
      {
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}.", qryIdx);

        ISelectResults results = region.Query(qrystr.Query);

        if (results.Size != QueryStatics.RegionQueryRowCounts[qryIdx])
        {
          ErrorOccurred = true;
          Util.Log("FAIL: Query # {0} expected result size is {1}, actual is {2}", qryIdx,
            QueryStatics.RegionQueryRowCounts[qryIdx], results.Size);
          qryIdx++;
          continue;
        }

        qryIdx++;
      }

      Assert.IsFalse(ErrorOccurred, "One or more query validation errors occurred.");
    }

    public void StepFourRQ()
    {
      bool ErrorOccurred = false;

      Region region = CacheHelper.GetRegion(QueryRegionNames[0]);

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.RegionQueries)
      {
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}.", qryIdx);

        bool existsValue = region.ExistsValue(qrystr.Query);
        bool expectedResult = QueryStatics.RegionQueryRowCounts[qryIdx] > 0 ? true : false;

        if (existsValue != expectedResult)
        {
          ErrorOccurred = true;
          Util.Log("FAIL: Query # {0} existsValue expected is {1}, actual is {2}", qryIdx,
            expectedResult?"true":"false", existsValue?"true":"false");
          qryIdx++;
          continue;
        }

        qryIdx++;
      }

      Assert.IsFalse(ErrorOccurred, "One or more query validation errors occurred.");
    }

    public void StepFiveRQ()
    {
      bool ErrorOccurred = false;

      Region region = CacheHelper.GetRegion(QueryRegionNames[0]);

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.RegionQueries)
      {
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}.", qryIdx);

        try
        {
          IGFSerializable result = region.SelectValue(qrystr.Query);

          if (!(QueryStatics.RegionQueryRowCounts[qryIdx] == 0 ||
            QueryStatics.RegionQueryRowCounts[qryIdx] == 1))
          {
            ErrorOccurred = true;
            Util.Log("FAIL: Query # {0} expected query exception did not occur", qryIdx);
            qryIdx++;
            continue;
          }
        }
        catch (QueryException)
        {
          if (QueryStatics.RegionQueryRowCounts[qryIdx] == 0 ||
            QueryStatics.RegionQueryRowCounts[qryIdx] == 1)
          {
            ErrorOccurred = true;
            Util.Log("FAIL: Query # {0} unexpected query exception occured", qryIdx);
            qryIdx++;
            continue;
          }
        }
        catch (Exception)
        {
          ErrorOccurred = true;
          Util.Log("FAIL: Query # {0} unexpected exception occured", qryIdx);
          qryIdx++;
          continue;
        }

        qryIdx++;
      }

      Assert.IsFalse(ErrorOccurred, "One or more query validation errors occurred.");
    }

    public void StepSixRQ()
    {
      bool ErrorOccurred = false;

      Region region = CacheHelper.GetRegion(QueryRegionNames[0]);

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.RegionQueries)
      {
        if (qrystr.Category != QueryCategory.Unsupported)
        {
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating unsupported query index {0}.", qryIdx);

        try
        {
          ISelectResults results = region.Query(qrystr.Query);

          Util.Log("Query # {0} expected exception did not occur", qryIdx);
          ErrorOccurred = true;
          qryIdx++;
        }
        catch (QueryException)
        {
          // ok, exception expected, do nothing.
          qryIdx++;
        }
        catch (Exception)
        {
          ErrorOccurred = true;
          Util.Log("FAIL: Query # {0} unexpected exception occured", qryIdx);
          qryIdx++;
        }
      }

      Assert.IsFalse(ErrorOccurred, "Query expected exceptions did not occur.");
    }
    public void QueryDiffServerConfig1(string endpoints, bool pool) 
    {
      char[] ch = { ',' };
      string[] endpointarr = endpoints.Split(ch);
      endpoint1 = endpointarr[0];
      endpoint2 = endpointarr[1];
      Region region0 = null;
      if (pool)
      {
        region0 = CacheHelper.CreateTCRegion_Pool(QueryRegionNames[0], true, true,
          null, endpoint1, (string)null, "__TESTPOOL1_", true);
      }
      else
      {
        region0 = CacheHelper.CreateTCRegion(QueryRegionNames[0], true, true,
          null, endpoint1, true);
      }
      QueryHelper qh = QueryHelper.GetHelper();
      qh.PopulatePortfolioData(region0,qh.PortfolioSetSize,qh.PortfolioNumSets);
      QueryService qs = null;
      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }
      string qry1Str = "select * from /" + QueryRegionNames[0];
      string qry2Str = "select * from /" + QueryRegionNames[1];
      ISelectResults results = qs.NewQuery(qry1Str).Execute();
      if (results.Size != qh.PortfolioSetSize * qh.PortfolioNumSets)
      {
        Assert.Fail("unexpected number of results");
      }
      try
      {
        results = qs.NewQuery(qry2Str).Execute();
        Assert.Fail("Expected a QueryException");
      }
      catch (QueryException qex)
      {
        Util.Log("False alarm everything is allright"+ qex.Message);
      }
      results = region0.Query(qry1Str);
      if (results.Size != qh.PortfolioSetSize * qh.PortfolioNumSets)
      {
        Assert.Fail("unexpected number of results");
      }
      try
      {
        results = region0.Query(qry2Str);
        Assert.Fail("Expected a QueryException");
      }
      catch (QueryException qex)
      {
        Util.Log("False alarm everything is allright"+qex.Message);
      }
      Util.Log("QueryDiffServerConfig1 complets");
    }
    private void CreateRegions(object p, object USE_ACK, object endPoint1, bool p_4)
    {
      throw new Exception("The method or operation is not implemented.");
    }
    public void QueryDiffServerConfig2(bool pool) //pxr start server2 having regions "Positions" before launching
    {
      Region region1 = null;
      if (pool)
      {
        region1 = CacheHelper.CreateTCRegion_Pool(QueryRegionNames[1], true, true,
          null, endpoint2, (string)null, "__TESTPOOL2_", true);
      }
      else
      {
        region1 = CacheHelper.CreateTCRegion(QueryRegionNames[1], true, true,
          null, endpoint2, true);
      }
      QueryHelper qh = QueryHelper.GetHelper();
      qh.PopulatePortfolioData(region1, qh.PortfolioSetSize, qh.PortfolioNumSets);
      string qry1Str = "select * from /" + QueryRegionNames[0];
      string qry2Str = "select * from /" + QueryRegionNames[1];
      QueryService qs = null;
      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }
      ISelectResults results = qs.NewQuery(qry1Str).Execute();
      if (results.Size != qh.PortfolioSetSize * qh.PortfolioNumSets)
      {
        Assert.Fail("unexpected number of results");
      }
      try
      {
        results = qs.NewQuery(qry2Str).Execute();
        Assert.Fail("Expected a QueryException");
      }
      catch (QueryException qex)
      {
        Util.Log("False alarm everything is allright"+qex.Source);
      }

      //Now region query.

      try
      {
        results = region1.Query(qry1Str);
        Assert.Fail("Expected a QueryException");
      }
      catch (QueryException qex)
      {
        Util.Log("False alarm everything is allright"+qex.Message);
      }
      results = region1.Query(qry2Str);
      if (results.Size != qh.PortfolioSetSize * qh.PortfolioNumSets)
      {
        Assert.Fail("unexpected number of results");
      }

      Util.Log("QueryDiffServerConfig2 completes");
    }
    public void CompareMap(CacheableHashMap map1, CacheableHashMap map2)
    {
      if (map1.Count != map2.Count)
        Assert.Fail("Number of Keys dont match");
      if (map1.Count == 0) return;
      foreach (KeyValuePair<ICacheableKey, IGFSerializable> entry in map1)
      {
        IGFSerializable value;
        if (!(map2.TryGetValue(entry.Key,out value)))
        {
          Assert.Fail("Key was not found");
          return;
        }
        if(entry.Value.Equals(value))
        {
          Assert.Fail("Value was not found");
          return;
        }
      }
    }
    public void GetAllRegionQuery()
    {
      Region region0 = CacheHelper.GetVerifyRegion(QueryRegionNames[0]);
      Region region1 = region0.GetSubRegion(QueryRegionNames[1] );
      Region region2 = CacheHelper.GetVerifyRegion(QueryRegionNames[1]);
      Region region3 = CacheHelper.GetVerifyRegion(QueryRegionNames[2]);
      Region region4 = CacheHelper.GetVerifyRegion(QueryRegionNames[3]);
      string[] SecIds = Portfolio.SecIds;
      int NumSecIds = SecIds.Length;
      List<ICacheableKey> PosKeys = new List<ICacheableKey>();
      List<ICacheableKey> PortKeys = new List<ICacheableKey>();
      CacheableHashMap ExpectedPosMap = new CacheableHashMap();
      CacheableHashMap ExpectedPortMap = new CacheableHashMap();
      QueryHelper qh = QueryHelper.GetHelper();
      int SetSize = qh.PositionSetSize;
      int NumSets = qh.PositionNumSets;
      for (int set = 1; set <= NumSets; set++)
      {
        for (int current = 1; current <= SetSize; current++)
        {
          CacheableKey PosKey  = "pos" + set + "-" + current;
          Position pos = new Position(SecIds[current % NumSecIds], current * 100 );
          PosKeys.Add(PosKey);
          ExpectedPosMap.Add(PosKey, pos);
        }
      }
      SetSize = qh.PortfolioSetSize;
      NumSets = qh.PortfolioNumSets;
      for (int set = 1; set <= NumSets; set++)
      {
        for (int current = 1; current <= SetSize; current++)
        {
          CacheableKey PortKey = "port" + set + "-" + current;
          Portfolio Port = new Portfolio(current,1);
          PortKeys.Add(PortKey);
          ExpectedPortMap.Add(PortKey, Port);
        }
      }
      CacheableHashMap ResMap = new CacheableHashMap();
      Dictionary<ICacheableKey, Exception> ExMap = new Dictionary<ICacheableKey, Exception>();
      region0.GetAll(PortKeys.ToArray(), ResMap, ExMap);
      CompareMap(ResMap, ExpectedPortMap);
      if (ExMap.Count != 0)
      {
        Assert.Fail("Expected No Exception");
      }
      ResMap.Clear();

      region1.GetAll(PosKeys.ToArray(), ResMap, ExMap);
      CompareMap(ResMap, ExpectedPosMap);
      if (ExMap.Count != 0)
      {
        Assert.Fail("Expected No Exception");
      }
      ResMap.Clear();
      region2.GetAll(PosKeys.ToArray(), ResMap, ExMap);
      CompareMap(ResMap, ExpectedPosMap);
      if (ExMap.Count != 0)
      {
        Assert.Fail("Expected No Exception");
      }
      ResMap.Clear();

      region3.GetAll(PortKeys.ToArray(), ResMap, ExMap);
      CompareMap(ResMap, ExpectedPortMap);
      if (ExMap.Count != 0)
      {
        Assert.Fail("Expected No Exception");
      }
      ResMap.Clear();

      region4.GetAll(PortKeys.ToArray(), ResMap, ExMap);
      CompareMap(ResMap, ExpectedPortMap);
      if (ExMap.Count != 0)
      {
        Assert.Fail("Expected No Exception");
      }
      ResMap.Clear();
    }
    #endregion

    void runRemoteQueryRS(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true, "remotequery.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "remotequery.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOne, CacheHelper.Endpoints, CacheHelper.Locators, pool, locator);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwo);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThreeRS, pool);
      Util.Log("StepThree complete.");

      m_client1.Call(StepFourRS, pool);
      Util.Log("StepFour complete.");

      m_client1.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    }

    void runRemoteParamQueryRS(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true, "remotequery.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "remotequery.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOne, CacheHelper.Endpoints, CacheHelper.Locators, pool, locator);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwo);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThreePQRS, pool);
      Util.Log("StepThree complete.");

      m_client1.Call(StepFourPQRS, pool);
      Util.Log("StepFour complete.");

      m_client1.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    } 

    void runRemoteQuerySS(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true, "remotequery.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "remotequery.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      m_client2.Call(StepOne, CacheHelper.Endpoints, CacheHelper.Locators, pool, locator);
      Util.Log("StepOne complete.");

      m_client2.Call(StepTwo);
      Util.Log("StepTwo complete.");

      m_client2.Call(StepThreeSS, pool);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFourSS, pool);
      Util.Log("StepFour complete.");

      //m_client2.Call(GetAllRegionQuery);

      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
      
      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    }

    void runRemoteParamQuerySS(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true, "remotequery.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "remotequery.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      m_client2.Call(StepOne, CacheHelper.Endpoints, CacheHelper.Locators, pool, locator);
      Util.Log("StepOne complete.");

      m_client2.Call(StepTwo);
      Util.Log("StepTwo complete.");

      m_client2.Call(StepThreePQSS, pool);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFourPQSS, pool);
      Util.Log("StepFour complete.");

      //m_client2.Call(GetAllRegionQuery);

      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    }

    void runRemoteQueryFailover(bool pool, bool locator)
    {
      try
      {
        m_client1.Call(StepOneFailover, pool, locator);
        Util.Log("StepOneFailover complete.");

        m_client1.Call(StepTwoFailover, pool, locator);
        Util.Log("StepTwoFailover complete.");

        m_client1.Call(Close);
        Util.Log("Client closed");
      }
      finally
      {
        m_client1.Call(CacheHelper.StopJavaServers);
        if (pool && locator)
        {
          m_client1.Call(CacheHelper.StopJavaLocator, 1);
        }
      }
    }

    void runRemoteParamQueryFailover(bool pool, bool locator)
    {
      try
      {
        m_client1.Call(StepOneFailover, pool, locator);
        Util.Log("StepOneFailover complete.");

        m_client1.Call(StepTwoPQFailover, pool, locator);
        Util.Log("StepTwoPQFailover complete.");

        m_client1.Call(Close);
        Util.Log("Client closed");
      }
      finally
      {
        m_client1.Call(CacheHelper.StopJavaServers);
        if (pool && locator)
        {
          m_client1.Call(CacheHelper.StopJavaLocator, 1);
        }
      }
    }

    void runQueryExclusiveness(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true, "cacheserver_remoteoql.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "cacheserver_remoteoql.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOneQE, CacheHelper.Endpoints, CacheHelper.Locators, pool, locator);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwoQE, pool);
      Util.Log("StepTwo complete.");

      m_client1.Call(Close);
      Util.Log("Client closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    }

    void runQueryTimeout(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(false, "remotequery.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "remotequery.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOne, CacheHelper.Endpoints, CacheHelper.Locators, pool, locator);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwoQT);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThreeQT, pool);
      Util.Log("StepThree complete.");

      Thread.Sleep(150000); // sleep 2.5min to allow server query to complete

      m_client1.Call(StepFourQT, pool);
      Util.Log("StepFour complete.");

      m_client1.Call(StepFiveQT, pool);
      Util.Log("StepFive complete.");

      Thread.Sleep(60000); // sleep 1min to allow server query to complete

      m_client1.Call(StepSixQT, pool);
      Util.Log("StepSix complete.");

      m_client1.Call(Close);
      Util.Log("Client closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    }

    void runParamQueryTimeout(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(false, "remotequery.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "remotequery.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOne, CacheHelper.Endpoints, CacheHelper.Locators, pool, locator);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwoQT);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThreePQT, pool);
      Util.Log("StepThreePQT complete.");

      Thread.Sleep(60000); // sleep 1min to allow server query to complete

      m_client1.Call(StepFourPQT, pool);
      Util.Log("StepFourPQT complete.");

      m_client1.Call(Close);
      Util.Log("Client closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    }

    void runRegionQuery(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true, "remotequery.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "remotequery.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      m_client2.Call(StepOne, CacheHelper.Endpoints, CacheHelper.Locators, pool, locator);
      Util.Log("StepOne complete.");

      m_client2.Call(StepTwo);
      Util.Log("StepTwo complete.");

      m_client2.Call(StepThreeRQ);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFourRQ);
      Util.Log("StepFour complete.");

      m_client2.Call(StepFiveRQ);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSixRQ);
      Util.Log("StepSix complete.");

      m_client2.Call(Close);
      Util.Log("Client closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    }

    void runRegionQueryDiffConfig(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true, "regionquery_diffconfig.xml", "regionquery_diffconfig2.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "regionquery_diffconfig.xml", "regionquery_diffconfig2.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");
      m_client1.Call(QueryDiffServerConfig1, CacheHelper.Endpoints, pool);
      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(2, "GFECS2");
      }
      Util.Log("Cacheserver 2 started.");
      m_client1.Call(QueryDiffServerConfig2, pool);
      m_client1.Call(Close);
      Util.Log("Client closed");
      CacheHelper.StopJavaServer(1);
      CacheHelper.StopJavaServer(2);
      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    [Test]
    public void RemoteQueryRS()
    {
      runRemoteQueryRS(false, false); // region config
      runRemoteQueryRS(true, false); // pool with server endpoints
      runRemoteQueryRS(true, true); // pool with locator
    }

    [Test]
    public void RemoteParamQueryRS()
    {
      runRemoteParamQueryRS(false, false); // region config
      runRemoteParamQueryRS(true, false); // pool with server endpoints
      runRemoteParamQueryRS(true, true); // pool with locator
    }
    
    [Test]
    public void RemoteQuerySS()
    {
      runRemoteQuerySS(false, false); // region config
      runRemoteQuerySS(true, false); // pool with server endpoints
      runRemoteQuerySS(true, true); // pool with locator
    }

    [Test]
    public void RemoteParamQuerySS()
    {
      runRemoteParamQuerySS(false, false); // region config
      runRemoteParamQuerySS(true, false); // pool with server endpoints
      runRemoteParamQuerySS(true, true); // pool with locator
    }
    
    [Test]
    public void RemoteQueryFailover()
    {
      runRemoteQueryFailover(false, false); // region config
      runRemoteQueryFailover(true, false); // pool with server endpoints
      runRemoteQueryFailover(true, true); // pool with locator
    }

    [Test]
    public void RemoteParamQueryFailover()
    {
      runRemoteParamQueryFailover(false, false); // region config
      runRemoteParamQueryFailover(true, false); // pool with server endpoints
      runRemoteParamQueryFailover(true, true); // pool with locator
    }

    [Test]
    public void QueryExclusiveness()
    {
      runQueryExclusiveness(false, false); // region config
      runQueryExclusiveness(true, false); // pool with server endpoints
      runQueryExclusiveness(true, true); // pool with locator
    }

    [Test]
    public void QueryTimeout()
    {
      runQueryTimeout(false, false); // region config
      runQueryTimeout(true, false); // pool with server endpoints
      runQueryTimeout(true, true); // pool with locator
    }

    [Test]
    public void ParamQueryTimeout()
    {
      runParamQueryTimeout(false, false); // region config
      runParamQueryTimeout(true, false); // pool with server endpoints
      runParamQueryTimeout(true, true); // pool with locator
    }

    [Test]
    public void RegionQuery()
    {
      runRegionQuery(false, false); // region config
      runRegionQuery(true, false); // pool with server endpoints
      runRegionQuery(true, true); // pool with locator
    }

    [Test]
    public void RegionQueryDiffConfig()
    {
      runRegionQueryDiffConfig(false, false); // region config
      runRegionQueryDiffConfig(true, false); // pool with server endpoints
      runRegionQueryDiffConfig(true, true); // pool with locator
    }
    
  }
}
