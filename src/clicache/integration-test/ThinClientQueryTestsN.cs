//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests.NewAPI;
  
  using GemStone.GemFire.Cache.Generic;

  using QueryStatics = GemStone.GemFire.Cache.Tests.QueryStatics;
  using QueryCategory = GemStone.GemFire.Cache.Tests.QueryCategory;
  using QueryStrings = GemStone.GemFire.Cache.Tests.QueryStrings;
  
  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("generics")]
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
        Serializable.RegisterTypeGeneric(Portfolio.CreateDeserializable);
        Serializable.RegisterTypeGeneric(Position.CreateDeserializable);
        Serializable.RegisterPdxType(GemStone.GemFire.Cache.Tests.NewAPI.PortfolioPdx.CreateDeserializable);
        Serializable.RegisterPdxType(GemStone.GemFire.Cache.Tests.NewAPI.PositionPdx.CreateDeserializable);
      }
      catch (IllegalStateException)
      {
        // ignore since we run multiple iterations for pool and non pool configs
      }
    }

    public void StepOneQE(string locators, bool isPdx)
    {
      m_isPdx = isPdx;
      CacheHelper.Init();
      try
      {
        QueryService<object, object> qsFail = null;
        qsFail = PoolManager/*<object, object>*/.CreateFactory().Create("_TESTFAILPOOL_").GetQueryService<object, object>();
        Query<object> qryFail = qsFail.NewQuery("select distinct * from /" + QERegionName);
        ISelectResults<object> resultsFail = qryFail.Execute();
        Assert.Fail("Since no endpoints defined, so exception expected");
      }
      catch (IllegalStateException ex)
      {
        Util.Log("Got expected exception: {0}", ex);
      }

      CacheHelper.CreateTCRegion_Pool<object, object>(QERegionName, true, true,
        null, locators, "__TESTPOOL1_", true);
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      if (!m_isPdx)
      {
        Portfolio p1 = new Portfolio(1, 100);
        Portfolio p2 = new Portfolio(2, 100);
        Portfolio p3 = new Portfolio(3, 100);
        Portfolio p4 = new Portfolio(4, 100);

        region["1"] = p1;
        region["2"] = p2;
        region["3"] = p3;
        region["4"] = p4;
      }
      else
      {
        PortfolioPdx p1 = new PortfolioPdx(1, 100);
        PortfolioPdx p2 = new PortfolioPdx(2, 100);
        PortfolioPdx p3 = new PortfolioPdx(3, 100);
        PortfolioPdx p4 = new PortfolioPdx(4, 100);

        region["1"] = p1;
        region["2"] = p2;
        region["3"] = p3;
        region["4"] = p4;
      }

      QueryService<object, object> qs = null;
      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();
    
      Query<object> qry = qs.NewQuery("select distinct * from /" + QERegionName);
      ISelectResults<object> results = qry.Execute();
      Int32 count = results.Size;
      Assert.AreEqual(4, count, "Expected 4 as number of portfolio objects.");

      // Bring down the region
      region.GetLocalView().DestroyRegion();
    }

    public void StepTwoQE()
    {
      QueryService<object, object> qs = null;
      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();
      Util.Log("Going to execute the query");
      Query<object> qry = qs.NewQuery("select distinct * from /" + QERegionName);
      ISelectResults<object> results = qry.Execute();
      Int32 count = results.Size;
      Assert.AreEqual(4, count, "Expected 4 as number of portfolio objects.");
    }
    
    public void StepOne(string locators, bool isPdx)
    {
      m_isPdx = isPdx;
      CacheHelper.CreateTCRegion_Pool<object, object>(QueryRegionNames[0], true, true,
      null, locators, "__TESTPOOL1_", true);
      CacheHelper.CreateTCRegion_Pool<object, object>(QueryRegionNames[1], true, true,
        null, locators, "__TESTPOOL1_", true);
      CacheHelper.CreateTCRegion_Pool<object, object>(QueryRegionNames[2], true, true,
        null, locators, "__TESTPOOL1_", true);
      CacheHelper.CreateTCRegion_Pool<object, object>(QueryRegionNames[3], true, true,
        null, locators, "__TESTPOOL1_", true);

      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);
      GemStone.GemFire.Cache.Generic.RegionAttributes<object, object> regattrs = region.Attributes;
      region.CreateSubRegion(QueryRegionNames[1], regattrs);
    }
    
    public void StepTwo(bool isPdx)
    {
      m_isPdx = isPdx;
      IRegion<object, object> region0 = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);
      IRegion<object, object> subRegion0 = (IRegion<object, object>) region0.GetSubRegion(QueryRegionNames[1]);
      IRegion<object, object> region1 = CacheHelper.GetRegion<object, object>(QueryRegionNames[1]);
      IRegion<object, object> region2 = CacheHelper.GetRegion<object, object>(QueryRegionNames[2]);
      IRegion<object, object> region3 = CacheHelper.GetRegion<object, object>(QueryRegionNames[3]);

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
      Util.Log("SetSize {0}, NumSets {1}.", qh.PortfolioSetSize,
        qh.PortfolioNumSets);

      if (!m_isPdx)
      {
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
      else
      {
        qh.PopulatePortfolioPdxData(region0, qh.PortfolioSetSize,
          qh.PortfolioNumSets);
        qh.PopulatePositionPdxData(subRegion0, qh.PortfolioSetSize,
          qh.PortfolioNumSets);
        qh.PopulatePositionPdxData(region1, qh.PortfolioSetSize,
          qh.PortfolioNumSets);
        qh.PopulatePortfolioPdxData(region2, qh.PortfolioSetSize,
          qh.PortfolioNumSets);
        qh.PopulatePortfolioPdxData(region3, qh.PortfolioSetSize,
          qh.PortfolioNumSets);
      }
    }

    public void StepTwoQT()
    {
      IRegion<object, object> region0 = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);
      IRegion<object, object> subRegion0 = region0.GetSubRegion(QueryRegionNames[1]);

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();

      if (!m_isPdx)
      {
        qh.PopulatePortfolioData(region0, 100, 20, 100);
        qh.PopulatePositionData(subRegion0, 100, 20);
      }
      else
      {
        qh.PopulatePortfolioPdxData(region0, 100, 20, 100);
        qh.PopulatePositionPdxData(subRegion0, 100, 20);
      }
    }

    public void StepThreeRS()
    {
      bool ErrorOccurred = false;

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.ResultSetQueries)
      {
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        if (m_isPdx == true)
        {
          if (qryIdx == 2 || qryIdx == 3 || qryIdx == 4)
          {
            Util.Log("Skipping query index {0} for Pdx because it is function type.", qryIdx);
            qryIdx++;
            continue;  
          }
        }

        Util.Log("Evaluating query index {0}. Query string {1}", qryIdx, qrystr.Query);

        Query<object> query = qs.NewQuery(qrystr.Query);

        ISelectResults<object> results = query.Execute();

        int expectedRowCount = qh.IsExpectedRowsConstantRS(qryIdx) ?
          QueryStatics.ResultSetRowCounts[qryIdx] : QueryStatics.ResultSetRowCounts[qryIdx] * qh.PortfolioNumSets;

        if (!qh.VerifyRS(results, expectedRowCount))
        {
          ErrorOccurred = true;
          Util.Log("Query verify failed for query index {0}.", qryIdx);
          qryIdx++;
          continue;
        }

        ResultSet<object> rs = results as ResultSet<object>;

        foreach (object item in rs)
        {
          if (!m_isPdx)
          {
            Portfolio port = item as Portfolio;
            if (port == null)
            {
              Position pos = item as Position;
              if (pos == null)
              {
                string cs = item.ToString();
                if (cs == null)
                {
                  Util.Log("Query got other/unknown object.");
                }
                else
                {
                  Util.Log("Query got string : {0}.", cs);
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
          else
          {
            PortfolioPdx port = item as PortfolioPdx;
            if (port == null)
            {
              PositionPdx pos = item as PositionPdx;
              if (pos == null)
              {
                string cs = item.ToString();
                if (cs == null)
                {
                  Util.Log("Query got other/unknown object.");
                }
                else
                {
                  Util.Log("Query got string : {0}.", cs);
                }
              }
              else
              {
                Util.Log("Query got Position object with secId {0}, shares {1}.", pos.secId, pos.getSharesOutstanding);
              }
            }
            else
            {
              Util.Log("Query got Portfolio object with ID {0}, pkid {1}.", port.ID, port.Pkid);
            }
          }
        }

        qryIdx++;        
      }

      Assert.IsFalse(ErrorOccurred, "One or more query validation errors occurred.");
    }

    public void StepThreePQRS()
    {
      bool ErrorOccurred = false;

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      int qryIdx = 0;

      foreach (QueryStrings paramqrystr in QueryStatics.ResultSetParamQueries)
      {
        if (paramqrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}. {1}", qryIdx, paramqrystr.Query);

        Query<object> query = qs.NewQuery(paramqrystr.Query);

        //Populate the parameter list (paramList) for the query.
        object[] paramList = new object[QueryStatics.NoOfQueryParam[qryIdx]];
        int numVal = 0;
        for (int ind = 0; ind < QueryStatics.NoOfQueryParam[qryIdx]; ind++)
        {
          //Util.Log("NIL::PQRS:: QueryStatics.QueryParamSet[{0},{1}] = {2}", qryIdx, ind, QueryStatics.QueryParamSet[qryIdx][ind]);

          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSet[qryIdx][ind]);
            paramList[ind] = numVal;
            //Util.Log("NIL::PQRS::361 Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = (System.String)QueryStatics.QueryParamSet[qryIdx][ind];
            //Util.Log("NIL::PQRS:: Interger Args:: routingObj[0] = {1}", ind, routingObj[ind].ToString());
          }
        }

        ISelectResults<object> results = query.Execute(paramList);

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

        ResultSet<object> rs = results as ResultSet<object>;

        foreach (object item in rs)
        {
          if (!m_isPdx)
          {
            Portfolio port = item as Portfolio;
            if (port == null)
            {
              Position pos = item as Position;
              if (pos == null)
              {
                string cs = item as string;
                if (cs == null)
                {
                  Util.Log("Query got other/unknown object.");
                }
                else
                {
                  Util.Log("Query got string : {0}.", cs);
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
          else
          {
            PortfolioPdx port = item as PortfolioPdx;
            if (port == null)
            {
              PositionPdx pos = item as PositionPdx;
              if (pos == null)
              {
                string cs = item as string;
                if (cs == null)
                {
                  Util.Log("Query got other/unknown object.");
                }
                else
                {
                  Util.Log("Query got string : {0}.", cs);
                }
              }
              else
              {
                Util.Log("Query got PositionPdx object with secId {0}, shares {1}.", pos.secId, pos.getSharesOutstanding);
              }
            }
            else
            {
              Util.Log("Query got PortfolioPdx object with ID {0}, pkid {1}.", port.ID, port.Pkid);
            }
          }
        }
        
        qryIdx++;
      }

      Assert.IsFalse(ErrorOccurred, "One or more query validation errors occurred.");
    }

    public void StepFourRS()
    {
      bool ErrorOccurred = false;

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.ResultSetQueries)
      {
        if (qrystr.Category != QueryCategory.Unsupported)
        {
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating unsupported query index {0}.", qryIdx);

        Query<object> query = qs.NewQuery(qrystr.Query);

        try
        {
          ISelectResults<object> results = query.Execute();

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

    public void StepFourPQRS()
    {
      bool ErrorOccurred = false;

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.ResultSetParamQueries)
      {
        if (qrystr.Category != QueryCategory.Unsupported)
        {
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating unsupported query index {0}.", qryIdx);

        Query<object> query = qs.NewQuery(qrystr.Query);

        object[] paramList = new object[QueryStatics.NoOfQueryParam[qryIdx]];

        Int32 numVal = 0;
        for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParam[qryIdx]; ind++)
        {
          //Util.Log("NIL::PQRS:: QueryStatics.QueryParamSet[{0},{1}] = {2}", qryIdx, ind, QueryStatics.QueryParamSet[qryIdx, ind]);

          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSet[qryIdx][ind]);
            paramList[ind] = numVal;
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = (System.String)QueryStatics.QueryParamSet[qryIdx][ind];
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind].ToString());
          }
        }

        try
        {
          ISelectResults<object> results = query.Execute(paramList);

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

    public void StepThreeSS()
    {
      bool ErrorOccurred = false;

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.StructSetQueries)
      {
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        if (m_isPdx == true)
        {
          if (qryIdx == 12 || qryIdx == 4 || qryIdx == 7 || qryIdx == 22 || qryIdx == 30 || qryIdx == 34)
          {
            Util.Log("Skipping query index {0} for pdx because it has function.", qryIdx);
            qryIdx++;
            continue;
          }
        }

        Util.Log("Evaluating query index {0}. {1}", qryIdx, qrystr.Query);

        Query<object> query = qs.NewQuery(qrystr.Query);

        ISelectResults<object> results = query.Execute();

        int expectedRowCount = qh.IsExpectedRowsConstantSS(qryIdx) ?
          QueryStatics.StructSetRowCounts[qryIdx] : QueryStatics.StructSetRowCounts[qryIdx] * qh.PortfolioNumSets;

        if (!qh.VerifySS(results, expectedRowCount, QueryStatics.StructSetFieldCounts[qryIdx]))
        {
          ErrorOccurred = true;
          Util.Log("Query verify failed for query index {0}.", qryIdx);
          qryIdx++;
          continue;
        }

        StructSet<object> ss = results as StructSet<object>;
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

    public void StepThreePQSS()
    {
      bool ErrorOccurred = false;

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.StructSetParamQueries)
      {
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}. {1}", qryIdx, qrystr.Query);

        if (m_isPdx == true)
        {
          if (qryIdx == 16)
          {
            Util.Log("Skipping query index {0} for pdx because it has function.", qryIdx);
            qryIdx++;
            continue;
          }
        }

        Query<object> query = qs.NewQuery(qrystr.Query);

        //Populate the param list, paramList for parameterized query 
        object[] paramList = new object[QueryStatics.NoOfQueryParamSS[qryIdx]];

        Int32 numVal = 0;
        for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[qryIdx]; ind++)
        {
          //Util.Log("NIL::PQRS:: QueryStatics.QueryParamSetSS[{0},{1}] = {2}", qryIdx, ind, QueryStatics.QueryParamSetSS[qryIdx, ind]);

          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[qryIdx][ind]);
            paramList[ind] = numVal;
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = (System.String)QueryStatics.QueryParamSetSS[qryIdx][ind];
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind].ToString());
          }
        }

        ISelectResults<object> results = query.Execute(paramList);

        int expectedRowCount = qh.IsExpectedRowsConstantPQSS(qryIdx) ?
        QueryStatics.StructSetPQRowCounts[qryIdx] : QueryStatics.StructSetPQRowCounts[qryIdx] * qh.PortfolioNumSets;

        if (!qh.VerifySS(results, expectedRowCount, QueryStatics.StructSetPQFieldCounts[qryIdx]))
        {
          ErrorOccurred = true;
          Util.Log("Query verify failed for query index {0}.", qryIdx);
          qryIdx++;
          continue;
        }

        StructSet<object> ss = results as StructSet<object>;
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

    public void StepFourSS()
    {
      bool ErrorOccurred = false;

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.StructSetQueries)
      {
        if (qrystr.Category != QueryCategory.Unsupported)
        {
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating unsupported query index {0}.", qryIdx);

        Query<object> query = qs.NewQuery(qrystr.Query);

        try
        {
          ISelectResults<object> results = query.Execute();

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

    public void StepFourPQSS()
    {
      bool ErrorOccurred = false;

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.StructSetParamQueries)
      {
        if (qrystr.Category != QueryCategory.Unsupported)
        {
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating unsupported query index {0}.", qryIdx);

        Query<object> query = qs.NewQuery(qrystr.Query);

        //Populate the param list
        object[] paramList = new object[QueryStatics.NoOfQueryParamSS[qryIdx]];

        Int32 numVal = 0;
        for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[qryIdx]; ind++)
        {
          //Util.Log("NIL::PQRS:: QueryStatics.QueryParamSetSS[{0},{1}] = {2}", qryIdx, ind, QueryStatics.QueryParamSetSS[qryIdx, ind]);

          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[qryIdx][ind]);
            paramList[ind] = numVal;
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = (System.String)QueryStatics.QueryParamSetSS[qryIdx][ind];
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind].ToString());
          }
        }

        try
        {
          ISelectResults<object> results = query.Execute(paramList);

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

    public void StepOneFailover(bool isPdx)
    {
      m_isPdx = isPdx;
      // This is here so that Client1 registers information of the cacheserver
      // that has been already started
      CacheHelper.SetupJavaServers(true,
        "cacheserver_remoteoqlN.xml",
        "cacheserver_remoteoql2N.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      CacheHelper.CreateTCRegion_Pool<object, object>(QueryRegionNames[0], true, true, null,
        CacheHelper.Locators, "__TESTPOOL1_", true);

      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(QueryRegionNames[0]);
      if (!m_isPdx)
      {
        Portfolio p1 = new Portfolio(1, 100);
        Portfolio p2 = new Portfolio(2, 200);
        Portfolio p3 = new Portfolio(3, 300);
        Portfolio p4 = new Portfolio(4, 400);

        region["1"] = p1;
        region["2"] = p2;
        region["3"] = p3;
        region["4"] = p4;
      }
      else
      {
        PortfolioPdx p1 = new PortfolioPdx(1, 100);
        PortfolioPdx p2 = new PortfolioPdx(2, 200);
        PortfolioPdx p3 = new PortfolioPdx(3, 300);
        PortfolioPdx p4 = new PortfolioPdx(4, 400);

        region["1"] = p1;
        region["2"] = p2;
        region["3"] = p3;
        region["4"] = p4;
      }
    }

    public void StepTwoFailover()
    {
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");

      IAsyncResult killRes = null;
      KillServerDelegate ksd = new KillServerDelegate(KillServer);

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      for (int i = 0; i < 10000; i++)
      {
        Query<object> qry = qs.NewQuery("select distinct * from /" + QueryRegionNames[0]);

        ISelectResults<object> results = qry.Execute();

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

    public void StepTwoPQFailover()
    {
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");

      IAsyncResult killRes = null;
      KillServerDelegate ksd = new KillServerDelegate(KillServer);

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      for (int i = 0; i < 10000; i++)
      {
        Query<object> qry = qs.NewQuery("select distinct * from /" + QueryRegionNames[0] + " where ID > $1");

        //Populate the param list
        object[] paramList = new object[1];
        paramList[0] = 1;

        ISelectResults<object> results = qry.Execute(paramList);

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

    public void StepThreeQT()
    {
      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
      QueryService<object, object> qs = null;
      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();
      Util.Log("query " + QueryStatics.ResultSetQueries[34].Query);
      Query<object> query = qs.NewQuery(QueryStatics.ResultSetQueries[34].Query);

      try
      {
        Util.Log("EXECUTE 1 START for query: ", query.QueryString);
        ISelectResults<object> results = query.Execute(3);
        Util.Log("EXECUTE 1 STOP");
        Util.Log("Result size is {0}", results.Size);
        Assert.Fail("Didnt get expected timeout exception for first execute");
      }
      catch (GemFireException excp)
      {
        Util.Log("First execute expected exception: {0}", excp.Message);
      }
    }

    public void StepFourQT()
    {
      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
      QueryService<object, object> qs = null;
      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      Query<object> query = qs.NewQuery(QueryStatics.ResultSetQueries[35].Query);

      try
      {
        Util.Log("EXECUTE 2 START for query: ", query.QueryString);
        ISelectResults<object> results = query.Execute(850);
        Util.Log("EXECUTE 2 STOP");
        Util.Log("Result size is {0}", results.Size);
      }
      catch (GemFireException excp)
      {
        Assert.Fail("Second execute unwanted exception: {0}", excp.Message);
      }
    }

    public void StepFiveQT()
    {
      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
      QueryService<object, object> qs = null;
      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      Query<object> query = qs.NewQuery(QueryStatics.StructSetQueries[17].Query);

      try
      {
        Util.Log("EXECUTE 3 START for query: ", query.QueryString);
        ISelectResults<object> results = query.Execute(2);
        Util.Log("EXECUTE 3 STOP");
        Util.Log("Result size is {0}", results.Size);
        Assert.Fail("Didnt get expected timeout exception for third execute");
      }
      catch (GemFireException excp)
      {
        Util.Log("Third execute expected exception: {0}", excp.Message);
      }
    }

    public void StepSixQT()
    {
      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
      QueryService<object, object> qs = null;
      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();
      Query<object> query = qs.NewQuery(QueryStatics.StructSetQueries[17].Query);

      try
      {
        Util.Log("EXECUTE 4 START for query: ", query.QueryString);
        ISelectResults<object> results = query.Execute(850);
        Util.Log("EXECUTE 4 STOP");
        Util.Log("Result size is {0}", results.Size);
      }
      catch (GemFireException excp)
      {
        Assert.Fail("Fourth execute unwanted exception: {0}", excp.Message);
      }
    }

    public void StepThreePQT()
    {
      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
      QueryService<object, object> qs = null;
      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      Query<object> query = qs.NewQuery(QueryStatics.StructSetParamQueries[5].Query);
      

      try
      {
        Util.Log("EXECUTE 5 START for query: ", query.QueryString);
        //Populate the param list, paramList for parameterized query 
        object[] paramList = new object[QueryStatics.NoOfQueryParamSS[5]];

        Int32 numVal = 0;
        for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[5]; ind++)
        {
          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[5][ind]);
            paramList[ind] = numVal;
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = (System.String)QueryStatics.QueryParamSetSS[5][ind];
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind].ToString());
          }
        }

        ISelectResults<object> results = query.Execute(paramList, 1);
        Util.Log("EXECUTE 5 STOP");
        Util.Log("Result size is {0}", results.Size);
        Assert.Fail("Didnt get expected timeout exception for Fifth execute");
      }
      catch (GemFireException excp)
      {
        Util.Log("Fifth execute expected exception: {0}", excp.Message);
      }
    }

    public void StepFourPQT()
    {
      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
      QueryService<object, object> qs = null;
      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      Query<object> query = qs.NewQuery(QueryStatics.StructSetParamQueries[5].Query);

      try
      {
        Util.Log("EXECUTE 6 START for query: ", query.QueryString);
        //Populate the param list, paramList for parameterized query 
        object[] paramList = new object[QueryStatics.NoOfQueryParamSS[5]];

        Int32 numVal = 0;
        for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[5]; ind++)
        {
          try
          {
            numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[5][ind]);
            paramList[ind] = numVal;
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind]);
          }
          catch (FormatException )
          {
            //Console.WriteLine("Param string is not a sequence of digits.");
            paramList[ind] = (System.String)QueryStatics.QueryParamSetSS[5][ind];
            //Util.Log("NIL::PQRS:: Interger Args:: paramList[0] = {1}", ind, paramList[ind].ToString());
          }
        }

        ISelectResults<object> results = query.Execute(paramList, 850);
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

      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.RegionQueries)
      {        
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}. {1}", qryIdx, qrystr.Query);

        if (m_isPdx)
        {
          if (qryIdx == 18)
          {
            Util.Log("Skipping query index {0} because it is unsupported for pdx type.", qryIdx);
            qryIdx++;
            continue;
          }
        }

        ISelectResults<object> results = region.Query<object>(qrystr.Query);

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

      try
      {
          ISelectResults<object> results = region.Query<object>("");
          Assert.Fail("Expected IllegalArgumentException exception for empty predicate");
      }
      catch (IllegalArgumentException ex)
      {
          Util.Log("got expected IllegalArgumentException exception for empty predicate:");
          Util.Log(ex.Message);
      }


      try
      {
          ISelectResults<object> results = region.Query<object>(QueryStatics.RegionQueries[0].Query, 2200000);
          Assert.Fail("Expected IllegalArgumentException exception for invalid timeout");
      }
      catch (IllegalArgumentException ex)
      {
          Util.Log("got expected IllegalArgumentException exception for invalid timeout:");
          Util.Log(ex.Message);
      }

      
      try
      {
          ISelectResults<object> results = region.Query<object>("bad predicate");
          Assert.Fail("Expected QueryException exception for wrong predicate");
      }
      catch (QueryException ex)
      {
          Util.Log("got expected QueryException exception for wrong predicate:");
          Util.Log(ex.Message);
      }
    }

    public void StepFourRQ()
    {
      bool ErrorOccurred = false;

      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.RegionQueries)
      {
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}.{1}", qryIdx, qrystr.Query);

        bool existsValue = region.ExistsValue(qrystr.Query);
        bool expectedResult = QueryStatics.RegionQueryRowCounts[qryIdx] > 0 ? true : false;

        if (existsValue != expectedResult)
        {
          ErrorOccurred = true;
          Util.Log("FAIL: Query # {0} existsValue expected is {1}, actual is {2}", qryIdx,
            expectedResult ? "true" : "false", existsValue ? "true" : "false");
          qryIdx++;
          continue;
        }
        
        qryIdx++;
      }

      Assert.IsFalse(ErrorOccurred, "One or more query validation errors occurred.");
      try
      {
          bool existsValue = region.ExistsValue("");
          Assert.Fail("Expected IllegalArgumentException exception for empty predicate");
      }
      catch (IllegalArgumentException ex)
      {
          Util.Log("got expected IllegalArgumentException exception for empty predicate:");
          Util.Log(ex.Message);
      }


      try
      {
          bool existsValue = region.ExistsValue(QueryStatics.RegionQueries[0].Query, 2200000);
          Assert.Fail("Expected IllegalArgumentException exception for invalid timeout");
      }
      catch (IllegalArgumentException ex)
      {
          Util.Log("got expected IllegalArgumentException exception for invalid timeout:");
          Util.Log(ex.Message);
      }

      
      try
      {
          bool existsValue = region.ExistsValue("bad predicate");
          Assert.Fail("Expected QueryException exception for wrong predicate");
      }
      catch (QueryException ex)
      {
          Util.Log("got expected QueryException exception for wrong predicate:");
          Util.Log(ex.Message);
      }
    }

    public void StepFiveRQ()
    {
      bool ErrorOccurred = false;

      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);

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
          Object result = region.SelectValue(qrystr.Query);

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

      try
      {
          Object result = region.SelectValue("");
          Assert.Fail("Expected IllegalArgumentException exception for empty predicate");
      }
      catch (IllegalArgumentException ex)
      {
          Util.Log("got expected IllegalArgumentException exception for empty predicate:");
          Util.Log(ex.Message);
      }


      try
      {
          Object result = region.SelectValue(QueryStatics.RegionQueries[0].Query, 2200000);
          Assert.Fail("Expected IllegalArgumentException exception for invalid timeout");
      }
      catch (IllegalArgumentException ex)
      {
          Util.Log("got expected IllegalArgumentException exception for invalid timeout:");
          Util.Log(ex.Message);
      }

      try
      {
          Object result = region.SelectValue("bad predicate");
          Assert.Fail("Expected QueryException exception for wrong predicate");
      }
      catch (QueryException ex)
      {
          Util.Log("got expected QueryException exception for wrong predicate:");
          Util.Log(ex.Message);
      }
    }

    public void StepSixRQ()
    {
      bool ErrorOccurred = false;

      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.RegionQueries)
      {
        if ((qrystr.Category != QueryCategory.Unsupported) || (qryIdx == 3))
        {
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating unsupported query index {0}.", qryIdx);

        try
        {
          ISelectResults<object> results = region.Query<object>(qrystr.Query);

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
    
    //private void CreateRegions(object p, object USE_ACK, object endPoint1, bool p_4)
    //{
    //  throw new Exception("The method or operation is not implemented.");
    //}

    
    //public void CompareMap(CacheableHashMap map1, CacheableHashMap map2)
    //{
    //  if (map1.Count != map2.Count)
    //    Assert.Fail("Number of Keys dont match");
    //  if (map1.Count == 0) return;
    //  foreach (KeyValuePair<ICacheableKey, IGFSerializable> entry in map1)
    //  {
    //    IGFSerializable value;
    //    if (!(map2.TryGetValue(entry.Key,out value)))
    //    {
    //      Assert.Fail("Key was not found");
    //      return;
    //    }
    //    if(entry.Value.Equals(value))
    //    {
    //      Assert.Fail("Value was not found");
    //      return;
    //    }
    //  }
    //}

    //public void GetAllRegionQuery()
    //{
    //  IRegion<object, object> region0 = CacheHelper.GetVerifyRegion(QueryRegionNames[0]);
    //  IRegion<object, object> region1 = region0.GetSubRegion(QueryRegionNames[1] );
    //  IRegion<object, object> region2 = CacheHelper.GetVerifyRegion(QueryRegionNames[1]);
    //  IRegion<object, object> region3 = CacheHelper.GetVerifyRegion(QueryRegionNames[2]);
    //  IRegion<object, object> region4 = CacheHelper.GetVerifyRegion(QueryRegionNames[3]);
    //  string[] SecIds = Portfolio.SecIds;
    //  int NumSecIds = SecIds.Length;
    //  List<ICacheableKey> PosKeys = new List<ICacheableKey>();
    //  List<ICacheableKey> PortKeys = new List<ICacheableKey>();
    //  CacheableHashMap ExpectedPosMap = new CacheableHashMap();
    //  CacheableHashMap ExpectedPortMap = new CacheableHashMap();
    //  QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
    //  int SetSize = qh.PositionSetSize;
    //  int NumSets = qh.PositionNumSets;
    //  for (int set = 1; set <= NumSets; set++)
    //  {
    //    for (int current = 1; current <= SetSize; current++)
    //    {
    //      CacheableKey PosKey  = "pos" + set + "-" + current;
    //      Position pos = new Position(SecIds[current % NumSecIds], current * 100 );
    //      PosKeys.Add(PosKey);
    //      ExpectedPosMap.Add(PosKey, pos);
    //    }
    //  }
    //  SetSize = qh.PortfolioSetSize;
    //  NumSets = qh.PortfolioNumSets;
    //  for (int set = 1; set <= NumSets; set++)
    //  {
    //    for (int current = 1; current <= SetSize; current++)
    //    {
    //      CacheableKey PortKey = "port" + set + "-" + current;
    //      Portfolio Port = new Portfolio(current,1);
    //      PortKeys.Add(PortKey);
    //      ExpectedPortMap.Add(PortKey, Port);
    //    }
    //  }
    //  CacheableHashMap ResMap = new CacheableHashMap();
    //  Dictionary<ICacheableKey, Exception> ExMap = new Dictionary<ICacheableKey, Exception>();
    //  region0.GetAll(PortKeys.ToArray(), ResMap, ExMap);
    //  CompareMap(ResMap, ExpectedPortMap);
    //  if (ExMap.Count != 0)
    //  {
    //    Assert.Fail("Expected No Exception");
    //  }
    //  ResMap.Clear();

    //  region1.GetAll(PosKeys.ToArray(), ResMap, ExMap);
    //  CompareMap(ResMap, ExpectedPosMap);
    //  if (ExMap.Count != 0)
    //  {
    //    Assert.Fail("Expected No Exception");
    //  }
    //  ResMap.Clear();
    //  region2.GetAll(PosKeys.ToArray(), ResMap, ExMap);
    //  CompareMap(ResMap, ExpectedPosMap);
    //  if (ExMap.Count != 0)
    //  {
    //    Assert.Fail("Expected No Exception");
    //  }
    //  ResMap.Clear();

    //  region3.GetAll(PortKeys.ToArray(), ResMap, ExMap);
    //  CompareMap(ResMap, ExpectedPortMap);
    //  if (ExMap.Count != 0)
    //  {
    //    Assert.Fail("Expected No Exception");
    //  }
    //  ResMap.Clear();

    //  region4.GetAll(PortKeys.ToArray(), ResMap, ExMap);
    //  CompareMap(ResMap, ExpectedPortMap);
    //  if (ExMap.Count != 0)
    //  {
    //    Assert.Fail("Expected No Exception");
    //  }
    //  ResMap.Clear();
    //}
    #endregion

    void runRemoteQueryRS()
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOne, CacheHelper.Locators, m_isPdx);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwo, m_isPdx);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThreeRS);
      Util.Log("StepThree complete.");

      m_client1.Call(StepFourRS);
      Util.Log("StepFour complete.");

      m_client1.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    void runRemoteParamQueryRS()
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOne, CacheHelper.Locators, m_isPdx);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwo, m_isPdx);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThreePQRS);
      Util.Log("StepThree complete.");

      m_client1.Call(StepFourPQRS);
      Util.Log("StepFour complete.");

      m_client1.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    void runRemoteQuerySS()
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client2.Call(StepOne, CacheHelper.Locators, m_isPdx);
      Util.Log("StepOne complete.");

      m_client2.Call(StepTwo, m_isPdx);
      Util.Log("StepTwo complete.");

      m_client2.Call(StepThreeSS);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFourSS);
      Util.Log("StepFour complete.");

      //m_client2.Call(GetAllRegionQuery);

      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    void runRemoteParamQuerySS()
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client2.Call(StepOne, CacheHelper.Locators, m_isPdx);
      Util.Log("StepOne complete.");

      m_client2.Call(StepTwo, m_isPdx);
      Util.Log("StepTwo complete.");

      m_client2.Call(StepThreePQSS);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFourPQSS);
      Util.Log("StepFour complete.");

      //m_client2.Call(GetAllRegionQuery);

      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    void runRemoteQueryFailover()
    {
      try
      {
        m_client1.Call(StepOneFailover, m_isPdx);
        Util.Log("StepOneFailover complete.");

        m_client1.Call(StepTwoFailover);
        Util.Log("StepTwoFailover complete.");

        m_client1.Call(Close);
        Util.Log("Client closed");
      }
      finally
      {
        m_client1.Call(CacheHelper.StopJavaServers);
        m_client1.Call(CacheHelper.StopJavaLocator, 1);
      }
    }

    void runRemoteParamQueryFailover()
    {
      try
      {
        m_client1.Call(StepOneFailover, m_isPdx);
        Util.Log("StepOneFailover complete.");

        m_client1.Call(StepTwoPQFailover);
        Util.Log("StepTwoPQFailover complete.");

        m_client1.Call(Close);
        Util.Log("Client closed");
      }
      finally
      {
        m_client1.Call(CacheHelper.StopJavaServers);
        m_client1.Call(CacheHelper.StopJavaLocator, 1);
      }
    }

    void runQueryExclusiveness()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_remoteoqlN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOneQE, CacheHelper.Locators, m_isPdx);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwoQE);
      Util.Log("StepTwo complete.");

      m_client1.Call(Close);
      Util.Log("Client closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    void runQueryTimeout()
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOne, CacheHelper.Locators, m_isPdx);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwoQT);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThreeQT);
      Util.Log("StepThree complete.");

      Thread.Sleep(150000); // sleep 2.5min to allow server query to complete

      m_client1.Call(StepFourQT);
      Util.Log("StepFour complete.");

      m_client1.Call(StepFiveQT);
      Util.Log("StepFive complete.");

      Thread.Sleep(60000); // sleep 1min to allow server query to complete

      m_client1.Call(StepSixQT);
      Util.Log("StepSix complete.");

      m_client1.Call(Close);
      Util.Log("Client closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    void runParamQueryTimeout()
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started. WITH PDX = " + m_isPdx);

      m_client1.Call(StepOne, CacheHelper.Locators, m_isPdx);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwoQT);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThreePQT);
      Util.Log("StepThreePQT complete.");

      Thread.Sleep(60000); // sleep 1min to allow server query to complete

      m_client1.Call(StepFourPQT);
      Util.Log("StepFourPQT complete.");

      m_client1.Call(Close);
      Util.Log("Client closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    void runRegionQuery()
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client2.Call(StepOne, CacheHelper.Locators, m_isPdx);
      Util.Log("StepOne complete.");

      m_client2.Call(StepTwo, m_isPdx);
      Util.Log("StepTwo complete.");

      //Extra Step
      //m_client1.Call(StepExtra);

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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    static bool m_isPdx = false;

    [Test]
    public void RemoteQueryRS()
    {
      for (int i = 0; i < 2; i++)
      {
        runRemoteQueryRS();
        m_isPdx = true;
      }
      m_isPdx = false;
    }

    [Test]
    public void RemoteParamQueryRS()
    {
      for (int i = 0; i < 2; i++)
      {
        runRemoteParamQueryRS();
        m_isPdx = true;
      }
      m_isPdx = false;
    }

    [Test]
    public void RemoteQuerySS()
    {
      for (int i = 0; i < 2; i++)
      {
        runRemoteQuerySS();
        m_isPdx = true;
      }
      m_isPdx = false;
    }

    [Test]
    public void RemoteParamQuerySS()
    {
      for (int i = 0; i < 2; i++)
      {
        runRemoteParamQuerySS();
        m_isPdx = true;
      }
      m_isPdx = false;
    }

    [Test]
    [Ignore]
    public void RemoteQueryFailover()
    {
      for (int i = 0; i < 2; i++)
      {
        runRemoteQueryFailover();
        m_isPdx = true;
      }
      m_isPdx = false;
    }

    [Test]
    [Ignore]
    public void RemoteParamQueryFailover()
    {
      for (int i = 0; i < 2; i++)
      {
        runRemoteParamQueryFailover();
        m_isPdx = true;
      }
      m_isPdx = false;
    }

    [Test]
    public void QueryExclusiveness()
    {
      for (int i = 0; i < 2; i++)
      {
        runQueryExclusiveness();
        m_isPdx = true;
      }
      m_isPdx = false;
    }

    [Test]
    [Ignore]
    public void QueryTimeout()
    {
      for (int i = 0; i < 2; i++)
      {
        runQueryTimeout();
        m_isPdx = true;
      }
      m_isPdx = false;
    }

    [Test]
    [Ignore]
    public void ParamQueryTimeout()
    {
      for (int i = 0; i < 2; i++)
      {
        runParamQueryTimeout();
        m_isPdx = true;
      }
      m_isPdx = false;
    }

    //Successful@8th_march
    [Test]
    public void RegionQuery()
    {
      for (int i = 0; i < 2; i++)
      {
        runRegionQuery();
        m_isPdx = true;
      }
      m_isPdx = false;
    }
    
  }
}
