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
  [Category("group4")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientCacheableStringArrayTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1;
    private UnitProcess m_client2;
    private static string[] QueryRegionNames = { "Portfolios", "Positions", "Portfolios2",
      "Portfolios3" };
    private static string QERegionName = "Portfolios";

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
        // ignore since we run multiple incarnations of client in same process
      }
    }
    public void StepOne(string endpoints, string locators, bool pool, bool locator)
    {
      if (pool)
      {
        CacheHelper.CreateTCRegion_Pool(QueryRegionNames[0], true, true,
          null, endpoints, locators, "__TESTPOOL1_", true);
        CacheHelper.CreateTCRegion_Pool(QueryRegionNames[1], true, true,
          null, endpoints, locators, "__TESTPOOL1_", true);
        CacheHelper.CreateTCRegion_Pool(QueryRegionNames[2], true, true,
          null, endpoints, locators, "__TESTPOOL1_", true);
        CacheHelper.CreateTCRegion_Pool(QueryRegionNames[3], true, true,
          null, endpoints, locators, "__TESTPOOL1_", true);
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

      string[] sta = {"C#aaa", "C#bbb", "C#ccc", "C#ddd"};
      CacheableStringArray cnm = CacheableStringArray.Create(sta);
      qh.PopulatePortfolioData(region0, qh.PortfolioSetSize,
        qh.PortfolioNumSets, 1, cnm);
      qh.PopulatePositionData(subRegion0, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
      qh.PopulatePositionData(region1, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
      qh.PopulatePortfolioData(region2, qh.PortfolioSetSize,
        qh.PortfolioNumSets, 1, cnm);
      qh.PopulatePortfolioData(region3, qh.PortfolioSetSize,
        qh.PortfolioNumSets, 1, cnm);
    }

    public void StepTwoQT()
    {
      Region region0 = CacheHelper.GetRegion(QueryRegionNames[0]);
      Region subRegion0 = region0.GetSubRegion(QueryRegionNames[1]);

      QueryHelper qh = QueryHelper.GetHelper();
      string[] sta = {"C#aaa", "C#bbb", "C#ccc", "C#ddd"};
      CacheableStringArray cnm = CacheableStringArray.Create(sta);
      qh.PopulatePortfolioData(region0, 4, 2, 2, cnm);
      qh.PopulatePositionData(subRegion0, 4, 2);
    }

    public void StepOneQE(string endpoints, string locators, bool pool, bool locator)
    {
      if (pool)
      {
        CacheHelper.CreateTCRegion_Pool(QERegionName, true, true,
          null, endpoints, locators, "__TESTPOOL1_", true);
      }
      else
      {
        CacheHelper.CreateTCRegion(QERegionName, true, true,
          null, endpoints, true);
      }

      Region region = CacheHelper.GetVerifyRegion(QERegionName);
      string[] sta = {"C#aaa", "C#bbb", "C#ccc", "C#ddd"};
      CacheableStringArray cnm = CacheableStringArray.Create(sta);
      Portfolio p1 = new Portfolio(1, 2, cnm);
      Portfolio p2 = new Portfolio(2, 2, cnm);
      Portfolio p3 = new Portfolio(3, 2, cnm);
      Portfolio p4 = new Portfolio(4, 2, cnm);

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
      Query qry = qs.NewQuery("select * from /" + QERegionName + "  p where p.ID!=3");
      ISelectResults results = qry.Execute();
      Util.Log("Results size {0}.", results.Size);

      SelectResultsIterator iter = results.GetIterator();
      
      while(iter.HasNext)
      {
	IGFSerializable item = iter.Next();
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
      // Bring down the region
      region.LocalDestroyRegion();
    }

    void runCacheableStringArrayTest(bool pool, bool locator)
    {
      CacheHelper.SetupJavaServers(pool && locator, "remotequery.xml");
      if (pool && locator)
      {
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOne, CacheHelper.Endpoints, CacheHelper.Locators, pool, locator);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwo);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepOneQE, CacheHelper.Endpoints, CacheHelper.Locators, pool, locator);
      Util.Log("StepOneQE complete.");

      m_client1.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    [Test]
    public void CacheableStringArrayTest()
    {
      runCacheableStringArrayTest(false, false); //region config
      runCacheableStringArrayTest(true, false); //region config
      runCacheableStringArrayTest(true, true); //region config
    }

  }
}
