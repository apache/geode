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


  [TestFixture]
  [Category("group4")]
  [Category("unicast_only")]
  [Category("generics")]
  
  public class ThinClientStringArrayTests : ThinClientRegionSteps
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
        Serializable.RegisterTypeGeneric(Portfolio.CreateDeserializable);
        Serializable.RegisterTypeGeneric(Position.CreateDeserializable);
      }
      catch (IllegalStateException)
      {
        // ignore since we run multiple incarnations of client in same process
      }
    }
    public void StepOne(string locators)
    {
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

    public void StepTwo()
    {
      IRegion<object, object> region0 = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);
      IRegion<object, object> subRegion0 = region0.GetSubRegion(QueryRegionNames[1]);
      IRegion<object, object> region1 = CacheHelper.GetRegion<object, object>(QueryRegionNames[1]);
      IRegion<object, object> region2 = CacheHelper.GetRegion<object, object>(QueryRegionNames[2]);
      IRegion<object, object> region3 = CacheHelper.GetRegion<object, object>(QueryRegionNames[3]);

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
      Util.Log("SetSize {0}, NumSets {1}.", qh.PortfolioSetSize,
        qh.PortfolioNumSets);

      string[] cnm = { "C#aaa", "C#bbb", "C#ccc", "C#ddd" };
      //CacheableStringArray cnm = CacheableStringArray.Create(sta);
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
      IRegion<object, object> region0 = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);
      IRegion<object, object> subRegion0 = region0.GetSubRegion(QueryRegionNames[1]);

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
      string[] /*sta*/ cnm = { "C#aaa", "C#bbb", "C#ccc", "C#ddd" };
      //CacheableStringArray cnm = CacheableStringArray.Create(sta);
      qh.PopulatePortfolioData(region0, 4, 2, 2, cnm);
      qh.PopulatePositionData(subRegion0, 4, 2);
    }

    public void StepOneQE(string locators)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(QERegionName, true, true,
        null, locators, "__TESTPOOL1_", true);
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      string[] /*sta*/ cnm = { "C#aaa", "C#bbb", "C#ccc", "C#ddd" };
      //CacheableStringArray cnm = CacheableStringArray.Create(sta);
      Portfolio p1 = new Portfolio(1, 2, cnm);
      Portfolio p2 = new Portfolio(2, 2, cnm);
      Portfolio p3 = new Portfolio(3, 2, cnm);
      Portfolio p4 = new Portfolio(4, 2, cnm);

      region["1"] = p1;
      region["2"] = p2;
      region["3"] = p3;
      region["4"] = p4;

      QueryService<object, object> qs = null;
      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();
      Query<object> qry = qs.NewQuery("select * from /" + QERegionName + "  p where p.ID!=3");
      ISelectResults<object> results = qry.Execute();
      Util.Log("Results size {0}.", results.Size);

      SelectResultsIterator<object> iter = results.GetIterator();

      while (iter.HasNext)
      {
        /*IGFSerializable*/ object item = iter.Next();
        Portfolio port = item as Portfolio;
        if (port == null)
        {
          Position pos = item as Position;
          if (pos == null)
          {
            //CacheableString cs = item as CacheableString;
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
      // Bring down the region
      region.GetLocalView().DestroyRegion();
    }

    void runStringArrayTest()
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOne, CacheHelper.Locators);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwo);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepOneQE, CacheHelper.Locators);
      Util.Log("StepOneQE complete.");

      m_client1.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    [Test]
    public void StringArrayTest()
    {
      runStringArrayTest();
    }

  }
}
