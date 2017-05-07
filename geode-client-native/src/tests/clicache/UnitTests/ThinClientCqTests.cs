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
  
  public class MyCqListener : ICqListener
  {
    #region Private members
    private bool m_failedOver = false;
    private UInt32 m_eventCountBefore = 0;
    private UInt32 m_errorCountBefore = 0;
    private UInt32 m_eventCountAfter = 0;
    private UInt32 m_errorCountAfter = 0;

    #endregion

    #region Public accessors

    public void failedOver()
    {
      m_failedOver = true;
    }
    public  UInt32 getEventCountBefore()
    {
      return m_eventCountBefore;
    }
    public  UInt32 getErrorCountBefore()
    {
      return m_errorCountBefore;
    }
    public  UInt32 getEventCountAfter()
    {
      return m_eventCountAfter;
    }
    public  UInt32 getErrorCountAfter()
    {
      return m_errorCountAfter;
    }
    #endregion

    public virtual void OnEvent(CqEvent ev)
    {
      Util.Log("MyCqListener::OnEvent called");
      if(m_failedOver == true)
         m_eventCountAfter++;
      else
         m_eventCountBefore++;
      IGFSerializable val = ev.getNewValue();
      ICacheableKey key = ev.getKey();
      CqOperationType opType = ev.getQueryOperation();
      CacheableString keyS = key as CacheableString;
      Portfolio pval = val as Portfolio;
      string opStr = "DESTROY";
      if(opType == CqOperationType.OP_TYPE_CREATE)
         opStr = "CREATE";
      else if(opType == CqOperationType.OP_TYPE_UPDATE)
         opStr = "UPDATE";
      Util.Log("key {0}, value ({1},{2}), op {3}.", keyS.Value,
        pval.ID, pval.Pkid,opStr);
    }
    public virtual void OnError(CqEvent ev)
    {
      Util.Log("MyCqListener::OnError called");
      if(m_failedOver == true)
         m_errorCountAfter++;
      else
         m_errorCountBefore++;
    }
    public virtual void Close()
    {
      Util.Log("MyCqListener::close called");
    }
  }

  [TestFixture]
  [Category("group3")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientCqTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1;
    private UnitProcess m_client2;
    private static string[] QueryRegionNames = { "Portfolios", "Positions", "Portfolios2",
      "Portfolios3" };
    private static string QERegionName = "Portfolios";
    private static string CqName = "MyCq";
    private static string CqName1 = "MyCq1";

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
        // ignore since we run multiple iterations for pool and non pool configs
      }
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

    public void StepOneQE(string endpoints, string locators, bool pool, bool locator)
    {
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
      CqAttributesFactory cqFac = new CqAttributesFactory();
      ICqListener  cqLstner = new MyCqListener();
      cqFac.AddCqListener(cqLstner);
      CqAttributes cqAttr = cqFac.Create();
      CqQuery qry = qs.NewCq(CqName, "select * from /" + QERegionName + "  p where p.ID!=2", cqAttr, true);
      qry.Execute();
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete
      region.Put("4", p1);
      region.Put("3", p2);
      region.Put("2", p3);
      region.Put("1", p4);
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete

      qry = qs.GetCq(CqName);

      CqServiceStatistics cqSvcStats = qs.GetCqStatistics();
      Assert.AreEqual(1, cqSvcStats.numCqsActive());
      Assert.AreEqual(1, cqSvcStats.numCqsCreated());
      Assert.AreEqual(1, cqSvcStats.numCqsOnClient());
      
      cqAttr = qry.GetCqAttributes();
      ICqListener[] vl =  cqAttr.getCqListeners();
      Assert.IsNotNull(vl);
      Assert.AreEqual(1, vl.Length);
      cqLstner = vl[0];
      Assert.IsNotNull(cqLstner);
      MyCqListener myLisner = cqLstner as MyCqListener;
      Util.Log("event count:{0}, error count {1}.", myLisner.getEventCountBefore(), myLisner.getErrorCountBefore());

      CqStatistics cqStats = qry.GetStatistics();
      Assert.AreEqual(cqStats.numEvents(), myLisner.getEventCountBefore());
      if (myLisner.getEventCountBefore() + myLisner.getErrorCountBefore() == 0)
      {
        Assert.Fail("cq before count zero");
      }
      qry.Stop();
      Assert.AreEqual(1, cqSvcStats.numCqsStopped());
      qry.Close();
      Assert.AreEqual(1, cqSvcStats.numCqsClosed());
      // Bring down the region
      region.LocalDestroyRegion();
    }

    public void KillServer()
    {
      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
    }

    public delegate void KillServerDelegate();

    public void StepOneFailover()
    {
      // This is here so that Client1 registers information of the cacheserver
      // that has been already started
      CacheHelper.SetupJavaServers("remotequery.xml",
        "cqqueryfailover.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      Util.Log("Cacheserver 1 started.");

      CacheHelper.CreateTCRegion(QueryRegionNames[0], true, true, null, CacheHelper.Endpoints, true);

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

    public void StepTwoFailover()
    {
      CacheHelper.StartJavaServer(2, "GFECS2");
      Util.Log("Cacheserver 2 started.");

      IAsyncResult killRes = null;
      KillServerDelegate ksd = new KillServerDelegate(KillServer);
      CacheHelper.CreateTCRegion(QueryRegionNames[0], true, true, null, CacheHelper.Endpoints, true);

      Region region = CacheHelper.GetVerifyRegion(QueryRegionNames[0]);

      QueryService qs = CacheHelper.DCache.GetQueryService();
      CqAttributesFactory cqFac = new CqAttributesFactory();
      ICqListener  cqLstner = new MyCqListener();
      cqFac.AddCqListener(cqLstner);
      CqAttributes cqAttr = cqFac.Create();
      CqQuery qry = qs.NewCq(CqName1, "select * from /" + QERegionName + "  p where p.ID!<4", cqAttr, true);
      qry.Execute();
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete
      qry = qs.GetCq(CqName1);
      cqAttr = qry.GetCqAttributes();
      ICqListener[] vl =  cqAttr.getCqListeners();
      Assert.IsNotNull(vl);
      Assert.AreEqual(1, vl.Length);
      cqLstner = vl[0];
      Assert.IsNotNull(cqLstner);
      MyCqListener myLisner = cqLstner as MyCqListener;
      if (myLisner.getEventCountAfter() + myLisner.getErrorCountAfter() != 0)
      {
        Assert.Fail("cq after count not zero");
      }

      killRes = ksd.BeginInvoke(null, null);
      Thread.Sleep(18000); // sleep 0.3min to allow failover complete
      myLisner.failedOver();

      Portfolio p1 = new Portfolio(1, 100);
      Portfolio p2 = new Portfolio(2, 200);
      Portfolio p3 = new Portfolio(3, 300);
      Portfolio p4 = new Portfolio(4, 400);

      region.Put("4", p1);
      region.Put("3", p2);
      region.Put("2", p3);
      region.Put("1", p4);
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete

      qry = qs.GetCq(CqName1);
      cqAttr = qry.GetCqAttributes();
      vl =  cqAttr.getCqListeners();
      cqLstner = vl[0];
      Assert.IsNotNull(vl);
      Assert.AreEqual(1, vl.Length);
      cqLstner = vl[0];
      Assert.IsNotNull(cqLstner);
      myLisner = cqLstner as MyCqListener;
      if (myLisner.getEventCountAfter() + myLisner.getErrorCountAfter() == 0)
      {
        Assert.Fail("no cq after failover");
      }

      killRes.AsyncWaitHandle.WaitOne();
      ksd.EndInvoke(killRes);
      qry.Stop();
      qry.Close();
    }

    void runCqQueryTest(bool pool, bool locator)
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

      m_client1.Call(StepOneQE, CacheHelper.Endpoints, CacheHelper.Locators, pool, locator);
      Util.Log("StepOne complete.");

      m_client1.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    }

    [Test]
    public void CqQueryTest()
    {
      runCqQueryTest(false, false); // region config
      runCqQueryTest(true, false); // pool with server endpoints
      runCqQueryTest(true, true); // pool with locators
    }

   // [Test]
   // public void CqFailover()
   // {
    //  try
    //  {
     //   m_client1.Call(StepOneFailover);
    //    Util.Log("StepOneFailover complete.");
//
 //       m_client1.Call(StepTwoFailover);
  //      Util.Log("StepTwoFailover complete.");
  //    }
  //    finally
   //   {
   //     m_client1.Call(CacheHelper.StopJavaServers);
    //  }
  //  }
  }
}
