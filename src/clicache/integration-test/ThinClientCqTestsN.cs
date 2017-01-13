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

  public class MyCqListener<TKey, TResult> : ICqListener<TKey, TResult>
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
    public UInt32 getEventCountBefore()
    {
      return m_eventCountBefore;
    }
    public UInt32 getErrorCountBefore()
    {
      return m_errorCountBefore;
    }
    public UInt32 getEventCountAfter()
    {
      return m_eventCountAfter;
    }
    public UInt32 getErrorCountAfter()
    {
      return m_errorCountAfter;
    }
    #endregion

    public virtual void OnEvent(CqEvent<TKey, TResult> ev)
    {
      Util.Log("MyCqListener::OnEvent called");
      if (m_failedOver == true)
        m_eventCountAfter++;
      else
        m_eventCountBefore++;

      //IGFSerializable val = ev.getNewValue();
      //ICacheableKey key = ev.getKey();

      TResult val = (TResult)ev.getNewValue();
      /*ICacheableKey*/
      TKey key = ev.getKey();

      CqOperationType opType = ev.getQueryOperation();
      //CacheableString keyS = key as CacheableString;
      string keyS = key.ToString(); //as string;
      Portfolio pval = val as Portfolio;
      PortfolioPdx pPdxVal = val as PortfolioPdx;
      Assert.IsTrue((pPdxVal != null) || (pval != null));
      //string opStr = "DESTROY";
      /*if (opType == CqOperationType.OP_TYPE_CREATE)
        opStr = "CREATE";
      else if (opType == CqOperationType.OP_TYPE_UPDATE)
        opStr = "UPDATE";*/

      //Util.Log("key {0}, value ({1},{2}), op {3}.", keyS,
      //  pval.ID, pval.Pkid, opStr);
    }
    public virtual void OnError(CqEvent<TKey, TResult> ev)
    {
      Util.Log("MyCqListener::OnError called");
      if (m_failedOver == true)
        m_errorCountAfter++;
      else
        m_errorCountBefore++;
    }
    public virtual void Close()
    {
      Util.Log("MyCqListener::close called");
    }
    public virtual void Clear()
    {
      Util.Log("MyCqListener::Clear called");
      m_eventCountBefore = 0;
      m_errorCountBefore = 0;
      m_eventCountAfter = 0;
      m_errorCountAfter = 0;
    }
  }

  public class MyCqListener1<TKey, TResult> : ICqListener<TKey, TResult>
  {
    public static UInt32 m_cntEvents = 0;

    public virtual void OnEvent(CqEvent<TKey, TResult> ev)
    {
      m_cntEvents++;
      Util.Log("MyCqListener1::OnEvent called");
      Object val = (Object)ev.getNewValue();
      Object pkey = (Object)ev.getKey();
      int value = (int)val;
      int key = (int)pkey;
      CqOperationType opType = ev.getQueryOperation();
      String opStr = "Default";
      if (opType == CqOperationType.OP_TYPE_CREATE)
        opStr = "CREATE";
      else if (opType == CqOperationType.OP_TYPE_UPDATE)
        opStr = "UPDATE";

      Util.Log("MyCqListener1::OnEvent called with {0} , key = {1}, value = {2} ",
      opStr, key, value);
    }
    public virtual void OnError(CqEvent<TKey, TResult> ev)
    {
      Util.Log("MyCqListener1::OnError called");
    }
    public virtual void Close()
    {
      Util.Log("MyCqListener1::close called");
    }
  } 
   


  public class MyCqStatusListener<TKey, TResult> : ICqStatusListener<TKey, TResult>
  {
    #region Private members
    private bool m_failedOver = false;
    private UInt32 m_eventCountBefore = 0;
    private UInt32 m_errorCountBefore = 0;
    private UInt32 m_eventCountAfter = 0;
    private UInt32 m_errorCountAfter = 0;
    private UInt32 m_CqConnectedCount = 0;
    private UInt32 m_CqDisConnectedCount = 0;

    #endregion

    #region Public accessors

    public MyCqStatusListener(int id)
    {
    }

    public void failedOver()
    {
      m_failedOver = true;
    }
    public UInt32 getEventCountBefore()
    {
      return m_eventCountBefore;
    }
    public UInt32 getErrorCountBefore()
    {
      return m_errorCountBefore;
    }
    public UInt32 getEventCountAfter()
    {
      return m_eventCountAfter;
    }
    public UInt32 getErrorCountAfter()
    {
      return m_errorCountAfter;
    }
    public UInt32 getCqConnectedCount()
    {
      return m_CqConnectedCount;
    }
    public UInt32 getCqDisConnectedCount()
    {
      return m_CqDisConnectedCount;
    }
    #endregion

    public virtual void OnEvent(CqEvent<TKey, TResult> ev)
    {
      Util.Log("MyCqStatusListener::OnEvent called");
      if (m_failedOver == true)
        m_eventCountAfter++;
      else
        m_eventCountBefore++;      

      TResult val = (TResult)ev.getNewValue();      
      TKey key = ev.getKey();

      CqOperationType opType = ev.getQueryOperation();      
      string keyS = key.ToString(); //as string;      
    }
    public virtual void OnError(CqEvent<TKey, TResult> ev)
    {
      Util.Log("MyCqStatusListener::OnError called");
      if (m_failedOver == true)
        m_errorCountAfter++;
      else
        m_errorCountBefore++;
    }
    public virtual void Close()
    {
      Util.Log("MyCqStatusListener::close called");
    }
    public virtual void OnCqConnected()
    {
      m_CqConnectedCount++;
      Util.Log("MyCqStatusListener::OnCqConnected called");
    }
    public virtual void OnCqDisconnected()
    {
      m_CqDisConnectedCount++;
      Util.Log("MyCqStatusListener::OnCqDisconnected called");
    }

    public virtual void Clear()
    {
      Util.Log("MyCqStatusListener::Clear called");
      m_eventCountBefore = 0;
      m_errorCountBefore = 0;
      m_eventCountAfter = 0;
      m_errorCountAfter = 0;
      m_CqConnectedCount = 0;
      m_CqDisConnectedCount = 0;
    }
  }

  [TestFixture]
  [Category("group3")]
  [Category("unicast_only")]
  [Category("generics")]

  public class ThinClientCqTests : ThinClientRegionSteps
  {
    #region Private members
    private static bool m_usePdxObjects = false;
    private UnitProcess m_client1;
    private UnitProcess m_client2;
    private static string[] QueryRegionNames = { "Portfolios", "Positions", "Portfolios2",
      "Portfolios3" };
    private static string QERegionName = "Portfolios";
    private static string CqName = "MyCq";

    private static string CqName1 = "testCQAllServersLeave";
    private static string CqName2 = "testCQAllServersLeave1";

    private static string CqQuery1 = "select * from /DistRegionAck";
    private static string CqQuery2 = "select * from /DistRegionAck1";
    //private static string CqName1 = "MyCq1";

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
        // ignore since we run multiple iterations for pool and non pool configs
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
      CacheHelper.CreateTCRegion_Pool<object, object>("DistRegionAck", true, true,
        null, locators, "__TESTPOOL1_", true);
      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);
      GemStone.GemFire.Cache.Generic.RegionAttributes<object, object> regattrs = region.Attributes;
      region.CreateSubRegion(QueryRegionNames[1], regattrs);
    }

    public void StepTwo(bool usePdxObject)
    {
      IRegion<object, object> region0 = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);
      IRegion<object, object> subRegion0 = region0.GetSubRegion(QueryRegionNames[1]);
      IRegion<object, object> region1 = CacheHelper.GetRegion<object, object>(QueryRegionNames[1]);
      IRegion<object, object> region2 = CacheHelper.GetRegion<object, object>(QueryRegionNames[2]);
      IRegion<object, object> region3 = CacheHelper.GetRegion<object, object>(QueryRegionNames[3]);

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
      Util.Log("Object type is pdx = " + m_usePdxObjects);

      Util.Log("SetSize {0}, NumSets {1}.", qh.PortfolioSetSize,
        qh.PortfolioNumSets);

      if (!usePdxObject)
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
        Serializable.RegisterPdxType(PortfolioPdx.CreateDeserializable);
        Serializable.RegisterPdxType(PositionPdx.CreateDeserializable);
        qh.PopulatePortfolioPdxData(region0, qh.PortfolioSetSize,
         qh.PortfolioNumSets);
        qh.PopulatePortfolioPdxData(subRegion0, qh.PortfolioSetSize,
          qh.PortfolioNumSets);
        qh.PopulatePortfolioPdxData(region1, qh.PortfolioSetSize,
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

      qh.PopulatePortfolioData(region0, 100, 20, 100);
      qh.PopulatePositionData(subRegion0, 100, 20);
    }

    public void StepOneQE(string locators)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(QERegionName, true, true,
        null, locators, "__TESTPOOL1_", true);
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      Portfolio p1 = new Portfolio(1, 100);
      Portfolio p2 = new Portfolio(2, 100);
      Portfolio p3 = new Portfolio(3, 100);
      Portfolio p4 = new Portfolio(4, 100);

      region["1"] = p1;
      region["2"] = p2;
      region["3"] = p3;
      region["4"] = p4;

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();
      CqAttributesFactory<object, object> cqFac = new CqAttributesFactory<object, object>();
      ICqListener<object, object> cqLstner = new MyCqListener<object, object>();
      cqFac.AddCqListener(cqLstner);
      CqAttributes<object, object> cqAttr = cqFac.Create();
      CqQuery<object, object> qry = qs.NewCq(CqName, "select * from /" + QERegionName + "  p where p.ID!=2", cqAttr, false);
      qry.Execute();
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete
      region["4"] = p1;
      region["3"] = p2;
      region["2"] = p3;
      region["1"] = p4;
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete

      qry = qs.GetCq(CqName);

      CqServiceStatistics cqSvcStats = qs.GetCqStatistics();
      Assert.AreEqual(1, cqSvcStats.numCqsActive());
      Assert.AreEqual(1, cqSvcStats.numCqsCreated());
      Assert.AreEqual(1, cqSvcStats.numCqsOnClient());

      cqAttr = qry.GetCqAttributes();
      ICqListener<object, object>[] vl = cqAttr.getCqListeners();
      Assert.IsNotNull(vl);
      Assert.AreEqual(1, vl.Length);
      cqLstner = vl[0];
      Assert.IsNotNull(cqLstner);
      MyCqListener<object, object> myLisner = (MyCqListener<object, object>)cqLstner;// as MyCqListener<object, object>;
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
      region.GetLocalView().DestroyRegion();
    }

    public void StepOnePdxQE(string locators)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(QERegionName, true, true,
      null, locators, "__TESTPOOL1_", true);
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      PortfolioPdx p1 = new PortfolioPdx(1, 100);
      PortfolioPdx p2 = new PortfolioPdx(2, 100);
      PortfolioPdx p3 = new PortfolioPdx(3, 100);
      PortfolioPdx p4 = new PortfolioPdx(4, 100);

      region["1"] = p1;
      region["2"] = p2;
      region["3"] = p3;
      region["4"] = p4;

      QueryService<object, object> qs = null;

      qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();
      CqAttributesFactory<object, object> cqFac = new CqAttributesFactory<object, object>();
      ICqListener<object, object> cqLstner = new MyCqListener<object, object>();
      cqFac.AddCqListener(cqLstner);
      CqAttributes<object, object> cqAttr = cqFac.Create();
      CqQuery<object, object> qry = qs.NewCq(CqName, "select * from /" + QERegionName + "  p where p.ID!=2", cqAttr, false);
      qry.Execute();
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete
      region["4"] = p1;
      region["3"] = p2;
      region["2"] = p3;
      region["1"] = p4;
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete

      qry = qs.GetCq(CqName);

      CqServiceStatistics cqSvcStats = qs.GetCqStatistics();
      Assert.AreEqual(1, cqSvcStats.numCqsActive());
      Assert.AreEqual(1, cqSvcStats.numCqsCreated());
      Assert.AreEqual(1, cqSvcStats.numCqsOnClient());

      cqAttr = qry.GetCqAttributes();
      ICqListener<object, object>[] vl = cqAttr.getCqListeners();
      Assert.IsNotNull(vl);
      Assert.AreEqual(1, vl.Length);
      cqLstner = vl[0];
      Assert.IsNotNull(cqLstner);
      MyCqListener<object, object> myLisner = (MyCqListener<object, object>)cqLstner;// as MyCqListener<object, object>;
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
      region.GetLocalView().DestroyRegion();
    }
    public void KillServer()
    {
      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
    }

    public delegate void KillServerDelegate();

    /*
    public void StepOneFailover()
    {
      // This is here so that Client1 registers information of the cacheserver
      // that has been already started
      CacheHelper.SetupJavaServers("remotequery.xml",
        "cqqueryfailover.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      Util.Log("Cacheserver 1 started.");

      CacheHelper.CreateTCRegion(QueryRegionNames[0], true, true, null, true);

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
    */
    /*
    public void StepTwoFailover()
    {
      CacheHelper.StartJavaServer(2, "GFECS2");
      Util.Log("Cacheserver 2 started.");

      IAsyncResult killRes = null;
      KillServerDelegate ksd = new KillServerDelegate(KillServer);
      CacheHelper.CreateTCRegion(QueryRegionNames[0], true, true, null, true);

      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(QueryRegionNames[0]);

      QueryService qs = CacheHelper.DCache.GetQueryService();
      CqAttributesFactory cqFac = new CqAttributesFactory();
      ICqListener cqLstner = new MyCqListener();
      cqFac.AddCqListener(cqLstner);
      CqAttributes cqAttr = cqFac.Create();
      CqQuery qry = qs.NewCq(CqName1, "select * from /" + QERegionName + "  p where p.ID!<4", cqAttr, true);
      qry.Execute();
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete
      qry = qs.GetCq(CqName1);
      cqAttr = qry.GetCqAttributes();
      ICqListener[] vl = cqAttr.getCqListeners();
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
      vl = cqAttr.getCqListeners();
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
    */

    public void ProcessCQ(string locators)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(QERegionName, true, true,
      null, locators, "__TESTPOOL1_", true);

      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(QERegionName);
      Portfolio p1 = new Portfolio(1, 100);
      Portfolio p2 = new Portfolio(2, 100);
      Portfolio p3 = new Portfolio(3, 100);
      Portfolio p4 = new Portfolio(4, 100);

      region["1"] = p1;
      region["2"] = p2;
      region["3"] = p3;
      region["4"] = p4;

      QueryService<object, object> qs = null;

      qs = PoolManager.Find("__TESTPOOL1_").GetQueryService<object, object>();
      
      CqAttributesFactory<object, object> cqFac = new CqAttributesFactory<object, object>();
      ICqListener<object, object> cqLstner = new MyCqListener<object, object>();
      ICqStatusListener<object, object> cqStatusLstner = new MyCqStatusListener<object, object>(1);

      ICqListener<object, object>[] v = new ICqListener<object, object>[2];
      cqFac.AddCqListener(cqLstner);
      v[0] = cqLstner;
      v[1] = cqStatusLstner;
      cqFac.InitCqListeners(v);
      Util.Log("InitCqListeners called");
      CqAttributes<object, object> cqAttr = cqFac.Create();
      CqQuery<object, object> qry1 = qs.NewCq("CQ1", "select * from /" + QERegionName + "  p where p.ID >= 1", cqAttr, false);
      qry1.Execute();

      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete
      region["4"] = p1;
      region["3"] = p2;
      region["2"] = p3;
      region["1"] = p4;
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete

      qry1 = qs.GetCq("CQ1");
      cqAttr = qry1.GetCqAttributes();
      ICqListener<object, object>[] vl = cqAttr.getCqListeners();
      Assert.IsNotNull(vl);
      Assert.AreEqual(2, vl.Length);
      cqLstner = vl[0];
      Assert.IsNotNull(cqLstner);
      MyCqListener<object, object> myLisner = (MyCqListener<object, object>)cqLstner;// as MyCqListener<object, object>;
      Util.Log("event count:{0}, error count {1}.", myLisner.getEventCountBefore(), myLisner.getErrorCountBefore());
      Assert.AreEqual(4, myLisner.getEventCountBefore());

      cqStatusLstner = (ICqStatusListener<object, object>)vl[1];
      Assert.IsNotNull(cqStatusLstner);
      MyCqStatusListener<object, object> myStatLisner = (MyCqStatusListener<object, object>)cqStatusLstner;// as MyCqStatusListener<object, object>;
      Util.Log("event count:{0}, error count {1}.", myStatLisner.getEventCountBefore(), myStatLisner.getErrorCountBefore());
      Assert.AreEqual(1, myStatLisner.getCqConnectedCount());
      Assert.AreEqual(4, myStatLisner.getEventCountBefore());

      CqAttributesMutator<object, object> mutator = qry1.GetCqAttributesMutator();
      mutator.RemoveCqListener(cqLstner);
      cqAttr = qry1.GetCqAttributes();
      Util.Log("cqAttr.getCqListeners().Length = {0}", cqAttr.getCqListeners().Length);
      Assert.AreEqual(1, cqAttr.getCqListeners().Length);

      mutator.RemoveCqListener(cqStatusLstner);
      cqAttr = qry1.GetCqAttributes();
      Util.Log("1 cqAttr.getCqListeners().Length = {0}", cqAttr.getCqListeners().Length);
      Assert.AreEqual(0, cqAttr.getCqListeners().Length);
      
      ICqListener<object, object>[] v2 = new ICqListener<object, object>[2];
      v2[0] = cqLstner;
      v2[1] = cqStatusLstner;
      MyCqListener<object, object> myLisner2 = (MyCqListener<object, object>)cqLstner;
      myLisner2.Clear();
      MyCqStatusListener<object, object> myStatLisner2 = (MyCqStatusListener<object, object>)cqStatusLstner;
      myStatLisner2.Clear();
      mutator.SetCqListeners(v2);
      cqAttr = qry1.GetCqAttributes();
      Assert.AreEqual(2, cqAttr.getCqListeners().Length);

      region["4"] = p1;
      region["3"] = p2;
      region["2"] = p3;
      region["1"] = p4;
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete

      qry1 = qs.GetCq("CQ1");
      cqAttr = qry1.GetCqAttributes();
      ICqListener<object, object>[] v3 = cqAttr.getCqListeners();
      Assert.IsNotNull(v3);
      Assert.AreEqual(2, vl.Length);
      cqLstner = v3[0];
      Assert.IsNotNull(cqLstner);
      myLisner2 = (MyCqListener<object, object>)cqLstner;// as MyCqListener<object, object>;
      Util.Log("event count:{0}, error count {1}.", myLisner2.getEventCountBefore(), myLisner2.getErrorCountBefore());
      Assert.AreEqual(4, myLisner2.getEventCountBefore());

      cqStatusLstner = (ICqStatusListener<object, object>)v3[1];
      Assert.IsNotNull(cqStatusLstner);
      myStatLisner2 = (MyCqStatusListener<object, object>)cqStatusLstner;// as MyCqStatusListener<object, object>;
      Util.Log("event count:{0}, error count {1}.", myStatLisner2.getEventCountBefore(), myStatLisner2.getErrorCountBefore());
      Assert.AreEqual(0, myStatLisner2.getCqConnectedCount());
      Assert.AreEqual(4, myStatLisner2.getEventCountBefore());

      mutator = qry1.GetCqAttributesMutator();
      mutator.RemoveCqListener(cqLstner);
      cqAttr = qry1.GetCqAttributes();
      Util.Log("cqAttr.getCqListeners().Length = {0}", cqAttr.getCqListeners().Length);
      Assert.AreEqual(1, cqAttr.getCqListeners().Length);

      mutator.RemoveCqListener(cqStatusLstner);
      cqAttr = qry1.GetCqAttributes();
      Util.Log("1 cqAttr.getCqListeners().Length = {0}", cqAttr.getCqListeners().Length);
      Assert.AreEqual(0, cqAttr.getCqListeners().Length);

      region["4"] = p1;
      region["3"] = p2;
      region["2"] = p3;
      region["1"] = p4;
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete

      qry1 = qs.GetCq("CQ1");
      cqAttr = qry1.GetCqAttributes();
      ICqListener<object, object>[] v4 = cqAttr.getCqListeners();      
      Assert.IsNotNull(v4);      
      Assert.AreEqual(0, v4.Length);
      Util.Log("cqAttr.getCqListeners() done");
    }

    public void CreateAndExecuteCQ_StatusListener(string poolName, string cqName, string cqQuery, int id)
    {
      QueryService<object, object> qs = null;
      qs = PoolManager.Find(poolName).GetQueryService<object, object>();
      CqAttributesFactory<object, object> cqFac = new CqAttributesFactory<object, object>();
      cqFac.AddCqListener(new MyCqStatusListener<object, object>(id));
      CqAttributes<object, object> cqAttr = cqFac.Create();
      CqQuery<object, object> qry = qs.NewCq(cqName, cqQuery, cqAttr, false);
      qry.Execute();
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete
    }

    public void CreateAndExecuteCQ_Listener(string poolName, string cqName, string cqQuery, int id)
    {
      QueryService<object, object> qs = null;
      qs = PoolManager.Find(poolName).GetQueryService<object, object>();
      CqAttributesFactory<object, object> cqFac = new CqAttributesFactory<object, object>();
      cqFac.AddCqListener(new MyCqListener<object, object>(/*id*/));
      CqAttributes<object, object> cqAttr = cqFac.Create();
      CqQuery<object, object> qry = qs.NewCq(cqName, cqQuery, cqAttr, false);
      qry.Execute();
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete
    }

    public void CheckCQStatusOnConnect(string poolName, string cqName, int onCqStatusConnect)
    {      
      QueryService<object, object> qs = null;
      qs = PoolManager.Find(poolName).GetQueryService<object, object>();
      CqQuery<object, object> query = qs.GetCq(cqName);
      CqAttributes<object, object> cqAttr = query.GetCqAttributes();
      ICqListener<object, object>[] vl = cqAttr.getCqListeners();
      MyCqStatusListener<object, object> myCqStatusLstr = (MyCqStatusListener<object, object>) vl[0];
      Util.Log("CheckCQStatusOnConnect = {0} ", myCqStatusLstr.getCqConnectedCount());
      Assert.AreEqual(onCqStatusConnect, myCqStatusLstr.getCqConnectedCount());
    }

    public void CheckCQStatusOnDisConnect(string poolName, string cqName, int onCqStatusDisConnect)
    {
      QueryService<object, object> qs = null;
      qs = PoolManager.Find(poolName).GetQueryService<object, object>();
      CqQuery<object, object> query = qs.GetCq(cqName);
      CqAttributes<object, object> cqAttr = query.GetCqAttributes();
      ICqListener<object, object>[] vl = cqAttr.getCqListeners();
      MyCqStatusListener<object, object> myCqStatusLstr = (MyCqStatusListener<object, object>)vl[0];
      Util.Log("CheckCQStatusOnDisConnect = {0} ", myCqStatusLstr.getCqDisConnectedCount());
      Assert.AreEqual(onCqStatusDisConnect, myCqStatusLstr.getCqDisConnectedCount());
    }

    public void PutEntries(string regionName)
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(regionName);
      for (int i = 1; i <= 10; i++) {
        region["key-" + i] = "val-" + i;
      }
      Thread.Sleep(18000); // sleep 0.3min to allow server c query to complete
    }

    public void CheckCQStatusOnPutEvent(string poolName, string cqName, int onCreateCount)
    {
      QueryService<object, object> qs = null;
      qs = PoolManager.Find(poolName).GetQueryService<object, object>();
      CqQuery<object, object> query = qs.GetCq(cqName);
      CqAttributes<object, object> cqAttr = query.GetCqAttributes();
      ICqListener<object, object>[] vl = cqAttr.getCqListeners();
      MyCqStatusListener<object, object> myCqStatusLstr = (MyCqStatusListener<object, object>)vl[0];
      Util.Log("CheckCQStatusOnPutEvent = {0} ", myCqStatusLstr.getEventCountBefore());
      Assert.AreEqual(onCreateCount, myCqStatusLstr.getEventCountBefore());
    }

    public void CreateRegion(string locators, string servergroup, string regionName, string poolName)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(regionName, true, true,
        null, locators, poolName, true, servergroup);
    }

    void runCqQueryTest()
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOne, CacheHelper.Locators);
      Util.Log("StepOne complete.");

      m_client1.Call(StepTwo, m_usePdxObjects);
      Util.Log("StepTwo complete.");

      if (!m_usePdxObjects)
        m_client1.Call(StepOneQE, CacheHelper.Locators);
      else
        m_client1.Call(StepOnePdxQE, CacheHelper.Locators);
      Util.Log("StepOne complete.");

      m_client1.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    void runCqQueryStatusTest()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver.xml", "cacheserver2.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(StepOne, CacheHelper.Locators);
      Util.Log("StepOne complete.");

      m_client1.Call(CreateAndExecuteCQ_StatusListener, "__TESTPOOL1_", CqName1, CqQuery1, 100);
      Util.Log("CreateAndExecuteCQ complete.");

      m_client1.Call(CheckCQStatusOnConnect, "__TESTPOOL1_", CqName1, 1);
      Util.Log("CheckCQStatusOnConnect complete.");

      m_client1.Call(PutEntries, "DistRegionAck");
      Util.Log("PutEntries complete.");

      m_client1.Call(CheckCQStatusOnPutEvent, "__TESTPOOL1_", CqName1, 10);
      Util.Log("CheckCQStatusOnPutEvent complete.");

      CacheHelper.SetupJavaServers(true, "cacheserver.xml", "cacheserver2.xml");
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("start server 2 complete.");

      Thread.Sleep(20000);
      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
      Thread.Sleep(20000);
      m_client1.Call(CheckCQStatusOnDisConnect, "__TESTPOOL1_", CqName1, 0);
      Util.Log("CheckCQStatusOnDisConnect complete.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");
      Thread.Sleep(20000);
      m_client1.Call(CheckCQStatusOnDisConnect, "__TESTPOOL1_", CqName1, 1);
      Util.Log("CheckCQStatusOnDisConnect complete.");

      CacheHelper.SetupJavaServers(true, "cacheserver.xml", "cacheserver2.xml");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");
      Thread.Sleep(20000);

      m_client1.Call(CheckCQStatusOnConnect, "__TESTPOOL1_", CqName1, 2);
      Util.Log("CheckCQStatusOnConnect complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
      Thread.Sleep(20000);

      m_client1.Call(CheckCQStatusOnDisConnect, "__TESTPOOL1_", CqName1, 2);
      Util.Log("CheckCQStatusOnDisConnect complete.");

      m_client1.Call(Close);

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    void runCqQueryStatusTest2()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_servergroup.xml", "cacheserver_servergroup2.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("start server 1 complete.");
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("start server 2 complete.");

      m_client1.Call(CreateRegion, CacheHelper.Locators, "group1", "DistRegionAck", "__TESTPOOL1_");
      Util.Log("CreateRegion DistRegionAck complete.");

      m_client1.Call(CreateRegion, CacheHelper.Locators, "group2", "DistRegionAck1", "__TESTPOOL2_");
      Util.Log("CreateRegion DistRegionAck1 complete.");

      m_client1.Call(CreateAndExecuteCQ_StatusListener, "__TESTPOOL1_", CqName1, CqQuery1, 100);
      Util.Log("CreateAndExecuteCQ1 complete.");

      m_client1.Call(CreateAndExecuteCQ_StatusListener, "__TESTPOOL2_", CqName2, CqQuery2, 101);
      Util.Log("CreateAndExecuteCQ2 complete.");

      m_client1.Call(CheckCQStatusOnConnect, "__TESTPOOL1_", CqName1, 1);
      Util.Log("CheckCQStatusOnConnect1 complete.");

      m_client1.Call(CheckCQStatusOnConnect, "__TESTPOOL2_", CqName2, 1);
      Util.Log("CheckCQStatusOnConnect2 complete.");

      m_client1.Call(PutEntries, "DistRegionAck");
      Util.Log("PutEntries1 complete.");

      m_client1.Call(PutEntries, "DistRegionAck1");
      Util.Log("PutEntries2 complete.");

      m_client1.Call(CheckCQStatusOnPutEvent, "__TESTPOOL1_", CqName1, 10);
      Util.Log("CheckCQStatusOnPutEvent complete.");
      
      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
      Thread.Sleep(20000);

      m_client1.Call(CheckCQStatusOnDisConnect, "__TESTPOOL1_", CqName1, 1);
      Util.Log("CheckCQStatusOnDisConnect complete.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");
      Thread.Sleep(20000);

      m_client1.Call(CheckCQStatusOnDisConnect, "__TESTPOOL2_", CqName2, 1);
      Util.Log("CheckCQStatusOnDisConnect complete.");

      m_client1.Call(Close);

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    void runCqQueryStatusTest3()
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(ProcessCQ, CacheHelper.Locators);
      Util.Log("ProcessCQ complete.");

      m_client1.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }

    [Test]
    public void CqQueryTest()
    {
      runCqQueryTest();
    }

    [Test]
    public void CqQueryPdxTest()
    {
      m_usePdxObjects = true;
      runCqQueryTest();
      m_usePdxObjects = false;
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

    [Test]
    public void CqQueryStatusTest()
    {
      runCqQueryStatusTest();
    }

    [Test]
    public void CqQueryStatusTest2()
    {
      runCqQueryStatusTest2();
    }

    [Test]
    public void CqQueryStatusTest3()
    {
      runCqQueryStatusTest3();
    }

  }
}
