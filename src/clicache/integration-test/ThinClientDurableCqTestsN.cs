using NUnit.Framework;
using GemStone.GemFire.DUnitFramework;
using GemStone.GemFire.Cache.Tests.NewAPI;
using GemStone.GemFire.Cache.Generic;
using System;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{

  [TestFixture]
  [Category("group3")]
  [Category("unicast_only")]
  [Category("generics")]
  public class ThinClientDurableCqTests : ThinClientRegionSteps
  {
    #region Private Members
    private UnitProcess m_client1 = null;
    private UnitProcess m_client2 = null;
    private string[] m_client1DurableCqNames = { "client1DurableCQ1", "client1DurableCQ2", "client1DurableCQ3", "client1DurableCQ4", "client1DurableCQ5", "client1DurableCQ6", "client1DurableCQ7", "client1DurableCQ8" };
    private string[] m_client2DurableCqNames = { "client2DurableCQ1", "client2DurableCQ2", "client2DurableCQ3", "client2DurableCQ4", "client2DurableCQ5", "client2DurableCQ6", "client2DurableCQ7", "client2DurableCQ8" };
    private static string[] QueryRegionNames = { "ListDurableCqs" };
    private static int m_NumberOfCqs = 110;
    #endregion

    #region Test helper methods

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
    }

    public void InitDurableClient(string locators, int redundancyLevel,
     string durableClientId, int durableTimeout)
    {
      CacheHelper.InitConfigForDurable_Pool(locators, redundancyLevel, durableClientId, durableTimeout);
      CacheHelper.CreateTCRegion_Pool(QueryRegionNames[0], true, true, (ICacheListener<object, object>)null, CacheHelper.Locators, "__TESTPOOL1_", true);
    }


    public void RegisterCqsClient1(bool isRecycle)
    {
      Util.Log("Registering Cqs for client1.");
      CqAttributesFactory<object, object> cqAf = new CqAttributesFactory<object, object>();
      CqAttributes<object, object> attributes = cqAf.Create();
      QueryService<object, object> qs = Generic.PoolManager.Find("__TESTPOOL1_").GetQueryService<object, object>();

      if (!isRecycle)
      {
        qs.NewCq(m_client1DurableCqNames[0], "Select * From /" + QueryRegionNames[0] + " where id = 1", attributes, true).ExecuteWithInitialResults();
        qs.NewCq(m_client1DurableCqNames[1], "Select * From /" + QueryRegionNames[0] + " where id = 10", attributes, true).ExecuteWithInitialResults();
        qs.NewCq(m_client1DurableCqNames[2], "Select * From /" + QueryRegionNames[0], attributes, false).ExecuteWithInitialResults();
        qs.NewCq(m_client1DurableCqNames[3], "Select * From /" + QueryRegionNames[0] + " where id = 3", attributes, false).ExecuteWithInitialResults();
      }
      else
      {
        qs.NewCq(m_client1DurableCqNames[4], "Select * From /" + QueryRegionNames[0] + " where id = 1", attributes, true).ExecuteWithInitialResults();
        qs.NewCq(m_client1DurableCqNames[5], "Select * From /" + QueryRegionNames[0] + " where id = 10", attributes, true).ExecuteWithInitialResults();
        qs.NewCq(m_client1DurableCqNames[6], "Select * From /" + QueryRegionNames[0], attributes, false).ExecuteWithInitialResults();
        qs.NewCq(m_client1DurableCqNames[7], "Select * From /" + QueryRegionNames[0] + " where id = 3", attributes, false).ExecuteWithInitialResults();
      }

    }

    public void RegisterCqsClient1MultipleChunks()
    {
      Util.Log("Registering Cqs for client1 for multiple chunks.");
      CqAttributesFactory<object, object> cqAf = new CqAttributesFactory<object, object>();
      CqAttributes<object, object> attributes = cqAf.Create();
      QueryService<object, object> qs = Generic.PoolManager.Find("__TESTPOOL1_").GetQueryService<object, object>();

      for (int i = 0; i < m_NumberOfCqs; i++)
        qs.NewCq("MyCq_" + i.ToString(), "Select * From /" + QueryRegionNames[0] + " where id = 1", attributes, true).ExecuteWithInitialResults();

    }

    public void RegisterCqsClient2(bool isRecycle)
    {
      Util.Log("Registering Cqs for client2.");
      CqAttributesFactory<object, object> cqAf = new CqAttributesFactory<object, object>();
      CqAttributes<object, object> attributes = cqAf.Create();
      QueryService<object, object> qs = Generic.PoolManager.Find("__TESTPOOL1_").GetQueryService<object, object>();

      if (!isRecycle)
      {
        qs.NewCq(m_client2DurableCqNames[0], "Select * From /" + QueryRegionNames[0] + " where id = 1", attributes, true).ExecuteWithInitialResults();
        qs.NewCq(m_client2DurableCqNames[1], "Select * From /" + QueryRegionNames[0] + " where id = 10", attributes, true).ExecuteWithInitialResults();
        qs.NewCq(m_client2DurableCqNames[2], "Select * From /" + QueryRegionNames[0], attributes, true).ExecuteWithInitialResults();
        qs.NewCq(m_client2DurableCqNames[3], "Select * From /" + QueryRegionNames[0] + " where id = 3", attributes, true).ExecuteWithInitialResults();
      }
      else
      {
        qs.NewCq(m_client2DurableCqNames[4], "Select * From /" + QueryRegionNames[0] + " where id = 1", attributes, true).ExecuteWithInitialResults();
        qs.NewCq(m_client2DurableCqNames[5], "Select * From /" + QueryRegionNames[0] + " where id = 10", attributes, true).ExecuteWithInitialResults();
        qs.NewCq(m_client2DurableCqNames[6], "Select * From /" + QueryRegionNames[0], attributes, true).ExecuteWithInitialResults();
        qs.NewCq(m_client2DurableCqNames[7], "Select * From /" + QueryRegionNames[0] + " where id = 3", attributes, true).ExecuteWithInitialResults();
      }
    }

    public void VerifyDurableCqListClient1MultipleChunks()
    {
      Util.Log("Verifying durable Cqs for client1.");
      QueryService<object, object> qs = Generic.PoolManager.Find("__TESTPOOL1_").GetQueryService<object, object>();
      System.Collections.Generic.List<string> durableCqList = qs.GetAllDurableCqsFromServer();
      Assert.AreNotEqual(null, durableCqList);

      Assert.AreEqual(m_NumberOfCqs, durableCqList.Count, "Durable CQ count sholuld be %d", m_NumberOfCqs);

      Util.Log("Completed verifying durable Cqs for client1.");
    }

    public void VerifyDurableCqListClient1(bool isRecycle)
    {
      Util.Log("Verifying durable Cqs for client1.");
      QueryService<object, object> qs = Generic.PoolManager.Find("__TESTPOOL1_").GetQueryService<object, object>();
      System.Collections.Generic.List<string> durableCqList = qs.GetAllDurableCqsFromServer();
      Assert.AreNotEqual(null, durableCqList);

      if (!isRecycle)
      {
        Assert.AreEqual(2, durableCqList.Count, "Durable CQ count sholuld be 2");
        Assert.AreEqual(true, durableCqList.Contains(m_client1DurableCqNames[0]));
        Assert.AreEqual(true, durableCqList.Contains(m_client1DurableCqNames[1]));
      }
      else
      {
        Assert.AreEqual(4, durableCqList.Count, "Durable CQ count sholuld be 4");
        Assert.AreEqual(true, durableCqList.Contains(m_client1DurableCqNames[0]));
        Assert.AreEqual(true, durableCqList.Contains(m_client1DurableCqNames[1]));
        Assert.AreEqual(true, durableCqList.Contains(m_client1DurableCqNames[4]));
        Assert.AreEqual(true, durableCqList.Contains(m_client1DurableCqNames[5]));
      }
      Util.Log("Completed verifying durable Cqs for client1.");
    }

    public void VerifyDurableCqListClient2(bool isRecycle)
    {
      Util.Log("Verifying durable Cqs for client2.");
      QueryService<object, object> qs = Generic.PoolManager.Find("__TESTPOOL1_").GetQueryService<object, object>();
      System.Collections.Generic.List<string> durableCqList = qs.GetAllDurableCqsFromServer();
      Assert.AreNotEqual(null, durableCqList);

      if (!isRecycle)
      {
        Assert.AreEqual(4, durableCqList.Count, "Durable CQ count sholuld be 4");
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[0]));
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[1]));
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[2]));
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[3]));
      }
      else
      {
        Assert.AreEqual(8, durableCqList.Count, "Durable CQ count sholuld be 8");
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[0]));
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[1]));
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[2]));
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[3]));
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[4]));
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[5]));
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[6]));
        Assert.AreEqual(true, durableCqList.Contains(m_client2DurableCqNames[7]));
      }
    }

    public void VerifyEmptyDurableCqListClient1()
    {
      Util.Log("Verifying empty durable Cqs for client1.");
      QueryService<object, object> qs = Generic.PoolManager.Find("__TESTPOOL1_").GetQueryService<object, object>();
      System.Collections.Generic.List<string> durableCqList = qs.GetAllDurableCqsFromServer();
      Assert.AreNotEqual(null, durableCqList);
      Assert.AreEqual(0, durableCqList.Count, "Durable CQ list sholuld be empty");
    }


    private void RunTestGetDurableCqsFromServer()
    {
      try
      {
        CacheHelper.SetupJavaServers(true, "cacheserverDurableCqs.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cache server 1 started");

        m_client1.Call(InitDurableClient, CacheHelper.Locators, 0, "DurableClient1", 300);
        m_client2.Call(InitDurableClient, CacheHelper.Locators, 0, "DurableClient2", 300);
        Util.Log("client initialization done.");

        m_client1.Call(RegisterCqsClient1, false);
        m_client2.Call(RegisterCqsClient2, false);
        Util.Log("Registered DurableCQs.");

        m_client1.Call(VerifyDurableCqListClient1, false);
        m_client2.Call(VerifyDurableCqListClient2, false);

        Util.Log("Verified DurableCQ List.");
      }
      finally
      {
        m_client1.Call(CacheHelper.Close);
        m_client2.Call(CacheHelper.Close);
        CacheHelper.StopJavaServer(1);
        CacheHelper.StopJavaLocator(1);
      }

    }

    private void RunTestGetDurableCqsFromServerCyclicClients()
    {
      try
      {
        CacheHelper.SetupJavaServers(true, "cacheserverDurableCqs.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cache server 1 started");

        m_client1.Call(InitDurableClient, CacheHelper.Locators, 0, "DurableClient1", 300);
        m_client2.Call(InitDurableClient, CacheHelper.Locators, 0, "DurableClient2", 300);
        Util.Log("client initialization done.");

        m_client1.Call(RegisterCqsClient1, false);
        m_client2.Call(RegisterCqsClient2, false);
        Util.Log("Registered DurableCQs.");

        m_client1.Call(VerifyDurableCqListClient1, false);
        m_client1.Call(VerifyDurableCqListClient1, false);
        Util.Log("Verified DurableCQ List.");


        m_client1.Call(CacheHelper.CloseKeepAlive);
        m_client2.Call(CacheHelper.CloseKeepAlive);


        m_client1.Call(InitDurableClient, CacheHelper.Locators, 0, "DurableClient1", 300);
        m_client2.Call(InitDurableClient, CacheHelper.Locators, 0, "DurableClient2", 300);
        Util.Log("client re-initialization done.");

        m_client1.Call(RegisterCqsClient1, true);
        m_client2.Call(RegisterCqsClient2, true);
        Util.Log("Registered DurableCQs.");

        m_client1.Call(VerifyDurableCqListClient1, true);
        m_client1.Call(VerifyDurableCqListClient1, true);

        Util.Log("Verified DurableCQ List.");
      }
      finally
      {
        m_client1.Call(CacheHelper.Close);
        m_client2.Call(CacheHelper.Close);

        CacheHelper.StopJavaServer(1);
        CacheHelper.StopJavaLocator(1);
      }
    }

    [TestFixtureSetUp]
    public override void InitTests()
    {
      base.InitTests();
    }

    [TestFixtureTearDown]
    public override void EndTests()
    {
      m_client1.Exit();
      m_client2.Exit();
      base.EndTests();
    }

    [SetUp]
    public override void InitTest()
    {
      base.InitTest();
    }

    [TearDown]
    public override void EndTest()
    {
      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);
      base.EndTest();
    }


    #endregion

    #region Tests

    [Test]
    public void TestGetDurableCqsFromServerWithLocator()
    {
      RunTestGetDurableCqsFromServer();
    }

    [Test]
    public void TestGetDurableCqsFromServerCyclicClientsWithLocator()
    {
      RunTestGetDurableCqsFromServerCyclicClients();
    }

    #endregion


  }
}
