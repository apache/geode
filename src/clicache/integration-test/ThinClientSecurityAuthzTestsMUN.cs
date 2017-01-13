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

  public class MyCqListener2<TKey, TResult> : ICqListener<TKey, TResult>
  {
    private int m_create = 0;
    private int m_update = 0;
    private int m_id = 0;

    public MyCqListener2(int id)
    {
      this.m_id = id;
    }

    public int Creates
    {
      get { return m_create; }
    }

    public int Updates
    {
      get { return m_update; }
    }

    #region ICqListener Members

    void ICqListener<TKey, TResult>.Close()
    {
      Util.Log("CqListener closed  with ID = " + m_id);
    }

    void ICqListener<TKey, TResult>.OnError(CqEvent<TKey, TResult> ev)
    {
      Util.Log("CqListener OnError called ");
    }

    void ICqListener<TKey, TResult>.OnEvent(CqEvent<TKey, TResult> ev)
    {
      Util.Log("CqListener OnEvent ops = " + ev.getBaseOperation());
      if (ev.getBaseOperation() == CqOperationType.OP_TYPE_CREATE)
        m_create++;
      else if (ev.getBaseOperation() == CqOperationType.OP_TYPE_UPDATE)
        m_update++;
    }

    #endregion
  }

  [TestFixture]
  [Category("group4")]
  [Category("unicast_only")]
  [Category("generics")]
  public class ThinClientSecurityAuthzTestsMU : ThinClientSecurityAuthzTestBase
  {
    #region Private members
    IRegion<object, object> region;
    IRegion<object, object> region1;
    private UnitProcess m_client1;
    private UnitProcess m_client2;
    private UnitProcess m_client3;
    private TallyListener<object, object> m_listener;
    private TallyWriter<object, object> m_writer;
    private string/*<object>*/ keys = "Key";
    private string value = "Value";

      
    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3 };
    }

    public void CreateRegion(string locators, bool caching,
      bool listener, bool writer)
    {
      Util.Log(" in CreateRegion " + listener + " : " + writer);
      if (listener)
      {
        m_listener = new TallyListener<object, object>();
      }
      else
      {
        m_listener = null;
      }
      IRegion<object, object> region = null;
      region = CacheHelper.CreateTCRegion_Pool<object, object>(RegionName, true, caching,
        m_listener, locators, "__TESTPOOL1_", true);
      
      if (writer)
      {
        m_writer = new TallyWriter<object, object>();
      }
      else
      {
        m_writer = null;
      }
      Util.Log("region created  ");
      AttributesMutator<object, object> at = region.AttributesMutator;
      at.SetCacheWriter(m_writer);
    }

    public void DoPut()
    {
      region = CacheHelper.GetRegion<object, object>(RegionName);
      region[keys.ToString()] = value;
    }

    public void CheckAssert()
    {
      Assert.AreEqual(true, m_writer.IsWriterInvoked, "Writer Should be invoked");
      Assert.AreEqual(false, m_listener.IsListenerInvoked, "Listener Should not be invoked");
      Assert.IsFalse(region.ContainsKey(keys.ToString()), "Key should have been found in the region");
    }

    public void DoLocalPut()
    {
      region1 = CacheHelper.GetRegion<object, object>(RegionName);
      region1.GetLocalView()[m_keys[2]] = m_vals[2];
      //this check is no loger valid as ContainsKey goes on server
      //Assert.IsTrue(region1.ContainsKey(m_keys[2]), "Key should have been found in the region");
      Assert.AreEqual(true, m_writer.IsWriterInvoked, "Writer Should be invoked");
      Assert.AreEqual(true, m_listener.IsListenerInvoked, "Listener Should be invoked");

      //try Update
      try
      {
        Util.Log("Trying UpdateEntry");
        m_listener.ResetListenerInvokation();
        UpdateEntry(RegionName, m_keys[2], m_nvals[2], false);
        Assert.Fail("Should have got NotAuthorizedException during updateEntry");
      }
      catch (NotAuthorizedException)
      {
        Util.Log("NotAuthorizedException Caught");
        Util.Log("Success");
      }
      catch (Exception other)
      {
        Util.Log("Stack trace: {0} ", other.StackTrace);
        Util.Log("Got  exception : {0}", other.Message);
      }
      Assert.AreEqual(true, m_writer.IsWriterInvoked, "Writer should be invoked");
      Assert.AreEqual(false, m_listener.IsListenerInvoked, "Listener should not be invoked");
      //Assert.IsTrue(region1.ContainsKey(m_keys[2]), "Key should have been found in the region");
      VerifyEntry(RegionName, m_keys[2], m_vals[2]);
      m_writer.SetWriterFailed();

      //test CacheWriter
      try
      {
        Util.Log("Testing CacheWriterException");
        UpdateEntry(RegionName, m_keys[2], m_nvals[2], false);
        Assert.Fail("Should have got NotAuthorizedException during updateEntry");
      }
      catch (CacheWriterException)
      {
        Util.Log("CacheWriterException Caught");
        Util.Log("Success");
      }
    
    }

    [TestFixtureTearDown]
    public override void EndTests()
    {
      CacheHelper.StopJavaServers();
      base.EndTests();
    }

    [TearDown]
    public override void EndTest()
    {
      try
      {
        if (m_clients != null)
        {
          foreach (ClientBase client in m_clients)
          {
            client.Call(CacheHelper.Close);
          }
        }
        CacheHelper.Close();
        CacheHelper.ClearEndpoints();
      }
      finally
      {
        CacheHelper.StopJavaServers();
      }
      base.EndTest();
    }

    protected const string RegionName_CQ = "Portfolios";
    static QueryService<object, object> gQueryService = null;

    static string [] QueryStrings = {
                        "select * from /Portfolios p where p.ID < 1",
                        "select * from /Portfolios p where p.ID < 2",
                        "select * from /Portfolios p where p.ID = 2",
                        "select * from /Portfolios p where p.ID >= 3",//this should pass
                        "select * from /Portfolios p where p.ID = 4",//this should pass
                        "select * from /Portfolios p where p.ID = 5",
                        "select * from /Portfolios p where p.ID = 6",
                        "select * from /Portfolios p where p.ID = 7"
                      };

    public void registerCQ(Properties<string, string> credentials, bool durableCQ)
    {
      Util.Log("registerCQ");

      try
      {
        Serializable.RegisterTypeGeneric(Portfolio.CreateDeserializable);
        Serializable.RegisterTypeGeneric(Position.CreateDeserializable);
        Util.Log("registerCQ portfolio registered");
      }
      catch (IllegalStateException)
      {
        Util.Log("registerCQ portfolio NOT registered");
        // ignore since we run multiple iterations for pool and non pool configs
      }

      // VJR: TODO fix cache.GetQueryService to also be generic
      gQueryService = CacheHelper.getMultiuserCache(credentials).GetQueryService<object, object>();

      for (int i = 0; i < QueryStrings.Length; i++)
      {
        CqAttributesFactory<object, object> cqAttrFact = new CqAttributesFactory<object, object>();
        cqAttrFact.AddCqListener(new MyCqListener2<object, object>(i));
        CqQuery<object, object> cq = gQueryService.NewCq("cq_" + i, QueryStrings[i], cqAttrFact.Create(), durableCQ);
        cq.Execute();
      }
      Util.Log("registerCQ Done.");
    }

    public void doCQPut(Properties<string, string> credentials)
    {
      Util.Log("doCQPut");

      try
      {
        Serializable.RegisterTypeGeneric(Portfolio.CreateDeserializable);
        Serializable.RegisterTypeGeneric(Position.CreateDeserializable);
        Util.Log("doCQPut portfolio registered");
      }
      catch (IllegalStateException)
      {
        Util.Log("doCQPut portfolio NOT registered");
        // ignore since we run multiple iterations for pool and non pool configs
      }
      //IRegion<object, object> region = CacheHelper.GetVerifyRegion(RegionName_CQ, credentials);

      IRegionService userRegionService = CacheHelper.getMultiuserCache(credentials);
      IRegion<object, object>[] regions = userRegionService.RootRegions<object, object>();

      IRegion<object, object> region = null;

      Console.Out.WriteLine("Number of regions " + regions.Length);
      for (int i = 0; i < regions.Length; i++)
      {
        if (regions[i].Name.Equals(RegionName_CQ))
        {
          region = regions[i];
          break;
        }
      }

        for (int i = 0; i < QueryStrings.Length; i++)
        {
          string key = "port1-" + i;

          Portfolio p = new Portfolio(i);

          region[key] = p;
        }

        IRegionService rgServ = region.RegionService;

        Cache cache = rgServ as Cache;

        Assert.IsNull(cache);

      Thread.Sleep(20000);
      Util.Log("doCQPut Done.");
    }

    public void verifyCQEvents(bool whetherResult, CqOperationType opType )
    {
      Util.Log("verifyCQEvents " + gQueryService);
      Assert.IsNotNull(gQueryService);

      CqQuery<object, object> cq = gQueryService.GetCq("cq_" + 3);
      ICqListener<object, object>[] cqL = cq.GetCqAttributes().getCqListeners();
      MyCqListener2<object, object> mcqL = (MyCqListener2<object, object>)cqL[0];

      Util.Log("got result for cq listener3 " + cq.Name + " : " + mcqL.Creates);

      if (opType == CqOperationType.OP_TYPE_CREATE)
      {
        if (whetherResult)
          Assert.AreEqual(1, mcqL.Creates, "CQ listener 3 should get one create event ");
        else
          Assert.AreEqual(0, mcqL.Creates, "CQ listener 3 should not get any create event ");
      }
      else if (opType == CqOperationType.OP_TYPE_UPDATE)
      {
        if (whetherResult)
          Assert.AreEqual(1, mcqL.Updates, "CQ listener 3 should get one update event ");
        else
          Assert.AreEqual(0, mcqL.Updates, "CQ listener 3 should not get any update event ");
      }

      cq = gQueryService.GetCq("cq_" + 4);
      cqL = cq.GetCqAttributes().getCqListeners();
      mcqL = (MyCqListener2<object, object>)cqL[0];

      Util.Log("got result for cq listener4 " + cq.Name + " : " + mcqL.Creates);

      if (opType == CqOperationType.OP_TYPE_CREATE)
      {
        if (whetherResult)
          Assert.AreEqual(1, mcqL.Creates, "CQ listener 4 should get one create event ");
        else
          Assert.AreEqual(0, mcqL.Creates, "CQ listener 4 should not get any create event ");
      }
      else if (opType == CqOperationType.OP_TYPE_UPDATE)
      {
        if (whetherResult)
          Assert.AreEqual(1, mcqL.Updates, "CQ listener 4 should get one update event ");
        else
          Assert.AreEqual(0, mcqL.Updates, "CQ listener 4 should not get any update event ");
      }

      cq = gQueryService.GetCq("cq_" + 0);
      cqL = cq.GetCqAttributes().getCqListeners();
      mcqL = (MyCqListener2<object, object>)cqL[0];

      Util.Log("got result for cq listener0 " + cq.Name + " : " + mcqL.Creates);

      Assert.AreEqual(0, mcqL.Creates, "CQ listener 0 should get one create event ");


    //  CacheHelper.getMultiuserCache(null).Close();

      gQueryService = null;
          
    }

    void runCQTest()
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");

      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");

      DummyAuthorization3 da = new DummyAuthorization3();

      string authenticator = da.Authenticator;
      string authInit = da.AuthInit;
      string accessorPP = da.AuthenticatorPP;

      Util.Log("testAllowPutsGets: Using authinit: " + authInit);
      Util.Log("testAllowPutsGets: Using authenticator: " + authenticator);
      Util.Log("testAllowPutsGets: Using accessorPP: " + accessorPP);

        // Start servers with all required properties
      string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
        null, accessorPP, null, null);

        // Start the two servers.
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs);
        Util.Log("Cacheserver 2 started.");

        // Start client1 with valid CREATE credentials, this index will be used to authorzie the user
        Properties<string, string> createCredentials = da.GetValidCredentials(4);

        Util.Log("runCQTest: ");
        m_client1.Call(SecurityTestUtil.CreateClientMU2, RegionName_CQ,
          CacheHelper.Locators, authInit, (Properties<string, string>)null, true, true);

        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName_CQ,
          CacheHelper.Locators, authInit, (Properties<string, string>)null, true);

        // Perform some put operations from client1
        m_client1.Call(registerCQ, createCredentials, false);
       
        // Verify that the gets succeed
        m_client2.Call(doCQPut, createCredentials);

        m_client1.Call(verifyCQEvents, true, CqOperationType.OP_TYPE_CREATE);

        m_client1.Call(CloseUserCache, false);
        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);
       // CacheHelper.StopJavaServer(2);
      

      CacheHelper.StopJavaLocator(1);

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    public void CloseUserCache(bool keepAlive)
    {
      Util.Log("CloseUserCache keepAlive: " + keepAlive);
      CacheHelper.CloseUserCache(keepAlive);
    }

    private static string DurableClientId1 = "DurableClientId1";
    //private static string DurableClientId2 = "DurableClientId2";

    void runDurableCQTest(bool logicalCacheClose, bool durableCQ, bool whetherResult)
    {
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");

      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");

      DummyAuthorization3 da = new DummyAuthorization3();

      string authenticator = da.Authenticator;
      string authInit = da.AuthInit;
      string accessorPP = da.AuthenticatorPP;

      Util.Log("testAllowPutsGets: Using authinit: " + authInit);
      Util.Log("testAllowPutsGets: Using authenticator: " + authenticator);
      Util.Log("testAllowPutsGets: Using accessorPP: " + accessorPP);

      // Start servers with all required properties
      string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
        null, accessorPP, null, null);

      // Start the two servers.
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
      Util.Log("Cacheserver 1 started.");
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs);
      Util.Log("Cacheserver 2 started.");

      // Start client1 with valid CREATE credentials, this index will be used to authorzie the user
      Properties<string, string> createCredentials = da.GetValidCredentials(4);

      Util.Log("runCQTest: ");
      /*
      regionName, string endpoints, string locators,
      authInit, Properties credentials, bool pool, bool locator, bool isMultiuser, bool notificationEnabled, string durableClientId)
       */
      m_client1.Call(SecurityTestUtil.CreateMUDurableClient, RegionName_CQ,
        CacheHelper.Locators, authInit, DurableClientId1, true, true );

      m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName_CQ,
       CacheHelper.Locators, authInit, (Properties<string, string>)null, true);

      m_client1.Call(ReadyForEvents2);

      // Perform some put operations from client1
      m_client1.Call(registerCQ, createCredentials, durableCQ);


      Properties<string, string> createCredentials2 = da.GetValidCredentials(3);
      // Verify that the gets succeed
      m_client2.Call(doCQPut, createCredentials2);

      //close cache client-1
      m_client1.Call(verifyCQEvents, true, CqOperationType.OP_TYPE_CREATE);

      Thread.Sleep(10000);

      Util.Log("Before calling CloseUserCache: " + logicalCacheClose);
      if (logicalCacheClose)
        m_client1.Call(CloseUserCache, logicalCacheClose);

      m_client1.Call(CloseKeepAlive);
      //put again from other client
      m_client2.Call(doCQPut, createCredentials2);

      //client-1 will up again
      m_client1.Call(SecurityTestUtil.CreateMUDurableClient, RegionName_CQ,
        CacheHelper.Locators, authInit, DurableClientId1, true, true);

      // Perform some put operations from client1
      m_client1.Call(registerCQ, createCredentials, durableCQ);

      m_client1.Call(ReadyForEvents2);
      Thread.Sleep(20000);
      m_client1.Call(verifyCQEvents, whetherResult, CqOperationType.OP_TYPE_UPDATE);


      m_client1.Call(Close);

      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      // CacheHelper.StopJavaServer(2);


      CacheHelper.StopJavaLocator(1);

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runAllowPutsGets()
    {
      CacheHelper.SetupJavaServers(true, CacheXml1, CacheXml2);

      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");

      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(true))
      {
        CredentialGenerator cGen = authzGen.GetCredentialGenerator();
        Properties<string, string> extraAuthProps = cGen.SystemProperties;
        Properties<string, string> javaProps = cGen.JavaProperties;
        Properties<string, string> extraAuthzProps = authzGen.SystemProperties;
        string authenticator = cGen.Authenticator;
        string authInit = cGen.AuthInit;
        string accessor = authzGen.AccessControl;

        Util.Log("testAllowPutsGets: Using authinit: " + authInit);
        Util.Log("testAllowPutsGets: Using authenticator: " + authenticator);
        Util.Log("testAllowPutsGets: Using accessor: " + accessor);

        // Start servers with all required properties
        string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
          accessor, null, SecurityTestUtil.ConcatProperties(extraAuthProps,
          extraAuthzProps), javaProps);

        // Start the two servers.
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs);
        Util.Log("Cacheserver 2 started.");

        // Start client1 with valid CREATE credentials
        Properties<string, string> createCredentials = authzGen.GetAllowedCredentials(
          new OperationCode[] { OperationCode.Put },
          new string[] { RegionName }, 1);
        javaProps = cGen.JavaProperties;
        Util.Log("AllowPutsGets: For first client PUT credentials: " +
          createCredentials);
        m_client1.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Locators, authInit, (Properties<string, string>)null, true);

        // Start client2 with valid GET credentials
        Properties<string, string> getCredentials = authzGen.GetAllowedCredentials(
          new OperationCode[] { OperationCode.Get },
          new string[] { RegionName }, 2);
        javaProps = cGen.JavaProperties;
        Util.Log("AllowPutsGets: For second client GET credentials: " +
          getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Locators, authInit, (Properties<string, string>)null, true);

        // Perform some put operations from client1
        m_client1.Call(DoPutsMU, 10 , createCredentials, true);

        // Verify that the gets succeed
        m_client2.Call(DoGetsMU, 10, getCredentials, true);

        m_client1.Call(DoPutsTx, 10, true, ExpectedResult.Success, getCredentials, true);

        m_client2.Call(DoGets, 10, true, ExpectedResult.Success, getCredentials, true);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);
        CacheHelper.StopJavaServer(2);
      }

      CacheHelper.StopJavaLocator(1);

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runDisallowPutsGets()
    {
      CacheHelper.SetupJavaServers(true, CacheXml1, CacheXml2);

      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");

      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(true))
      {
        CredentialGenerator cGen = authzGen.GetCredentialGenerator();
        Properties<string, string> extraAuthProps = cGen.SystemProperties;
        Properties<string, string> javaProps = cGen.JavaProperties;
        Properties<string, string> extraAuthzProps = authzGen.SystemProperties;
        string authenticator = cGen.Authenticator;
        string authInit = cGen.AuthInit;
        string accessor = authzGen.AccessControl;

        Util.Log("DisallowPutsGets: Using authinit: " + authInit);
        Util.Log("DisallowPutsGets: Using authenticator: " + authenticator);
        Util.Log("DisallowPutsGets: Using accessor: " + accessor);

        // Check that we indeed can obtain valid credentials not allowed to do
        // gets
        Properties<string, string> createCredentials = authzGen.GetAllowedCredentials(
          new OperationCode[] { OperationCode.Put },
          new string[] { RegionName }, 1);
        Properties<string, string> createJavaProps = cGen.JavaProperties;
        Properties<string, string> getCredentials = authzGen.GetDisallowedCredentials(
          new OperationCode[] { OperationCode.Get },
          new string[] { RegionName }, 2);
        Properties<string, string> getJavaProps = cGen.JavaProperties;
        if (getCredentials == null || getCredentials.Size == 0)
        {
          Util.Log("DisallowPutsGets: Unable to obtain valid credentials " +
            "with no GET permission; skipping this combination.");
          continue;
        }

        // Start servers with all required properties
        string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
          accessor, null, SecurityTestUtil.ConcatProperties(extraAuthProps,
          extraAuthzProps), javaProps);

        // Start the two servers.
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs);
        Util.Log("Cacheserver 2 started.");

        // Start client1 with valid CREATE credentials
        createCredentials = authzGen.GetAllowedCredentials(
            new OperationCode[] { OperationCode.Put },
            new string[] { RegionName }, 1);
        javaProps = cGen.JavaProperties;
        Util.Log("DisallowPutsGets: For first client PUT credentials: " +
          createCredentials);
        m_client1.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Locators, authInit, (Properties<string, string>)null, true);

        // Start client2 with invalid GET credentials
        getCredentials = authzGen.GetDisallowedCredentials(
            new OperationCode[] { OperationCode.Get },
            new string[] { RegionName }, 2);
        javaProps = cGen.JavaProperties;
        Util.Log("DisallowPutsGets: For second client invalid GET " +
          "credentials: " + getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Locators, authInit, (Properties<string, string>)null, true);

        // Perform some put operations from client1
        m_client1.Call(DoPutsMU, 10, createCredentials, true);

        // Verify that the gets throw exception
        m_client2.Call(DoGetsMU, 10, getCredentials, true, ExpectedResult.NotAuthorizedException);

        // Try to connect client2 with reader credentials
        getCredentials = authzGen.GetAllowedCredentials(
            new OperationCode[] { OperationCode.Get },
            new string[] { RegionName }, 5);
        javaProps = cGen.JavaProperties;
        Util.Log("DisallowPutsGets: For second client valid GET " +
          "credentials: " + getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Locators, authInit, (Properties<string, string>)null, true);

        // Verify that the gets succeed
        m_client2.Call(DoGetsMU, 10, getCredentials, true);

        // Verify that the puts throw exception
        m_client2.Call(DoPutsMU, 10, getCredentials, true, ExpectedResult.NotAuthorizedException);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);
        CacheHelper.StopJavaServer(2);
      }

      CacheHelper.StopJavaLocator(1);

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runInvalidAccessor()
    {
      CacheHelper.SetupJavaServers(true, CacheXml1, CacheXml2);
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");

      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(true))
      {
        CredentialGenerator cGen = authzGen.GetCredentialGenerator();
        Util.Log("NIl:792:Current credential is = {0}", cGen);
        
        //if (cGen.GetClassCode() == CredentialGenerator.ClassCode.LDAP)
        //  continue;

        Properties<string, string> extraAuthProps = cGen.SystemProperties;
        Properties<string, string> javaProps = cGen.JavaProperties;
        Properties<string, string> extraAuthzProps = authzGen.SystemProperties;
        string authenticator = cGen.Authenticator;
        string authInit = cGen.AuthInit;
        string accessor = authzGen.AccessControl;

        Util.Log("InvalidAccessor: Using authinit: " + authInit);
        Util.Log("InvalidAccessor: Using authenticator: " + authenticator);

        // Start server1 with invalid accessor
        string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
          "com.gemstone.none", null, SecurityTestUtil.ConcatProperties(extraAuthProps,
          extraAuthzProps), javaProps);
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
        Util.Log("Cacheserver 1 started.");

        // Client creation should throw exceptions
        Properties<string, string> createCredentials = authzGen.GetAllowedCredentials(
            new OperationCode[] { OperationCode.Put },
            new string[] { RegionName }, 3);
        javaProps = cGen.JavaProperties;
        Util.Log("InvalidAccessor: For first client PUT credentials: " +
          createCredentials);
        m_client1.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Locators, authInit, (Properties<string, string>)null, true);

        // Now perform some put operations from client1
        m_client1.Call(DoPutsMU, 10, createCredentials, true, ExpectedResult.OtherException);

        Properties<string, string> getCredentials = authzGen.GetAllowedCredentials(
            new OperationCode[] { OperationCode.Get },
            new string[] { RegionName }, 7);
        javaProps = cGen.JavaProperties;
        Util.Log("InvalidAccessor: For second client GET credentials: " +
          getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Locators, authInit, (Properties<string, string>)null, true);

        // Now perform some put operations from client1
        m_client2.Call(DoGetsMU, 10, getCredentials, true, ExpectedResult.OtherException);

        // Now start server2 that has valid accessor
        Util.Log("InvalidAccessor: Using accessor: " + accessor);
        serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
          accessor, null, SecurityTestUtil.ConcatProperties(extraAuthProps,
          extraAuthzProps), javaProps);
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs);
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StopJavaServer(1);

        // Client creation should be successful now
        m_client1.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Locators, authInit, (Properties<string, string>)null, true);
        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Locators, authInit, (Properties<string, string>)null, true);

        // Now perform some put operations from client1
        m_client1.Call(DoPutsMU, 10, createCredentials, true);

        // Verify that the gets succeed
        m_client2.Call(DoGetsMU, 10, getCredentials, true);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(2);
      }

      CacheHelper.StopJavaLocator(1);

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runAllOpsWithFailover()
    {
      OperationWithAction[] allOps = {
        // Test CREATE and verify with a GET
        new OperationWithAction(OperationCode.Put, 3, OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.Put),
        new OperationWithAction(OperationCode.Get, 3, OpFlags.CheckNoKey
            | OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.CheckNoKey, 4),
        // OPBLOCK_END indicates end of an operation block; the above block of
        // three operations will be first executed on server1 and then on
        // server2 after failover
        
        OperationWithAction.OpBlockEnd,

        // Test GetServerKeys (KEY_SET) operation.
        new OperationWithAction(OperationCode.GetServerKeys),
        new OperationWithAction(OperationCode.GetServerKeys, 3, OpFlags.CheckNotAuthz, 4),

        OperationWithAction.OpBlockEnd,

        // Test UPDATE and verify with a GET
        new OperationWithAction(OperationCode.Put, 3, OpFlags.UseNewVal
            | OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.Put, 1, OpFlags.UseNewVal, 4),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.UseNewVal, 4),

        OperationWithAction.OpBlockEnd,

        // Test DESTROY and verify with a GET and that key should not exist
        new OperationWithAction(OperationCode.Destroy, 3, OpFlags.UseNewVal
            | OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.Destroy),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.CheckFail, 4),
        // Repopulate the region
        new OperationWithAction(OperationCode.Put, 1, OpFlags.UseNewVal, 4),

        OperationWithAction.OpBlockEnd,

        // Check QUERY
        new OperationWithAction(OperationCode.Put),
        new OperationWithAction(OperationCode.Query, 3,
          OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.Query),

        OperationWithAction.OpBlockEnd,

        // UPDATE and test with GET
        new OperationWithAction(OperationCode.Put),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.Put, 1, OpFlags.UseOldConn
            | OpFlags.UseNewVal, 4),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),
         OperationWithAction.OpBlockEnd,

        /// PutAll, GetAll, ExecuteCQ and ExecuteFunction ops
        new OperationWithAction(OperationCode.PutAll),
        // NOTE: GetAll depends on previous PutAll so it should happen right after.
        new OperationWithAction(OperationCode.GetAll),
        new OperationWithAction(OperationCode.RemoveAll),
        //new OperationWithAction(OperationCode.ExecuteCQ),
        new OperationWithAction(OperationCode.ExecuteFunction),

        OperationWithAction.OpBlockEnd,

        // UPDATE and test with GET
        new OperationWithAction(OperationCode.Put, 2),
        new OperationWithAction(OperationCode.Get, 1, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.Put, 2, OpFlags.UseOldConn
            | OpFlags.UseNewVal, 4),
        new OperationWithAction(OperationCode.Get, 1, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        OperationWithAction.OpBlockEnd,

        // UPDATE and test with GET
        new OperationWithAction(OperationCode.Put),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.Put, 1, OpFlags.UseOldConn
            | OpFlags.UseNewVal, 4),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        OperationWithAction.OpBlockEnd,

        // Do REGION_DESTROY of the sub-region and check with GET
        new OperationWithAction(OperationCode.Put, 1, OpFlags.UseSubRegion,
            8),
        new OperationWithAction(OperationCode.RegionDestroy, 3,
            OpFlags.UseSubRegion | OpFlags.CheckNotAuthz, 1),
        new OperationWithAction(OperationCode.RegionDestroy, 1,
            OpFlags.UseSubRegion, 1),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseSubRegion
            | OpFlags.CheckNoKey | OpFlags.CheckException, 8),

        // Do REGION_DESTROY of the region and check with GET
        new OperationWithAction(OperationCode.RegionDestroy, 3,
            OpFlags.CheckNotAuthz, 1),
        new OperationWithAction(OperationCode.RegionDestroy, 1, OpFlags.None,
            1),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.CheckNoKey | OpFlags.CheckException, 8),

        // Skip failover for region destroy since it shall fail
        // without restarting the server
        OperationWithAction.OpBlockNoFailover
      };
      RunOpsWithFailover(allOps, "AllOpsWithFailover", true);
    }

    void runThinClientWriterExceptionTest()
    {
      CacheHelper.SetupJavaServers(true, CacheXml1);
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");

      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(true))
      {
        for (int i = 1; i <= 2; ++i)
        {
          CredentialGenerator cGen = authzGen.GetCredentialGenerator();
          //TODO: its not working for multiuser mode.. need to fix later
          if (cGen.GetClassCode() == CredentialGenerator.ClassCode.PKCS)
            continue;
          Properties<string, string> extraAuthProps = cGen.SystemProperties;
          Properties<string, string> javaProps = cGen.JavaProperties;
          Properties<string, string> extraAuthzProps = authzGen.SystemProperties;
          string authenticator = cGen.Authenticator;
          string authInit = cGen.AuthInit;
          string accessor = authzGen.AccessControl;

          Util.Log("ThinClientWriterException: Using authinit: " + authInit);
          Util.Log("ThinClientWriterException: Using authenticator: " + authenticator);
          Util.Log("ThinClientWriterException: Using accessor: " + accessor);

          // Start servers with all required properties
          string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
            accessor, null, SecurityTestUtil.ConcatProperties(extraAuthProps,
            extraAuthzProps), javaProps);

          // Start the server.
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
          Util.Log("Cacheserver 1 started.");

          // Start client1 with valid CREATE credentials
          Properties<string, string> createCredentials = authzGen.GetDisallowedCredentials(
            new OperationCode[] { OperationCode.Put },
            new string[] { RegionName }, 1);
          javaProps = cGen.JavaProperties;
          Util.Log("DisallowPuts: For first client PUT credentials: " +
            createCredentials);
          m_client1.Call(SecurityTestUtil.CreateClientR0, RegionName,
          CacheHelper.Locators, authInit, createCredentials);

          Util.Log("Creating region in client1 , no-ack, no-cache, with listener and writer");
          m_client1.Call(CreateRegion, CacheHelper.Locators,
            true, true, true);
          m_client1.Call(RegisterAllKeys, new string[] { RegionName });

          try
          {
            Util.Log("Trying put Operation");
            m_client1.Call(DoPut);
            Util.Log(" Put Operation Successful");
            Assert.Fail("Should have got NotAuthorizedException during put");
          }
          catch (NotAuthorizedException)
          {
            Util.Log("NotAuthorizedException Caught");
            Util.Log("Success");
          }
          catch (Exception other)
          {
            Util.Log("Stack trace: {0} ", other.StackTrace);
            Util.Log("Got  exception : {0}",
             other.Message);
          }
          m_client1.Call(CheckAssert);
          // Do LocalPut
          m_client1.Call(DoLocalPut);

          m_client1.Call(Close);

          CacheHelper.StopJavaServer(1);
        }
      }
      CacheHelper.StopJavaLocator(1);
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    #region Tests

    [Test]
	  public void TestCQ()
    { 
      runCQTest();
    }

    [Test]
    public void TestDurableCQ()
    {
      //for all run real cache will be true
      //logical cache true/false and durable cq true/false combination....
      //result..whether events should be there or not
      runDurableCQTest(false, true, true);//no usercache close as it will close user's durable cq
      runDurableCQTest(true, false, false);
      runDurableCQTest(false, true, true);
      runDurableCQTest(false, false, false);
    }

    [Test]
    public void AllowPutsGets()
    {
      runAllowPutsGets();
    }

    [Test]
    public void DisallowPutsGets()
    {
      runDisallowPutsGets();
    }
    
    [Test]
    public void AllOpsWithFailover()
    {
      runAllOpsWithFailover();
    }

    [Test]
    public void ThinClientWriterExceptionTest()
    {
      runThinClientWriterExceptionTest();
    }
    #endregion
  }
}
