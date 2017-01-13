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
  [Category("group3")]
  [Category("unicast_only")]
  [Category("generics")]
  public class ThinClientSecurityAuthzTests : ThinClientSecurityAuthzTestBase
  {
    #region Private members
    IRegion<object, object> region;
    IRegion<object, object> region1;
    private UnitProcess m_client1;
    private UnitProcess m_client2;
    private UnitProcess m_client3;
    private TallyListener<object, object> m_listener;
    private TallyWriter<object, object> m_writer;
    private string keys = "Key";
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
      AttributesMutator<object, object> at = region.AttributesMutator;
      at.SetCacheWriter(m_writer);
    }

    public void DoPut()
    {
      region = CacheHelper.GetRegion<object, object>(RegionName);
      region[keys] = value;
    }

    public void CheckAssert()
    {
      Assert.AreEqual(true, m_writer.IsWriterInvoked, "Writer Should be invoked");
      Assert.AreEqual(false, m_listener.IsListenerInvoked, "Listener Should not be invoked");
      Assert.IsFalse(region.GetLocalView().ContainsKey(keys), "Key should have been found in the region");
    }

    public void DoLocalPut()
    {
      region1 = CacheHelper.GetRegion<object, object>(RegionName);
      region1.GetLocalView()[m_keys[2]] = m_vals[2];
      Assert.IsTrue(region1.GetLocalView().ContainsKey(m_keys[2]), "Key should have been found in the region");
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
      Assert.IsTrue(region1.GetLocalView().ContainsKey(m_keys[2]), "Key should have been found in the region");
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

    void runAllowPutsGets()
    {
      CacheHelper.SetupJavaServers(true, CacheXml1, CacheXml2);

      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");

      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(false))
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
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, createCredentials);

        // Start client2 with valid GET credentials
        Properties<string, string> getCredentials = authzGen.GetAllowedCredentials(
          new OperationCode[] { OperationCode.Get },
          new string[] { RegionName }, 2);
        javaProps = cGen.JavaProperties;
        Util.Log("AllowPutsGets: For second client GET credentials: " +
          getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, getCredentials);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 2);

        // Verify that the gets succeed
        m_client2.Call(DoGets, 2);

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

      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(false))
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
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, createCredentials);

        // Start client2 with invalid GET credentials
        getCredentials = authzGen.GetDisallowedCredentials(
            new OperationCode[] { OperationCode.Get },
            new string[] { RegionName }, 2);
        javaProps = cGen.JavaProperties;
        Util.Log("DisallowPutsGets: For second client invalid GET " +
          "credentials: " + getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, getCredentials);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 2);

        // Verify that the gets throw exception
        m_client2.Call(DoGets, 2, false, ExpectedResult.NotAuthorizedException);

        // Try to connect client2 with reader credentials
        getCredentials = authzGen.GetAllowedCredentials(
            new OperationCode[] { OperationCode.Get },
            new string[] { RegionName }, 5);
        javaProps = cGen.JavaProperties;
        Util.Log("DisallowPutsGets: For second client valid GET " +
          "credentials: " + getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, getCredentials);

        // Verify that the gets succeed
        m_client2.Call(DoGets, 2);

        // Verify that the puts throw exception
        m_client2.Call(DoPuts, 2, true, ExpectedResult.NotAuthorizedException);

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
      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(false))
      {
        CredentialGenerator cGen = authzGen.GetCredentialGenerator();
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
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, createCredentials,
          ExpectedResult.OtherException);
        Properties<string, string> getCredentials = authzGen.GetAllowedCredentials(
            new OperationCode[] { OperationCode.Get },
            new string[] { RegionName }, 7);
        javaProps = cGen.JavaProperties;
        Util.Log("InvalidAccessor: For second client GET credentials: " +
          getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, getCredentials,
          ExpectedResult.OtherException);

        // Now start server2 that has valid accessor
        Util.Log("InvalidAccessor: Using accessor: " + accessor);
        serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
          accessor, null, SecurityTestUtil.ConcatProperties(extraAuthProps,
          extraAuthzProps), javaProps);
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs);
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StopJavaServer(1);

        // Client creation should be successful now
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, createCredentials);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, getCredentials);

        // Now perform some put operations from client1
        m_client1.Call(DoPuts, 4);

        // Verify that the gets succeed
        m_client2.Call(DoGets, 4);

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
      runAllOpsWithFailover(false, false);
    }

    void runAllOpsWithFailover(bool ssl, bool withPassword)
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

        // Register interest in all keys
        new OperationWithAction(OperationCode.RegisterInterest, 3,
            OpFlags.UseAllKeys | OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.RegisterInterest, 2,
            OpFlags.UseAllKeys, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.Put),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        // Unregister interest in all keys
        new OperationWithAction(OperationCode.UnregisterInterest, 2,
            OpFlags.UseAllKeys | OpFlags.UseOldConn, 4),
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
        new OperationWithAction(OperationCode.ExecuteCQ),
        new OperationWithAction(OperationCode.ExecuteFunction),

        OperationWithAction.OpBlockEnd,

        // Register interest in all keys using list
        new OperationWithAction(OperationCode.RegisterInterest, 3,
            OpFlags.UseList | OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.RegisterInterest, 1,
            OpFlags.UseList, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.Put, 2),
        new OperationWithAction(OperationCode.Get, 1, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        // Unregister interest in all keys using list
        new OperationWithAction(OperationCode.UnregisterInterest, 1,
            OpFlags.UseOldConn | OpFlags.UseList, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.Put, 2, OpFlags.UseOldConn
            | OpFlags.UseNewVal, 4),
        new OperationWithAction(OperationCode.Get, 1, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        OperationWithAction.OpBlockEnd,

        // Register interest in all keys using regular expression
        new OperationWithAction(OperationCode.RegisterInterest, 3,
            OpFlags.UseRegex | OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.RegisterInterest, 2,
            OpFlags.UseRegex, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.Put),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        // Unregister interest in all keys using regular expression
        new OperationWithAction(OperationCode.UnregisterInterest, 2,
            OpFlags.UseOldConn | OpFlags.UseRegex, 4),
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
      if (ssl == true && withPassword == true)
      {
        RunOpsWithFailoverSSL(allOps, "AllOpsWithFailover", true);
      }
      else  if(ssl == true)
        RunOpsWithFailoverSSL(allOps, "AllOpsWithFailover", false);
      else
        RunOpsWithFailover(allOps, "AllOpsWithFailover");
    }

    void runThinClientWriterExceptionTest()
    {
      CacheHelper.SetupJavaServers(true, CacheXml1);
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(false))
      {
        for (int i = 1; i <= 2; ++i)
        {
          CredentialGenerator cGen = authzGen.GetCredentialGenerator();
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
          m_client1.Call(CreateRegion,CacheHelper.Locators,
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
    public void AllowPutsGets()
    {
      runAllowPutsGets();
    }

    [Test]
    public void DisallowPutsGets()
    {
      runDisallowPutsGets();
    }

    //this test no more valid as the way we do auth now change, so it gets different exception
    //[Test]
    public void InvalidAccessor()
    {
      //runInvalidAccessor();
    }

    [Test]
    public void AllOpsWithFailover()
    {
      runAllOpsWithFailover();
    }
    
    [Test]
    public void AllOpsWithFailoverWithSSL()
    {
      // SSL CAN ONLY BE ENABLED WITH LOCATORS THUS REQUIRING POOLS.
      runAllOpsWithFailover(true, false); // pool with locator with ssl
    }

    [Test]
    public void AllOpsWithFailoverWithSSLWithPassword()
    {
      // SSL CAN ONLY BE ENABLED WITH LOCATORS THUS REQUIRING POOLS.
      runAllOpsWithFailover(true, true); // pool with locator with ssl
    }

    [Test]
    public void ThinClientWriterExceptionTest()
    {
      runThinClientWriterExceptionTest();
    }
    #endregion
  }
}
