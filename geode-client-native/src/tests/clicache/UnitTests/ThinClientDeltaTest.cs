//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;

  public class CqDeltaListener : ICqListener
  {

    public CqDeltaListener()
    {
      m_deltaCount = 0;
      m_valueCount = 0;
    }

    public void OnEvent(CqEvent aCqEvent)
    {
      byte[] deltaValue = aCqEvent.getDeltaValue();
      DeltaTestImpl newValue = new DeltaTestImpl();
      DataInput input = new DataInput(deltaValue);
      newValue.FromDelta(input);
      if (newValue.GetIntVar() == 5)
      {
        m_deltaCount++;
      }
      DeltaTestImpl fullObject = (DeltaTestImpl)aCqEvent.getNewValue();
      if (fullObject.GetIntVar() == 5)
      {
        m_valueCount++;
      }

    }

    public void OnError(CqEvent aCqEvent)
    {
    }

    public void Close()
    {
    }

    public int GetDeltaCount()
    {
      return m_deltaCount;
    }

    public int GetValueCount()
    {
      return m_valueCount;
    }

    private int m_deltaCount;
    private int m_valueCount;
  }

  public class DeltaTestAD : IGFDelta, IGFSerializable
  {
    private int _deltaUpdate;
    private string _staticData;

    public static DeltaTestAD Create()
    {
      return new DeltaTestAD();
    }

    public DeltaTestAD()
    {
      _deltaUpdate = 1;
      _staticData = "Data which don't get updated";
    }


    #region IGFDelta Members

    public void FromDelta(DataInput input)
    {
      _deltaUpdate = input.ReadInt32();
    }

    public bool HasDelta()
    {
      _deltaUpdate++;
      bool isDelta = (_deltaUpdate % 2) == 1;
      Util.Log("In DeltaTestAD.HasDelta _deltaUpdate:" + _deltaUpdate + " : isDelta:" + isDelta);
      return isDelta;
    }

    public void ToDelta(DataOutput output)
    {
      output.WriteInt32(_deltaUpdate);
    }

    #endregion

    #region IGFSerializable Members

    public uint ClassId
    {
      get { return 151; }
    }

    public IGFSerializable FromData(DataInput input)
    {
      _deltaUpdate = input.ReadInt32();
      _staticData = input.ReadUTF();

      return this;
    }

    public uint ObjectSize
    {
      get {return (uint)(4 + _staticData.Length); }
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(_deltaUpdate);
      output.WriteUTF(_staticData);
    }

    public int DeltaUpdate
    {
      get { return _deltaUpdate; }
      set { _deltaUpdate = value; }
    }

    #endregion
  }

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientDeltaTest : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1, m_client2;
    private CqDeltaListener myCqListener;

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
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

    public void createLRURegionAndAttachPool(string regionName, string poolName)
    {
      CacheHelper.CreateLRUTCRegion_Pool(regionName, true, true, null, null, null, poolName, false, 3);
    }

    public void createRegionAndAttachPool(string regionName, string poolName)
    {
      createRegionAndAttachPool(regionName, poolName, false);
    }

    public void createRegionAndAttachPool(string regionName, string poolName, bool cloningEnabled)
    {
      CacheHelper.CreateTCRegion_Pool(regionName, true, true, null, null, null, poolName, false,
        false, cloningEnabled);
    }

    public void createPooledRegion(string regionName, string poolName, string endpoints, string locators)
    {
      CacheHelper.CreateTCRegion_Pool(regionName, true, true, null, endpoints, locators, poolName, false);
    }

    public void createPool(string name, string endpoints, string locators, string serverGroup,
      int redundancy, bool subscription)
    {
      CacheHelper.CreatePool(name, endpoints, locators, serverGroup, redundancy, subscription);
    }

    public void createExpirationRegion(string name, string poolName)
    {
      AttributesFactory attrFac = new AttributesFactory();
      attrFac.SetEntryTimeToLive(ExpirationAction.LocalInvalidate, 5);
      attrFac.SetClientNotificationEnabled(true);
      if (poolName != null)
      {
        attrFac.SetPoolName(poolName);
      }
      RegionAttributes ra = attrFac.CreateRegionAttributes();
      CacheHelper.CreateRegion(name,ra);
    }

    public void createExpirationRegion(string name)
    {
      createExpirationRegion(name, null);
    }

    public void CreateRegion(string name)
    {
      CreateRegion(name, false);
    }

    public void CreateRegion(string name, bool enableNotification)
    {
      CreateRegion(name, enableNotification, false);
    }
    public void CreateRegion(string name, bool enableNotification, bool cloningEnabled)
    {
      AttributesFactory attrFac = new AttributesFactory();
      attrFac.SetClientNotificationEnabled(enableNotification);
      attrFac.SetCacheListener(new SimpleCacheListener());
      attrFac.SetCloningEnabled(cloningEnabled);
      CacheHelper.CreateRegion(name, attrFac.CreateRegionAttributes());
    }

    public void CreateOverflowRegion(string name, uint entriesLimit)
    {
      AttributesFactory af = new AttributesFactory();
      af.SetScope(ScopeType.DistributedAck);
      af.SetCachingEnabled(true);
      af.SetClientNotificationEnabled(true);
      af.SetLruEntriesLimit(entriesLimit);// LRU Entry limit set to 3

      af.SetDiskPolicy(DiskPolicyType.Overflows);
      Properties bdbProperties = Properties.Create();
      bdbProperties.Insert("CacheSizeGb", "0");
      bdbProperties.Insert("CacheSizeMb", "512");
      bdbProperties.Insert("PageSize", "65536");
      bdbProperties.Insert("MaxFileSize", "512000000");
      String wdPath = Directory.GetCurrentDirectory();
      String absPersistenceDir = wdPath + "/absBDB";
      String absEnvDir = wdPath + "/absBDBEnv";
      bdbProperties.Insert("PersistenceDirectory", absPersistenceDir);
      bdbProperties.Insert("EnvironmentDirectory", absEnvDir);
      af.SetPersistenceManager("BDBImpl", "createBDBInstance", bdbProperties);

      CacheHelper.CreateRegion(name, af.CreateRegionAttributes());
    }

    void DoPutWithDelta()
    {
      try
      {
        Serializable.RegisterType(DeltaEx.create);
      }
      catch (IllegalStateException)
      {
        //do nothng
      }
      CacheableString cKey = new CacheableString(m_keys[0]);
      DeltaEx val = new DeltaEx();
      Region reg = CacheHelper.GetRegion("DistRegionAck");

      reg.Put(cKey, val);
      val.SetDelta(true);
      reg.Put(cKey, val);

      DeltaEx val1 = new DeltaEx(0); // In this case JAVA side will throw invalid DeltaException
      reg.Put(cKey, val1);
      val1.SetDelta(true);
      reg.Put(cKey, val1);
      if (DeltaEx.ToDeltaCount != 2)
      {
        Util.Log("DeltaEx.ToDataCount = " + DeltaEx.ToDataCount);
        Assert.Fail(" Delta count should have been 2, is " + DeltaEx.ToDeltaCount);
      }
      if (DeltaEx.ToDataCount != 3)
        Assert.Fail("Data count should have been 3, is " + DeltaEx.ToDataCount);
      DeltaEx.ToDeltaCount = 0;
      DeltaEx.ToDataCount = 0;
      DeltaEx.FromDataCount = 0;
      DeltaEx.FromDeltaCount = 0;
    }

    void DoNotificationWithDelta()
    {
      try
      {
        Serializable.RegisterType(DeltaEx.create);
      }
      catch (IllegalStateException)
      {
        //do nothig.
      }

      CacheableString cKey = new CacheableString(m_keys[0]);
      DeltaEx val = new DeltaEx();
      Region reg = CacheHelper.GetRegion("DistRegionAck");
      reg.Put(cKey, val);
      val.SetDelta(true);
      reg.Put(cKey, val);

      CacheableString cKey1 = new CacheableString(m_keys[1]);
      DeltaEx val1 = new DeltaEx();
      reg.Put(cKey1, val1);
      val1.SetDelta(true);
      reg.Put(cKey1, val1);
      DeltaEx.ToDeltaCount = 0;
      DeltaEx.ToDataCount = 0;
    }
    void DoNotificationWithDefaultCloning()
    {
      CacheableString cKey = new CacheableString(m_keys[0]);
      DeltaTestImpl val = new DeltaTestImpl();
      Region reg = CacheHelper.GetRegion("DistRegionAck");
      reg.Put(cKey, val);
      val.SetIntVar(2);
      val.SetDelta(true);
      reg.Put(cKey, val);
    }
    void DoNotificationWithDeltaLRU()
    {
      try
      {
        Serializable.RegisterType(DeltaEx.create);
      }
      catch (IllegalStateException)
      {
        //do nothig.
      }

      CacheableString cKey1 = new CacheableString("key1");
      CacheableString cKey2 = new CacheableString("key2");
      CacheableString cKey3 = new CacheableString("key3");
      CacheableString cKey4 = new CacheableString("key4");
      CacheableString cKey5 = new CacheableString("key5");
      CacheableString cKey6 = new CacheableString("key6");
      DeltaEx val1 = new DeltaEx();
      DeltaEx val2 = new DeltaEx();
      Region reg = CacheHelper.GetRegion("DistRegionAck");
      reg.Put(cKey1, val1);
      reg.Put(cKey2, val1);
      reg.Put(cKey3, val1);
      reg.Put(cKey4, val1);
      reg.Put(cKey5, val1);
      reg.Put(cKey6, val1);
      val2.SetDelta(true);
      reg.Put(cKey1, val2);

      DeltaEx.ToDeltaCount = 0;
      DeltaEx.ToDataCount = 0;
    }
    void DoExpirationWithDelta()
    {
      try
      {
        Serializable.RegisterType(DeltaEx.create);
      }
      catch (IllegalStateException)
      {
        //do nothig.
      }

      DeltaEx val1 = new DeltaEx();
      Region reg = CacheHelper.GetRegion("DistRegionAck");
      reg.Put(1, val1);
      // Sleep 10 seconds to allow expiration of entry in client 2
      Thread.Sleep(10000);
      val1.SetDelta(true);
      reg.Put(1, val1);
      DeltaEx.ToDeltaCount = 0;
      DeltaEx.ToDataCount = 0;
    }
    void DoCqWithDelta()
    {
      CacheableString cKey1 = new CacheableString("key1");
      Region reg = CacheHelper.GetRegion("DistRegionAck");
      DeltaTestImpl value = new DeltaTestImpl();
      reg.Put(cKey1, value);
      value.SetIntVar(5);
      value.SetDelta(true);
      reg.Put(cKey1, value);
    }

    void initializeDeltaClientAD()
    {
      try
      {
        Serializable.RegisterType(DeltaTestAD.Create);
      }
      catch (IllegalStateException)
      {
        //do nothng
      }
    }

    void DoDeltaAD_C1_1()
    {           
      DeltaTestAD val = new DeltaTestAD();
      Region reg = CacheHelper.GetRegion("DistRegionAck");
      reg.RegisterAllKeys();
      Util.Log("clientAD1 put");
      reg.Put(1, val);
      Util.Log("clientAD1 put done");
    }

    void DoDeltaAD_C2_1()
    {
      Region reg = CacheHelper.GetRegion("DistRegionAck");

      Util.Log("clientAD2 get");
      DeltaTestAD val = (DeltaTestAD)reg.Get(1);

      Assert.AreEqual(2, val.DeltaUpdate);
      Util.Log("clientAD2 get done");
      reg.Put(1, val);
      Util.Log("clientAD2 put done");
    }

    void DoDeltaAD_C1_afterC2Put()
    {
      Thread.Sleep(15000);
      DeltaTestAD val = null;
      Region reg = CacheHelper.GetRegion("DistRegionAck");
      Util.Log("client fetching entry from local cache");
      val = (DeltaTestAD)reg.GetEntry(1).Value;
      Assert.IsNotNull(val);
      Assert.AreEqual(3, val.DeltaUpdate);
      Util.Log("done");
    }

    void runDeltaWithAppdomian(bool usePools)
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_with_deltaAD.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC1");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS5", 1);
      string regionName = "DistRegionAck";
      if (usePools)
      {
        //CacheHelper.CreateTCRegion_Pool_AD("DistRegionAck", false, false, null, null, CacheHelper.Locators, "__TEST_POOL1__", false, false, false);
        m_client1.Call(CacheHelper.CreateTCRegion_Pool_AD1, regionName, false, true, (string)null, CacheHelper.Locators, (string)"__TEST_POOL1__", true);
        m_client2.Call(CacheHelper.CreateTCRegion_Pool_AD1, regionName, false, true, (string)null, CacheHelper.Locators, (string)"__TEST_POOL1__", false);
       // m_client1.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, false);
       // m_client1.Call(createRegionAndAttachPool, "DistRegionAck", "__TEST_POOL1__");
      }
      else
      {
        //CacheHelper.CreateTCRegion_Pool_AD("DistRegionAck", false, false, null, CacheHelper.Endpoints, (string)null, "__TEST_POOL1__", false, false, false);
        m_client1.Call(CacheHelper.CreateTCRegion_Pool_AD1, regionName, false, true, CacheHelper.Endpoints, (string)null, (string)"__TEST_POOL1__", true);
        m_client2.Call(CacheHelper.CreateTCRegion_Pool_AD1, regionName, false, true, CacheHelper.Endpoints, (string)null, (string)"__TEST_POOL1__", false);
        //m_client1.Call(CacheHelper.InitConfig,CacheHelper.Endpoints,0);
        //m_client1.Call(CreateRegion, "DistRegionAck");
      }

      m_client1.Call(initializeDeltaClientAD);
      m_client2.Call(initializeDeltaClientAD);

      m_client1.Call(DoDeltaAD_C1_1);
      m_client2.Call(DoDeltaAD_C2_1);
      m_client1.Call(DoDeltaAD_C1_afterC2Put);

      CacheHelper.StopJavaServer(1);
      CacheHelper.StopJavaLocator(1);
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runPutWithDelta(bool usePools)
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_with_delta.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC1");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS5", 1);
      if (usePools)
      {
        m_client1.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, false);
        m_client1.Call(createRegionAndAttachPool, "DistRegionAck", "__TEST_POOL1__");
      }
      else
      {
        m_client1.Call(CacheHelper.InitConfig, CacheHelper.Endpoints, 0);
        m_client1.Call(CreateRegion, "DistRegionAck");
      }

      m_client1.Call(DoPutWithDelta);
      m_client1.Call(Close);
      
      CacheHelper.StopJavaServer(1);
      CacheHelper.StopJavaLocator(1);
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }
    
    void registerClassCl2()
    {
      try
      {
        Serializable.RegisterType(DeltaEx.create);
      }
      catch (IllegalStateException)
      {
        //do nothing
      }
      Region reg = CacheHelper.GetRegion("DistRegionAck");
      reg.RegisterRegex(".*");
      AttributesMutator attrMutator = reg.GetAttributesMutator();
      attrMutator.SetCacheListener(new SimpleCacheListener());
    }

    void registerClassDeltaTestImpl()
    {
      try
      {
        Serializable.RegisterType(DeltaTestImpl.CreateDeserializable);
      }
      catch (IllegalStateException)
      {
        // ARB: ignore exception caused by type reregistration.
      }
      DeltaTestImpl.ResetDataCount();
      Region reg = CacheHelper.GetRegion("DistRegionAck");
      try
      {
        reg.RegisterRegex(".*");
      }
      catch (Exception)
      {
        // ARB: ignore regex exception for missing notification channel.
      }
    }
    void registerCq(bool usePools)
    {
      Pool thePool = PoolManager.Find("__TEST_POOL1__");
      QueryService cqService = null;
      if (usePools)
      {
        cqService = thePool.GetQueryService();
      }
      else
      {
        cqService = CacheHelper.QueryServiceInstance;
      }
      CqAttributesFactory attrFac = new CqAttributesFactory();
      myCqListener = new CqDeltaListener();
      attrFac.AddCqListener(myCqListener);
      CqAttributes cqAttr = attrFac.Create();
      CqQuery theQuery = cqService.NewCq("select * from /DistRegionAck d where d.intVar > 4", cqAttr, false);
      theQuery.Execute();
    }
    void VerifyDeltaCount()
    {
      Thread.Sleep(1000);
      Util.Log("Total Data count" + DeltaEx.FromDataCount);
      Util.Log("Total Data count" + DeltaEx.FromDeltaCount);
      if (DeltaEx.FromDataCount != 3)
       Assert.Fail("Count of fromData called should be 3 ");
      if (DeltaEx.FromDeltaCount != 2)
       Assert.Fail("Count of fromDelta called should be 2 ");
     if (SimpleCacheListener.isSuccess == false)
       Assert.Fail("Listener failure");
      SimpleCacheListener.isSuccess = false;
      if (DeltaEx.CloneCount != 2)
        Assert.Fail("Clone count should be 2, is " + DeltaEx.CloneCount);

      DeltaEx.FromDataCount = 0;
      DeltaEx.FromDeltaCount = 0;
      DeltaEx.CloneCount = 0;
    }
    void VerifyCloning()
    {
      Thread.Sleep(1000);
      CacheableString cKey = new CacheableString(m_keys[0]);
      Region reg = CacheHelper.GetRegion("DistRegionAck");
      DeltaTestImpl val = (DeltaTestImpl) reg.Get(cKey);
      if (val.GetIntVar() != 2)
        Assert.Fail("Int value after cloning should be 2, is " + val.GetIntVar());
      if (DeltaTestImpl.GetFromDataCount() != 2)
        Assert.Fail("After cloning, fromDataCount should have been 2, is " + DeltaTestImpl.GetFromDataCount());
      if (DeltaTestImpl.GetToDataCount() != 1)
        Assert.Fail("After cloning, toDataCount should have been 1, is " + DeltaTestImpl.GetToDataCount());
    }
    void VerifyDeltaCountLRU()
    {
      Thread.Sleep(1000);
      if (DeltaEx.FromDataCount != 8)
      {
        Util.Log("DeltaEx.FromDataCount = " + DeltaEx.FromDataCount);
        Util.Log("DeltaEx.FromDeltaCount = " + DeltaEx.FromDeltaCount);
        Assert.Fail("Count should have been 8. 6 for common put and two when pulled from database and deserialized");
      }
      if (DeltaEx.FromDeltaCount != 1)
      {
        Util.Log("DeltaEx.FromDeltaCount = " + DeltaEx.FromDeltaCount);
        Assert.Fail("Count should have been 1");
      }
      DeltaEx.FromDataCount = 0;
      DeltaEx.FromDeltaCount = 0;
    }
    void VerifyCqDeltaCount()
    {
      // Wait for Cq event processing in listener
      Thread.Sleep(1000);
      if (myCqListener.GetDeltaCount() != 1)
      {
        Assert.Fail("Delta from CQ event does not have expected value");
      }
      if (myCqListener.GetValueCount() != 1)
      {
        Assert.Fail("Value from CQ event is incorrect");
      }
    }
    void VerifyExpirationDeltaCount()
    {
      Thread.Sleep(1000);
      if (DeltaEx.FromDataCount != 2)
        Assert.Fail("Count should have been 2.");
      if (DeltaEx.FromDeltaCount != 0)
        Assert.Fail("Count should have been 0.");
      DeltaEx.FromDataCount = 0;
      DeltaEx.FromDeltaCount = 0;
    }
    void runNotificationWithDelta(bool usePools)
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_with_delta.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC1");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS5", 1);

      if (usePools)
      {
        m_client1.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, true);
        m_client1.Call(createRegionAndAttachPool, "DistRegionAck", "__TEST_POOL1__", true);

        m_client2.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, true);
        m_client2.Call(createRegionAndAttachPool, "DistRegionAck", "__TEST_POOL1__", true);
      }
      else
      {
        m_client1.Call(CacheHelper.InitConfig, CacheHelper.Endpoints, 0);
        m_client1.Call(CreateRegion, "DistRegionAck", true, true);
      
        m_client2.Call(CacheHelper.InitConfig, CacheHelper.Endpoints, 0);
        m_client2.Call(CreateRegion, "DistRegionAck", true, true);
      }

      m_client2.Call(registerClassCl2);

      m_client1.Call(DoNotificationWithDelta);
      m_client2.Call(VerifyDeltaCount);
      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      CacheHelper.StopJavaLocator(1);
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runNotificationWithDefaultCloning(bool usePools)
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_with_delta_test_impl.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC1");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS5", 1);

      if (usePools)
      {
        m_client1.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, true);
        m_client1.Call(createRegionAndAttachPool, "DistRegionAck", "__TEST_POOL1__", true);

        m_client2.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, true);
        m_client2.Call(createRegionAndAttachPool, "DistRegionAck", "__TEST_POOL1__", true);
      }
      else
      {
        m_client1.Call(CacheHelper.InitConfig, CacheHelper.Endpoints, 0);
        m_client1.Call(CreateRegion, "DistRegionAck", true, true);

        m_client2.Call(CacheHelper.InitConfig, CacheHelper.Endpoints, 0);
        m_client2.Call(CreateRegion, "DistRegionAck", true, true);
      }

      m_client1.Call(registerClassDeltaTestImpl);
      m_client2.Call(registerClassDeltaTestImpl);

      m_client1.Call(DoNotificationWithDefaultCloning);
      m_client2.Call(VerifyCloning);
      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      CacheHelper.StopJavaLocator(1);
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runNotificationWithDeltaWithOverFlow(bool usePools)
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_with_delta.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC1");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);

      if (usePools)
      {
        m_client1.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, true);
        m_client1.Call(createLRURegionAndAttachPool, "DistRegionAck", "__TEST_POOL1__");

        m_client2.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, true);
        m_client2.Call(createLRURegionAndAttachPool, "DistRegionAck", "__TEST_POOL1__");
      }
      else
      {
        m_client1.Call(CacheHelper.InitConfig, CacheHelper.Endpoints, 0);
        m_client1.Call(CreateRegion, "DistRegionAck");

        m_client2.Call(CacheHelper.InitConfig, CacheHelper.Endpoints, 0);
        m_client2.Call(CreateOverflowRegion, "DistRegionAck", (uint)3);
      }
        
      m_client2.Call(registerClassCl2);

      m_client1.Call(DoNotificationWithDeltaLRU);
      m_client2.Call(VerifyDeltaCountLRU);
      m_client1.Call(Close);
      m_client2.Call(Close);
      CacheHelper.StopJavaServer(1);
      CacheHelper.StopJavaLocator(1);
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runCqWithDelta(bool usePools)
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_with_delta_test_impl.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC1");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS5", 1);
      if (usePools)
      {
        m_client1.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, true);
        m_client1.Call(createRegionAndAttachPool, "DistRegionAck", "__TEST_POOL1__");
        
        m_client2.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, true);
        m_client2.Call(createRegionAndAttachPool, "DistRegionAck", "__TEST_POOL1__");
      }
      else
      {
        m_client1.Call(CacheHelper.InitConfig, CacheHelper.Endpoints, 0);
        m_client1.Call(CreateRegion, "DistRegionAck");

        m_client2.Call(CacheHelper.InitConfig, CacheHelper.Endpoints, 0);
        m_client2.Call(CreateRegion, "DistRegionAck", true);
      }

      m_client1.Call(registerClassDeltaTestImpl);
      m_client2.Call(registerClassDeltaTestImpl);
      m_client2.Call(registerCq, usePools);

      m_client1.Call(DoCqWithDelta);
      m_client2.Call(VerifyCqDeltaCount);
      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      CacheHelper.StopJavaLocator(1);
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runExpirationWithDelta(bool usePools)
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_with_delta.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC1");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS5", 1);

      if (usePools)
      {
        m_client1.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, true);
        m_client1.Call(createRegionAndAttachPool, "DistRegionAck", "__TEST_POOL1__");

        m_client2.Call(createPool, "__TEST_POOL1__", (string)null, CacheHelper.Locators, (string)null, 0, true);
        m_client2.Call(createExpirationRegion, "DistRegionAck", "__TEST_POOL1__");
      }
      else
      {
        m_client1.Call(CacheHelper.InitConfig, CacheHelper.Endpoints, 0);
        m_client1.Call(CreateRegion, "DistRegionAck");

        m_client2.Call(CacheHelper.InitConfig, CacheHelper.Endpoints, 0);
        m_client2.Call(createExpirationRegion, "DistRegionAck");
      }

      m_client2.Call(registerClassCl2);

      m_client1.Call(DoExpirationWithDelta);
      m_client2.Call(VerifyExpirationDeltaCount);
      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      CacheHelper.StopJavaLocator(1);
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    #region Tests

    [Test]
    public void PutWithDeltaAD()
    {
      runDeltaWithAppdomian(true);
      runDeltaWithAppdomian(false);
    }

    [Test]
    public void PutWithDelta()
    {
      runPutWithDelta(true);
      runPutWithDelta(false);
    }

    [Test]
    public void NotificationWithDelta()
    {
      runNotificationWithDelta(true);
      runNotificationWithDelta(false);
    }

    [Test]
    public void NotificationWithDefaultCloning()
    {
      runNotificationWithDefaultCloning(true);
      runNotificationWithDefaultCloning(false);
    }

    [Test]
    public void NotificationWithDeltaWithOverFlow()
    {
      runNotificationWithDeltaWithOverFlow(true);
      runNotificationWithDeltaWithOverFlow(false);
    }

    [Test]
    public void CqWithDelta()
    {
      runCqWithDelta(true);
      runCqWithDelta(false);
    }
    
    [Test]
    public void ExpirationWithDelta()
    {
      runExpirationWithDelta(true);
      runExpirationWithDelta(false);
    }

    #endregion
  }
}

