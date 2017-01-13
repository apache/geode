//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Collections;
using System.Collections.ObjectModel;
using System.IO;
using System.Threading;
using PdxTests;
using System.Reflection;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;

  
  [TestFixture]
  [Category("group4")]
  [Category("unicast_only")]
  [Category("generics")]
   class ThinClientPdxTests : ThinClientRegionSteps
  {
     static bool m_useWeakHashMap = false;
    #region Private members

     private UnitProcess m_client1, m_client2, m_client3, m_client4;

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      m_client4 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3, m_client4 };
      //return new ClientBase[] { m_client1, m_client2 };
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
      try {
        m_client1.Call(DestroyRegions);
        m_client2.Call(DestroyRegions);
        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
      finally {
        CacheHelper.StopJavaServers();
        CacheHelper.StopJavaLocators();
      }
      base.EndTest();
    }

    void cleanup()
    { 
      {
        CacheHelper.SetExtraPropertiesFile(null);
        if (m_clients != null)
        {
          foreach (ClientBase client in m_clients)
          {
            try
            {
              client.Call(CacheHelper.Close);
            }
            catch (System.Runtime.Remoting.RemotingException)
            {
            }
            catch (System.Net.Sockets.SocketException)
            {
            }
          }
        }
        CacheHelper.Close();
      }
    }

    void runDistOps()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateNonExistentRegion, CacheHelper.Locators);
      m_client1.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepTwo (pool locators) complete.");

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      m_client1.Call(CheckServerKeys);
      m_client1.Call(StepFive, true);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSix, true);
      Util.Log("StepSix complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void PutAndVerifyPdxInGet()
    {
      Serializable.RegisterPdxType(PdxType.CreateDeserializable);

      Region region0 = CacheHelper.GetVerifyRegion<object,object>(m_regionNames[0]);

      region0[1] = new PdxType();

      PdxType pRet = (PdxType)region0[1];
      checkPdxInstanceToStringAtServer(region0);

      Assert.AreEqual(CacheHelper.DCache.GetPdxReadSerialized(), false, "Pdx read serialized property should be false.");

      //Statistics chk for Pdx.
      StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
      StatisticsType type = factory.FindType("CachePerfStats");
      if (type != null) {
        Statistics rStats = factory.FindFirstStatisticsByType(type);
        if (rStats != null) {
          Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
          Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
          Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
          Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
          Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"), 
            "Total pdxDeserializations should be equal to Total pdxSerializations.");
          Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"), 
            "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
        }
      }
    }

     void VerifyGetOnly()
     {
       Serializable.RegisterPdxType(PdxType.CreateDeserializable);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       
       PdxType pRet = (PdxType)region0[1];
       checkPdxInstanceToStringAtServer(region0);

       //Statistics chk for Pdx.
       StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
       StatisticsType type = factory.FindType("CachePerfStats");
       if (type != null) {
         Statistics rStats = factory.FindFirstStatisticsByType(type);
         if (rStats != null) {
           Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
           Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
           Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
           Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
           Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
             "Total pdxDeserializations should be greater than Total pdxSerializations.");
           Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
             "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
         }
       }
     }

    void PutAndVerifyVariousPdxTypes()
    {
      Serializable.RegisterPdxType(PdxTypes1.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes2.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes3.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes4.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes5.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes6.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes7.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes8.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes9.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTests.PortfolioPdx.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTests.PositionPdx.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTests.AllPdxTypes.Create);


      Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

      {
        PdxTypes1 p1 = new PdxTypes1();
        region0[11] = p1;
        PdxTypes1 pRet = (PdxTypes1)region0[11];
        Assert.AreEqual(p1, pRet);
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
      }

      {
        PdxTypes2 p2 = new PdxTypes2();
        region0[12] = p2;
        PdxTypes2 pRet2 = (PdxTypes2)region0[12];
        Assert.AreEqual(p2, pRet2);
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
      }

      {
        PdxTypes3 p3 = new PdxTypes3();
        region0[13] = p3;
        PdxTypes3 pRet3 = (PdxTypes3)region0[13];
        Assert.AreEqual(p3, pRet3);
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
      }

      {
        PdxTypes4 p4 = new PdxTypes4();
        region0[14] = p4;
        PdxTypes4 pRet4 = (PdxTypes4)region0[14];
        Assert.AreEqual(p4, pRet4);
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
      }

      {
        PdxTypes5 p5 = new PdxTypes5();
        region0[15] = p5;
        PdxTypes5 pRet5 = (PdxTypes5)region0[15];
        Assert.AreEqual(p5, pRet5);
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
      }

      {
        PdxTypes6 p6 = new PdxTypes6();
        region0[16] = p6;
        PdxTypes6 pRet6 = (PdxTypes6)region0[16];
        Assert.AreEqual(p6, pRet6);
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
      }

      {
        PdxTypes7 p7 = new PdxTypes7();
        region0[17] = p7;
        PdxTypes7 pRet7 = (PdxTypes7)region0[17];
        Assert.AreEqual(p7, pRet7);
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
      }

      {
        PdxTypes8 p8 = new PdxTypes8();
        region0[18] = p8;
        PdxTypes8 pRet8 = (PdxTypes8)region0[18];
        Assert.AreEqual(p8, pRet8);
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
      }
      {
        PdxTypes9 p9 = new PdxTypes9();
        region0[19] = p9;
        PdxTypes9 pRet9 = (PdxTypes9)region0[19];
        Assert.AreEqual(p9, pRet9);
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
      }

      {
        PortfolioPdx pf = new PortfolioPdx(1001, 10);
        region0[20] = pf;
        PortfolioPdx retpf = (PortfolioPdx)region0[20];
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
        //Assert.AreEqual(p9, pRet9);
      }

      {
        PortfolioPdx pf = new PortfolioPdx(1001, 10, new string[] { "one", "two", "three" });
        region0[21] = pf;
        PortfolioPdx retpf = (PortfolioPdx)region0[21];
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
        //Assert.AreEqual(p9, pRet9);
      }
      {
        PdxTypes10 p10 = new PdxTypes10();
        region0[22] = p10;
        PdxTypes10 pRet10 = (PdxTypes10)region0[22];
        Assert.AreEqual(p10, pRet10);
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
      }
      {
        AllPdxTypes apt = new AllPdxTypes(true);
        region0[23] = apt;
        AllPdxTypes aptRet = (AllPdxTypes)region0[23];
        Assert.AreEqual(apt, aptRet);
        checkPdxInstanceToStringAtServer(region0);
        //Statistics chk for Pdx.
        StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
        StatisticsType type = factory.FindType("CachePerfStats");
        if (type != null) {
          Statistics rStats = factory.FindFirstStatisticsByType(type);
          if (rStats != null) {
            Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
            Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
            Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
            Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
            Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
              "Total pdxDeserializations should be equal to Total pdxSerializations.");
            Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
              "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
          }
        }
      }
    }

     void VerifyVariousPdxGets()
     {
       Serializable.RegisterPdxType(PdxTypes1.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes2.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes3.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes4.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes5.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes6.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes7.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes8.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes9.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTests.PortfolioPdx.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTests.PositionPdx.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTests.AllPdxTypes.Create);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       {
         PdxTypes1 p1 = new PdxTypes1();
         PdxTypes1 pRet = (PdxTypes1)region0[11];
         Assert.AreEqual(p1, pRet);
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }

       {
         PdxTypes2 p2 = new PdxTypes2();
         PdxTypes2 pRet2 = (PdxTypes2)region0[12];
         Assert.AreEqual(p2, pRet2);
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }

       {
         PdxTypes3 p3 = new PdxTypes3();
         PdxTypes3 pRet3 = (PdxTypes3)region0[13];
         Assert.AreEqual(p3, pRet3);
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }

       {
         PdxTypes4 p4 = new PdxTypes4();
         PdxTypes4 pRet4 = (PdxTypes4)region0[14];
         Assert.AreEqual(p4, pRet4);
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }

       {
         PdxTypes5 p5 = new PdxTypes5();
         PdxTypes5 pRet5 = (PdxTypes5)region0[15];
         Assert.AreEqual(p5, pRet5);
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }

       {
         PdxTypes6 p6 = new PdxTypes6();
         PdxTypes6 pRet6 = (PdxTypes6)region0[16];
         Assert.AreEqual(p6, pRet6);
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }

       {
         PdxTypes7 p7 = new PdxTypes7();
         PdxTypes7 pRet7 = (PdxTypes7)region0[17];
         Assert.AreEqual(p7, pRet7);
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }

       {
         PdxTypes8 p8 = new PdxTypes8();
         PdxTypes8 pRet8 = (PdxTypes8)region0[18];
         Assert.AreEqual(p8, pRet8);
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }
       {
         PdxTypes9 p9 = new PdxTypes9();
         PdxTypes9 pRet9 = (PdxTypes9)region0[19];
         Assert.AreEqual(p9, pRet9);
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }       
       {
         PortfolioPdx retpf = (PortfolioPdx)region0[20];
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }
       {
         PortfolioPdx retpf = (PortfolioPdx)region0[21];
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }
       {
         PdxTypes10 p10 = new PdxTypes10();
         PdxTypes10 pRet10 = (PdxTypes10)region0[22];
         Assert.AreEqual(p10, pRet10);
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }
       {
         AllPdxTypes apt = new AllPdxTypes(true);
         AllPdxTypes aptRet = (AllPdxTypes)region0[23];
         Assert.AreEqual(apt, aptRet);
         checkPdxInstanceToStringAtServer(region0);
         //Statistics chk for Pdx.
         StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
         StatisticsType type = factory.FindType("CachePerfStats");
         if (type != null) {
           Statistics rStats = factory.FindFirstStatisticsByType(type);
           if (rStats != null) {
             Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
             Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
             Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
             Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
             Assert.Greater(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
               "Total pdxDeserializations should be greater than Total pdxSerializations.");
             Assert.Greater(rStats.GetLong((string)"pdxDeserializedBytes"), rStats.GetLong((string)"pdxSerializedBytes"),
               "Total pdxDeserializedBytes should be greater than Total pdxSerializationsBytes");
           }
         }
       }
     }

     void checkPdxInstanceToStringAtServer(Region region)
     {
       bool retVal = (bool)region["success"];
       Assert.IsTrue(retVal);
     }

     private void DoputAndVerifyClientName()
     {
       //CacheableString cVal = new CacheableString(new string('A', 1024));
       Region region = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       string cVal = new string('A', 5);
         
       Util.Log("Putting key = key-0");
       region["key-0"] = cVal;
       Util.Log("Put Operation done successfully");

       //Verify the Client Name.
       string cReceivedName = region["clientName1"] as string;
       Util.Log(" DoputAndVerifyClientName Received Client Name = {0} ", cReceivedName);
       Assert.AreEqual(cReceivedName.Equals("Client-1"), true, "Did not find the expected value.");    
     }

     private void DoGetAndVerifyClientName()
     {
       Region region = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       string cReceivedName = region["clientName2"] as string; ;
       Util.Log("Get Operation done successfully");

       //Verify the Client Name.
       Util.Log(" DoGetAndVerifyClientName Received Client Name = {0} ", cReceivedName);
       Assert.AreEqual(cReceivedName.Equals("Client-2"), true, "Did not find the expected value.");
     }

     public void ConfigClient1AndCreateRegions_Pool(string[] regionNames,
        string locators, string poolName, bool clientNotification, bool ssl, bool cachingEnable)
     {
       //Configure Client "name" for Client-1
       Properties<string, string> props = Properties<string, string>.Create<string, string>();
       props.Insert("name", "Client-1");
       CacheHelper.InitConfig(props);

       CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[0], true, cachingEnable,
          null, locators, poolName, clientNotification, ssl, false);
       CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[1], false, cachingEnable,
           null, locators, poolName, clientNotification, ssl, false);
       m_regionNames = regionNames;

     }

     public void ConfigClient2AndCreateRegions_Pool(string[] regionNames,
       string locators, string poolName, bool clientNotification, bool ssl, bool cachingEnable)
     {
       //Configure Client "name" for Client-2
       Properties<string, string> props = Properties<string, string>.Create<string, string>();
       props.Insert("name", "Client-2");
       CacheHelper.InitConfig(props);

       CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[0], true, cachingEnable,
          null, locators, poolName, clientNotification, ssl, false);
       CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[1], false, cachingEnable,
           null, locators, poolName, clientNotification, ssl, false);
       m_regionNames = regionNames;
     }

     void runtestForBug866()
     {


        CacheHelper.SetupJavaServers(true, "cacheserverPdx.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

      //Client Notification and local caching enabled for clients
      m_client1.Call(ConfigClient1AndCreateRegions_Pool, RegionNames, CacheHelper.Locators, "__TESTPOOL1_", true, false, true);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(ConfigClient2AndCreateRegions_Pool, RegionNames, CacheHelper.Locators, "__TESTPOOL1_", true, false, true);
      Util.Log("StepTwo (pool locators) complete.");

       m_client1.Call(DoputAndVerifyClientName);
       Util.Log("StepThree complete.");

       m_client2.Call(DoGetAndVerifyClientName);
       Util.Log("StepFour complete.");

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }//END:: testBug866

    void runPdxDistOps()
    {

      CacheHelper.SetupJavaServers(true, "cacheserverPdx.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
      Util.Log("StepTwo (pool locators) complete.");

      m_client1.Call(PutAndVerifyPdxInGet);
      Util.Log("StepThree complete.");

      m_client2.Call(VerifyGetOnly);
      Util.Log("StepFour complete.");

      m_client1.Call(PutAndVerifyVariousPdxTypes);
      Util.Log("StepFive complete.");

      m_client2.Call(VerifyVariousPdxGets);
      Util.Log("StepSeven complete.");
      
      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      //Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

     void VerifyDataOutputAdvance()
     {
       Serializable.RegisterPdxType(MyClass.Create);
       Serializable.RegisterPdxType(MyClasses.Create);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       MyClasses mcs = new MyClasses("1", 1000);

       region0[1] = mcs;

       object ret = region0[1];

       Assert.AreEqual(mcs, ret);
     }

     void runPdxDistOps2()
     {


        CacheHelper.SetupJavaServers(true, "cacheserverPdxSerializer.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

        m_client1.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepOne (pool locators) complete.");

        m_client2.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepTwo (pool locators) complete.");


       m_client1.Call(VerifyDataOutputAdvance);
       Util.Log("StepThree complete.");

    
       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

    void PutAndVerifyNestedPdxInGet()
    {
      Serializable.RegisterPdxType(NestedPdx.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes1.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes2.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes3.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes4.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes5.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes6.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes7.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTypes8.CreateDeserializable);

      Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      NestedPdx np = new NestedPdx();
      region0[1] = np;

      NestedPdx pRet = (NestedPdx)region0[1];

      Assert.AreEqual(np, pRet);
    }

     void VerifyNestedGetOnly()
     {
       Serializable.RegisterPdxType(NestedPdx.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes1.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes2.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes3.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes4.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes5.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes6.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes7.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTypes8.CreateDeserializable);


       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       NestedPdx orig = new NestedPdx();
       NestedPdx pRet = (NestedPdx)region0[1];

       Assert.AreEqual(orig, pRet);
     }

     void runNestedPdxOps()
     {


        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

        m_client1.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepOne (pool locators) complete.");

        m_client2.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepTwo (pool locators) complete.");


       m_client1.Call(PutAndVerifyNestedPdxInGet);
       Util.Log("StepThree complete.");

       m_client2.Call(VerifyNestedGetOnly);
       Util.Log("StepFour complete.");
       
       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     void PutAndVerifyPdxInIGFSInGet()
     {
       try
       {
         Serializable.RegisterTypeGeneric(PdxTests.PdxInsideIGFSerializable.CreateDeserializable);
         Serializable.RegisterPdxType(NestedPdx.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes1.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes2.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes3.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes4.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes5.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes6.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes7.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes8.CreateDeserializable);
       }
       catch (Exception )
       { 
       }

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       PdxInsideIGFSerializable np = new PdxInsideIGFSerializable();
       region0[1] = np;

       PdxInsideIGFSerializable pRet = (PdxInsideIGFSerializable)region0[1];

       Assert.AreEqual(np, pRet);
     }

     void VerifyPdxInIGFSGetOnly()
     {
       try
       {
         Serializable.RegisterTypeGeneric(PdxTests.PdxInsideIGFSerializable.CreateDeserializable);
         Serializable.RegisterPdxType(NestedPdx.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes1.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes2.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes3.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes4.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes5.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes6.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes7.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes8.CreateDeserializable);
       }
       catch (Exception )
       { }


       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       PdxInsideIGFSerializable orig = new PdxInsideIGFSerializable();
       PdxInsideIGFSerializable pRet = (PdxInsideIGFSerializable)region0[1];

       Assert.AreEqual(orig, pRet);
     }

     void runPdxInIGFSOps()
     {


        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

        m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepOne (pool locators) complete.");

        m_client2.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepTwo (pool locators) complete.");

       m_client1.Call(PutAndVerifyPdxInIGFSInGet);
       Util.Log("StepThree complete.");

       m_client2.Call(VerifyPdxInIGFSGetOnly);
       Util.Log("StepFour complete.");

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
 
       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     void JavaPutGet_LinedListType()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       
       //Do some put to invike attached listener,
       region0[1] = 123;

       //Get
       int value = (int)region0[1];
       //Util.Log("JavaPutGet_LinedListType value received = " + value);
       
       //verify that listener methods have been called.
       Assert.IsTrue((bool)region0["success"]);

       //LinkedList validation
       LinkedList<Object> myList1 = new LinkedList<Object>();
       myList1.AddFirst("Manan");
       myList1.AddLast("Nishka");

       //get the JSON document (as PdxInstance) that has been put from java in attached cacheListener code.
       IPdxInstance ret = (IPdxInstance)region0["jsondoc1"];
       LinkedList<Object> linkedList = (LinkedList<Object>)ret.GetField("kids");
       
       //verify sizes
       Assert.AreEqual((linkedList.Count == myList1.Count), true, " LinkedList size should be equal.");

       LinkedList<Object>.Enumerator e1 = linkedList.GetEnumerator();
       LinkedList<Object>.Enumerator e2 = myList1.GetEnumerator();
             
       //verify content of LinkedList
       while (e1.MoveNext() && e2.MoveNext())
       {
         //Util.Log("JavaPutGet_LinedListType Kids = " + e1.Current);
         PdxType.GenericValCompare(e1.Current, e2.Current);
       }
        
        Util.Log("successfully completed JavaPutGet_LinedListType");
     }
      
     void JavaPutGet()
     {
       try
       {
         Serializable.RegisterPdxType(PdxTests.PdxType.CreateDeserializable);         
       }
       catch (Exception )
       {
       }

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       PdxType np = new PdxType();
       region0[1] = np;

       PdxType pRet = (PdxType)region0[1];

       //Assert.AreEqual(np, pRet);

       Assert.IsTrue((bool)region0["success"]);
     }

     void JavaGet()
     {
       try
       {
         Serializable.RegisterPdxType(PdxTests.PdxType.CreateDeserializable);
       }
       catch (Exception )
       {
       }

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       
       PdxType np = new PdxType();
       
       PdxType pRet = (PdxType)region0[1];

       PdxType putFromjava = (PdxType)region0["putFromjava"];
     }

     void runJavaInterOpsWithLinkedListType()
     {
 

        CacheHelper.SetupJavaServers(true, "cacheserverForPdx.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");

        m_client1.Call(CreateTCRegions_Pool_PDXWithLL, RegionNames,
              CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("Clinet-1 CreateTCRegions_Pool_PDXWithLL (pool with locator) completed.");

        m_client1.Call(JavaPutGet_LinedListType);
        Util.Log("JavaPutGet_LinedListType complete.");

       
        m_client1.Call(Close);
        Util.Log("Client 1 closed");

        CacheHelper.StopJavaServer(1);
        Util.Log("Cacheserver 1 stopped.");

        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
     }

     void runJavaInteroperableOps()
     {

        CacheHelper.SetupJavaServers(true, "cacheserverForPdx.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

        m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepOne (pool locators) complete.");

        m_client2.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepTwo (pool locators) complete.");


       m_client1.Call(JavaPutGet);
       Util.Log("StepThree complete.");

       m_client2.Call(JavaGet);
       Util.Log("StepFour complete.");

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
 
       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     void putallAndGetallPdx()
     {
       try
       {
         Serializable.RegisterTypeGeneric(PdxTests.PdxInsideIGFSerializable.CreateDeserializable);
         Serializable.RegisterPdxType(NestedPdx.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes1.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes2.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes3.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes4.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes5.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes6.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes7.CreateDeserializable);
         Serializable.RegisterPdxType(PdxTypes8.CreateDeserializable);
       }
       catch (Exception )
       { }

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       IDictionary<object, object> all = new Dictionary<object, object>();

       PdxTypes1 p1 = new PdxTypes1();
       PdxTypes2 p2 = new PdxTypes2();
       PdxTypes3 p3 = new PdxTypes3();
       PdxTypes4 p4 = new PdxTypes4();
       PdxTypes5 p5 = new PdxTypes5();
       PdxTypes6 p6 = new PdxTypes6();
       PdxTypes7 p7 = new PdxTypes7();
       PdxTypes8 p8 = new PdxTypes8();

       all.Add(21, p1);
       all.Add(22, p2);
       all.Add(23, p3);
       all.Add(24, p4);
       all.Add(25, p5);
       all.Add(26, p6);
       all.Add(27, p7);
       all.Add(28, p8);
       //all.Add(p1, "Gemstone");
       //all.Add(p2, p1);
       region0.PutAll(all);
       
       
       /*
        
       all.Clear();
       all.Add(p1, "Gemstone");
       region0.PutAll(all);
       
       all.Clear();
       all.Add(p2, p1);
       region0.PutAll(all);
         */
       ICollection<object> keys = new List<object>();
       IDictionary<object, object> getall = new Dictionary<object, object>();

       keys.Add(21);
       keys.Add(22);
       keys.Add(23);
       keys.Add(24);
       keys.Add(25);
       keys.Add(26);
       keys.Add(27);
       keys.Add(28);
       //keys.Add(p1);
       //keys.Add(p2);
       region0.GetAll(keys, getall, null);
       foreach (KeyValuePair<object, object> kv in all)
       {
         object key = kv.Key;
         Util.Log("putall keys "+ key.GetType() + " : " + key);
       }
       //IEnumerator<KeyValuePair<object, object>> ie = getall.GetEnumerator();
       foreach (KeyValuePair<object, object> kv in getall)
       {
         object key = kv.Key;
         if (key != null)
           Util.Log("got key " + key.GetType() + " : " + key);
         else
           Util.Log("got NULL key ");
         object origVal = all[key];
         Assert.AreEqual(kv.Value, origVal);
       }

       

       //Statistics chk for Pdx.
       StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
       StatisticsType type = factory.FindType("CachePerfStats");
       if (type != null) {
         Statistics rStats = factory.FindFirstStatisticsByType(type);
         if (rStats != null) {
           Util.Log("pdxSerializations {0} ", rStats.GetInt((string)"pdxSerializations"));
           Util.Log("pdxDeserializations = {0} ", rStats.GetInt((string)"pdxDeserializations"));
           Util.Log("pdxSerializedBytes = {0} ", rStats.GetLong((string)"pdxSerializedBytes"));
           Util.Log("pdxDeserializedBytes = {0} ", rStats.GetLong((string)"pdxDeserializedBytes"));
           Assert.AreEqual(rStats.GetInt((string)"pdxDeserializations"), rStats.GetInt((string)"pdxSerializations"),
             "Total pdxDeserializations should be equal to Total pdxSerializations.");
           Assert.AreEqual(rStats.GetLong((string)"pdxSerializedBytes"), rStats.GetLong((string)"pdxDeserializedBytes"),
             "Total pdxDeserializedBytes should be equal to Total pdxSerializationsBytes");
         }
       }
     }

     
     void runPutAllGetAllOps()
     {


        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");
      
       m_client1.Call(putallAndGetallPdx);
       Util.Log("StepThree complete.");

       m_client2.Call(putallAndGetallPdx);
       Util.Log("StepFour complete.");
      
       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }
     void LocalOpsStep()
     {
         try
         {
             Serializable.RegisterTypeGeneric(PdxTests.PdxInsideIGFSerializable.CreateDeserializable);
             Serializable.RegisterPdxType(NestedPdx.CreateDeserializable);
             Serializable.RegisterPdxType(PdxTypes1.CreateDeserializable);
             Serializable.RegisterPdxType(PdxTypes2.CreateDeserializable);
             Serializable.RegisterPdxType(PdxTypes3.CreateDeserializable);
             Serializable.RegisterPdxType(PdxTypes4.CreateDeserializable);
             Serializable.RegisterPdxType(PdxTypes5.CreateDeserializable);
             Serializable.RegisterPdxType(PdxTypes6.CreateDeserializable);

         }
         catch (Exception)
         { }
         Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
         IRegion<object, object> localregion = region0.GetLocalView();
          

         PdxTypes1 p1 = new PdxTypes1();
         string x = "";
         localregion.Add(p1, x);
         object val = localregion[p1];
         //object val = region0[p1];
         val = localregion[p1];
         val = localregion[p1];
         Assert.IsTrue(val.Equals(x), "value should be equal");
         Assert.IsTrue(localregion.Remove(new KeyValuePair<Object, Object>(p1, x)), "Result of remove should be true, as this value null exists locally.");
         Assert.IsFalse(localregion.ContainsKey(p1), "containsKey should be false");
         try
         {
             localregion[p1] = null;
             Assert.Fail("Expected IllegalArgumentException here for put");
         }
         catch (IllegalArgumentException)
         {
             Util.Log("Got Expected IllegalArgumentException");
         }

         localregion[p1] = 1;
         localregion.Invalidate(p1);
         try
         {
             object retVal = localregion[p1];
         }
         catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
         {
             Util.Log("Got expected KeyNotFoundException exception");
         }
         Assert.IsFalse(localregion.Remove(new KeyValuePair<Object, Object>(p1, 1)), "Result of remove should be false, as this value does not exists locally.");
         Assert.IsTrue(localregion.ContainsKey(p1), "containsKey should be true");
         localregion[p1] = 1;
         Assert.IsTrue(localregion.Remove(p1), "Result of remove should be true, as this value exists locally.");
         Assert.IsFalse(localregion.ContainsKey(p1), "containsKey should be false");

         PdxTypes2 p2 = new PdxTypes2();
         localregion.Add(p2, 1);
         object intVal1 = localregion[p2]; // local get work for pdx object as key but it wont work with caching enable. Throws KeyNotFoundException.
         Assert.IsTrue(intVal1.Equals(1), "intVal should be 1.");
         
         PdxTypes3 p3 = new PdxTypes3();
         localregion.Add(p3, "testString");
         if (localregion.ContainsKey(p3))
         {
             object strVal1 = localregion[p3];
             Assert.IsTrue(strVal1.Equals("testString"), "strVal should be testString.");
         }

         try
         {
             if (localregion.ContainsKey(p3))
             {
                 localregion.Add(p3, 11);
                 Assert.Fail("Expected EntryExistException here");
             }
         }
         catch (EntryExistsException)
         {
             Util.Log(" Expected EntryExistsException exception thrown by localCreate");
         }

         PdxTypes4 p4 = new PdxTypes4();
         localregion.Add(p4, p1);
         object objVal1 = localregion[p4];
         Assert.IsTrue(objVal1.Equals(p1), "valObject and objVal should match.");
         Assert.IsTrue(localregion.Remove(new KeyValuePair<Object, Object>(p4, p1)), "Result of remove should be true, as this value exists locally.");
         Assert.IsFalse(localregion.ContainsKey(p4), "containsKey should be false");
         localregion[p4] = p1;
         Assert.IsTrue(localregion.Remove(p4), "Result of remove should be true, as this value exists locally.");
         Assert.IsFalse(localregion.ContainsKey(p4), "containsKey should be false");

         PdxTypes5 p5 = new PdxTypes5();

         //object cval = region0[p1]; //this will only work when caching is enable else throws KeyNotFoundException
         
         localregion.Clear();

     }
     void runLocalOps()
     {

       CacheHelper.SetupJavaServers(true, "cacheserver.xml");
       CacheHelper.StartJavaLocator(1, "GFELOC");
       Util.Log("Locator 1 started.");
       CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

       m_client1.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching false*/);
       Util.Log("StepOne (pool locators) complete.");


       m_client1.Call(LocalOpsStep);
       Util.Log("localOps complete.");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }
     Assembly m_pdxVesionOneAsm;
     Assembly m_pdxVesionTwoAsm;

     #region "Version Fisrt will be here PdxType1"
     void initializePdxAssemblyOne(bool useWeakHashmap)
     {
       m_pdxVesionOneAsm = Assembly.LoadFrom("PdxVersion1Lib.dll");
       
       Serializable.RegisterPdxType(registerPdxTypeOne);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes1");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
       m_useWeakHashMap = useWeakHashmap;
     }

     IPdxSerializable registerPdxTypeOne()
     {
       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes1");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }

     void initializePdxAssemblyTwo(bool useWeakHashmap)
     {
       m_pdxVesionTwoAsm = Assembly.LoadFrom("PdxVersion2Lib.dll");

       Serializable.RegisterPdxType(registerPdxTypeTwo);
       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes1");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
       m_useWeakHashMap = useWeakHashmap;
     }
     IPdxSerializable registerPdxTypeTwo()
     {
       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes1");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }
    
     void putAtVersionOne11(bool useWeakHashmap)
     {
       initializePdxAssemblyOne(useWeakHashmap);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes1");
       object np = pt.InvokeMember("PdxTypes1", BindingFlags.CreateInstance , null, null, null);
       region0[1] = np;

       object pRet = region0[1];

       Console.WriteLine( np.ToString());
       Console.WriteLine( pRet.ToString());

       bool isEqual = np.Equals(pRet);
       Assert.IsTrue(isEqual);

     }

     void getPutAtVersionTwo12(bool useWeakHashmap)
     {
       initializePdxAssemblyTwo(useWeakHashmap);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes1");
       object np = pt.InvokeMember("PdxTypes1", BindingFlags.CreateInstance , null, null, null);

       object pRet = (object)region0[1];

       Console.WriteLine(np.ToString());
       Console.WriteLine(pRet.ToString());

       bool isEqual = np.Equals(pRet);

       Assert.IsTrue(isEqual);

       region0[1] = pRet;
     }

      public void getPutAtVersionOne13()
      {
        Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

        Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes1");
        object np = pt.InvokeMember("PdxTypes1", BindingFlags.CreateInstance, null, null, null);        

        object pRet = region0[1];

        Console.WriteLine(np.ToString());
        Console.WriteLine(pRet.ToString());
        bool isEqual = np.Equals(pRet);
        Assert.IsTrue(isEqual);

        region0[1] = pRet;
      }
       
       public void getPutAtVersionTwo14()
       {
         Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

         Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes1");
         object np = pt.InvokeMember("PdxTypes1", BindingFlags.CreateInstance, null, null, null);

         object pRet = (object)region0[1];

         Console.WriteLine(np.ToString());
         Console.WriteLine(pRet.ToString());
         bool isEqual = np.Equals(pRet);

         Assert.IsTrue(isEqual);

         region0[1] = pRet;
       }
       public void getPutAtVersionOne15()
       {
         Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

         Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes1");
         object np = pt.InvokeMember("PdxTypes1", BindingFlags.CreateInstance, null, null, null);         

         object pRet = region0[1];
         Console.WriteLine(np.ToString());
         Console.WriteLine(pRet.ToString());
         bool isEqual = np.Equals(pRet);
         Assert.IsTrue(isEqual);

         region0[1] = pRet;
         if (m_useWeakHashMap == false)
         {
           Assert.AreEqual(Generic.Internal.PdxTypeRegistry.testNumberOfPreservedData(), 0);
         }
         else
         {
           Assert.IsTrue(Generic.Internal.PdxTypeRegistry.testNumberOfPreservedData() > 0); 
         }
       }

     public void getPutAtVersionTwo16()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes1");
       object np = pt.InvokeMember("PdxTypes1", BindingFlags.CreateInstance, null, null, null);

       object pRet = (object)region0[1];

       Console.WriteLine(np.ToString());
       Console.WriteLine(pRet.ToString());
       bool isEqual = np.Equals(pRet);

       Assert.IsTrue(isEqual);

       region0[1] = pRet;

       if (m_useWeakHashMap == false)
       {
         Assert.AreEqual(Generic.Internal.PdxTypeRegistry.testNumberOfPreservedData(), 0);
       }
       else
       {
         //it has extra fields, so no need to preserve data
         Assert.IsTrue(Generic.Internal.PdxTypeRegistry.testNumberOfPreservedData() == 0); 
       }
     }

     #endregion
     void runBasicMergeOps()
     {

        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");

       m_client1.Call(putAtVersionOne11, m_useWeakHashMap);
       Util.Log("StepThree complete.");

       m_client2.Call(getPutAtVersionTwo12, m_useWeakHashMap);
       Util.Log("StepFour complete.");

       m_client1.Call(getPutAtVersionOne13);
       Util.Log("StepFive complete.");

       m_client2.Call(getPutAtVersionTwo14);
       Util.Log("StepSix complete.");

       for (int i = 0; i < 10; i++)
       {
         m_client1.Call(getPutAtVersionOne15);
         Util.Log("StepSeven complete." + i);

         m_client2.Call(getPutAtVersionTwo16);
         Util.Log("StepEight complete." + i);
       }

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }


     void initializePdxAssemblyOnePS(bool useWeakHashmap)
     {
       m_pdxVesionOneAsm = Assembly.LoadFrom("PdxVersion1Lib.dll");

       
       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.TestDiffTypePdxS");

       //object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
       m_useWeakHashMap = useWeakHashmap;
     }

     void putFromVersion1_PS(bool useWeakHashmap)
     {
       //local cache is on
       initializePdxAssemblyOnePS(useWeakHashmap);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.TestDiffTypePdxS");
       object np = pt.InvokeMember("Create", BindingFlags.InvokeMethod, null, null, null);

       Serializable.RegisterPdxSerializer((IPdxSerializer)np);

       //created new object
       np = pt.InvokeMember("TestDiffTypePdxS", BindingFlags.CreateInstance, null, null, new object[] { true });

       Type keytype = m_pdxVesionOneAsm.GetType("PdxVersionTests.TestKey");
       object key = keytype.InvokeMember("TestKey", BindingFlags.CreateInstance, null, null, new object[] { "key-1" });

       region0[key] = np;

       object pRet = region0[key];

       Console.WriteLine(np.ToString());
       Console.WriteLine(pRet.ToString());

       bool isEqual = np.Equals(pRet);
       Assert.IsTrue(isEqual);

       //this should come from local caching
       pRet = region0.GetLocalView()[key];

       Assert.IsNotNull(pRet);

       region0.GetLocalView().Invalidate(key);
       bool isKNFE = false;
       try
       {
         pRet = region0.GetLocalView()[key];
       }
       catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException )
       {
         isKNFE = true;
       }

       Assert.IsTrue(isKNFE);

       pRet = region0[key];

       isEqual = np.Equals(pRet);
       Assert.IsTrue(isEqual);

       region0.GetLocalView().Remove(key);

     }

     void initializePdxAssemblyTwoPS(bool useWeakHashmap)
     {
       m_pdxVesionTwoAsm = Assembly.LoadFrom("PdxVersion2Lib.dll");


       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.TestDiffTypePdxS");

       //object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
       m_useWeakHashMap = useWeakHashmap;
     }

     void putFromVersion2_PS(bool useWeakHashmap)
     {
       initializePdxAssemblyTwoPS(useWeakHashmap);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.TestDiffTypePdxS");

       object np = pt.InvokeMember("Create", BindingFlags.InvokeMethod, null, null, null);

       Serializable.RegisterPdxSerializer((IPdxSerializer)np);

       np = pt.InvokeMember("TestDiffTypePdxS", BindingFlags.CreateInstance, null, null, new object[] { true });

       Type keytype = m_pdxVesionTwoAsm.GetType("PdxVersionTests.TestKey");
       object key = keytype.InvokeMember("TestKey", BindingFlags.CreateInstance, null, null, new object[] { "key-1" });

       region0[key] = np;

       object pRet = region0[key];

       Console.WriteLine(np.ToString());
       Console.WriteLine(pRet.ToString());

       bool isEqual = np.Equals(pRet);
       Assert.IsTrue(isEqual);

       object key2 = keytype.InvokeMember("TestKey", BindingFlags.CreateInstance, null, null, new object[] { "key-2" });
       region0[key2] = np;
     }


     void getputFromVersion1_PS()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.TestDiffTypePdxS");
       object np = pt.InvokeMember("TestDiffTypePdxS", BindingFlags.CreateInstance, null, null, new object[] { true });


       Type keytype = m_pdxVesionOneAsm.GetType("PdxVersionTests.TestKey");
       object key = keytype.InvokeMember("TestKey", BindingFlags.CreateInstance, null, null, new object[] { "key-1" });


       object pRet = region0[key];

       Assert.IsTrue(np.Equals(pRet));

       //get then put.. this should merge data back
       region0[key] = pRet;

       object key2 = keytype.InvokeMember("TestKey", BindingFlags.CreateInstance, null, null, new object[] { "key-2" });

       pRet = region0[key2];

       Assert.IsTrue(np.Equals(pRet));

       //get then put.. this should Not merge data back
       region0[key2] = np;

     }

     void getAtVersion2_PS()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.TestDiffTypePdxS");
       object np = pt.InvokeMember("TestDiffTypePdxS", BindingFlags.CreateInstance, null, null, new object[] { true });


       Type keytype = m_pdxVesionTwoAsm.GetType("PdxVersionTests.TestKey");
       object key = keytype.InvokeMember("TestKey", BindingFlags.CreateInstance, null, null, new object[] { "key-1" });

       bool gotexcep = false;
       try
       {
         object r = region0.GetLocalView()[key];
       }
       catch (Exception )
       {
         gotexcep = true;
       }
       Assert.IsTrue(gotexcep);

       object pRet = region0[key];

       Console.WriteLine(np.ToString());
       Console.WriteLine(pRet.ToString());

       bool isEqual = np.Equals(pRet);
       Assert.IsTrue(isEqual);

       object key2 = keytype.InvokeMember("TestKey", BindingFlags.CreateInstance, null, null, new object[] { "key-2" });

       np = pt.InvokeMember("TestDiffTypePdxS", BindingFlags.CreateInstance, null, null, new object[] { true });

       pRet = region0[key2];

       Console.WriteLine(np.ToString());
       Console.WriteLine(pRet.ToString());

       Assert.IsTrue(!np.Equals(pRet));
     }

     void runBasicMergeOpsWithPdxSerializer()
     {


        CacheHelper.SetupJavaServers(true, "cacheserverPdxSerializer.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

        m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching true*/);
        Util.Log("StepOne (pool locators) complete.");

        m_client2.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepTwo (pool locators) complete.");


       m_client1.Call(putFromVersion1_PS, m_useWeakHashMap);
       Util.Log("StepOne complete.");

       m_client2.Call(putFromVersion2_PS, m_useWeakHashMap);
       Util.Log("StepTwo complete.");

       m_client1.Call(getputFromVersion1_PS);
       Util.Log("Stepthree complete.");

       m_client2.Call(getAtVersion2_PS);
       Util.Log("StepFour complete.");

       m_client1.Call(dinitPdxSerializer);
       m_client2.Call(dinitPdxSerializer);

       //m_client1.Call(getPutAtVersionOne13);
       //Util.Log("StepFive complete.");

       //m_client2.Call(getPutAtVersionTwo14);
       //Util.Log("StepSix complete.");

       //for (int i = 0; i < 10; i++)
       //{
       //  m_client1.Call(getPutAtVersionOne15);
       //  Util.Log("StepSeven complete." + i);

       //  m_client2.Call(getPutAtVersionTwo16);
       //  Util.Log("StepEight complete." + i);
       //}

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     #region Basic merge three PDxType2

     void initializePdxAssemblyOne2(bool useWeakHashmap)
     {
       m_pdxVesionOneAsm = Assembly.LoadFrom("PdxVersion1Lib.dll");

       Serializable.RegisterPdxType(registerPdxTypeOne2);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes2");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
     }

     IPdxSerializable registerPdxTypeOne2()
     {
       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes2");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }

     void initializePdxAssemblyTwo2(bool useWeakHashmap)
     {
       m_pdxVesionTwoAsm = Assembly.LoadFrom("PdxVersion2Lib.dll");

       Serializable.RegisterPdxType(registerPdxTypeTwo2);
       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes2");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
     }
     IPdxSerializable registerPdxTypeTwo2()
     {
       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes2");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }

    public void putAtVersionOne21(bool useWeakHashmap)
    {
      initializePdxAssemblyOne2(useWeakHashmap);

      Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

      Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes2");
      object np = pt.InvokeMember("PdxTypes2", BindingFlags.CreateInstance, null, null, null);
      region0[1] = np;

      object pRet = region0[1];

      Console.WriteLine(np.ToString());
      Console.WriteLine(pRet.ToString());

      bool isEqual = np.Equals(pRet);
      Assert.IsTrue(isEqual);

      pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.Pdx1");
      object pdx1 = pt.InvokeMember("Pdx1", BindingFlags.CreateInstance, null, null, null);

      for (int i = 1000; i < 1010; i++) {
        region0[i] = pdx1;
      }
    }

     public void getPutAtVersionTwo22(bool useWeakHashmap)
    {
      initializePdxAssemblyTwo2(useWeakHashmap);
      Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

      Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes2");
      object np = pt.InvokeMember("PdxTypes2", BindingFlags.CreateInstance, null, null, null);

      object pRet = (object)region0[1];

      Console.WriteLine(np.ToString());
      Console.WriteLine(pRet.ToString());

      bool isEqual = np.Equals(pRet);

      Assert.IsTrue(isEqual);

      region0[1] = pRet;

      pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.Pdx1");
      object pdx1 = pt.InvokeMember("Pdx1", BindingFlags.CreateInstance, null, null, null);

      for (int i = 1000; i < 1010; i++)
      {
        object ret = region0[i];
      }
    }
     public void getPutAtVersionOne23()
    {
      Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

      Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes2");
      object np = pt.InvokeMember("PdxTypes2", BindingFlags.CreateInstance, null, null, null);

      object pRet = (object)region0[1];

      Console.WriteLine(np.ToString());
      Console.WriteLine(pRet.ToString());
      bool isEqual = np.Equals(pRet);

      Assert.IsTrue(isEqual);

      region0[1] = pRet;
    }

     public void getPutAtVersionTwo24()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes2");
       object np = pt.InvokeMember("PdxTypes2", BindingFlags.CreateInstance, null, null, null);

       object pRet = (object)region0[1];

       Console.WriteLine(np.ToString());
       Console.WriteLine(pRet.ToString());

       bool isEqual = np.Equals(pRet);

       Assert.IsTrue(isEqual);

       region0[1] = pRet;
     }

     void runBasicMergeOps2()
     {


        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

        m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepOne (pool locators) complete.");

        m_client2.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepTwo (pool locators) complete.");


       m_client1.Call(putAtVersionOne21, m_useWeakHashMap);
       Util.Log("StepThree complete.");

       m_client2.Call(getPutAtVersionTwo22, m_useWeakHashMap);

       for (int i = 0; i < 10; i++)
       {
         m_client1.Call(getPutAtVersionOne23);
         m_client2.Call(getPutAtVersionTwo24);

         Util.Log("step complete " + i);
       }
       
       
       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }
     #endregion

     #region Basic merge three PDxType3

     void initializePdxAssemblyOne3(bool useWeakHashmap)
     {
       m_pdxVesionOneAsm = Assembly.LoadFrom("PdxVersion1Lib.dll");

       Serializable.RegisterPdxType(registerPdxTypeOne3);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes3");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
     }

     IPdxSerializable registerPdxTypeOne3()
     {
       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes3");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }

     void initializePdxAssemblyTwo3(bool useWeakHashmap)
     {
       m_pdxVesionTwoAsm = Assembly.LoadFrom("PdxVersion2Lib.dll");

       Serializable.RegisterPdxType(registerPdxTypeTwo3);
       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes3");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
     }
     IPdxSerializable registerPdxTypeTwo3()
     {
       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes3");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }

     public void putAtVersionOne31(bool useWeakHashmap)
     {
       initializePdxAssemblyOne3(useWeakHashmap);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes3");
       object np = pt.InvokeMember("PdxTypes3", BindingFlags.CreateInstance, null, null, null);
       region0[1] = np;

       object pRet = region0[1];

       Console.WriteLine(np.ToString());
       Console.WriteLine(pRet.ToString());

       bool isEqual = np.Equals(pRet);
       Assert.IsTrue(isEqual);
     }

     public void getPutAtVersionTwo32(bool useWeakHashmap)
     {
       initializePdxAssemblyTwo3(useWeakHashmap);
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes3");
       object np = pt.InvokeMember("PdxTypes3", BindingFlags.CreateInstance, null, null, null);

       object pRet = (object)region0[1];

       Console.WriteLine(np.ToString());
       Console.WriteLine(pRet.ToString());

       bool isEqual = np.Equals(pRet);

       Assert.IsTrue(isEqual);

       region0[1] = pRet;
     }
     public void getPutAtVersionOne33()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes3");
       object np = pt.InvokeMember("PdxTypes3", BindingFlags.CreateInstance, null, null, null);

       object pRet = (object)region0[1];

       Console.WriteLine(np.ToString());
       Console.WriteLine(pRet.ToString());

       bool isEqual = np.Equals(pRet);

       Assert.IsTrue(isEqual);

       region0[1] = pRet;
     }

     public void getPutAtVersionTwo34()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes3");
       object np = pt.InvokeMember("PdxTypes3", BindingFlags.CreateInstance, null, null, null);

       object pRet = (object)region0[1];

       Console.WriteLine(np.ToString());
       Console.WriteLine(pRet.ToString());

       bool isEqual = np.Equals(pRet);

       Assert.IsTrue(isEqual);

       region0[1] = pRet;
     }

     void runBasicMergeOps3()
     {


         CacheHelper.SetupJavaServers(true, "cacheserver.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");
 

       m_client1.Call(putAtVersionOne31, m_useWeakHashMap);
       Util.Log("StepThree complete.");

       m_client2.Call(getPutAtVersionTwo32, m_useWeakHashMap);

       for (int i = 0; i < 10; i++)
       {
         m_client1.Call(getPutAtVersionOne33);
         m_client2.Call(getPutAtVersionTwo34);

         Util.Log("step complete " + i);
       }


       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }
     #endregion

     #region "Version two will first here"

     void initializePdxAssemblyOneR1(bool useWeakHashmap)
     {
       m_pdxVesionOneAsm = Assembly.LoadFrom("PdxVersion1Lib.dll");

       Serializable.RegisterPdxType(registerPdxTypeOneR1);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesR1");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });

     }

     IPdxSerializable registerPdxTypeOneR1()
     {
       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesR1");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }


     void initializePdxAssemblyTwoR1(bool useWeakHashmap)
     {
       m_pdxVesionTwoAsm = Assembly.LoadFrom("PdxVersion2Lib.dll");

       Serializable.RegisterPdxType(registerPdxTypeTwoR1);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesR1");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
     }


     IPdxSerializable registerPdxTypeTwoR1()
     {
       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesR1");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }

     public void putAtVersionTwo1(bool useWeakHashmap)
      {
        initializePdxAssemblyTwoR1(useWeakHashmap);

        Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

        Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesR1");
        object np = pt.InvokeMember("PdxTypesR1", BindingFlags.CreateInstance, null, null, null);
        region0[1] = np;

        object pRet = region0[1];

        Console.WriteLine(np);
        Console.WriteLine(pRet);

        Assert.AreEqual(np, pRet);
      }

     public void getPutAtVersionOne2(bool useWeakHashmap)
       {
         initializePdxAssemblyOneR1(useWeakHashmap);

         Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

         Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesR1");
         object np = pt.InvokeMember("PdxTypesR1", BindingFlags.CreateInstance, null, null, null);
         
         object pRet = region0[1];

         Console.WriteLine(np);
         Console.WriteLine(pRet);

         bool retVal = np.Equals(pRet);
         Assert.IsTrue(retVal);

         region0[1] = pRet;
       }
       
       public void getPutAtVersionTwo3()
       {
         Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

         Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesR1");
         object np = pt.InvokeMember("PdxTypesR1", BindingFlags.CreateInstance, null, null, null);
         
         object pRet = region0[1];//get

         Console.WriteLine(np);
         Console.WriteLine(pRet);

         bool retVal = np.Equals(pRet);
         Assert.IsTrue(retVal);

         region0[1] = pRet;
       }
       
       public void getPutAtVersionOne4()
       {
         Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

         Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesR1");
         object np = pt.InvokeMember("PdxTypesR1", BindingFlags.CreateInstance, null, null, null);

         object pRet = region0[1];

         Console.WriteLine(np);
         Console.WriteLine(pRet);

         bool retVal = np.Equals(pRet);
         Assert.IsTrue(retVal);

         region0[1] = pRet;
       }
       
       public void getPutAtVersionTwo5()
       {
         Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

         Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesR1");
         object np = pt.InvokeMember("PdxTypesR1", BindingFlags.CreateInstance, null, null, null);
         
         object pRet = region0[1];
         
         Console.WriteLine(np);
         Console.WriteLine(pRet);

         bool retVal = np.Equals(pRet);
         Assert.IsTrue(retVal);

         region0[1] = pRet;
       }

     public void getPutAtVersionOne6()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesR1");
       object np = pt.InvokeMember("PdxTypesR1", BindingFlags.CreateInstance, null, null, null);

       object pRet = region0[1];
       
       Console.WriteLine(np);
       Console.WriteLine(pRet);

       bool retVal = np.Equals(pRet);
       Assert.IsTrue(retVal);

       region0[1] = pRet;
     }
     #endregion

     void runBasicMergeOpsR1()
     {


         CacheHelper.SetupJavaServers(true, "cacheserver.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");


       m_client2.Call(putAtVersionTwo1, m_useWeakHashMap);       
       Util.Log("StepThree complete.");

       m_client1.Call(getPutAtVersionOne2, m_useWeakHashMap);       
       Util.Log("StepFour complete.");

       m_client2.Call(getPutAtVersionTwo3);
       Util.Log("StepFive complete.");

       m_client1.Call(getPutAtVersionOne4);
       Util.Log("StepSix complete.");

       for (int i = 0; i < 10; i++)
       {
         m_client2.Call(getPutAtVersionTwo5);
         Util.Log("StepSeven complete." + i);

         m_client1.Call(getPutAtVersionOne6);
         Util.Log("StepEight complete." + i);
       }

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     IPdxSerializable registerPdxUIV1()
     {
       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesIgnoreUnreadFields");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }

     void initializePdxUIAssemblyOne(bool useWeakHashmap)
     {
       m_pdxVesionOneAsm = Assembly.LoadFrom("PdxVersion1Lib.dll");

       Serializable.RegisterPdxType(registerPdxUIV1);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesIgnoreUnreadFields");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
     }

     public void putV1PdxUI(bool useWeakHashmap)
     {
       initializePdxUIAssemblyOne(useWeakHashmap);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesIgnoreUnreadFields");
       object np = pt.InvokeMember("PdxTypesIgnoreUnreadFields", BindingFlags.CreateInstance, null, null, null);
       object pRet = region0[1];
       region0[1] = pRet;

       

       Console.WriteLine(np);
       Console.WriteLine(pRet);

       //Assert.AreEqual(np, pRet);
     }

     IPdxSerializable registerPdxUIV2()
     {
       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesIgnoreUnreadFields");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }

     void initializePdxUIAssemblyTwo(bool useWeakHashmap)
     {
       m_pdxVesionTwoAsm = Assembly.LoadFrom("PdxVersion2Lib.dll");

       Serializable.RegisterPdxType(registerPdxUIV2);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesIgnoreUnreadFields");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
     }

     public void putV2PdxUI(bool useWeakHashmap)
     {
       initializePdxUIAssemblyTwo(useWeakHashmap);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesIgnoreUnreadFields");
       object np = pt.InvokeMember("PdxTypesIgnoreUnreadFields", BindingFlags.CreateInstance, null, null, null);
       region0[1] = np;

       object pRet = region0[1];

       Console.WriteLine(np);
       Console.WriteLine(pRet);

       Assert.AreEqual(np, pRet);
       region0[1] = pRet;
       Console.WriteLine(" " + pRet);
     }

     public void getV2PdxUI()
     {

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesIgnoreUnreadFields");
       object np = pt.InvokeMember("PdxTypesIgnoreUnreadFields", BindingFlags.CreateInstance, null, null, null);       

       object pRet = region0[1];

       Console.WriteLine(np);
       Console.WriteLine(pRet);

       Assert.AreEqual(np, pRet);
     }
    void runPdxIgnoreUnreadFieldTest()
    {
        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

        m_client1.Call(CreateTCRegions_Pool_PDX, RegionNames,
            CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepOne (pool locators) complete.");

        m_client2.Call(CreateTCRegions_Pool_PDX, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
        Util.Log("StepTwo (pool locators) complete.");


      m_client2.Call(putV2PdxUI, m_useWeakHashMap);
      Util.Log("StepThree complete.");

      m_client1.Call(putV1PdxUI, m_useWeakHashMap);
      Util.Log("StepFour complete.");

      m_client2.Call(getV2PdxUI);
      Util.Log("StepFive complete.");
     
      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      //Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

     #region PdxType2 Version two first

     void initializePdxAssemblyOneR2(bool useWeakHashmap)
     {
       m_pdxVesionOneAsm = Assembly.LoadFrom("PdxVersion1Lib.dll");

       Serializable.RegisterPdxType(registerPdxTypeOneR2);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesR2");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });

     }

     IPdxSerializable registerPdxTypeOneR2()
     {
       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesR2");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }


     void initializePdxAssemblyTwoR2(bool useWeakHashmap)
     {
       m_pdxVesionTwoAsm = Assembly.LoadFrom("PdxVersion2Lib.dll");

       Serializable.RegisterPdxType(registerPdxTypeTwoR2);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesR2");

       object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
     }


     IPdxSerializable registerPdxTypeTwoR2()
     {
       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesR2");

       object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

       return (IPdxSerializable)ob;
     }

     public void putAtVersionTwoR21(bool useWeakHashmap)
     {
       initializePdxAssemblyTwoR2(useWeakHashmap);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesR2");
       object np = pt.InvokeMember("PdxTypesR2", BindingFlags.CreateInstance, null, null, null);
       region0[1] = np;

       object pRet = region0[1];

       Console.WriteLine(np);
       Console.WriteLine(pRet);

       bool isEqual = np.Equals(pRet);

       Assert.IsTrue(isEqual);
     }

     public void getPutAtVersionOneR22(bool useWeakHashmap)
     {
       initializePdxAssemblyOneR2(useWeakHashmap);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesR2");
       object np = pt.InvokeMember("PdxTypesR2", BindingFlags.CreateInstance, null, null, null);

       object pRet = region0[1];

       Console.WriteLine(np);
       Console.WriteLine(pRet);

       bool retVal = np.Equals(pRet);
       Assert.IsTrue(retVal);

       region0[1] = pRet;
     }
       
     public void getPutAtVersionTwoR23()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypesR2");
       object np = pt.InvokeMember("PdxTypesR2", BindingFlags.CreateInstance, null, null, null);

       object pRet = region0[1];//get

       Console.WriteLine(np);
       Console.WriteLine(pRet);

       bool retVal = np.Equals(pRet);
       Assert.IsTrue(retVal);

       region0[1] = pRet;
     }

     public void getPutAtVersionOneR24()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypesR2");
       object np = pt.InvokeMember("PdxTypesR2", BindingFlags.CreateInstance, null, null, null);

       object pRet = region0[1];

       Console.WriteLine(np);
       Console.WriteLine(pRet);

       bool retVal = np.Equals(pRet);
       Assert.IsTrue(retVal);

       region0[1] = pRet;
     }


     void runBasicMergeOpsR2()
     {
         CacheHelper.SetupJavaServers(true, "cacheserver.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");


       m_client2.Call(putAtVersionTwoR21, m_useWeakHashMap);
       Util.Log("StepThree complete.");

       m_client1.Call(getPutAtVersionOneR22, m_useWeakHashMap);
       Util.Log("StepFour complete.");

       for (int i = 0; i < 10; i++)
       {
         m_client2.Call(getPutAtVersionTwoR23);
         Util.Log("StepFive complete.");

         m_client1.Call(getPutAtVersionOneR24);
         Util.Log("StepSix complete.");
       }
       
       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     #endregion

     public void putFromPool1()
     {
       Serializable.RegisterPdxType(PdxTypes1.CreateDeserializable);
       Util.Log("Put from pool-1 started");
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);

       region0[1] = new PdxTypes1();

       region0[2] = new PdxTests.PdxType();
       Util.Log("Put from pool-1 Completed");
     }

     public void putFromPool2()
     {
       Util.Log("Put from pool-21 started");
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[1]);

       region0[1] = new PdxTypes1();
       region0[2] = new PdxTests.PdxType();
       object ret = region0[1];
       ret = region0[2];
       Util.Log("Put from pool-2 completed");

       int pdxIds = GemStone.GemFire.Cache.Generic.Internal.PdxTypeRegistry.testGetNumberOfPdxIds();

       Assert.AreEqual(3, pdxIds);
     }

     public void runMultipleDSTest()
     {
       Util.Log("Starting runMultipleDSTest. " );

       CacheHelper.SetupJavaServers(true, "cacheserverMDS1.xml", "cacheserverMDS2.xml");
       CacheHelper.StartJavaLocator_MDS(1, "GFELOC", null, 1/*ds id is one*/);
       Util.Log("Locator 1 started.");
       CacheHelper.StartJavaLocator_MDS(2, "GFELOC2", null, 2/*ds id is one*/);
       Util.Log("Locator 2 started.");

       CacheHelper.StartJavaServerWithLocator_MDS(1, "GFECS1", 1);
       Util.Log("Server 1 started with locator 1.");

       CacheHelper.StartJavaServerWithLocator_MDS(2, "GFECS2", 2);
       Util.Log("Server 2 started with locator 2.");
       
      //client intialization 
       /*
        *  CreateTCRegion_Pool(string name, bool ack, bool caching,
      ICacheListener listener, string endpoints, string locators, string poolName, bool clientNotification, bool ssl,
      bool cloningEnabled)
        * 
        */

       m_client1.Call(CacheHelper.CreateTCRegion_Pool_MDS,
         RegionNames[0], true, false,
         CacheHelper.LocatorFirst, "__TESTPOOL1_", 
         false, false, false);

       Util.Log("StepOne (pool-1 locators) complete. " + CacheHelper.LocatorFirst);

       m_client1.Call(CacheHelper.CreateTCRegion_Pool_MDS,
         RegionNames[1], false, false,
         CacheHelper.LocatorSecond, "__TESTPOOL2_",
         false, false, false);

       Util.Log("StepTwo (pool-2 locators) complete. " + CacheHelper.LocatorSecond);


       m_client1.Call(putFromPool1);

       m_client1.Call(putFromPool2);
      
       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       //m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");
       CacheHelper.StopJavaServer(2);
       Util.Log("Cacheserver 2 stopped.");

       CacheHelper.StopJavaLocator(1);
       Util.Log("Locator 1 stopped.");
       CacheHelper.StopJavaLocator(2);
       Util.Log("Locator 2 stopped.");
       
       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
      
     }

    void initializePdxSerializer()
    {
      Serializable.RegisterPdxSerializer(new PdxSerializer());

      //Serializable.RegisterTypeForPdxSerializer(SerializePdx1.CreateDeserializable);
    }

    void doPutGetWithPdxSerializer()
    {
      Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

      for (int i = 0; i < 10; i++)
      {
        object put = new SerializePdx1(true); ;

        region0[i] = put;

        object ret = region0[i];

        Assert.AreEqual(put, ret);

        put = new SerializePdx2(true);
        region0[i + 10] = put;


        ret = region0[i + 10];

        Assert.AreEqual(put, ret);


        put = new SerializePdx3(true, i % 2);
        region0[i + 20] = put;

        ret = region0[i + 20];

        Assert.AreEqual(put, ret);

      }
    }

    void doGetWithPdxSerializerC2()
    {
      Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      SerializePdx3 sp3Even = new SerializePdx3(true, 0);
      SerializePdx3 sp3Odd = new SerializePdx3(true, 1);

      for (int i = 0; i < 10; i++)
      {
        object local = new SerializePdx1(true); ;

        object ret = region0[i];

        Assert.AreEqual(local, ret);

        ret = region0[i + 10];
        Assert.AreEqual(new SerializePdx2(true), ret);

        ret = region0[i + 20];
        if (i % 2 == 0)
        {
          Assert.AreEqual(ret, sp3Even);
          Assert.AreNotEqual(ret, sp3Odd);
        }
        else
        {
          Assert.AreEqual(ret, sp3Odd);
          Assert.AreNotEqual(ret, sp3Even);
        }
      }
    }
  
     void doQueryTest()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       for (int i = 0; i < 10; i++)
       {
         ISelectResults<object> result = region0.Query<object>("i1 = " + i);
         Util.Log(" query result size " + result.Size);
       }
       ISelectResults<object> result2 = region0.Query<object>("1 = 1");
       Util.Log(" query result size " + result2.Size);

       //private Address[] _addressArray;
       //private int arrayCount = 10;
       //private ArrayList _addressList;
       //private Address _address;
     //private Hashtable _hashTable;

       //QueryService<object, object> qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

       //Query<object> qry = qs.NewQuery("select _addressArray from /" + m_regionNames[0] + " where arrayCount = 10");
       //ISelectResults<object> results = qry.Execute();
       //Assert.Greater(results.Size, 5, "query should have result");
       //IEnumerator<object> ie = results.GetEnumerator();
       //Address[] ad;
       //while (ie.MoveNext())
       //{
       //  Address[] ar = (Address[])ie.Current;
       //  Assert.AreEqual(ar.Length, 10, "Array size should be 10");
       //}

       
     }

     void runPdxSerializerTest()
     {
       Util.Log("Starting iteration for pool locator runPdxSerializerTest");

         CacheHelper.SetupJavaServers(true, "cacheserver.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool_PDX, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool_PDX, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");


       m_client1.Call(initializePdxSerializer);
       m_client2.Call(initializePdxSerializer);
       Util.Log("StepThree complete.");

       m_client1.Call(doPutGetWithPdxSerializer);
       Util.Log("StepFour complete.");

       m_client2.Call(doGetWithPdxSerializerC2);
       Util.Log("StepFive complete.");

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     void initializeReflectionPdxSerializer()
     {
       Serializable.RegisterPdxSerializer(new AutoSerializerEx());

       //Serializable.RegisterTypeForPdxSerializer(SerializePdx1.CreateDeserializable);
     }

     void doPutGetWithPdxSerializerR()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       for (int i = 0; i < 10; i++)
       {      
         object put = new SerializePdx1(true); ;

         region0[i] = put;

         object ret = region0[i];

         Assert.AreEqual(put, ret);

         put = new SerializePdx2(true);
         region0[i + 10] = put;


         ret = region0[i + 10];

         Assert.AreEqual(put, ret);

         put = new PdxTypesReflectionTest(true);
         region0[i + 20] = put;


         ret = region0[i + 20];

         Assert.AreEqual(put, ret);

         put = new SerializePdx3(true, i % 2);
         region0[i + 30] = put;


         ret = region0[i + 30];

         Assert.AreEqual(put, ret);

         put = new SerializePdx4(true);
         region0[i + 40] = put;


         ret = region0[i + 40];

         Assert.AreEqual(put, ret);

         object p1 = region0[i + 30];
         object p2 = region0[i + 40];

         Assert.AreNotEqual(p1, p2, "This should NOt be equals");

         PdxFieldTest pft = new PdxFieldTest(true);
         region0[i + 50] = pft;
         ret = region0[i + 50];

         Assert.AreNotEqual(pft, ret);

         pft.NotInclude = "default_value";
         Assert.AreEqual(pft, ret);
       }
       IDictionary<object, object> putall = new Dictionary<object, object>();
       putall.Add(100, new SerializePdx3(true, 0));
       putall.Add(200, new SerializePdx3(true, 1));
       putall.Add(300, new SerializePdx4(true));
       region0.PutAll(putall);
     }

     void doGetWithPdxSerializerC2R()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       for (int i = 0; i < 10; i++)
       {
         object local = new SerializePdx1(true); ;

         object ret = region0[i];

         Assert.AreEqual(local, ret);

         ret = region0[i + 10];
         Assert.AreEqual(new SerializePdx2(true), ret);

         ret = region0[i + 20];
         Assert.AreEqual(new PdxTypesReflectionTest(true), ret);

         SerializePdx3 sp3Odd = new SerializePdx3(true, 1);
         SerializePdx3 sp3Even = new SerializePdx3(true, 0);

         ret = region0[i + 30];

         if (i % 2 == 0)
         {
           Assert.AreEqual(sp3Even, ret);
           Assert.AreNotEqual(sp3Odd, ret);
         }
         else
         {
           Assert.AreEqual(sp3Odd, ret);
           Assert.AreNotEqual(sp3Even, ret);
         }

         ret = region0[i + 40];
         SerializePdx4 sp4 = new SerializePdx4(true);
         Assert.AreEqual(sp4, ret);
         Console.WriteLine(sp4 + "===" + ret);

         object p1 = region0[i + 30];
         object p2 = region0[i + 40];

         Assert.AreNotEqual(p1, p2, "This should NOt be equal");
       }

       IDictionary<object, object> getall = new Dictionary<object, object>();
       ICollection<object> keys = new List<object>();
       keys.Add(100);
       keys.Add(200);
       keys.Add(300);
       //putall.Add(100, new SerializePdx3(true, 0));
       //putall.Add(200, new SerializePdx3(true, 1));
       //putall.Add(300, new SerializePdx4(true));
       region0.GetAll(keys, getall, null);

       Assert.AreEqual(getall[100], new SerializePdx3(true, 0));
       Assert.AreEqual(getall[200], new SerializePdx3(true, 1));
       Assert.AreEqual(getall[300], new SerializePdx4(true));
     }
     void runReflectionPdxSerializerTest()
     {
       Util.Log("Starting iteration for pool locator runPdxSerializerTest");

        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool_PDX, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool_PDX, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");

       m_client1.Call(initializeReflectionPdxSerializer);
       m_client2.Call(initializeReflectionPdxSerializer);
       Util.Log("StepThree complete.");

       m_client1.Call(doPutGetWithPdxSerializerR);
       Util.Log("StepFour complete.");

       m_client2.Call(doGetWithPdxSerializerC2R);
       Util.Log("StepFive complete.");

       m_client2.Call(doQueryTest);
       Util.Log("StepSix complete.");

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     void dinitPdxSerializer()
     {
       Serializable.RegisterPdxSerializer(null);
     }

     void doPutGetWithPdxSerializerNoReg()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       for (int i = 0; i < 10; i++)
       {
         object put = new SerializePdxNoRegister(true); ;

         region0[i] = put;

         object ret = region0[i];

         Assert.AreEqual(put, ret);         
       }
     }

     void doGetWithPdxSerializerC2NoReg()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       for (int i = 0; i < 10; i++)
       {
         object local = new SerializePdxNoRegister(true); ;

         object ret = region0[i];

         Assert.AreEqual(local, ret);         
       }
     }
     void runPdxTestWithNoTypeRegister()
     {
       Util.Log("Starting iteration for pool locator runPdxTestWithNoTypeRegister");

         CacheHelper.SetupJavaServers(true, "cacheserver.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool_PDX, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool_PDX, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");


       Util.Log("StepThree complete.");

       m_client1.Call(dinitPdxSerializer);
       m_client2.Call(dinitPdxSerializer);
       m_client1.Call(doPutGetWithPdxSerializerNoReg);
       Util.Log("StepFour complete.");

       m_client2.Call(doGetWithPdxSerializerC2NoReg);
       Util.Log("StepFive complete.");

       m_client2.Call(doQueryTest);
       Util.Log("StepSix complete.");

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     void pdxPut()
     {
       Serializable.RegisterPdxType(PdxTests.PdxType.CreateDeserializable);
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       region0["pdxput"] = new PdxTests.PdxType();
       region0["pdxput2"] = new ParentPdx(1);
     }

     void getObject()
     {
       Serializable.RegisterPdxType(PdxTests.PdxType.CreateDeserializable);
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       IPdxInstance ret = (IPdxInstance)region0["pdxput"];

       Assert.AreEqual(ret.GetClassName(), "PdxTests.PdxType", "PdxInstance.GetClassName should return PdxTests.PdxType");

       PdxType pt = (PdxType)ret.GetObject();

       PdxType ptorig = new PdxType();


       Assert.AreEqual(pt, ptorig, "PdxInstance.getObject not equals original object.");

       ret = (IPdxInstance)region0["pdxput2"];
       ParentPdx pp = (ParentPdx)ret.GetObject();

       ParentPdx ppOrig = new ParentPdx(1);

       Assert.AreEqual(pp, ppOrig, "Parent pdx should be equal ");

       //Statistics chk for Pdx.
       StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
       StatisticsType type = factory.FindType("CachePerfStats");
       if (type != null) {
         Statistics rStats = factory.FindFirstStatisticsByType(type);
         if (rStats != null) {
           Util.Log("pdxInstanceDeserializations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceDeserializations"));
           Util.Log("pdxInstanceCreations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceCreations"));
           Util.Log("pdxInstanceDeserializationTime for PdxInstance getObject = {0} ", rStats.GetLong((string)"pdxInstanceDeserializationTime"));
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceDeserializations"), 2, "pdxInstanceDeserializations should be 2.");
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceCreations"), 2, "pdxInstanceCreations should be 2");
           Assert.Greater(rStats.GetLong((string)"pdxInstanceDeserializationTime"), 0, "pdxInstanceDeserializationTime should be greater than 0");
         }
       }
     }

     void verifyPdxInstanceEquals()
     {
       Serializable.RegisterPdxType(PdxTests.PdxType.CreateDeserializable);
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       IPdxInstance ret = (IPdxInstance)region0["pdxput"];
       IPdxInstance ret2 = (IPdxInstance)region0["pdxput"];

       
       Assert.AreEqual(ret, ret2, "PdxInstance equals are not matched.");

       Util.Log(ret.ToString());
       Util.Log(ret2.ToString());

       ret = (IPdxInstance)region0["pdxput2"];
       ret2 = (IPdxInstance)region0["pdxput2"];

       //Statistics chk for Pdx.
       StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
       StatisticsType type = factory.FindType("CachePerfStats");
       if (type != null) {
         Statistics rStats = factory.FindFirstStatisticsByType(type);
         if (rStats != null) {
           Util.Log("pdxInstanceDeserializations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceDeserializations"));
           Util.Log("pdxInstanceCreations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceCreations"));
           Util.Log("pdxInstanceDeserializationTime for PdxInstance getObject = {0} ", rStats.GetLong((string)"pdxInstanceDeserializationTime"));
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceDeserializations"), 2, "pdxInstanceDeserializations should be 2.");
           Assert.Greater(rStats.GetInt((string)"pdxInstanceCreations"), 2, "pdxInstanceCreations should be greater than 2");
           Assert.Greater(rStats.GetLong((string)"pdxInstanceDeserializationTime"), 0, "pdxInstanceDeserializationTime should be greater than 0");
         }
       }

       Assert.AreEqual(ret, ret2, "parent pdx equals are not matched.");
     }

     void verifyPdxInstanceHashcode()
     {
       Serializable.RegisterPdxType(PdxTests.PdxType.CreateDeserializable);
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       IPdxInstance ret = (IPdxInstance)region0["pdxput"];
       PdxType dPdxType = new PdxType();

       int pdxInstHashcode = ret.GetHashCode();
       Util.Log("pdxinstance hash code "+ pdxInstHashcode);

       int javaPdxHC = (int)region0["javaPdxHC"];

       //TODO: need to fix this is beacause Enum hashcode is different in java and .net
       //Assert.AreEqual(javaPdxHC, pdxInstHashcode, "Pdxhashcode hashcode not matched with java padx hash code.");

       //for parent pdx
       ret = (IPdxInstance)region0["pdxput2"];
       pdxInstHashcode = ret.GetHashCode();
       Assert.AreEqual(javaPdxHC, pdxInstHashcode, "Pdxhashcode hashcode not matched with java padx hash code for Parentpdx class.");

       //Statistics chk for Pdx.
       StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
       StatisticsType type = factory.FindType("CachePerfStats");
       if (type != null) {
         Statistics rStats = factory.FindFirstStatisticsByType(type);
         if (rStats != null) {
           Util.Log("pdxInstanceDeserializations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceDeserializations"));
           Util.Log("pdxInstanceCreations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceCreations"));
           Util.Log("pdxInstanceDeserializationTime for PdxInstance getObject = {0} ", rStats.GetLong((string)"pdxInstanceDeserializationTime"));
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceDeserializations"), 2, "pdxInstanceDeserializations should be 2.");
           Assert.Greater(rStats.GetInt((string)"pdxInstanceCreations"), 2, "pdxInstanceCreations should be greater than 2");
           Assert.Greater(rStats.GetLong((string)"pdxInstanceDeserializationTime"), 0, "pdxInstanceDeserializationTime should be greater than 0");
         }
       }
     }

     void accessPdxInstance()
     {
       Serializable.RegisterPdxType(PdxTests.PdxType.CreateDeserializable);
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       IPdxInstance ret = (IPdxInstance)region0["pdxput"];
       PdxType dPdxType = new PdxType();

       string retStr = (string)ret.GetField("m_string");

       Assert.AreEqual(dPdxType.PString, retStr);

       PdxType.GenericValCompare((char)ret.GetField("m_char"), dPdxType.Char);

       byte[][] baa = (byte[][])ret.GetField("m_byteByteArray");
       PdxType.compareByteByteArray(baa, dPdxType.ByteByteArray);

       PdxType.GenericCompare((char[])ret.GetField("m_charArray"), dPdxType.CharArray);

       bool bl = (bool)ret.GetField("m_bool");
       PdxType.GenericValCompare(bl, dPdxType.Bool);
       PdxType.GenericCompare((bool[])ret.GetField("m_boolArray"), dPdxType.BoolArray);

       PdxType.GenericValCompare((sbyte)ret.GetField("m_byte"), dPdxType.Byte);
       PdxType.GenericCompare((byte[])ret.GetField("m_byteArray"), dPdxType.ByteArray);
      
         
       List<object> tmpl = (List<object>)ret.GetField("m_arraylist");
       
       PdxType.compareCompareCollection(tmpl, dPdxType.Arraylist);

       IDictionary<object, object> tmpM = (IDictionary<object, object>)ret.GetField("m_map");
       if (tmpM.Count != dPdxType.Map.Count)
         throw new IllegalStateException("Not got expected value for type: " + dPdxType.Map.GetType().ToString());

       Hashtable tmpH = (Hashtable)ret.GetField("m_hashtable");

       if (tmpH.Count != dPdxType.Hashtable.Count)
         throw new IllegalStateException("Not got expected value for type: " + dPdxType.Hashtable.GetType().ToString());

       ArrayList arrAl = (ArrayList)ret.GetField("m_vector");

       if (arrAl.Count != dPdxType.Vector.Count)
         throw new IllegalStateException("Not got expected value for type: " + dPdxType.Vector.GetType().ToString());

       CacheableHashSet rmpChs = (CacheableHashSet)ret.GetField("m_chs");

       if (rmpChs.Count != dPdxType.Chs.Count)
         throw new IllegalStateException("Not got expected value for type: " + dPdxType.Chs.GetType().ToString());

       CacheableLinkedHashSet rmpClhs = (CacheableLinkedHashSet)ret.GetField("m_clhs");

       if (rmpClhs.Count != dPdxType.Clhs.Count)
         throw new IllegalStateException("Not got expected value for type: " + dPdxType.Clhs.GetType().ToString());


       PdxType.GenericValCompare((string)ret.GetField("m_string"), dPdxType.String);

       PdxType.compareData((DateTime)ret.GetField("m_dateTime"), dPdxType.DateTime);

       PdxType.GenericValCompare((double)ret.GetField("m_double"), dPdxType.Double);

       PdxType.GenericCompare((long[])ret.GetField("m_longArray"), dPdxType.LongArray);
       PdxType.GenericCompare((Int16[])ret.GetField("m_int16Array"), dPdxType.Int16Array);
       PdxType.GenericValCompare((sbyte)ret.GetField("m_sbyte"), dPdxType.Sbyte);
       PdxType.GenericCompare((byte[])ret.GetField("m_sbyteArray"), dPdxType.SbyteArray);
       PdxType.GenericCompare((string[])ret.GetField("m_stringArray"), dPdxType.StringArray);
       PdxType.GenericValCompare((Int16)ret.GetField("m_uint16"), dPdxType.Uint16);
       PdxType.GenericValCompare((int)ret.GetField("m_uint32"), dPdxType.Uint32);
       PdxType.GenericValCompare((long)ret.GetField("m_ulong"), dPdxType.Ulong);
       PdxType.GenericCompare((int[])ret.GetField("m_uint32Array"), dPdxType.Uint32Array);

       PdxType.GenericCompare((double[])ret.GetField("m_doubleArray"), dPdxType.DoubleArray);
       PdxType.GenericValCompare((float)ret.GetField("m_float"), dPdxType.Float);
       PdxType.GenericCompare((float[])ret.GetField("m_floatArray"), dPdxType.FloatArray);
       PdxType.GenericValCompare((Int16)ret.GetField("m_int16"), dPdxType.Int16);
       PdxType.GenericValCompare((Int32)ret.GetField("m_int32"), dPdxType.Int32);
       PdxType.GenericValCompare((long)ret.GetField("m_long"), dPdxType.Long);
       PdxType.GenericCompare((int[])ret.GetField("m_int32Array"), dPdxType.Int32Array);
      
       PdxType.GenericCompare((long[])ret.GetField("m_ulongArray"), dPdxType.UlongArray);
       PdxType.GenericCompare((Int16[])ret.GetField("m_uint16Array"), dPdxType.Uint16Array);

       byte[] retbA = (byte[])ret.GetField("m_byte252");
       if (retbA.Length != 252)
         throw new Exception("Array len 252 not found");

       retbA = (byte[])ret.GetField("m_byte253");
       if (retbA.Length != 253)
         throw new Exception("Array len 253 not found");

       retbA = (byte[])ret.GetField("m_byte65535");
       if (retbA.Length != 65535)
         throw new Exception("Array len 65535 not found");

       retbA = (byte[])ret.GetField("m_byte65536");
       if (retbA.Length != 65536)
         throw new Exception("Array len 65536 not found");

       pdxEnumTest ev = (pdxEnumTest)ret.GetField("m_pdxEnum");
       if(ev != dPdxType.PdxEnum)
         throw new Exception("Pdx enum is not equal");

       IPdxInstance[] addreaaPdxI = (IPdxInstance[])ret.GetField("m_address");
       Assert.AreEqual(addreaaPdxI.Length, dPdxType.AddressArray.Length);

       Assert.AreEqual(addreaaPdxI[0].GetObject(), dPdxType.AddressArray[0]);


       List<object> objArr = (List<object>)ret.GetField("m_objectArray");
       Assert.AreEqual(objArr.Count, dPdxType.ObjectArray.Count);

       Assert.AreEqual(((IPdxInstance)objArr[0]).GetObject(), dPdxType.ObjectArray[0]);

       
       ret = (IPdxInstance)region0["pdxput2"];

       IPdxInstance cpi = (IPdxInstance)ret.GetField("_childPdx");

       ChildPdx cpo = (ChildPdx)cpi.GetObject();

       Assert.AreEqual(cpo, new ChildPdx(1393), "child pdx should be equal");
     }

     void modifyPdxInstance()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       IPdxInstance newpdxins;
       IPdxInstance pdxins = (IPdxInstance)region0["pdxput"];
       
       int oldVal = (int)pdxins.GetField("m_int32");

       IWritablePdxInstance iwpi = pdxins.CreateWriter();
       
       iwpi.SetField("m_int32", oldVal + 1);
       iwpi.SetField("m_string", "change the string");
       region0["pdxput"] = iwpi;

       newpdxins = (IPdxInstance)region0["pdxput"];

       int newVal = (int)newpdxins.GetField("m_int32");

       Assert.AreEqual(oldVal + 1, newVal);

       string cStr = (string)newpdxins.GetField("m_string");
       Assert.AreEqual("change the string", cStr);

       List<object> arr = (List<object>)newpdxins.GetField("m_arraylist");

       Assert.AreEqual(arr.Count, 2);

       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_char", 'D');
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((char)newpdxins.GetField("m_char"), 'D', "Char is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");


       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_bool", false);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((bool)newpdxins.GetField("m_bool"), false, "bool is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_byte", (sbyte)0x75);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((sbyte)newpdxins.GetField("m_byte"), (sbyte)0x75, "sbyte is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_sbyte", (sbyte)0x57);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((sbyte)newpdxins.GetField("m_sbyte"), (sbyte)0x57, "sbyte is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_int16", (short)0x5678);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((Int16)newpdxins.GetField("m_int16"), (short)0x5678, "int16 is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_long", (long)0x56787878);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((long)newpdxins.GetField("m_long"), (long)0x56787878, "long is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_float", 18389.34f);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((float)newpdxins.GetField("m_float"), 18389.34f, "float is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_float", 18389.34f);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((float)newpdxins.GetField("m_float"), 18389.34f, "float is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_double", 18389.34d);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((double)newpdxins.GetField("m_double"), 18389.34d, "double is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_boolArray", new bool[] { true, false, true, false, true, true, false, true });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((bool[])newpdxins.GetField("m_boolArray"), new bool[] { true, false, true, false, true, true, false, true }, "bool array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_byteArray", new byte[] { 0x34, 0x64, 0x34, 0x64 });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((byte[])newpdxins.GetField("m_byteArray"), new byte[] { 0x34, 0x64, 0x34, 0x64 }, "byte array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_charArray", new char[] { 'c', 'v', 'c', 'v' });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((char[])newpdxins.GetField("m_charArray"), new char[] { 'c', 'v', 'c', 'v' }, "char array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       long ticks = 634460644691580000L;
       DateTime tdt = new DateTime(ticks);
       iwpi.SetField("m_dateTime", tdt);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((DateTime)newpdxins.GetField("m_dateTime"), tdt, "datetime is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_int16Array", new short[] { 0x2332, 0x4545, 0x88, 0x898 });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((Int16[])newpdxins.GetField("m_int16Array"), new short[] { 0x2332, 0x4545, 0x88, 0x898 }, "short array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_int32Array", new int[] { 23, 676868, 34343 });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((Int32[])newpdxins.GetField("m_int32Array"), new int[] { 23, 676868, 34343 }, "int32 array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_longArray", new Int64[] { 3245435, 3425435 });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((long[])newpdxins.GetField("m_longArray"), new Int64[] { 3245435, 3425435 }, "long array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_floatArray", new float[] { 232.565f, 234323354.67f });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((float[])newpdxins.GetField("m_floatArray"), new float[] { 232.565f, 234323354.67f }, "float array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_doubleArray", new double[] { 23423432d, 43242354315d });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((double[])newpdxins.GetField("m_doubleArray"), new double[] { 23423432d, 43242354315d }, "double array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       byte[][] tmpbb = new byte[][]{new byte[] {0x23},
                    new byte[]{0x34, 0x55},
                    new byte[] {0x23},
                    new byte[]{0x34, 0x55}
                    };
       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_byteByteArray", tmpbb);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       byte[][] retbb = (byte[][])newpdxins.GetField("m_byteByteArray");

       PdxType.compareByteByteArray(tmpbb, retbb);

       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_stringArray", new string[] { "one", "two", "eeeee" });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((string[])newpdxins.GetField("m_stringArray"), new string[] { "one", "two", "eeeee" }, "string array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       List<object> tl = new List<object>();
       tl.Add(new PdxType());
       tl.Add(new byte[] { 0x34, 0x55 });

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_arraylist", tl);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual(((List<object>)newpdxins.GetField("m_arraylist")).Count, tl.Count, "list<object> is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       Dictionary<object, object>  map = new Dictionary<object, object>();
       map.Add(1, new bool[] { true, false, true, false, true, true, false, true });
       map.Add(2, new string[] { "one", "two", "eeeee" });

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_map", map);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual(((Dictionary<object, object>)newpdxins.GetField("m_map")).Count, map.Count , "map is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       Hashtable hashtable = new Hashtable();
       hashtable.Add(1, new string[] { "one", "two", "eeeee" });
       hashtable.Add(2, new int[] { 23, 676868, 34343 });

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_hashtable", hashtable);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual(((Hashtable)newpdxins.GetField("m_hashtable")).Count, hashtable.Count, "hashtable is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_pdxEnum", pdxEnumTest.pdx1);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual(((pdxEnumTest)newpdxins.GetField("m_pdxEnum")), pdxEnumTest.pdx1, "pdx enum is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       ArrayList vector = new ArrayList();
       vector.Add(1);
       vector.Add(2);
       
       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_vector", vector);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual(((ArrayList)newpdxins.GetField("m_vector")).Count, vector.Count, "vector is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       CacheableHashSet chm = CacheableHashSet.Create();
       chm.Add(1);
       chm.Add("jkfdkjdsfl");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_chs", chm);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((CacheableHashSet)newpdxins.GetField("m_chs"), chm, "CacheableHashSet is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       CacheableLinkedHashSet clhs = CacheableLinkedHashSet.Create();
       clhs.Add(111);
       clhs.Add(111343);

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_clhs", clhs);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual((CacheableLinkedHashSet)newpdxins.GetField("m_clhs"), clhs, "CacheableLinkedHashSet is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       PdxTests.Address[] aa = new PdxTests.Address[2];
       for (int i = 0; i < aa.Length; i++)
       {
         aa[i] = new PdxTests.Address(i + 1, "street" + i.ToString(), "city" + i.ToString());
       }

       iwpi = pdxins.CreateWriter();

       iwpi.SetField("m_address", aa);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       IPdxInstance[] iaa = (IPdxInstance[])newpdxins.GetField("m_address");
       Assert.AreEqual(iaa.Length, aa.Length, "address array length should equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal for address array");

       List<object> oa = new List<object>();
       oa.Add(new PdxTests.Address(1, "1", "12"));
       oa.Add(new PdxTests.Address(1, "1", "12"));

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_objectArray", oa);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput"];
       Assert.AreEqual(((List<object>)newpdxins.GetField("m_objectArray")).Count, oa.Count, "Object arary is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");


       pdxins = (IPdxInstance)region0["pdxput2"];
       IPdxInstance cpi = (IPdxInstance)pdxins.GetField("_childPdx");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("_childPdx", new ChildPdx(2));
       region0["pdxput2"] = iwpi;
       newpdxins = (IPdxInstance)region0["pdxput2"];
       Console.WriteLine(pdxins);
       Console.WriteLine(newpdxins);
       Assert.AreNotEqual(pdxins, newpdxins, "parent pdx should be not equal");
       Assert.AreNotEqual(cpi, newpdxins.GetField("_childPdx"), "child pdx instance should be equal");
       Assert.AreEqual(new ChildPdx(2), ((IPdxInstance)(newpdxins.GetField("_childPdx"))).GetObject(), "child pdx instance should be equal");

       //Statistics chk for Pdx.
       StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
       StatisticsType type = factory.FindType("CachePerfStats");
       if (type != null) {
         Statistics rStats = factory.FindFirstStatisticsByType(type);
         if (rStats != null) {
           Util.Log("pdxInstanceDeserializations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceDeserializations"));
           Util.Log("pdxInstanceCreations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceCreations"));
           Util.Log("pdxInstanceDeserializationTime for PdxInstance getObject = {0} ", rStats.GetLong((string)"pdxInstanceDeserializationTime"));
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceDeserializations"), 6, "pdxInstanceDeserializations should be 6.");
           Assert.Greater(rStats.GetInt((string)"pdxInstanceCreations"), 2, "pdxInstanceCreations should be greater than 2");
           Assert.Greater(rStats.GetLong((string)"pdxInstanceDeserializationTime"), 0, "pdxInstanceDeserializationTime should be greater than 0");
         }
       }
     }

     void runPdxInstanceTest()
     {
       Util.Log("Starting iteration for pool locator runPdxInstanceTest");

         CacheHelper.SetupJavaServers(true, "cacheserver_pdxinstance_hashcode.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool_PDX2, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool_PDX2, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");


       m_client1.Call(pdxPut);
       m_client2.Call(getObject);
       m_client2.Call(verifyPdxInstanceEquals);
       m_client2.Call(verifyPdxInstanceHashcode);
       m_client2.Call(accessPdxInstance);

       m_client2.Call(modifyPdxInstance);

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     
     public void InitClientXml(string cacheXml, int serverport1, int serverport2)
     {
       CacheHelper.HOST_PORT_1 = serverport1;
       CacheHelper.HOST_PORT_2 = serverport2;
       CacheHelper.InitConfig(cacheXml);
     }

     void testReadSerializedXMLProperty()
     {
       Assert.AreEqual(CacheHelper.DCache.GetPdxReadSerialized(), true);
     }

     void putPdxWithIdentityField()
     {
       Serializable.RegisterPdxType(SerializePdx.Create);
       SerializePdx sp = new SerializePdx(true);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);

       region0[1] = sp;     
     }

     void verifyPdxIdentityField()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);

       IPdxInstance pi = (IPdxInstance)region0[1];

       Assert.AreEqual(pi.GetFieldNames().Count, 4, "number of fields should be four in SerializePdx");

       Assert.AreEqual(pi.IsIdentityField("i1"), true, "SerializePdx1.i1 should be identity field");

       Assert.AreEqual(pi.IsIdentityField("i2"), false, "SerializePdx1.i2 should NOT be identity field");

       Assert.AreEqual(pi.HasField("i1"), true, "SerializePdx1.i1 should be in PdxInstance stream");

       Assert.AreEqual(pi.HasField("i3"), false, "There is no field i3 in SerializePdx1's PdxInstance stream");

       int javaPdxHC = (int)region0["javaPdxHC"];

       Assert.AreEqual(javaPdxHC, pi.GetHashCode(), "Pdxhashcode for identity field object SerializePdx1 not matched with java pdx hash code.");

       IPdxInstance pi2 = (IPdxInstance)region0[1];

       Assert.AreEqual(pi, pi2, "Both pdx instance should equal.");

       //Statistics chk for Pdx.
       StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
       StatisticsType type = factory.FindType("CachePerfStats");
       if (type != null) {
         Statistics rStats = factory.FindFirstStatisticsByType(type);
         if (rStats != null) {
           Util.Log("pdxInstanceDeserializations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceDeserializations"));
           Util.Log("pdxInstanceCreations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceCreations"));
           Util.Log("pdxInstanceDeserializationTime for PdxInstance getObject = {0} ", rStats.GetLong((string)"pdxInstanceDeserializationTime"));
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceDeserializations"), 0, "pdxInstanceDeserializations should be 0.");
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceCreations"), 2, "pdxInstanceCreations should be 2");
           Assert.AreEqual(rStats.GetLong((string)"pdxInstanceDeserializationTime"), 0, "pdxInstanceDeserializationTime should be 0");
         }
       }
     }

     void putPdxWithNullIdentityFields()
     {
       SerializePdx sp = new SerializePdx(false); //not initialized

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);

       region0[2] = sp;   
     }

     void verifyPdxNullIdentityFieldHC()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);

       IPdxInstance pi = (IPdxInstance)region0[2];

       int javaPdxHC = (int)region0["javaPdxHC"];

       Assert.AreEqual(javaPdxHC, pi.GetHashCode(), "Pdxhashcode for identity field object SerializePdx1 not matched with java pdx hash code.");

       IPdxInstance pi2 = (IPdxInstance)region0[2];

       Assert.AreEqual(pi, pi2, "Both pdx instance should equal.");

       Dictionary<object, object> values = new Dictionary<object,object>();
       List<object> keys = new List<object>();
       keys.Add(1);
       keys.Add(2);
       region0.GetAll(keys, values, null);

       Assert.AreEqual(values.Count, 2, "Getall count should be two");

       //Statistics chk for Pdx.
       StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
       StatisticsType type = factory.FindType("CachePerfStats");
       if (type != null) {
         Statistics rStats = factory.FindFirstStatisticsByType(type);
         if (rStats != null) {
           Util.Log("pdxInstanceDeserializations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceDeserializations"));
           Util.Log("pdxInstanceCreations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceCreations"));
           Util.Log("pdxInstanceDeserializationTime for PdxInstance getObject = {0} ", rStats.GetLong((string)"pdxInstanceDeserializationTime"));
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceDeserializations"), 0, "pdxInstanceDeserializations should be 0.");
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceCreations"), 6, "pdxInstanceCreations should be 6");
           Assert.AreEqual(rStats.GetLong((string)"pdxInstanceDeserializationTime"), 0, "pdxInstanceDeserializationTime should be 0");
         }
       }
     }

     void runPdxReadSerializedTest()
     {
       Util.Log("runPdxReadSerializedTest");

       CacheHelper.SetupJavaServers(false, "cacheserver_pdxinstance_hashcode.xml");
         CacheHelper.StartJavaServer(1, "GFECS1");
    
       Util.Log("Cacheserver 1 started.");

       m_client1.Call(InitClientXml, "client_pdx.xml", CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2);
       m_client2.Call(InitClientXml, "client_pdx.xml", CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2);

       m_client1.Call(testReadSerializedXMLProperty);
       m_client2.Call(testReadSerializedXMLProperty);

       m_client1.Call(putPdxWithIdentityField);
       m_client2.Call(verifyPdxIdentityField);

       m_client1.Call(putPdxWithNullIdentityFields);
       m_client2.Call(verifyPdxNullIdentityFieldHC);

       m_client1.Call(Close);
       m_client2.Call(Close);

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
    }

    void initializePdxAssemblyForEqualTestv1()
    {
      m_pdxVesionOneAsm = Assembly.LoadFrom("PdxVersion1Lib.dll");

      Serializable.RegisterPdxType(registerPdxTypeForEqualv1);

     // Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.TestEquals");

      //object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
    }


    IPdxSerializable registerPdxTypeForEqualv1()
    {
      Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.TestEquals");

      object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

      return (IPdxSerializable)ob;
    }

    void initializePdxAssemblyForEqualTestv2()
    {
      m_pdxVesionTwoAsm= Assembly.LoadFrom("PdxVersion2Lib.dll");

      Serializable.RegisterPdxType(registerPdxTypeForEqualv2);

      // Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.TestEquals");

      //object ob = pt.InvokeMember("Reset", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, new object[] { useWeakHashmap });
    }


    IPdxSerializable registerPdxTypeForEqualv2()
    {
      Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.TestEquals");

      object ob = pt.InvokeMember("CreateDeserializable", BindingFlags.Default | BindingFlags.InvokeMethod, null, null, null);

      return (IPdxSerializable)ob;
    }

     void pdxVersion1Put()
     {
       initializePdxAssemblyForEqualTestv1();

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.TestEquals");
       
       object np = pt.InvokeMember("TestEquals", BindingFlags.CreateInstance, null, null, null);
       region0[1] = np; 
     }

     void pdxVersion2Put()
     {
       initializePdxAssemblyForEqualTestv2();

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.TestEquals");
       object np = pt.InvokeMember("TestEquals", BindingFlags.CreateInstance, null, null, null);
       region0[2] = np;
     }

     void getVersionObject()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       IPdxInstance v1 = (IPdxInstance)region0[1];
       IPdxInstance v2 = (IPdxInstance)region0[2];

       Assert.AreEqual(v1, v2, "both pdxinstance should be equal");
     }

     void runPdxVersionClassesEqualTest()
     {
       Util.Log("Starting iteration for pool locator runPdxInstanceTest");

         CacheHelper.SetupJavaServers(true, "cacheserver_pdxinstance_hashcode.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool_PDX2, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool_PDX2, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");


       m_client1.Call(pdxVersion1Put);
       m_client2.Call(pdxVersion2Put);
       m_client2.Call(getVersionObject);
       
       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     void modifyPdxInstanceAndCheckLocally()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       IPdxInstance newpdxins;
       IPdxInstance pdxins = (IPdxInstance)region0["pdxput"];

       int oldVal = (int)pdxins.GetField("m_int32");

       IWritablePdxInstance iwpi = pdxins.CreateWriter();

       iwpi.SetField("m_int32", oldVal + 1);
       iwpi.SetField("m_string", "change the string");
       region0["pdxput"] = iwpi;

       IRegion<object, object> lRegion = region0.GetLocalView();

       newpdxins = (IPdxInstance)lRegion["pdxput"];

       int newVal = (int)newpdxins.GetField("m_int32");

       Assert.AreEqual(oldVal + 1, newVal);

       string cStr = (string)newpdxins.GetField("m_string");
       Assert.AreEqual("change the string", cStr);

       List<object> arr = (List<object>)newpdxins.GetField("m_arraylist");

       Assert.AreEqual(arr.Count, 2);

       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_char", 'D');
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((char)newpdxins.GetField("m_char"), 'D', "Char is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");


       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_bool", false);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((bool)newpdxins.GetField("m_bool"), false, "bool is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_byte", (sbyte)0x75);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((sbyte)newpdxins.GetField("m_byte"), (sbyte)0x75, "sbyte is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_sbyte", (sbyte)0x57);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((sbyte)newpdxins.GetField("m_sbyte"), (sbyte)0x57, "sbyte is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_int16", (short)0x5678);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((Int16)newpdxins.GetField("m_int16"), (short)0x5678, "int16 is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_long", (long)0x56787878);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((long)newpdxins.GetField("m_long"), (long)0x56787878, "long is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_float", 18389.34f);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((float)newpdxins.GetField("m_float"), 18389.34f, "float is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_float", 18389.34f);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((float)newpdxins.GetField("m_float"), 18389.34f, "float is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_double", 18389.34d);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((double)newpdxins.GetField("m_double"), 18389.34d, "double is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_boolArray", new bool[] { true, false, true, false, true, true, false, true });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((bool[])newpdxins.GetField("m_boolArray"), new bool[] { true, false, true, false, true, true, false, true }, "bool array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_byteArray", new byte[] { 0x34, 0x64, 0x34, 0x64 });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((byte[])newpdxins.GetField("m_byteArray"), new byte[] { 0x34, 0x64, 0x34, 0x64 }, "byte array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_charArray", new char[] { 'c', 'v', 'c', 'v' });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((char[])newpdxins.GetField("m_charArray"), new char[] { 'c', 'v', 'c', 'v' }, "char array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       long ticks = 634460644691580000L;
       DateTime tdt = new DateTime(ticks);
       iwpi.SetField("m_dateTime", tdt);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((DateTime)newpdxins.GetField("m_dateTime"), tdt, "datetime is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_int16Array", new short[] { 0x2332, 0x4545, 0x88, 0x898 });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((Int16[])newpdxins.GetField("m_int16Array"), new short[] { 0x2332, 0x4545, 0x88, 0x898 }, "short array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_int32Array", new int[] { 23, 676868, 34343 });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((Int32[])newpdxins.GetField("m_int32Array"), new int[] { 23, 676868, 34343 }, "int32 array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_longArray", new Int64[] { 3245435, 3425435 });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((long[])newpdxins.GetField("m_longArray"), new Int64[] { 3245435, 3425435 }, "long array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_floatArray", new float[] { 232.565f, 234323354.67f });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((float[])newpdxins.GetField("m_floatArray"), new float[] { 232.565f, 234323354.67f }, "float array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_doubleArray", new double[] { 23423432d, 43242354315d });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((double[])newpdxins.GetField("m_doubleArray"), new double[] { 23423432d, 43242354315d }, "double array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       byte[][] tmpbb = new byte[][]{new byte[] {0x23},
                    new byte[]{0x34, 0x55},
                    new byte[] {0x23},
                    new byte[]{0x34, 0x55}
                    };
       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_byteByteArray", tmpbb);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       byte[][] retbb = (byte[][])newpdxins.GetField("m_byteByteArray");

       PdxType.compareByteByteArray(tmpbb, retbb);

       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_stringArray", new string[] { "one", "two", "eeeee" });
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((string[])newpdxins.GetField("m_stringArray"), new string[] { "one", "two", "eeeee" }, "string array is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       List<object> tl = new List<object>();
       tl.Add(new PdxType());
       tl.Add(new byte[] { 0x34, 0x55 });

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_arraylist", tl);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual(((List<object>)newpdxins.GetField("m_arraylist")).Count, tl.Count, "list<object> is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       Dictionary<object, object> map = new Dictionary<object, object>();
       map.Add(1, new bool[] { true, false, true, false, true, true, false, true });
       map.Add(2, new string[] { "one", "two", "eeeee" });

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_map", map);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual(((Dictionary<object, object>)newpdxins.GetField("m_map")).Count, map.Count, "map is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       Hashtable hashtable = new Hashtable();
       hashtable.Add(1, new string[] { "one", "two", "eeeee" });
       hashtable.Add(2, new int[] { 23, 676868, 34343 });

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_hashtable", hashtable);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual(((Hashtable)newpdxins.GetField("m_hashtable")).Count, hashtable.Count, "hashtable is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       ArrayList vector = new ArrayList();
       vector.Add(1);
       vector.Add(2);

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_vector", vector);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual(((ArrayList)newpdxins.GetField("m_vector")).Count, vector.Count, "vector is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       CacheableHashSet chm = CacheableHashSet.Create();
       chm.Add(1);
       chm.Add("jkfdkjdsfl");

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_chs", chm);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((CacheableHashSet)newpdxins.GetField("m_chs"), chm, "CacheableHashSet is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       CacheableLinkedHashSet clhs = CacheableLinkedHashSet.Create();
       clhs.Add(111);
       clhs.Add(111343);

       iwpi = pdxins.CreateWriter();
       iwpi.SetField("m_clhs", clhs);
       region0["pdxput"] = iwpi;
       newpdxins = (IPdxInstance)lRegion["pdxput"];
       Assert.AreEqual((CacheableLinkedHashSet)newpdxins.GetField("m_clhs"), clhs, "CacheableLinkedHashSet is not equal");
       Assert.AreNotEqual(pdxins, newpdxins, "PdxInstance should not be equal");

       //Statistics chk for Pdx.
       StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
       StatisticsType type = factory.FindType("CachePerfStats");
       if (type != null) {
         Statistics rStats = factory.FindFirstStatisticsByType(type);
         if (rStats != null) {
           Util.Log("pdxInstanceDeserializations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceDeserializations"));
           Util.Log("pdxInstanceCreations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceCreations"));
           Util.Log("pdxInstanceDeserializationTime for PdxInstance getObject = {0} ", rStats.GetLong((string)"pdxInstanceDeserializationTime"));
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceDeserializations"), 0, "pdxInstanceDeserializations should be 0.");
           Assert.Greater(rStats.GetInt((string)"pdxInstanceCreations"), 0, "pdxInstanceCreations should be greater than 0");
           Assert.AreEqual(rStats.GetLong((string)"pdxInstanceDeserializationTime"), 0, "pdxInstanceDeserializationTime should be 0");
         }
       }
     }

     void runPdxInstanceLocalTest()
     {
       Util.Log("Starting iteration for pool locator runPdxInstanceTest");

         CacheHelper.SetupJavaServers(true, "cacheserver_pdxinstance_hashcode.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);

         m_client1.Call(CreateTCRegions_Pool_PDX2, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool_PDX2, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");


       m_client1.Call(pdxPut);
       m_client2.Call(modifyPdxInstanceAndCheckLocally);

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     }

     public void pdxIFPutGetTest()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       PdxTests.PdxType pt = new PdxType();

       IPdxInstanceFactory pif = CacheHelper.DCache.CreatePdxInstanceFactory("PdxTests.PdxType");

       pif.WriteInt("m_int32", pt.Int32);
       pif.WriteString("m_string", pt.PString);
       pif.WriteObject("m_arraylist", pt.Arraylist);
       pif.WriteChar("m_char", pt.Char);
       pif.WriteBoolean("m_bool", pt.Bool);
       pif.WriteByte("m_sbyte", pt.Sbyte);
       pif.WriteByte("m_byte", pt.Byte);
       pif.WriteShort("m_int16", pt.Int16);
       pif.WriteByteArray("m_byteArray", pt.ByteArray);
       pif.WriteLong("m_long", pt.Long);
       pif.WriteFloat("m_float", pt.Float);
       pif.WriteDouble("m_double", pt.Double);
       pif.WriteBooleanArray("m_boolArray", pt.BoolArray);
       pif.WriteByteArray("m_sbyteArray", pt.SbyteArray);
       pif.WriteCharArray("m_charArray", pt.CharArray);
       pif.WriteDate("m_dateTime", pt.DateTime);
       pif.WriteShortArray("m_int16Array", pt.Int16Array);
       pif.WriteIntArray("m_int32Array", pt.Int32Array);
       pif.WriteLongArray("m_longArray", pt.LongArray);
       pif.WriteFloatArray("m_floatArray", pt.FloatArray);
       pif.WriteDoubleArray("m_doubleArray", pt.DoubleArray);
       pif.WriteArrayOfByteArrays("m_byteByteArray", pt.ByteByteArray);
       pif.WriteStringArray("m_stringArray", pt.StringArray);
       pif.WriteObject("m_map", pt.Map);
       pif.WriteObject("m_hashtable", pt.Hashtable);
       pif.WriteObject("m_vector", pt.Vector);
       pif.WriteObject("m_chs", pt.Chs);
       pif.WriteObject("m_clhs", pt.Clhs);
       pif.WriteInt("m_uint32", pt.Uint32);
       pif.WriteLong("m_ulong", pt.Ulong);
       pif.WriteShort("m_uint16", pt.Uint16);
       pif.WriteIntArray("m_uint32Array", pt.Uint32Array);
       pif.WriteLongArray("m_ulongArray", pt.UlongArray);
       pif.WriteShortArray("m_uint16Array", pt.Uint16Array);
       pif.WriteByteArray("m_byte252", pt.Byte252);
       pif.WriteByteArray("m_byte253", pt.Byte253);
       pif.WriteByteArray("m_byte65535", pt.Byte65535);
       pif.WriteByteArray("m_byte65536", pt.Byte65536);
       pif.WriteObject("m_pdxEnum", pt.PdxEnum);

       pif.WriteObject("m_address", pt.AddressArray);
       pif.WriteObjectArray("m_objectArray", pt.ObjectArray);

       IPdxInstance pi = pif.Create();

       Assert.AreEqual(pi.GetClassName(), "PdxTests.PdxType", "PdxInstanceFactory created PdxInstance. PdxInstance.GetClassName should return PdxTests.PdxType");

       object piObject = pi.GetObject();

       Assert.AreEqual(piObject, pt);

       region0["pi"] = pi;

       Object ret = region0["pi"];

       Assert.AreEqual(ret, pt);
       
       bool gotexcep = false;
       try
       {
         pif.Create();
       }
       catch (IllegalStateException ) { gotexcep = true; }

       Assert.IsTrue(gotexcep, "Pdx instance factory should have thrown IllegalStateException");

       ParentPdx pp = new ParentPdx(2);
       IPdxInstanceFactory if2 = CacheHelper.DCache.CreatePdxInstanceFactory(pp.GetType().FullName);
       if2.WriteInt("_parentId", pp._parentId);
       if2.WriteObject("_gender", pp._gender);
       if2.WriteString("_parentName", pp._parentName);
       if2.WriteObject("_childPdx", pp._childPdx);

       IPdxInstance ip2 = if2.Create();
       region0["pp"] = ip2;

       ret = region0["pp"];

       Assert.AreEqual(ret, pp, "parent pdx should be same");
       //Statistics chk for Pdx.
       StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
       StatisticsType type = factory.FindType("CachePerfStats");
       if (type != null) {
         Statistics rStats = factory.FindFirstStatisticsByType(type);
         if (rStats != null) {
           Util.Log("pdxInstanceDeserializations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceDeserializations"));
           Util.Log("pdxInstanceCreations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceCreations"));
           Util.Log("pdxInstanceDeserializationTime for PdxInstance getObject = {0} ", rStats.GetLong((string)"pdxInstanceDeserializationTime"));
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceDeserializations"), 1, "pdxInstanceDeserializations should be 1.");
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceCreations"), 0, "pdxInstanceCreations should be 0");
           Assert.Greater(rStats.GetLong((string)"pdxInstanceDeserializationTime"), 0, "pdxInstanceDeserializationTime should be greater than 0");
         }
       }
     }

    //this test use write field Api
     public void pdxIFPutGetTest2()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       PdxTests.PdxType pt = new PdxType();

       IPdxInstanceFactory pif = CacheHelper.DCache.CreatePdxInstanceFactory("PdxTests.PdxType");

       pif.WriteField("m_int32", pt.Int32, pt.Int32.GetType());
       pif.WriteField("m_string", pt.PString, pt.PString.GetType());
      // pif.WriteField("m_arraylist", pt.Arraylist, pt.Arraylist.GetType());
       //we treat arraylist as ObjectArray as well, so here need to differentiate it
       pif.WriteObject("m_arraylist", pt.Arraylist);
       pif.WriteField("m_char", pt.Char, pt.Char.GetType());
       pif.WriteField("m_bool", pt.Bool, pt.Bool.GetType());
       pif.WriteField("m_sbyte", pt.Sbyte, pt.Sbyte.GetType());
       pif.WriteField("m_byte", pt.Byte, pt.Byte.GetType());
       pif.WriteField("m_int16", pt.Int16, pt.Int16.GetType());
       pif.WriteField("m_byteArray", pt.ByteArray, pt.ByteArray.GetType());
       pif.WriteField("m_long", pt.Long, pt.Long.GetType());
       pif.WriteField("m_float", pt.Float, pt.Float.GetType());
       pif.WriteField("m_double", pt.Double, pt.Double.GetType());
       pif.WriteField("m_boolArray", pt.BoolArray, pt.BoolArray.GetType());
       pif.WriteField("m_sbyteArray", pt.SbyteArray, pt.SbyteArray.GetType());
       pif.WriteField("m_charArray", pt.CharArray, pt.CharArray.GetType());
       pif.WriteField("m_dateTime", pt.DateTime, pt.DateTime.GetType());
       pif.WriteField("m_int16Array", pt.Int16Array, pt.Int16Array.GetType());
       pif.WriteField("m_int32Array", pt.Int32Array, pt.Int32Array.GetType());
       pif.WriteField("m_longArray", pt.LongArray, pt.LongArray.GetType());
       pif.WriteField("m_floatArray", pt.FloatArray, pt.FloatArray.GetType());
       pif.WriteField("m_doubleArray", pt.DoubleArray, pt.DoubleArray.GetType());
       pif.WriteField("m_byteByteArray", pt.ByteByteArray, pt.ByteByteArray.GetType());
       pif.WriteField("m_stringArray", pt.StringArray, pt.StringArray.GetType());
       pif.WriteField("m_map", pt.Map, pt.Map.GetType());
       pif.WriteField("m_hashtable", pt.Hashtable, pt.Hashtable.GetType());
       pif.WriteField("m_vector", pt.Vector, pt.Vector.GetType());
       pif.WriteField("m_chs", pt.Chs, pt.Chs.GetType());
       pif.WriteField("m_clhs", pt.Clhs, pt.Clhs.GetType());
       pif.WriteField("m_uint32", pt.Uint32, pt.Uint32.GetType());
       pif.WriteField("m_ulong", pt.Ulong, pt.Ulong.GetType());
       pif.WriteField("m_uint16", pt.Uint16, pt.Uint16.GetType());
       pif.WriteField("m_uint32Array", pt.Uint32Array, pt.Uint32Array.GetType());
       pif.WriteField("m_ulongArray", pt.UlongArray, pt.UlongArray.GetType());
       pif.WriteField("m_uint16Array", pt.Uint16Array, pt.Uint16Array.GetType());
       pif.WriteField("m_byte252", pt.Byte252, pt.Byte252.GetType());
       pif.WriteField("m_byte253", pt.Byte253, pt.Byte253.GetType());
       pif.WriteField("m_byte65535", pt.Byte65535, pt.Byte65535.GetType());
       pif.WriteField("m_byte65536", pt.Byte65536, pt.Byte65536.GetType());
       pif.WriteField("m_pdxEnum", pt.PdxEnum, pt.PdxEnum.GetType());

       PdxTests.Address[] aa = new PdxTests.Address[10];

       for (int i = 0; i < 10; i++)
       {
         aa[i] = new PdxTests.Address(i + 1, "street" + i.ToString(), "city" + i.ToString());
       }

       pif.WriteField("m_address", pt.AddressArray, aa.GetType());
       pif.WriteField("m_objectArray", pt.ObjectArray, pt.ObjectArray.GetType());

       IPdxInstance pi = pif.Create();

       object piObject = pi.GetObject();

       Assert.AreEqual(piObject, pt);

       region0["pi2"] = pi;

       Object ret = region0["pi2"];

       Assert.AreEqual(ret, pt);

       ParentPdx pp = new ParentPdx(2);
       IPdxInstanceFactory if2 = CacheHelper.DCache.CreatePdxInstanceFactory(pp.GetType().FullName);
       if2.WriteField("_parentId", pp._parentId, pp._parentId.GetType());
       if2.WriteField("_gender", pp._gender, pp._gender.GetType());
       if2.WriteField("_parentName", pp._parentName, pp._parentName.GetType());
       if2.WriteField("_childPdx", pp._childPdx, pp._childPdx.GetType());

       IPdxInstance ip2 = if2.Create();
       region0["ppwf"] = ip2;

       ret = region0["ppwf"];

       Assert.AreEqual(ret, pp, "parent pdx should be same");

       //Statistics chk for Pdx.
       StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
       StatisticsType type = factory.FindType("CachePerfStats");
       if (type != null) {
         Statistics rStats = factory.FindFirstStatisticsByType(type);
         if (rStats != null) {
           Util.Log("pdxInstanceDeserializations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceDeserializations"));
           Util.Log("pdxInstanceCreations for PdxInstance getObject = {0} ", rStats.GetInt((string)"pdxInstanceCreations"));
           Util.Log("pdxInstanceDeserializationTime for PdxInstance getObject = {0} ", rStats.GetLong((string)"pdxInstanceDeserializationTime"));
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceDeserializations"), 2, "pdxInstanceDeserializations should be 2.");
           Assert.AreEqual(rStats.GetInt((string)"pdxInstanceCreations"), 0, "pdxInstanceCreations should be 0");
           Assert.Greater(rStats.GetLong((string)"pdxInstanceDeserializationTime"), 0, "pdxInstanceDeserializationTime should be greater than 0");
         }
       }
     }

     public void runPdxInstanceFactoryTest()
     { 


         CacheHelper.SetupJavaServers(true, "cacheserverPdxSerializer.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");


       m_client1.Call(pdxIFPutGetTest);
       //m_client2.Call();
       Util.Log("StepThree complete.");
       m_client1.Call(pdxIFPutGetTest2);
       Util.Log("StepFour complete.");
    
       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();
     
     }

     public void pdxTypeMapperTest1()
     {
       Console.WriteLine("pdxTypeMapperTest 1");
       Serializable.SetPdxTypeMapper(new PdxTypeMapper());
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       if(region0 == null)
         Console.WriteLine("pdxTypeMapperTest region is null");
       else
         Console.WriteLine("pdxTypeMapperTest region is NOT null");


       PdxTests.PdxType pt = new PdxType();

       for (int i = 0; i < 10; i++)
       {
         region0[i] = pt;
       }
       for (int i = 0; i < 10; i++)
       {
         object ret = region0[i];

         Assert.AreEqual(ret, pt);
       }
     }

     public void pdxTypeMapperTest2()
     {
       Serializable.SetPdxTypeMapper(new PdxTypeMapper());
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       PdxTests.PdxType pt = new PdxType();

       for (int i = 0; i < 10; i++)
       {
         object ret = region0[1];

         Assert.AreEqual(ret, pt);
       }
     }

     public void pdxITypeMapperTest()
     {
       Serializable.SetPdxTypeMapper(new PdxTypeMapper());
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       PdxTests.PdxType pt = new PdxType();

       for (int i = 0; i < 10; i++)
       {
         object ret = region0[1];

         IPdxInstance pi = ret as IPdxInstance;
         Assert.IsNotNull(pi);
         //IDisposable dis = (IDisposable)pi;
         using (pi)
         {
           Assert.AreEqual(pi.GetObject(), pt);
         }
       }
     }

     public void pdxASTypeMapperTest()
     {
       Serializable.RegisterPdxSerializer(new AutoSerializerEx());
       Serializable.SetPdxTypeMapper(new PdxTypeMapper());
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       SerializePdx3 sp3 = new SerializePdx3(true, 2);

       for (int i = 100; i < 110; i++)
       {
         region0[i] = sp3;
       }

       for (int i = 100; i < 110; i++)
       {
         object ret = region0[i];
         Assert.AreEqual(sp3, ret);
       } 
     }

     public void runPdxTypeMapperTest()
     {


         CacheHelper.SetupJavaServers(true, "cacheserverPdxSerializer.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");
        
         m_client4.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);

         m_client3.Call(CreateTCRegions_Pool_PDX2, RegionNames,
            CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);

       
         Util.Log("StepTwo (pool locators) complete.");
       Console.WriteLine("client created");

       m_client1.Call(pdxTypeMapperTest1);
       Console.WriteLine("client created2");
       m_client2.Call(pdxTypeMapperTest2);
       m_client3.Call(pdxITypeMapperTest);
       m_client4.Call(pdxASTypeMapperTest);


       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       m_client3.Call(Close);
       m_client4.Call(Close);
       //Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();

     }
     int nPdxPuts = 100000;
     int pdxobjsize = 5000;
     void checkLocalCache()
     {
      Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> localregion = region0.GetLocalView();
      
      int entryNotFound = 0;
      int entryFound = 0;
      for (int i = 0; i < nPdxPuts; i++)
      {
        try
        {
          object ret = localregion[i];
          if (ret != null)
          {
            Heaptest ht = ret as Heaptest;
            if (ht != null)
              entryFound++;
          }
        }
        catch (Generic.KeyNotFoundException )
        {
          entryNotFound++;
        }
      }
      Assert.Greater(entryFound, 100, "enteries should be in local cache");
      Assert.Greater(nPdxPuts, entryFound + 50000, "pdx object should have evicted");
     }
     void putPdxheaptest()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       for (int i = 0; i < nPdxPuts; i++)
       {
         region0[i] = new Heaptest(pdxobjsize);
       }
       checkLocalCache();
     }

     void getPdxHeaptest()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       for (int i = 0; i < nPdxPuts; i++)
       {
         object ret = region0[i];
       }
       checkLocalCache();
     }

     public void runPdxTypeObjectSizeTest()
     {


         CacheHelper.SetupJavaServers(true, "cacheserverPdxSerializer.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

       m_client1.Call(SetHeaplimit, 100, 10);
       m_client2.Call(SetHeaplimit, 100, 10);

         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");       

       m_client1.Call(putPdxheaptest);
       m_client2.Call(getPdxHeaptest);

       m_client1.Call(UnsetHeapLimit);
       m_client2.Call(UnsetHeapLimit);
       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();

     }

     int nBAPuts = 25;
     int baSize = 16240000;
     void checkLocalCacheBA(bool checkmem)
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       IRegion<object, object> localregion = region0.GetLocalView();

       int entryNotFound = 0;
       int entryFound = 0;
       for (int i = 0; i < nBAPuts; i++)
       {
         try
         {
           object ret = localregion[i];
           if (ret != null)
           {
             Byte[] ht = ret as Byte[];
             if (ht != null)
               entryFound++;
           }
         }
         catch (Generic.KeyNotFoundException )
         {
           entryNotFound++;
         }
       }
       Assert.Greater(entryFound, 8, "enteries should be in local cache");
       Assert.Greater(nBAPuts, entryFound + 10, "pdx object should have evicted");

       int mem = (int)GC.GetTotalMemory(true);
       // if(checkmem)
       //Assert.Less(mem, 200000000, "Memory should be less then 200 mb");
     }
     void putBAheaptest()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       for (int i = 0; i < nBAPuts; i++)
       {
         region0[i] = new byte[baSize];
       }
       checkLocalCacheBA(false);
     }

     void getBAHeaptest()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       for (int i = 0; i < nBAPuts; i++)
       {
         object ret = region0[i];
       }
       checkLocalCacheBA(true);
     }

     public void runByteArrayObjectSizeTest()
     {


         CacheHelper.SetupJavaServers(true, "cacheserverPdxSerializer.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

       m_client1.Call(SetHeaplimit, 150, 5);
       m_client2.Call(SetHeaplimit, 150, 5);

         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");

       m_client1.Call(putBAheaptest);
       m_client2.Call(getBAHeaptest);

       m_client1.Call(UnsetHeapLimit);
       m_client2.Call(UnsetHeapLimit);
       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();

     }

     private void putPdxWithEnum()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       region0[0] = new PdxEnumTestClass(0);
       region0[1] = new PdxEnumTestClass(1);
       region0[2] = new PdxEnumTestClass(2);
     }
     private void pdxEnumQuery()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       ISelectResults<object> sr = region0.Query<object>("_enumid.name = 'id2'");

       Assert.AreEqual(1, sr.Size, "query result should have one item");

      IEnumerator<object> en =  sr.GetEnumerator();

      while (en.MoveNext())
      {
        PdxEnumTestClass re = (PdxEnumTestClass)en.Current;
        Assert.AreEqual(1, re.ID, "query should have return id 1");
      }
     }

     public void runPdxEnumQueryTest()
     {


         CacheHelper.SetupJavaServers(true, "cacheserverPdxSerializer.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");
         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");

       m_client1.Call(putPdxWithEnum);
       m_client1.Call(pdxEnumQuery);

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();

     }

     void registerPdxDelta()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       region0.GetSubscriptionService().RegisterAllKeys(); 
     }

     void putPdxDelta()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       javaobject.PdxDelta pd = new javaobject.PdxDelta(1001);
       for (int i = 0; i < 10; i++)
       {
         region0["pdxdelta"] = pd;
       }
       Assert.Greater(javaobject.PdxDelta.GotDelta, 7, "this should have more todelta");
     }

     void verifyPdxDelta()
     {
       System.Threading.Thread.Sleep(5000);
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       //Assert.Greater(javaobject.PdxDelta.GotDelta, 7, "this should have recieve delta");
       javaobject.PdxDelta pd = (javaobject.PdxDelta)region0.GetLocalView()["pdxdelta"];
       Assert.Greater(pd.Delta, 7, "this should have recieve delta");
       Assert.Greater(javaobject.PdxDelta.GotDelta, 7, "this should have more todelta");
     }

     public void runPdxDeltaTest()
     {


         CacheHelper.SetupJavaServers(true, "cacheserverForPdx.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");
         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", true, false, true/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");
       m_client2.Call(registerPdxDelta);
       m_client1.Call(putPdxDelta);
       m_client2.Call(verifyPdxDelta);

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();

     }

     private void generateJavaPdxType()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       string args = "saveAllJavaPdxTypes";
       List<object> filter = new List<object>();
       filter.Add(1);
       Execution<object> execution = FunctionService<object>.OnRegion<object, object>(region0)
                       .WithArgs<object>(args).WithFilter<object>(filter);



       IResultCollector<object> resultCollector = execution.Execute("ComparePdxTypes");
       
       ICollection<object> executeFunctionResult = resultCollector.GetResult();

       bool gotResult = false;
       foreach (object item in executeFunctionResult)
       {
         Assert.AreEqual(item, true, "Function should return true");
         gotResult = true;
       }
       Assert.AreEqual(gotResult, true, "Function should return true");
     }

     private void putAllPdxTypes()
     {
       Region r = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
       PdxTypes1 p1 = new PdxTypes1();
       r[p1.GetType().FullName] = p1;

       PdxTypes2 p2 = new PdxTypes2();
       r[p2.GetType().FullName] = p2;

       PdxTypes3 p3 = new PdxTypes3();
       r[p3.GetType().FullName] = p3;

       PdxTypes4 p4 = new PdxTypes4();
       r[p4.GetType().FullName] = p4;

       PdxTypes5 p5 = new PdxTypes5();
       r[p5.GetType().FullName] = p5;

       PdxTypes6 p6 = new PdxTypes6();
       r[p6.GetType().FullName] = p6;

       PdxTypes7 p7 = new PdxTypes7();
       r[p7.GetType().FullName] = p7;

       PdxTypes8 p8 = new PdxTypes8();
       r[p8.GetType().FullName] = p8;

       PdxTypes9 p9 = new PdxTypes9();
       r[p9.GetType().FullName] = p9;

       PdxTypes10 p10 = new PdxTypes10();
       r[p10.GetType().FullName] = p10;
     }

     private void verifyDotNetPdxTypes()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       string args = "compareDotNETPdxTypes";
       List<object> filter = new List<object>();
       filter.Add(1);
       Execution<object> execution = FunctionService<object>.OnRegion<object, object>(region0)
                       .WithArgs<object>(args).WithFilter<object>(filter);



       IResultCollector<object> resultCollector = execution.Execute("ComparePdxTypes");

       ICollection<object> executeFunctionResult = resultCollector.GetResult();

       bool gotResult = false;
       foreach (object item in executeFunctionResult)
       {
         Assert.AreEqual(item, true, "Function should return true");
         gotResult = true;
       }
       Assert.AreEqual(gotResult, true, "Function should return true");
     }

     public void runPdxMetadataCheckTest()
     {


        CacheHelper.SetupJavaServers(true, "cacheserverPdx2.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

       m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", true, false, true/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");
       
       m_client1.Call(generateJavaPdxType);
       m_client1.Call(putAllPdxTypes);
       m_client1.Call(verifyDotNetPdxTypes);

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");
 
       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();

     }

     private void client1PutsV1Object()
     {
       initializePdxAssemblyOne3(false);


       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionOneAsm.GetType("PdxVersionTests.PdxTypes3");
       object np = pt.InvokeMember("PdxTypes3", BindingFlags.CreateInstance, null, null, null);
       region0[1] = np;

       object pRet = region0[1];
     }

     //this has v2 object
     private void client2GetsV1ObjectAndPutsV2Object()
     {
       initializePdxAssemblyTwo3(false);

       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       Type pt = m_pdxVesionTwoAsm.GetType("PdxVersionTests.PdxTypes3");
       object np = pt.InvokeMember("PdxTypes3", BindingFlags.CreateInstance, null, null, null);

       //get v1 ojbject ..
       object pRet = (object)region0[1];

       //now put v2 object
       region0[2] = np;
     }

     //this should fails..
     private void client3GetsV2Object()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       List<object> filter = new List<object>();
       filter.Add(1);
       Execution<object> execution = FunctionService<object>.OnRegion<object, object>(region0);

       IResultCollector<object> resultCollector = execution.Execute("IterateRegion");

       ICollection<object> executeFunctionResult = resultCollector.GetResult();

       bool gotResult = false;
       foreach (object item in executeFunctionResult)
       {
         Assert.AreEqual(item, true, "Function should return true");
         gotResult = true;
       }
       Assert.AreEqual(gotResult, true, "Function should return true");
     }
     
     
     public void runPdxBankTest()
     {


         CacheHelper.SetupJavaServers(true, "cacheserverPdx2.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");
         m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, true/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", true, false, true/*local caching false*/);

         m_client3.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", true, false, true/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");
       m_client1.Call(client1PutsV1Object);
       m_client2.Call(client2GetsV1ObjectAndPutsV2Object);
       m_client3.Call(client3GetsV2Object);

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       Util.Log("Client 2 closed");
       m_client3.Call(Close);
       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();

     }

     void putFromLongRunningClient() 
     {
       Serializable.RegisterPdxType(PdxTests.PdxTypes1.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTests.PdxTypes2.CreateDeserializable);
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       region0[1] = new PdxTests.PdxTypes1();
       region0[2] = new PdxTests.PdxTypes2();

     }

     void VerifyEntryFLRC()
     {
       Serializable.RegisterPdxType(PdxTests.PdxTypes1.CreateDeserializable);
       Serializable.RegisterPdxType(PdxTests.PdxTypes2.CreateDeserializable);
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       object ret = region0[1];
     }

     void put2FromLongRunningClient()
     {
       Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

       region0[2] = new PdxTests.PdxTypes2();
     }

     void VerifyEntry2FLRC()
     {
       bool gotException = false;
       try
       {
         Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
         object orig = new PdxTests.PdxTypes2();
         object ret = region0[2];
         Util.Log("ret object : " + ret.GetType().ToString());
         if (orig.Equals(ret))
         {
           Util.Log("both objects are equal");
         }
         else {
           Util.Log("both objects are NOT equal");
         }
       }
       catch (Exception ex) {
         Util.Log("Got exception " + ex.Message);
         gotException = true;
       }

       Assert.IsFalse(gotException);
     }

     string testSysPropFileName = "testLR.properties";
     void createExtraSysPropFile(string name, string value) 
     {
       // create a file for alternate properties...
       StreamWriter sw = new StreamWriter(testSysPropFileName);
       sw.WriteLine(name + "=" + value);
       sw.Close();
     }
     void runPdxLongrunningClientTest()
     {


         CacheHelper.SetupJavaServers(true, "cacheserverPdx.xml");
         CacheHelper.StartJavaLocator(1, "GFELOC");
         Util.Log("Locator 1 started.");
         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started.");

       createExtraSysPropFile("on-client-disconnect-clear-pdxType-Ids", "true");
       m_client1.Call(CacheHelper.SetExtraPropertiesFile, testSysPropFileName);
       m_client2.Call(CacheHelper.SetExtraPropertiesFile, testSysPropFileName);

       m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepOne (pool locators) complete.");

         m_client2.Call(CreateTCRegions_Pool, RegionNames,
           CacheHelper.Locators, "__TESTPOOL1_", false, false, false/*local caching false*/);
         Util.Log("StepTwo (pool locators) complete.");
       

       m_client1.Call(putFromLongRunningClient);
       Util.Log("StepThree complete.");

       //m_client2.Call(VerifyEntryFLRC);
       Util.Log("StepFour complete.");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
       Util.Log("Cacheserver 1 started again.");

       //different object
       m_client1.Call(put2FromLongRunningClient);
       Util.Log("StepFive complete.");

       //this should throw exception
       m_client2.Call(VerifyEntry2FLRC);
       Util.Log("StepSeven complete.");

       m_client1.Call(Close);
       Util.Log("Client 1 closed");
       m_client2.Call(Close);
       Util.Log("Client 2 closed");

       CacheHelper.StopJavaServer(1);
       Util.Log("Cacheserver 1 stopped.");

         CacheHelper.StopJavaLocator(1);
         Util.Log("Locator 1 stopped.");

       CacheHelper.ClearEndpoints();
       CacheHelper.ClearLocators();

       m_client1.Call(CacheHelper.SetExtraPropertiesFile, (string)null);
       m_client2.Call(CacheHelper.SetExtraPropertiesFile, (string)null);
     }

     #region Tests
     
     [Test]
     //NON PDX UnitTest for Ticket#866 on NC OR SR#13306117704. Set client name via native client API
     public void testBug866()
     {
       runtestForBug866();
     }

     [Test]
     public void DistOps()
     {
       runPdxDistOps();
     }

     [Test]
     public void DistOps2()
     {
       runPdxDistOps2();
     }

     [Test]
     public void NestedPdxOps()
     {
       runNestedPdxOps();
     }

     [Test]
     public void PdxInIGFSOps()
     {
       runPdxInIGFSOps();
     }
     
     [Test]
     public void JavaInteroperableOps()
     {
       runJavaInteroperableOps();
     }
     
     [Test]
     public void JavaInterOpsWithLinkedListType()
     {
       runJavaInterOpsWithLinkedListType();
     }
     
     [Test]
     public void PutAllGetAllOps()
     {
       runPutAllGetAllOps();
     }
      [Test]
     public void LocalOps()
     {
       runLocalOps();
     }
      
     [Test]
     public void BasicMergeOps()
     {
       m_useWeakHashMap = true;
       runBasicMergeOps();
       m_useWeakHashMap = false;
       runBasicMergeOps();
     }

     [Test]
     public void BasicMergeOpsWithPdxSerializer()
     {
       m_useWeakHashMap = true;
       runBasicMergeOpsWithPdxSerializer();
       m_useWeakHashMap = false;
       runBasicMergeOpsWithPdxSerializer();
     }

     [Test]
     public void BasicMergeOpsR1()//first register with higher version
     {
       m_useWeakHashMap = true;
       runBasicMergeOpsR1();
       m_useWeakHashMap = false;
       runBasicMergeOpsR1();
     }

     [Test]
     public void BasicMergeOpsR2()//first register with higher version
     {
       m_useWeakHashMap = true;
       runBasicMergeOpsR2();
       m_useWeakHashMap = false;
       runBasicMergeOpsR2();
     }

     [Test]
     public void BasicMergeOps2()//first register with higher version
     {
       m_useWeakHashMap = true;
       runBasicMergeOps2();
       m_useWeakHashMap = false;
       runBasicMergeOps2();
     }

     [Test]
     public void BasicMergeOps3()//first register with higher version
     {
       m_useWeakHashMap = true;
       runBasicMergeOps3();
       m_useWeakHashMap = false;
       runBasicMergeOps3();
     }

     [Test]
     public void PdxIgnoreUnreadFieldTest()
     {
       m_useWeakHashMap = true;
       runPdxIgnoreUnreadFieldTest();
       m_useWeakHashMap = false;
       runPdxIgnoreUnreadFieldTest();
     }

     [Test]
     public void MultipleDSTest()
     {
       runMultipleDSTest();
     }

     [Test]
     public void PdxSerializerTest()
     {
       runPdxSerializerTest();
     }
     
     [Test]
     public void ReflectionPdxSerializerTest()
     {
       runReflectionPdxSerializerTest();
     }
     
     [Test]
     public void PdxTestWithNoTypeRegister()
     {
       runPdxTestWithNoTypeRegister();
     }

     [Test]
     public void PdxInstanceTest()
     {
       runPdxInstanceTest();
     }

     [Test]
     public void PdxReadSerializedTest()
     {
       runPdxReadSerializedTest();
     }

     [Test]
     public void PdxVersionClassesEqualTest()
     {
       runPdxVersionClassesEqualTest();
     }

     [Test]
     public void PdxInstanceLocalTest()
     {
       runPdxInstanceLocalTest();
     }

     [Test]
     public void PdxInstanceFactoryTest()
     {
       runPdxInstanceFactoryTest();
     }

     [Test]
     public void PdxTypeMapperTest()
     {
       runPdxTypeMapperTest();
     }

      //[Test]
      public void PdxTypeObjectSizeTest()
      {
        runPdxTypeObjectSizeTest();
      }

      [Test]
      public void ByteArrayObjectSizeTest()
      {
        runByteArrayObjectSizeTest();
      }

      [Test]
      public void PdxEnumQueryTest()
      {
        runPdxEnumQueryTest();
      }

     [Test]
     public void PdxDeltaTest()
     {
       runPdxDeltaTest();
     }

     [Test]
     public void PdxMetadataCheckTest()
     {
       runPdxMetadataCheckTest();
     }

     [Test]
     public void PdxBankTest()
     {
       runPdxBankTest();
     }
     
     [Test]
     public void PdxLongrunningClientTest()
     {
       runPdxLongrunningClientTest();
     }
     
    #endregion
   }

   #region IpDxSerializer stuff
  public class SerializePdx : IPdxSerializable
  {
    [PdxIdentityField]
    public int i1;
    public int i2;
    public string s1;
    public string s2;

    /*public static SerializePdx1 CreateDeserializable()
    {
      return new SerializePdx1(false);
    }*/

    //public SerializePdx()
    //{
    //}

    public static SerializePdx Create()
    {
      return new SerializePdx(false);
    }
    public SerializePdx(bool init)
    {
      if (init)
      {
        i1 = 1;
        i2 = 2;
        s1 = "s1";
        s2 = "s2";
      }
    }

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      if (obj == this)
        return true;

      SerializePdx other = obj as SerializePdx;

      if (obj == null)
        return false;

      if (i1 == other.i1
         && i2 == other.i2
          && s1 == other.s1
           && s2 == other.s2)
        return true;

      return false;
    }
    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      i1 = reader.ReadInt("i1");
      i2 = reader.ReadInt("i2");
      s1 = reader.ReadString("s1");
      s2 = reader.ReadString("s2");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("i1", i1);
      writer.MarkIdentityField("i1");
      writer.WriteInt("i2", i2);
      writer.WriteString("s1", s1);
      writer.MarkIdentityField("s1");
      writer.WriteString("s2", s2);
    }

    #endregion
  }
   public class SerializePdx1
   {
     [PdxIdentityField]
     public int i1;
     public int i2;
     public string s1;
     public string s2;

     /*public static SerializePdx1 CreateDeserializable()
     {
       return new SerializePdx1(false);
     }*/

     public SerializePdx1()
     {
     }
     public SerializePdx1(bool init)
     {
       if (init)
       {
         i1 = 1;
         i2 = 2;
         s1 = "s1";
         s2 = "s2";
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       if (obj == this)
         return true;

       SerializePdx1 other = obj as SerializePdx1;

       if (other == null)
         return false;

       if (i1 == other.i1
          && i2 == other.i2
           && s1 == other.s1
            && s2 == other.s2)
         return true;

       return false;
     }
     public override int GetHashCode()
     {
         return base.GetHashCode();
     }
   }

   public class SerializePdx2
   {
     public string s0;
     [PdxIdentityField]
     public int i1;
     public int i2;
     public string s1;
     public string s2;

     public SerializePdx2()
     {

     }
     public override string ToString()
     {
       return i1 + i2 + s1 + s2;
     }
     public SerializePdx2(bool init)
     {
       if (init)
       {
         s0 = "s9999999999999999999999999999999999";
         i1 = 1;
         i2 = 2;
         s1 = "s1";
         s2 = "s2";
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       if (obj == this)
         return true;

       SerializePdx2 other = obj as SerializePdx2;

       if (other == null)
         return false;

       if (s0 == other.s0
          && i1 == other.i1
          && i2 == other.i2
           && s1 == other.s1
            && s2 == other.s2)
         return true;

       return false;
     }
     public override int GetHashCode()
     {
         return base.GetHashCode();
     }
   }

   public class BaseClass
   {
     //private readonly int _b1 = 1000;
     [NonSerialized]
     //private int _nonserialized = 1001;
     //private static int _static = 1002;

     private const int _const = 1003;

     private int _baseclassmember;

     public BaseClass()
     {

     }
     public BaseClass(bool init)
     {
       if (init)
       {
         _baseclassmember = 101;
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       BaseClass bc = obj as BaseClass;
       if (bc == null)
         return false;

       if (bc == this)
         return true;

       if (bc._baseclassmember == this._baseclassmember)
       {
         return true;
       }
       return false;
     }

     public override int GetHashCode()
     {
         return base.GetHashCode();
     }

     public void ToData(IPdxWriter w)
     {
       w.WriteInt("_baseclassmember", _baseclassmember);
     }

     public void FromData(IPdxReader r)
     {
       _baseclassmember = r.ReadInt("_baseclassmember");
     }
   }

   public class Address
   {
     private static Guid oddGuid = new Guid("924243B5-9C2A-41d7-86B1-E0B905C7EED3");
     private static Guid evenGuid = new Guid("47AA8F17-FF6B-4a7d-B398-D83790977574");
     private string _street;
     private string _aptName;
     private int _flatNumber;
     private Guid _guid;
     public Address()
     { }
     public Address(int id)
     {
       _flatNumber = id;
       _aptName = id.ToString();
       _street = id.ToString() + "_street";
       if (id % 2 == 0)
         _guid = evenGuid;
       else
         _guid = oddGuid;
     }
     public override string ToString()
     {
       return _flatNumber + " " + _aptName + " " + _street + "  " + _guid.ToString();
     }
     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       Address other = obj as Address;

       if (other == null)
         return false;

       if (_street == other._street &&
           _aptName == other._aptName &&
           _flatNumber == other._flatNumber &&
           _guid.Equals(other._guid))
         return true;

       return false;
     }
     public override int GetHashCode()
     {
         return base.GetHashCode();
     }
     public void ToData(IPdxWriter w)
     {
       w.WriteString("_street", _street);
       w.WriteString("_aptName", _aptName);
       w.WriteInt("_flatNumber", _flatNumber);
       w.WriteString("_guid", _guid.ToString());
     }

     public void FromData(IPdxReader r)
     {
       _street = r.ReadString("_street");
       _aptName = r.ReadString("_aptName");
       _flatNumber = r.ReadInt("_flatNumber");
       string s = r.ReadString("_guid");
       _guid = new Guid(s);
     }
   }

   public class SerializePdx3 : BaseClass
   {
     private string s0;
     [PdxIdentityField]
     private int i1;
     public int i2;
     public string s1;
     public string s2;
     private SerializePdx2 nestedObject;
     private ArrayList _addressList;
     private Address _address;
     private Hashtable _hashTable;
     //private int arrayCountS3= 10;
     private List<object> _addressListObj;
     //private Address[] _arrayOfAddress;

     public SerializePdx3()
       : base()
     {

     }

     public SerializePdx3(bool init, int nAddress)
       : base(init)
     {
       if (init)
       {
         s0 = "s9999999999999999999999999999999999";
         i1 = 1;
         i2 = 2;
         s1 = "s1";
         s2 = "s2";
         nestedObject = new SerializePdx2(true);

         _addressList = new ArrayList();
         _hashTable = new Hashtable();
         _addressListObj = new List<object>();

         for (int i = 0; i < 10; i++)
         {
           _addressList.Add(new Address(i));
           _hashTable.Add(i, new SerializePdx2(true));
           _addressListObj.Add(new Address(i));
         }

         _address = new Address(nAddress);

         //_arrayOfAddress = new Address[3];

         //for (int i = 0; i < 3; i++)
         //{
         //  _arrayOfAddress[i] = new Address(i);
         //}
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       if (obj == this)
         return true;

       SerializePdx3 other = obj as SerializePdx3;

       if (other == null)
         return false;

       if (s0 == other.s0
          && i1 == other.i1
          && i2 == other.i2
           && s1 == other.s1
            && s2 == other.s2)
       {
         bool ret = nestedObject.Equals(other.nestedObject);
         if (ret)
         {
           if (_addressList.Count == 10 &&
             _addressList.Count == other._addressList.Count//&&
             //_arrayOfAddress.Length == other._arrayOfAddress.Length &&
             //_arrayOfAddress[0].Equals(other._arrayOfAddress[0])
             )
           {
             for (int i = 0; i < _addressList.Count; i++)
             {
               ret = _addressList[i].Equals(other._addressList[i]);
               if (!ret)
                 return false;
             }

             if (_hashTable.Count != other._hashTable.Count)
               return false;
             foreach (DictionaryEntry de in _hashTable)
             {
               object otherHe = other._hashTable[de.Key];
               ret = de.Value.Equals(otherHe);
               if (!ret)
                 return false;
             }

             if (!_address.Equals(other._address))
               return false;
             return base.Equals(other);
           }
         }
       }

       return false;
     }
     public override int GetHashCode()
     {
         return base.GetHashCode();
     }

     public new void ToData(IPdxWriter w)
     {
       base.ToData(w);
       w.WriteString("s0", s0);
       w.WriteInt("i1", i1);
       w.WriteInt("i2", i2);
       w.WriteString("s1", s1);
       w.WriteString("s2", s2);
       w.WriteObject("nestedObject", nestedObject);
       w.WriteObject("_addressList", _addressList);
       w.WriteObject("_address", _address);
       w.WriteObject("_hashTable", _hashTable);
     }

     public new void FromData(IPdxReader r)
     {
       base.FromData(r);
       s0 = r.ReadString("s0");
       i1 = r.ReadInt("i1");
       i2 = r.ReadInt("i2");
       s1 = r.ReadString("s1");
       s2 = r.ReadString("s2");
       nestedObject = (SerializePdx2)r.ReadObject("nestedObject");
       _addressList = (ArrayList)r.ReadObject("_addressList");
       _address = (Address)r.ReadObject("_address");
       _hashTable = (Hashtable)r.ReadObject("_hashTable");
     }
   }

   public class SerializePdx4 : BaseClass
   {
     private string s0;
     [PdxIdentityField]
     private int i1;
     public int i2;
     public string s1;
     public string s2;
     private SerializePdx2 nestedObject;
     private ArrayList _addressList;
     private Address[] _addressArray;
     //private int arrayCount = 10;
     public SerializePdx4()
       : base()
     {

     }
     public override string ToString()
     {
       return i1 + ":" + i2 + ":" + s1 + ":" + s2 + nestedObject.ToString() + " add: " + _addressList[0].ToString();
     }
     public SerializePdx4(bool init)
       : base(init)
     {
       if (init)
       {
         s0 = "s9999999999999999999999999999999999";
         i1 = 1;
         i2 = 2;
         s1 = "s1";
         s2 = "s2";
         nestedObject = new SerializePdx2(true);

         _addressList = new ArrayList();
         _addressArray = new Address[10];

         for (int i = 0; i < 10; i++)
         {
           _addressList.Add(new Address(i));
           _addressArray[i] = new Address(i);
         }
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       if (obj == this)
         return true;

       SerializePdx4 other = obj as SerializePdx4;

       if (other == null)
         return false;

       if (s0 == other.s0
          && i1 == other.i1
          && i2 == other.i2
           && s1 == other.s1
            && s2 == other.s2)
       {
         bool ret = nestedObject.Equals(other.nestedObject);
         if (ret)
         {
           if (_addressList.Count == other._addressList.Count &&
             _addressList[0].Equals(other._addressList[0]))
           {
             for (int i = 0; i < _addressList.Count; i++)
             {
               ret = _addressList[i].Equals(other._addressList[i]);
               if (!ret)
                 return false;
             }
             for (int i = 0; i < _addressArray.Length; i++)
             {
               ret = _addressArray[i].Equals(other._addressArray[i]);
               if (!ret)
                 return false;
             }
             return base.Equals(other);
           }
         }
       }

       return false;
     }

     public override int GetHashCode()
     {
         return base.GetHashCode();
     }
   }

   public class PdxFieldTest
   {
     string _notInclude = "default_value";
     int _nameChange;
     int _identityField;

     public PdxFieldTest()
     { 
     
     }

     public string NotInclude
     {
       set { _notInclude = "default_value"; }
     }

     public PdxFieldTest(bool init)
     {
       if (init)
       {
         _notInclude = "valuechange";
         _nameChange = 11213;
         _identityField = 1038193;
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;

       PdxFieldTest other = obj as PdxFieldTest;

       if (other == null)
         return false;

       if (_notInclude == other._notInclude
           && _nameChange == other._nameChange
              && _identityField == other._identityField)
         return true;


       return false;
     }
     public override int GetHashCode()
     {
         return base.GetHashCode();
     }
   }

   public class PdxSerializer : IPdxSerializer
   {

     #region IPdxSerializer Members

     public object FromData(String className, IPdxReader reader)
     {
       object o = Activator.CreateInstance(Type.GetType(className));
       SerializePdx1 obj = o as SerializePdx1;

       if (obj != null)
       {
         obj.i1 = reader.ReadInt("i1");
         obj.i2 = reader.ReadInt("i2");
         obj.s1 = reader.ReadString("s1");
         obj.s2 = reader.ReadString("s2");
         return o;
       }
       else
       {
         SerializePdx2 obj2 = o as SerializePdx2;
         if (obj2 != null)
         {
           obj2.s0 = reader.ReadString("s0");
           obj2.i1 = reader.ReadInt("i1");
           obj2.i2 = reader.ReadInt("i2");
           obj2.s1 = reader.ReadString("s1");
           obj2.s2 = reader.ReadString("s2");
           return o;
         }
         else
         {
           SerializePdx3 sp3 = o as SerializePdx3;

           if (sp3 != null)
           {
             sp3.FromData(reader);
             return o;
           }
           else
           {
             Address ad = o as Address;
             if (ad != null)
             {
               ad.FromData(reader);
               return o;
             }
           }
           return null;
         }
       }
     }

     public bool ToData(object o, IPdxWriter writer)
     {
       SerializePdx1 obj = o as SerializePdx1;

       if (obj != null)
       {
         writer.WriteInt("i1", obj.i1);
         writer.WriteInt("i2", obj.i2);
         writer.WriteString("s1", obj.s1);
         writer.WriteString("s2", obj.s2);
         return true;
       }
       else
       {
         SerializePdx2 obj2 = o as SerializePdx2;
         if (obj2 != null)
         {
           writer.WriteString("s0", obj2.s0);
           writer.WriteInt("i1", obj2.i1);
           writer.WriteInt("i2", obj2.i2);
           writer.WriteString("s1", obj2.s1);
           writer.WriteString("s2", obj2.s2);
           return true;
         }
         else
         {
           SerializePdx3 sp3 = o as SerializePdx3;

           if (sp3 != null)
           {
             sp3.ToData(writer);
             return true;
           }
           else
           {
             Address ad = o as Address;
             if (ad != null)
             {
               ad.ToData(writer);
               return true;
             }
           }
         }
         return false;
       }
     }

     #endregion
   }

  public class SerializePdxNoRegister :IPdxSerializable
  {
    public int i1;
    public int i2;
    public string s1;
    public string s2;

    /*public static SerializePdx1 CreateDeserializable()
    {
      return new SerializePdx1(false);
    }*/

    public SerializePdxNoRegister()
    {
    }
    public SerializePdxNoRegister(bool init)
    {
      if (init)
      {
        i1 = 1;
        i2 = 2;
        s1 = "s1";
        s2 = "s2";
      }
    }

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      if (obj == this)
        return true;

      SerializePdxNoRegister other = obj as SerializePdxNoRegister;

      if (other == null)
        return false;

      if (i1 == other.i1
         && i2 == other.i2
          && s1 == other.s1
           && s2 == other.s2)
        return true;

      return false;
    }
      public override int GetHashCode()
     {
         return base.GetHashCode();
     }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      i1 = reader.ReadInt("i1");
      i2 = reader.ReadInt("i2");
      s1 = reader.ReadString("s1");
      s2 = reader.ReadString("s2");
    }

    public void ToData(IPdxWriter writer)
    {
       writer.WriteInt("i1" , i1);
       writer.WriteInt("i2", i2);
       writer.WriteString("s1" ,s1);
       writer.WriteString("s2", s2);
    }

    #endregion


  }
   #endregion

  #region Extension of ReflectionBasedAutoSerializer

  public class AutoSerializerEx : ReflectionBasedAutoSerializer
  {
    public override bool IsIdentityField(FieldInfo fi, Type type)
    {
      if (fi.Name == "_identityField")
        return true;
      return base.IsIdentityField(fi, type);
    }
    public override string GetFieldName(FieldInfo fi, Type type)
    {
      if (fi.Name == "_nameChange")
        return fi.Name + "NewName";

      return fi.Name ;
    }

    public override bool IsFieldIncluded(FieldInfo fi, Type type)
    {
      if (fi.Name == "_notInclude")
        return false;
      return base.IsFieldIncluded(fi, type);
    }

    public override FieldType GetFieldType(FieldInfo fi, Type type)
    {
      if (fi.FieldType.Equals(Type.GetType("System.Guid")))
        return FieldType.STRING;
      return base.GetFieldType(fi, type);
    }

    public override object WriteTransform(FieldInfo fi, Type type, object originalValue)
    {
      if (fi.FieldType.Equals(Type.GetType("System.Guid")))
      {
        //writer.WriteField(fi.Name, fi.GetValue(o).ToString(), Type.GetType("System.String"));
        return originalValue.ToString();
      }
      else
        return base.WriteTransform(fi, type, originalValue);
    }
    public override object ReadTransform(FieldInfo fi, Type type, object serializeValue)
    {
      if (fi.FieldType.Equals(Type.GetType("System.Guid")))
      {
        Guid g = new Guid((string)serializeValue);

        //fi.SetValue(o, g);
        return g;
      }
      else
        return base.ReadTransform(fi, type, serializeValue);
    }

    /*public override void SerializeField(object o, FieldInfo fi, IPdxWriter writer)
    {
      if (fi.FieldType.Equals(Type.GetType("System.Guid")))
      {
        writer.WriteField(fi.Name, fi.GetValue(o).ToString(), Type.GetType("System.String"));
      }
      else
      base.SerializeField(o, fi, writer);
    }*/

   /* public override object DeserializeField(object o, FieldInfo fi, IPdxReader reader)
    {
      if (fi.FieldType.Equals(Type.GetType("System.Guid")))
      {
        string gStr = (string)reader.ReadField(fi.Name, Type.GetType("System.String"));
        Guid g = new Guid(gStr);

        //fi.SetValue(o, g);
        return g;
      }
      else
        return base.DeserializeField(o, fi, reader);
    }*/
  }

  #endregion

  #region Classes for per test and Dataoutput.Advance method
  class MyClasses : IPdxSerializable
  {
    [PdxIdentityField]
    public string Key;
    public List<Object> Children;

    public MyClasses()
    {
    }

    public MyClasses(string key, int nClasses)
    {
      Key = key;
      Children = new List<object>(nClasses);
      for (int i = 0; i < nClasses; i++)
      {
        MyClass my = new MyClass(i);
        Children.Add(my);
      }
    }

    public static IPdxSerializable Create()
    {
      return new MyClasses();
    }

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;

      MyClasses other = obj as MyClasses;
      if (other == null)
        return false;

      if (Children.Count == other.Children.Count)
        return true;
      return false;
    }
    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      Key = reader.ReadString("Key");
      Children = (List<Object>)(reader.ReadObject("Children"));
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteString("Key", Key);
      writer.WriteObject("Children", Children);
    }

    #endregion
  }

  class MyClass : IPdxSerializable
  {
    [PdxIdentityField]
    public string Key;
    public int SecKey;
    public double ShareQuantity;
    public double Cost;
    public double Price;
    public int SettleSecKey;
    public double SettleFxRate;
    public double ValueBasis;
    public double OpenDate;
    public double Strategy;

    public MyClass() { }

    public MyClass(int key)
    {
      Key = key.ToString();
      SecKey = key;
      ShareQuantity = key * 9278;
      Cost = ShareQuantity * 100;
      Price = Cost * 10;
      SettleSecKey = SecKey + 100000;
      SettleFxRate = Price * 1.5;
      ValueBasis = 1.5;
      OpenDate = 100000;
      Strategy = 3.6;
    }

    public static IPdxSerializable Create()
    {
      return new MyClass();
    }
    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      Key = reader.ReadString("Key");
      SecKey = reader.ReadInt("SecKey");
      ShareQuantity = reader.ReadDouble("ShareQuantity");
      Cost = reader.ReadDouble("Cost");
      Price = reader.ReadDouble("Price");
      SettleSecKey = reader.ReadInt("SettleSecKey");
      SettleFxRate = reader.ReadDouble("SettleFxRate");
      ValueBasis = reader.ReadDouble("ValueBasis");
      OpenDate = reader.ReadDouble("OpenDate");
      Strategy = reader.ReadDouble("Strategy");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteString("Key", Key);
      writer.WriteInt("SecKey", SecKey);
      writer.WriteDouble("ShareQuantity", ShareQuantity);
      writer.WriteDouble("Cost", Cost);
      writer.WriteDouble("Price", Price);
      writer.WriteInt("SettleSecKey", SettleSecKey);
      writer.WriteDouble("SettleFxRate", SettleFxRate);
      writer.WriteDouble("ValueBasis", ValueBasis);
      writer.WriteDouble("OpenDate", OpenDate);
      writer.WriteDouble("Strategy", Strategy);
    }

    #endregion
  }
  #endregion

  #region PdxTypeMapper

  public class PdxTypeMapper : IPdxTypeMapper
  { 
    public string ToPdxTypeName(string localTypeName)
    {
      return "my" + localTypeName;
    }

    public string FromPdxTypeName(string pdxTypeName)
    {
      return pdxTypeName.Substring(2);//need to extract "my"
    }
  }
  #endregion

  #region Pdx nested class
  public enum Gender { male, female, other};
  public class ChildPdx : IPdxSerializable
  {
    public int _childId;
    public Gender _gender;
    public string _childName;

    public ChildPdx() { }
    public ChildPdx(int id)
    {
      _childId = id;
      _childName = "name" + id.ToString();
      if (id % 2 == 0)
        _gender = Gender.female;
      else
        _gender = Gender.male;
    }
    public override string ToString()
    {
      return _childId + ":" + _childName + ":" + _gender;
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      ChildPdx other = obj as ChildPdx;
      if (other == null)
        return false;
      if (_childName == other._childName
          && _gender == other._gender
            && _childId == other._childId)
        return true;
      return false;
    }
    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      _childId = reader.ReadInt("_childId");      
      _gender = (Gender)reader.ReadObject("_gender");
      _childName = reader.ReadString("_childName");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("_childId", _childId);
      writer.MarkIdentityField("_childId");
      writer.WriteObject("_gender", _gender);
      writer.WriteString("_childName", _childName);
    }

    #endregion
  }
  
  public class ParentPdx : IPdxSerializable
  {
    public int _parentId;
    public Gender _gender;
    public string _parentName;
    public ChildPdx _childPdx;

    public ParentPdx() { }
    public ParentPdx(int id)
    {
      _parentId = id;
      _parentName = "name" + id.ToString();
      if (id % 2 == 0)
        _gender = Gender.female;
      else
        _gender = Gender.male;
      _childPdx = new ChildPdx(id * 1393);
    }
    public override string ToString()
    {
      return _parentId + ":" + _gender + ":" + _parentName+ ":" + _childPdx;
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      ParentPdx other = obj as ParentPdx;
      if (other == null)
        return false;
      if (_parentId == other._parentId
          && _gender == other._gender
            && _parentName == other._parentName
              && _childPdx.Equals(other._childPdx))
        return true;
      return  false;
    }
    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      _parentId = reader.ReadInt("_parentId");
      _gender = (Gender)reader.ReadObject("_gender");
      _parentName = reader.ReadString("_parentName");
      _childPdx = (ChildPdx)reader.ReadObject("_childPdx");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("_parentId", _parentId);
      writer.MarkIdentityField("_parentId");
      writer.WriteObject("_gender", _gender);
      writer.WriteString("_parentName", _parentName);
      writer.WriteObject("_childPdx", _childPdx);
      writer.MarkIdentityField("_childPdx");
    }

    #endregion
  }

  public class ChildPdxAS 
  {
    [PdxIdentityField]
    private int _childId;
    private Gender _gender;
    private string _childName;
     public ChildPdxAS() { }
    public ChildPdxAS(int id)
    {
      _childId = id;
      _childName = "name" + id.ToString();
      if (id % 2 == 0)
        _gender = Gender.female;
      else
        _gender = Gender.male;
    }

    public override string ToString()
    {
      return _childId + ":" + _childName + ":" + _gender;
    }
    
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      ChildPdxAS other = obj as ChildPdxAS;
      if (other == null)
        return false;
      if (_childName == other._childName
          && _gender == other._gender
            && _childId == other._childId)
        return true;
      return false;
    }
    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
  }

  public class ParentPdxAS 
  {
    [PdxIdentityField]
    private int _parentId;
    private string _parentName;
    private Gender _gender;
    [PdxIdentityField]
    private ChildPdxAS _childPdx;
     public ParentPdxAS() { }
    public ParentPdxAS(int id)
    {
      _parentId = id;
      _parentName = "name" + id.ToString();
      if (id % 2 == 0)
        _gender = Gender.female;
      else
        _gender = Gender.male;
      _childPdx = new ChildPdxAS(id * 1393);
    }

    public override string ToString()
    {
      return _parentId + ":" + _gender + ":" + _parentName + ":" + _childPdx;
    }

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      ParentPdxAS other = obj as ParentPdxAS;
      if (other == null)
        return false;
      if (_parentId == other._parentId
          && _gender == other._gender
            && _parentName == other._parentName
              && _childPdx.Equals(other._childPdx))
        return true;
      return false;
    }
    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
  }

  public class Heaptest : IPdxSerializable
  {
    int _id;
    byte[] _data;

    public Heaptest() { }

    public Heaptest(int id)
    {
      _id = id;
      _data = new byte[id];
    }


    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      _id = reader.ReadInt("_id");
      _data = reader.ReadByteArray("_data");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("_id", _id);
      writer.WriteByteArray("_data", _data);
    }

    #endregion
  }

  public enum enumQuerytest { id1, id2, id3};

  public class PdxEnumTestClass :IPdxSerializable
  {
    int _id;
    enumQuerytest _enumid;
    public int ID
    {
      get { return _id; }
    }
    public PdxEnumTestClass(int id)
    {
      _id = id;
      switch (id)
      { 
        case 0:
          _enumid = enumQuerytest.id1;
          break;
        case 1:
          _enumid = enumQuerytest.id2;
          break;
        case 2:
          _enumid = enumQuerytest.id3;
          break;
        default:
           _enumid = enumQuerytest.id1;
           break;
      }
    }

    public PdxEnumTestClass() { }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      _id = reader.ReadInt("_id");
      _enumid = (enumQuerytest)reader.ReadObject("_enumid");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("_id", _id);
      writer.WriteObject("_enumid", _enumid);
    }

    #endregion
  }

  #endregion
 
}

namespace javaobject
{
  using GemStone.GemFire.Cache.Generic;
  #region Pdx Delta class
  public class PdxDelta : IPdxSerializable, IGFDelta, ICloneable
  {
    public static int GotDelta = 0;
    int _delta = 0;
    int _id;

    public PdxDelta() { }
    public PdxDelta(int id)
    {
      _id = id;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      _id = reader.ReadInt("_id");
      _delta = reader.ReadInt("_delta");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("_id", _id);
      writer.WriteInt("_delta", _delta);
    }

    #endregion
    public int Delta
    {
      get { return _delta; }
    }
    #region IGFDelta Members

    public void FromDelta(DataInput input)
    {
      Console.WriteLine(" in fromdelta " + GotDelta);
      _delta = input.ReadInt32();
      GotDelta++;
    }

    public bool HasDelta()
    {
      Console.WriteLine(" in hasdelta " + _delta);
      if (_delta > 0)
      {
        _delta++;
        return true;
      }
      else
      {
        _delta++;
        return false;
      }
    }

    public void ToDelta(DataOutput output)
    {
      Console.WriteLine(" in todelta " + GotDelta);
      output.WriteInt32(_delta);
      GotDelta++;
    }

    #endregion

    #region ICloneable Members

    public object Clone()
    {
      PdxDelta pd  = new PdxDelta();
      pd._id = _id;
      pd._delta = _delta;
      return pd;
    }

    #endregion
  }
  #endregion
}
