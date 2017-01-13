//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using System.Threading;

  using GemStone.GemFire.Cache.Generic;

  public class TestStatisticsType
  {
    public StatisticsType testStatsType;
    public int statIdIntCounter;
    public int statIdIntGauge;
    public int statIdLongCounter;
    public int statIdLongGauge;
    public int statIdDoubleCounter;
    public int statIdDoubleGauge;
  };

  public class IncThread
  {
    private Statistics m_stat;
    private TestStatisticsType m_type;

    public IncThread (Statistics stat,TestStatisticsType type)
    {
      this.m_stat = stat;
      this.m_type = type;
    }

    public void ThreadOperation()
    {
      /* Just 1000 Inc, Stop after that  */
      for ( int incIdx = 0 ; incIdx < 1000 ; incIdx++ ) {
      m_stat.IncInt(m_type.statIdIntCounter, 1 ); 
      m_stat.IncInt(m_type.statIdIntGauge, 1 ); 
      m_stat.IncLong(m_type.statIdLongCounter, 1 ); 
      m_stat.IncLong(m_type.statIdLongGauge, 1 ); 
      m_stat.IncDouble(m_type.statIdDoubleCounter, 1.0 ); 
      m_stat.IncDouble(m_type.statIdDoubleGauge, 1.0 ); 
      }
    }
  };

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("generics")]
  public class ThinClientStatisticTests : UnitTests
  {
    //#region Private members
    private UnitProcess m_client1;
    //#endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      return new ClientBase[] { m_client1 };
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

    [Test]
    public void StatisticsCheckTest()
    {
      CacheHelper.SetupJavaServers(false, "cacheserver.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CacheHelper.InitClient);

      m_client1.Call(statisticsTest);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.ClearEndpoints();
      m_client1.Call(CacheHelper.Close);
    }

    #region Functions invoked by the tests

    public void Close()
    {
      CacheHelper.Close();
    }

    void createType(StatisticsFactory statFactory, TestStatisticsType testType)
    {
      StatisticDescriptor[] statDescriptorArr = new StatisticDescriptor[6];

      statDescriptorArr[0] = statFactory.CreateIntCounter("IntCounter",
      "Test Statistic Descriptor int_t Counter.","TestUnit");

      statDescriptorArr[1] = statFactory.CreateIntGauge("IntGauge",
      "Test Statistic Descriptor int_t Gauge.","TestUnit");

      statDescriptorArr[2] = statFactory.CreateLongCounter("LongCounter",
      "Test Statistic Descriptor Long Counter.","TestUnit");

      statDescriptorArr[3] = statFactory.CreateLongGauge("LongGauge",
      "Test Statistic Descriptor Long Gauge.","TestUnit");

      statDescriptorArr[4] = statFactory.CreateDoubleCounter("DoubleCounter",
      "Test Statistic Descriptor Double Counter.","TestUnit");

      statDescriptorArr[5] = statFactory.CreateDoubleGauge("DoubleGauge",
      "Test Statistic Descriptor Double Gauge.","TestUnit");
      
      StatisticsType statsType = statFactory.CreateType("TestStatsType",
                               "Statistics for Unit Test.",statDescriptorArr, 6);

      Assert.IsNotNull(statsType, "Error in creating Stats Type");
      
      testType.testStatsType = statsType;
      testType.statIdIntCounter = statsType.NameToId("IntCounter");
      testType.statIdIntGauge = statsType.NameToId("IntGauge");
      testType.statIdLongCounter = statsType.NameToId("LongCounter");
      testType.statIdLongGauge = statsType.NameToId("LongGauge");
      testType.statIdDoubleCounter = statsType.NameToId("DoubleCounter");
      testType.statIdDoubleGauge = statsType.NameToId("DoubleGauge");

      StatisticsType statsType1 = statFactory.CreateType("TestStatsType1",
                              "Statistics for Unit Test", statDescriptorArr, 6);
      testType.testStatsType = statsType1;

      /* Test Find */
      Assert.IsNotNull(statFactory.FindType("TestStatsType"),"stat not found");
      Assert.IsNotNull(statFactory.FindType("TestStatsType1"), "stat not found");   
      Assert.IsNull(statFactory.FindType("TestStatsType2"),"stat not to be found");
    }

    void testGetSetIncFunctions(Statistics stat,  TestStatisticsType type )
    {
      /* Set a initial value =  10 */
      stat.SetInt(type.statIdIntCounter, 10);
      stat.SetInt(type.statIdIntGauge, 10);
      stat.SetLong(type.statIdLongCounter, 10);
      stat.SetLong(type.statIdLongGauge, 10);
      stat.SetDouble(type.statIdDoubleCounter, 10.0);
      stat.SetDouble(type.statIdDoubleGauge, 10.0);
      Util.Log(" Setting Initial Value Complete");
      
      /* Check Initial Value = 10*/
      Assert.AreEqual(10, stat.GetInt(type.statIdIntCounter), " Check1 1 Failed ");
      Assert.AreEqual(10, stat.GetInt(type.statIdIntGauge), " Check1 2 Failed ");
      Assert.AreEqual(10, stat.GetLong(type.statIdLongCounter), " Check1 3 Failed ");
      Assert.AreEqual(10, stat.GetLong(type.statIdLongGauge), " Check1 4 Failed ");
      Assert.AreEqual(10.0, stat.GetDouble(type.statIdDoubleCounter), " Check1 5 Failed ");
      Assert.AreEqual(10.0, stat.GetDouble(type.statIdDoubleGauge), " Check1 6 Failed ");
      Util.Log(" All Set() were correct.");
      
      /* Increment single thread for 100 times */
      for ( int incIdx = 0 ; incIdx < 100 ; incIdx++ ) {
        stat.IncInt(type.statIdIntCounter, 1);
        stat.IncInt(type.statIdIntGauge, 1);
        stat.IncLong(type.statIdLongCounter, 1);
        stat.IncLong(type.statIdLongGauge, 1);
        stat.IncDouble(type.statIdDoubleCounter, 1.0);
        stat.IncDouble(type.statIdDoubleGauge, 1.0);
        Thread.Sleep(10);
      }
      Util.Log(" Incremented 100 times by 1.");
      
      /* Check Incremented Value = 110 */
      Assert.AreEqual(110, stat.GetInt(type.statIdIntCounter), " Check2 1 Failed ");
      Assert.AreEqual(110, stat.GetInt(type.statIdIntGauge), " Check2 2 Failed ");
      Assert.AreEqual(110, stat.GetLong(type.statIdLongCounter), " Check2 3 Failed ");
      Assert.AreEqual(110, stat.GetLong(type.statIdLongGauge), " Check2 4 Failed ");
      Assert.AreEqual(110.0, stat.GetDouble(type.statIdDoubleCounter), " Check2 5 Failed ");
      Assert.AreEqual(110.0, stat.GetDouble(type.statIdDoubleGauge), " Check2 6 Failed ");
      Util.Log(" Single thread Inc() Passed.");

      IncThread[] myThreads = new IncThread[10];
      Thread[] thread = new Thread[10];

      for (int i = 0; i < 10; i++)
      {
        myThreads[i] = new IncThread(stat, type);
        thread[i] = new Thread(new ThreadStart(myThreads[i].ThreadOperation));
        thread[i].Start();
      }
      Thread.Sleep(1000);
      for (int i = 0; i < 10; i++)
      {
        thread[i].Join();
      }

      /* Check Final Value = 10,110 */
      Assert.AreEqual(10110, stat.GetInt(type.statIdIntCounter), " Check2 1 Failed ");
      Assert.AreEqual(10110, stat.GetInt(type.statIdIntGauge), " Check2 2 Failed ");
      Assert.AreEqual(10110, stat.GetLong(type.statIdLongCounter), " Check2 3 Failed ");
      Assert.AreEqual(10110, stat.GetLong(type.statIdLongGauge), " Check2 4 Failed ");
      Assert.AreEqual(10110.0, stat.GetDouble(type.statIdDoubleCounter), " Check2 5 Failed ");
      Assert.AreEqual(10110.0, stat.GetDouble(type.statIdDoubleGauge), " Check2 6 Failed ");
      Util.Log(" Parallel Inc() Passed.");

      /* Check value of Gauge type */
      stat.SetInt(type.statIdIntGauge, 50);
      stat.SetDouble(type.statIdDoubleGauge, 50.0);
      stat.SetLong(type.statIdLongGauge, 50);

      Assert.AreEqual(50, stat.GetInt(type.statIdIntGauge), " Check3 1 Failed");
      Assert.AreEqual(50, stat.GetLong(type.statIdLongGauge), "Check3 2 Failed");
      Assert.AreEqual(50.0, stat.GetDouble(type.statIdDoubleGauge), "Check3 3 Failed");
    }

    void statisticsTest()
    {
    /* Create Statistics in right and wrong manner */
      StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
    
    /* Register a type */
      TestStatisticsType testType = new TestStatisticsType();
      createType(factory, testType);
      Util.Log("Statistics Type TestStats Registered");
    
    /* Create a statistics */
      Statistics testStat1 = factory.CreateStatistics(testType.testStatsType,"TestStatistics");
      Assert.IsNotNull(testStat1, "Test Statistics Creation Failed");
    
    /* Tests Find Type , Find Statistics */
      Statistics temp = factory.FindFirstStatisticsByType(testType.testStatsType);
      Assert.IsNotNull(temp , "findFirstStatisticsByType Failed");
      Util.Log("Statistics testStat1 Created Successfully.");
    
    /* Test Set Functions */
      testGetSetIncFunctions( testStat1, testType );
      Util.Log("Get / Set / Inc Functions Tested ");
    
    /* Close Statistics */ 
      testStat1.Close();
      Statistics temp2 = factory.FindFirstStatisticsByType(testType.testStatsType);
      Assert.IsNull(temp2, "Statistics close() Failed");
      
      Util.Log("StatisticsTest Completed");
    }

    #endregion
  };
}
