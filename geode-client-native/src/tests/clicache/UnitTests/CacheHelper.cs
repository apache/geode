//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  public class PutGetTestsAD : MarshalByRefObject
  {
    private static PutGetTests m_putGetTestInstance = new PutGetTests();

    public int InitKeys(UInt32 typeId, int numKeys, int maxSize)
    {
      return m_putGetTestInstance.InitKeys(typeId, numKeys, maxSize);
    }

    public void InitValues(UInt32 typeId, int numValues, int maxSize)
    {
      m_putGetTestInstance.InitValues(typeId, numValues, maxSize);
    }

    public void DoPuts()
    {
      m_putGetTestInstance.DoPuts();
    }

    public void DoKeyChecksumPuts()
    {
      m_putGetTestInstance.DoKeyChecksumPuts();
    }

    public void DoValChecksumPuts()
    {
      m_putGetTestInstance.DoValChecksumPuts();
    }

    public void DoGetsVerify()
    {
      m_putGetTestInstance.DoGetsVerify();
    }

    public void InvalidateRegion(string regionName)
    {
      CacheHelper.InvalidateRegion(regionName, true, true);
    }

    public void DoRunQuery(bool pool)
    {
      m_putGetTestInstance.DoRunQuery(pool);
    }

    public void SetRegion(string regionName)
    {
      m_putGetTestInstance.SetRegion(regionName);
    }

    public void DoGets()
    {
      m_putGetTestInstance.DoGets();
    }
  }

  public class CacheHelperWrapper : MarshalByRefObject
  {
    public void CreateTCRegions_Pool_AD(string[] regionNames,
      string endpoints, string locators, string poolName, bool clientNotification, bool ssl)
    {
      try
      {
        Util.Log("creating region " + regionNames[0]);
        CacheHelper.CreateTCRegion_Pool_AD(regionNames[0], true, true,
          null, endpoints, locators, poolName, clientNotification, ssl, false);
        //CacheHelper.CreateTCRegion_Pool(regionNames[1], false, true,
        //null, endpoints, locators, poolName, clientNotification, ssl, false);
        //     m_regionNames = regionNames;
        Util.Log("created region " + regionNames[0]);
      }
      catch (AlreadyConnectedException)
      {
        Console.WriteLine("Got already connected exception  in TEST");
        Util.Log("Got already connected exception " + regionNames[0]);
      }
    }

    public void CallDistrinbutedConnect()
    {
      try
      {
        //Console.WriteLine("hitesh cakk CallDistrinbutedConnect");
        CacheHelper.DSConnectAD();
      }
      catch (Exception ex)
      {
        //Console.WriteLine("hitesh cakk CallDistrinbutedConnect 33");
        Util.Log("Hitesh got AlreadyConnectedException " + ex.Message);
      }
    }

    public void RegisterBuiltins()
    {
      CacheableHelper.RegisterBuiltinsAD();
    }

    public void InvalidateRegion(string regionName)
    {
      CacheHelper.InvalidateRegion(regionName, true, true);
    }

    public void CloseCache()
    {
      CacheHelper.Close();
    }

    public void SetLogFile(string logFileName)
    {
      Util.LogFile = logFileName;
    }
  }

  /// <summary>
  /// Helper class to create/destroy Distributed System, cache and regions.
  /// This class is intentionally not thread-safe.
  /// </summary>
  public class CacheHelper
  {
    public static string TestDir
    {
      get
      {
        if (m_testDir == null)
        {
          m_testDir = Util.GetEnvironmentVariable("TESTSRC");
          if (m_testDir == null)
          {
            return ".";
          }
        }
        return m_testDir;
      }
    }

    public static int HOST_PORT_1;
    public static int HOST_PORT_2;
    public static int HOST_PORT_3;
    public static int HOST_PORT_4;


    public static int LOCATOR_PORT_1;
    public static int LOCATOR_PORT_2;
    public static int LOCATOR_PORT_3;
    public static int LOCATOR_PORT_4;

    #region Private static members and constants

    private static DistributedSystem m_dsys = null;
    private static bool m_doDisconnect = true;
    private static Cache m_cache = null;
    private static IRegionService m_cacheForMultiUser = null;
    private static Region m_currRegion = null;
    private static string m_gfeDir = null;
    private static string m_gfeLogLevel = null;
    private static string m_gfeSecLogLevel = null;
    private static string m_endpoints = null;
    private static string m_locators = null;
    private static string[] m_cacheXmls = null;
    private static bool m_localServer = true;
    private static string m_extraPropertiesFile = null;

    private const string DefaultDSName = "dstest";
    private const string DefaultCacheName = "cachetest";

    private const string JavaServerName = "cacheserver.bat";
    private const string GemFireName = "gemfire.bat";
    private static int JavaMcastPort = -1;
    private const string JavaServerStartArgs =
      "start -J-Xmx512m -J-Xms128m -J-XX:+UseConcMarkSweepGC -J-XX:+UseParNewGC -J-Xss256k cache-xml-file=";
    private const string JavaServerStopArgs = "stop";
    private const string LocatorStartArgs = "start-locator";
    private const string LocatorStopArgs = "stop-locator";
    private const int LocatorPort = 34755;
    private const int MaxWaitMillis = 60000;
    private static char PathSep = Path.DirectorySeparatorChar;

    private static string m_testDir = null;
    private static Dictionary<int, string> m_runningJavaServers =
      new Dictionary<int, string>();
    private static Dictionary<int, string> m_runningLocators =
      new Dictionary<int, string>();

    private static bool m_SkipSetNewAndDelete = false;

    #endregion

    #region Public accessors

    public static Region CurrentRegion
    {
      get
      {
        return m_currRegion;
      }
    }

    public static DistributedSystem DSYS
    {
      get
      {
        return m_dsys;
      }
      set
      {
        m_dsys = value;
      }
    }

    public static Cache DCache
    {
      get
      {
        return m_cache;
      }
      set
      {
        m_cache = value;
      }
    }

    public static string Endpoints
    {
      get
      {
        return m_endpoints;
      }
    }

    public static string Locators
    {
      get
      {
        return m_locators;
      }
    }

    public static string ExtraPropertiesFile
    {
      get
      {
        return m_extraPropertiesFile;
      }
    }

    public static QueryService QueryServiceInstance
    {
      get
      {
        return m_cache.GetQueryService();
      }
    }

    public const string DefaultRegionName = "regiontest";

    #endregion

    #region Functions to initialize or close a cache and distributed system

    public static void SetLogging()
    {
      if (Util.LogFile != null)
      {
        string logFile = Regex.Replace(Util.LogFile, "\\....$", string.Empty);
        LogLevel logLevel;
        if (Util.CurrentLogLevel != Util.DefaultLogLevel)
        {
          logLevel = (LogLevel)Util.CurrentLogLevel;
        }
        else
        {
          logLevel = Log.Level();
        }
        Log.Close();
        Log.Init(logLevel, logFile);
      }
    }

    // For native layer C++ runtime mismatch.
    private static void CheckEnvAndSetNewAndDelete()
    {
      if (!m_SkipSetNewAndDelete)
      {
        CacheFactory.SetNewAndDelete();
        m_SkipSetNewAndDelete = true;
      }
    }

    public static void DSConnectAD()
    {
      m_dsys = DistributedSystem.Connect("DSName", null);
    }

    private static void SetLogConfig(ref Properties config)
    {
      if (Util.LogFile != null)
      {
        Log.Close();
        if (config == null)
        {
          config = new Properties();
        }
        if (Util.LogFile != null && Util.LogFile.Length > 0)
        {
          string logFile = Regex.Replace(Util.LogFile, "\\....$", string.Empty);
          config.Insert("log-file", logFile);
        }
        if (Util.CurrentLogLevel != Util.DefaultLogLevel)
        {
          config.Insert("log-level", Util.CurrentLogLevel.ToString().ToLower());
        }
      }
    }

    private static void DSConnect(string dsName, Properties config)
    {
      SetLogConfig(ref config);
      m_dsys = DistributedSystem.Connect(dsName, config);
    }

    public static void ConnectName(string dsName)
    {
      CheckEnvAndSetNewAndDelete();

      ConnectConfig(dsName, null);
    }

    public static void ConnectConfig(string dsName, Properties config)
    {
      CheckEnvAndSetNewAndDelete();

      if (DistributedSystem.IsConnected)
      {
        if (m_dsys == null)
        {
          m_dsys = DistributedSystem.GetInstance();
        }
      }
      else
      {
        DSConnect(dsName, config);
      }
    }

    public static void Init()
    {
      InitName(DefaultDSName, DefaultCacheName);
    }

    public static void InitName(string dsName, string cacheName)
    {
      InitConfig(dsName, cacheName, null, null, null);
    }

    public static void InitConfig(Properties config)
    {
      InitConfig(DefaultDSName, DefaultCacheName, config,
        null, null);
    }

    public static void InitConfig(string endpoints, int redundancyLevel)
    {
      CheckEnvAndSetNewAndDelete();

      CacheAttributesFactory cFact = new CacheAttributesFactory();
      cFact.SetEndpoints(endpoints);
      cFact.SetRedundancyLevel(redundancyLevel);
      InitConfig(DefaultDSName, DefaultCacheName, null,
        cFact.CreateCacheAttributes(), null);
    }

    public static void InitConfigForEventId(string endpoints, int redundancyLevel, int ackInterval, int dupCheckLife)
    {
      CheckEnvAndSetNewAndDelete();

      CacheAttributesFactory cFact = new CacheAttributesFactory();
      cFact.SetEndpoints(endpoints);
      cFact.SetRedundancyLevel(redundancyLevel);
      Properties config = new Properties();
      config.Insert("notify-ack-interval", ackInterval);
      config.Insert("notify-dupcheck-life", dupCheckLife);
      InitConfig(DefaultDSName, DefaultCacheName, config,
        cFact.CreateCacheAttributes(), null);
    }

    public static void InitConfigForDurable(string endpoints, int redundancyLevel,
      string durableClientId, int durableTimeout)
    {
      InitConfigForDurable(endpoints, redundancyLevel, durableClientId, durableTimeout, 1);
    }

    public static void InitConfigForDurable(string endpoints, int redundancyLevel,
      string durableClientId, int durableTimeout, int ackInterval)
    {
      CheckEnvAndSetNewAndDelete();

      CacheAttributesFactory cFact = new CacheAttributesFactory();
      cFact.SetEndpoints(endpoints);
      cFact.SetRedundancyLevel(redundancyLevel);
      Properties config = new Properties();
      config.Insert("durable-client-id", durableClientId);
      config.Insert("durable-timeout", durableTimeout);
      config.Insert("notify-ack-interval", ackInterval);
      InitConfig(DefaultDSName, DefaultCacheName, config,
        cFact.CreateCacheAttributes(), null);
    }

    public static void InitConfigForDurable_Pool(string endpoints, string locators, int redundancyLevel,
      string durableClientId, int durableTimeout)
    {
      InitConfigForDurable_Pool(endpoints, locators, redundancyLevel, durableClientId, durableTimeout, 1);
    }

    public static void InitConfigForDurable_Pool(string endpoints, string locators, int redundancyLevel,
      string durableClientId, int durableTimeout, int ackInterval)
    {
      CheckEnvAndSetNewAndDelete();

      CacheAttributesFactory cFact = new CacheAttributesFactory();
      Properties config = new Properties();
      config.Insert("durable-client-id", durableClientId);
      config.Insert("durable-timeout", durableTimeout);
      InitConfig(DefaultDSName, DefaultCacheName, config,
        cFact.CreateCacheAttributes(), null);
      CreatePool("__TESTPOOL1_", endpoints, locators, (string)null, redundancyLevel, true,
        ackInterval, 300);
    }

    public static void InitConfigForConflation(string endpoints, string durableClientId, string conflation)
    {
      CheckEnvAndSetNewAndDelete();

      CacheAttributesFactory cFact = new CacheAttributesFactory();
      cFact.SetEndpoints(endpoints);
      Properties config = new Properties();
      config.Insert("durable-client-id", durableClientId);
      config.Insert("durable-timeout", 300);
      config.Insert("notify-ack-interval", 1);
      if (conflation != null && conflation.Length > 0)
      {
        config.Insert("conflate-events", conflation);
      }
      InitConfig(DefaultDSName, DefaultCacheName, config,
        cFact.CreateCacheAttributes(), null);
    }

    public static void InitConfigForConflation_Pool(string endpoints, string locators,
      string durableClientId, string conflation)
    {
      CheckEnvAndSetNewAndDelete();

      CacheAttributesFactory cFact = new CacheAttributesFactory();
      Properties config = new Properties();
      config.Insert("durable-client-id", durableClientId);
      config.Insert("durable-timeout", 300);
      config.Insert("notify-ack-interval", 1);
      if (conflation != null && conflation.Length > 0)
      {
        config.Insert("conflate-events", conflation);
      }
      InitConfig(DefaultDSName, DefaultCacheName, config,
        cFact.CreateCacheAttributes(), null);
      CreatePool("__TESTPOOL1_", endpoints, locators, (string)null, 0, true);
    }

    public static void InitConfig(string cacheXml)
    {
      InitConfig(DefaultDSName, DefaultCacheName, null, null, cacheXml);
    }

    public static void InitConfig(string dsName, string cacheName,
      Properties config, CacheAttributes cAttrs,
      string cacheXml)
    {
      CheckEnvAndSetNewAndDelete();

      if (cacheXml != null)
      {
        string duplicateXMLFile = Util.Rand(3536776).ToString() + cacheXml;
        createDuplicateXMLFile(cacheXml, duplicateXMLFile);
        cacheXml = duplicateXMLFile;
      }
      if (m_extraPropertiesFile != null)
      {
        if (config == null)
        {
          config = new Properties();
        }
        config.Load(m_extraPropertiesFile);
      }

      if (m_cache == null || m_cache.IsClosed)
      {
        bool usingCacheEndpoints = (cAttrs != null) && (cAttrs.Endpoints != null) && (cAttrs.Endpoints.Length > 0);

        try
        {
          if (usingCacheEndpoints)
          {
            CacheHelper.m_doDisconnect = true;
            ConnectConfig(dsName, config);
            m_cache = CacheFactory.Create(cacheName, m_dsys, cacheXml, cAttrs);
          }
          else
          {
            CacheHelper.m_doDisconnect = false;

            SetLogConfig(ref config);

            if (cacheXml != null && cacheXml.Length > 0)
            {
              m_cache = CacheFactory.CreateCacheFactory(config).Set("cache-xml-file", cacheXml).Create();
            }
            else
            {
              m_cache = CacheFactory.CreateCacheFactory(config).Create();
            }
          }
        }
        catch (CacheExistsException)
        {
          if (usingCacheEndpoints)
          {
            m_cache = CacheFactory.GetInstance(m_dsys);
          }
          else
          {
            m_cache = CacheFactory.GetAnyInstance();
          }
        }
      }

      m_dsys = m_cache.DistributedSystem;
    }

    public static void SetExtraPropertiesFile(string fName)
    {
      m_extraPropertiesFile = fName;
    }

    public static void InitClient()
    {
      CheckEnvAndSetNewAndDelete();

      CacheHelper.Close();
      Properties config = new Properties();
      config.Load("gfcpp.properties");
      CacheHelper.InitConfig(config);
    }

    public static void Close()
    {
      Util.Log("in cache close " + DistributedSystem.IsConnected + " : " + System.Threading.Thread.GetDomainID());
      if (DistributedSystem.IsConnected)
      {
        CloseCache();
        if (m_doDisconnect)
        {
          DistributedSystem.Disconnect();
        }
      }
      m_dsys = null;
      m_cacheForMultiUser = null;
    }


    public static void CloseUserCache(bool keepAlive)
    {
      //TODO:hitesh need to look 
      m_cacheForMultiUser.Close();
    }

    public static void CloseCache()
    {
      Util.Log("App domain cacheclose " + (m_cache != null ? m_cache.IsClosed.ToString() : "cache is null"));
      if (m_cache != null && !m_cache.IsClosed)
      {
        m_cache.Close();
      }
      m_cache = null;
    }

    public static void CloseKeepAlive()
    {
      if (DistributedSystem.IsConnected)
      {
        CloseCacheKeepAlive();
        if (m_doDisconnect)
        {
          DistributedSystem.Disconnect();
        }
      }
      m_dsys = null;
    }

    public static void CloseCacheKeepAlive()
    {
      if (m_cache != null && !m_cache.IsClosed)
      {
        m_cache.Close(true);
      }
      m_cache = null;
    }

    public static void ReadyForEvents()
    {
      if (m_cache != null && !m_cache.IsClosed)
      {
        m_cache.ReadyForEvents();
      }
    }

    #endregion

    #region Functions to create or destroy a region

    public static Region CreateRegion(string name, RegionAttributes attrs)
    {
      Init();
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegion(name, attrs);
      Assert.IsNotNull(region, "Region was not created.");
      m_currRegion = region;
      return region;
    }

    public static void CreateDefaultRegion()
    {
      CreatePlainRegion(DefaultRegionName);
    }

    public static Region CreatePlainRegion(string name)
    {
      Init();
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.LOCAL).Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
      return region;
    }

    public static void CreateScopeRegion(string name, ScopeType scope, bool caching)
    {
      Init();
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.PROXY).SetCachingEnabled(caching).Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
    }

    public static void CreateDistribRegion(string name, bool ack,
      bool caching)
    {
      Init();
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        //.SetScope(ack ? ScopeType.DistributedAck : ScopeType.DistributedNoAck)
        .SetCachingEnabled(caching).SetInitialCapacity(100000).Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
    }

    public static Region CreateDistRegion(string rootName, ScopeType rootScope,
      string name, int size)
    {
      Init();
      CreateScopeRegion(rootName, rootScope, true);
      AttributesFactory af = new AttributesFactory();
      af.SetLruEntriesLimit(0);
      af.SetInitialCapacity(size);
      af.SetScope(ScopeType.DistributedAck);
      RegionAttributes rattrs = af.CreateRegionAttributes();
      Region region = m_currRegion.CreateSubRegion(name, rattrs);
      Assert.IsNotNull(region, "SubRegion {0} was not created.", name);
      return region;
    }

    public static Region CreateILRegion(string name, bool ack, bool caching,
      ICacheListener listener)
    {
      Init();
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetInitialCapacity(100000) //.SetScope(ack ? ScopeType.DistributedAck : ScopeType.DistributedNoAck)
        .SetCachingEnabled(caching);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }

      region = regionFactory.Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
      return region;
    }

    public static Region CreateSizeRegion(string name, int size, bool ack,
      bool caching)
    {
      Init();
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetLruEntriesLimit(0).SetInitialCapacity(size)
        //.SetScope(ack ? ScopeType.DistributedAck : ScopeType.DistributedNoAck)
        .SetCachingEnabled(caching).Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
      return region;
    }

    public static Region CreateLRURegion(string name, uint size)
    {
      return CreateLRURegion(name, size, ScopeType.Local);
    }

    public static Region CreateLRURegion(string name, uint size, ScopeType scope)
    {
      Init();
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.LOCAL_ENTRY_LRU)
        .SetLruEntriesLimit(size).SetInitialCapacity((int)size)
        /*.SetScope(scope)*/ .Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
      return region;
    }

    public static Region CreateTCRegion(string name, bool ack, bool caching,
      ICacheListener listener, string endpoints, bool clientNotification)
    {
      Init();
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      AttributesFactory af = new AttributesFactory();
      af.SetInitialCapacity(100000);
      af.SetScope(ack ? ScopeType.DistributedAck : ScopeType.DistributedNoAck);
      af.SetCachingEnabled(caching);
      if (listener != null)
      {
        af.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }
      if (endpoints != null)
      {
        af.SetEndpoints(endpoints);
      }
      af.SetClientNotificationEnabled(clientNotification);
      RegionAttributes rattrs = af.CreateRegionAttributes();
      region = m_cache.CreateRegion(name, rattrs);
      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
      Util.Log("Region {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static Pool CreatePool(string name, string endpoints, string locators, string serverGroup,
      int redundancy, bool subscription)
    {
      return CreatePool(name, endpoints, locators, serverGroup, redundancy, subscription, 5, 1, 300);
    }

    public static Pool CreatePool(string name, string endpoints, string locators, string serverGroup,
      int redundancy, bool subscription, int numConnections, bool isMultiuserMode)
    {
      return CreatePool(name, endpoints, locators, serverGroup, redundancy, subscription, numConnections, 1, 300, isMultiuserMode);
    }

    public static Pool CreatePool(string name, string endpoints, string locators, string serverGroup,
      int redundancy, bool subscription, int numConnections)
    {
      return CreatePool(name, endpoints, locators, serverGroup, redundancy, subscription, numConnections, 1, 300);
    }

    public static Pool CreatePool(string name, string endpoints, string locators, string serverGroup,
      int redundancy, bool subscription, int ackInterval, int dupCheckLife)
    {
      return CreatePool(name, endpoints, locators, serverGroup, redundancy, subscription,
        5, ackInterval, dupCheckLife);
    }

    public static Pool CreatePool(string name, string endpoints, string locators, string serverGroup,
      int redundancy, bool subscription, int numConnections, int ackInterval, int dupCheckLife)
    {
      return CreatePool(name, endpoints, locators, serverGroup, redundancy, subscription, numConnections, ackInterval, 300, false);
    }

    public static Pool CreatePool(string name, string endpoints, string locators, string serverGroup,
      int redundancy, bool subscription, int numConnections, int ackInterval, int dupCheckLife, bool isMultiuserMode)
    {
      Init();

      Pool existing = PoolManager.Find(name);

      if (existing == null)
      {
        PoolFactory fact = PoolManager.CreateFactory();
        if (endpoints != null)
        {
          string[] list = endpoints.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddServer(parts[0], int.Parse(parts[1]));
          }
        }
        else if (locators != null)
        {
          string[] list = locators.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddLocator(parts[0], int.Parse(parts[1]));
          }
        }
        else
        {
          Util.Log("No locators or servers specified for pool");
        }
        if (serverGroup != null)
        {
          fact.SetServerGroup(serverGroup);
        }
        fact.SetSubscriptionRedundancy(redundancy);
        fact.SetSubscriptionEnabled(subscription);
        fact.SetSubscriptionAckInterval(ackInterval);
        fact.SetSubscriptionMessageTrackingTimeout(dupCheckLife);
        fact.SetMultiuserAuthentication(isMultiuserMode);
        if (numConnections >= 0)
        {
          fact.SetMinConnections(numConnections);
          fact.SetMaxConnections(numConnections);
        }
        Pool pool = fact.Create(name);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
        return pool;
      }
      else
      {
        return existing;
      }
    }

    public static Region CreateTCRegion_Pool(string name, bool ack, bool caching,
      ICacheListener listener, string endpoints, string locators, string poolName, bool clientNotification)
    {
      return CreateTCRegion_Pool(name, ack, caching, listener, endpoints, locators, poolName,
        clientNotification, false, false);
    }

    public static void CreateTCRegion_Pool_AD1(string name, bool ack, bool caching,
       string endpoints, string locators, string poolName, bool clientNotification)
    {
      CreateTCRegion_Pool_AD(name, ack, caching, null, endpoints, locators, poolName, clientNotification, false, false);
    }

    public static GemStone.GemFire.Cache.Region CreateTCRegion_Pool_AD(string name, bool ack, bool caching,
      ICacheListener listener, string endpoints, string locators, string poolName, bool clientNotification, bool ssl,
      bool cloningEnabled)
    {
      if (ssl)
      {
        Properties sysProps = new Properties();
        string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
        sysProps.Insert("ssl-enabled", "true");
        sysProps.Insert("ssl-keystore", keystore + "/client_keystore.pem");
        sysProps.Insert("ssl-truststore", keystore + "/client_truststore.pem");
        InitConfig(sysProps);
      }
      else
      {
        Properties sysProps = new Properties();
        sysProps.Insert("appdomain-enabled", "true");

        InitConfig(sysProps);
      }
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      if (PoolManager.Find(poolName) == null)
      {
        PoolFactory fact = PoolManager.CreateFactory();
        fact.SetSubscriptionEnabled(clientNotification);
        if (endpoints != null)
        {
          string[] list = endpoints.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddServer(parts[0], int.Parse(parts[1]));
          }
        }
        else if (locators != null)
        {
          string[] list = locators.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddLocator(parts[0], int.Parse(parts[1]));
          }
        }
        else
        {
          Util.Log("No locators or servers specified for pool");
        }
        Pool pool = fact.Create(poolName);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
      }

      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetInitialCapacity(100000).SetPoolName(poolName).SetCloningEnabled(cloningEnabled)
        //.SetScope(ack ? ScopeType.DistributedAck : ScopeType.DistributedNoAck)
        .SetCachingEnabled(caching);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }

      region = regionFactory.Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
      Util.Log("Region {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static Region CreateTCRegion_Pool(string name, bool ack, bool caching,
      ICacheListener listener, string endpoints, string locators, string poolName, bool clientNotification, bool ssl,
      bool cloningEnabled)
    {
      if (ssl)
      {
        Properties sysProps = new Properties();
        string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
        sysProps.Insert("ssl-enabled", "true");
        sysProps.Insert("ssl-keystore", keystore + "/client_keystore.pem");
        sysProps.Insert("ssl-truststore", keystore + "/client_truststore.pem");
        InitConfig(sysProps);
      }
      else
      {
        Init();
      }
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      if (PoolManager.Find(poolName) == null)
      {
        PoolFactory fact = PoolManager.CreateFactory();
        fact.SetSubscriptionEnabled(clientNotification);
        if (endpoints != null)
        {
          string[] list = endpoints.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddServer(parts[0], int.Parse(parts[1]));
          }
        }
        else if (locators != null)
        {
          string[] list = locators.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddLocator(parts[0], int.Parse(parts[1]));
          }
        }
        else
        {
          Util.Log("No locators or servers specified for pool");
        }
        Pool pool = fact.Create(poolName);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
      }

      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetInitialCapacity(100000).SetPoolName(poolName).SetCloningEnabled(cloningEnabled)
        //.SetScope(ack ? ScopeType.DistributedAck : ScopeType.DistributedNoAck)
        .SetCachingEnabled(caching);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }

      region = regionFactory.Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
      Util.Log("Region {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static Region CreateTCRegion_Pool2(string name, bool ack, bool caching,
      ICacheListener listener, string endpoints, string locators, string poolName, bool clientNotification, bool ssl,
      bool cloningEnabled, IPartitionResolver pr)
    {
      if (ssl)
      {
        Properties sysProps = new Properties();
        string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
        sysProps.Insert("ssl-enabled", "true");
        sysProps.Insert("ssl-keystore", keystore + "/client_keystore.pem");
        sysProps.Insert("ssl-truststore", keystore + "/client_truststore.pem");
        InitConfig(sysProps);
      }
      else
      {
        Init();
      }
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      if (PoolManager.Find(poolName) == null)
      {
        PoolFactory fact = PoolManager.CreateFactory();
        fact.SetSubscriptionEnabled(clientNotification);
        if (endpoints != null)
        {
          string[] list = endpoints.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddServer(parts[0], int.Parse(parts[1]));
          }
        }
        else if (locators != null)
        {
          string[] list = locators.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddLocator(parts[0], int.Parse(parts[1]));
          }
        }
        else
        {
          Util.Log("No locators or servers specified for pool");
        }
        Pool pool = fact.Create(poolName);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
      }

      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetInitialCapacity(100000).SetPoolName(poolName).SetCloningEnabled(cloningEnabled)
        //.SetScope(ack ? ScopeType.DistributedAck : ScopeType.DistributedNoAck)
        .SetCachingEnabled(caching);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }

      if (pr != null)
      {
        Util.Log("setting custom partition resolver {0} ", pr.GetName());
        regionFactory.SetPartitionResolver(pr);
      }
      else
      {
        Util.Log("Resolver is null {0}", pr);
      }
      region = regionFactory.Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
      Util.Log("Region {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static Region CreateTCRegion_Pool1(string name, bool ack, bool caching,
    ICacheListener listener, string endpoints, string locators, string poolName, bool clientNotification, bool ssl,
    bool cloningEnabled, IPartitionResolver pr)
    {
      if (ssl)
      {
        Properties sysProps = new Properties();
        string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
        sysProps.Insert("ssl-enabled", "true");
        sysProps.Insert("ssl-keystore", keystore + "/client_keystore.pem");
        sysProps.Insert("ssl-truststore", keystore + "/client_truststore.pem");
        InitConfig(sysProps);
      }
      else
      {
        Init();
      }
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      if (PoolManager.Find(poolName) == null)
      {
        PoolFactory fact = PoolManager.CreateFactory();
        fact.SetSubscriptionEnabled(clientNotification);
        if (endpoints != null)
        {
          string[] list = endpoints.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddServer(parts[0], int.Parse(parts[1]));
          }
        }
        else if (locators != null)
        {
          string[] list = locators.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddLocator(parts[0], int.Parse(parts[1]));
          }
        }
        else
        {
          Util.Log("No locators or servers specified for pool");
        }
        Pool pool = fact.Create(poolName);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
      }

      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetInitialCapacity(100000).SetPoolName(poolName).SetCloningEnabled(cloningEnabled)
        //.SetScope(ack ? ScopeType.DistributedAck : ScopeType.DistributedNoAck)
        .SetCachingEnabled(caching);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }

      if (pr != null)
      {
        Util.Log("setting custom partition resolver {0} ", pr.GetName());
        regionFactory.SetPartitionResolver(pr);
      }
      else
      {
        Util.Log("Resolver is null {0}", pr);
      }
      region = regionFactory.Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
      Util.Log("Region {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static Region CreateLRUTCRegion_Pool(string name, bool ack, bool caching,
     ICacheListener listener, string endpoints, string locators, string poolName, bool clientNotification, uint lru)
    {
      Init();
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      if (PoolManager.Find(poolName) == null)
      {
        PoolFactory fact = PoolManager.CreateFactory();
        fact.SetSubscriptionEnabled(clientNotification);
        if (endpoints != null)
        {
          string[] list = endpoints.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddServer(parts[0], int.Parse(parts[1]));
          }
        }
        else if (locators != null)
        {
          string[] list = locators.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddLocator(parts[0], int.Parse(parts[1]));
          }
        }
        else
        {
          Util.Log("No locators or servers specified for pool");
        }
        Pool pool = fact.Create(poolName);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
      }

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

      RegionFactory regionFactory = m_cache
        .CreateRegionFactory(RegionShortcut.CACHING_PROXY_ENTRY_LRU)
        .SetDiskPolicy(DiskPolicyType.Overflows)
        .SetInitialCapacity(100000).SetPoolName(poolName)
        //.SetScope(ack ? ScopeType.DistributedAck : ScopeType.DistributedNoAck)
        .SetCachingEnabled(caching).SetLruEntriesLimit(lru)
        .SetPersistenceManager("BDBImpl", "createBDBInstance", bdbProperties);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }

      region = regionFactory.Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
      Util.Log("Region {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }
    public static Region CreateTCRegion_Pool(string name, bool ack, bool caching,
      ICacheListener listener, string locators, string poolName, bool clientNotification, string serverGroup)
    {
      Init();
      Region region = GetRegion(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.LocalDestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "Region {0} was not destroyed.", name);
      }

      if (PoolManager.Find(poolName) == null)
      {
        PoolFactory fact = PoolManager.CreateFactory();
        fact.SetSubscriptionEnabled(clientNotification);
        if (serverGroup != null)
        {
          fact.SetServerGroup(serverGroup);
        }
        if (locators != null)
        {
          string[] list = locators.Split(',');
          foreach (string item in list)
          {
            string[] parts = item.Split(':');
            fact.AddLocator(parts[0], int.Parse(parts[1]));
          }
        }
        else
        {
          Util.Log("No locators or servers specified for pool");
        }
        Pool pool = fact.Create(poolName);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
      }

      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetInitialCapacity(100000).SetPoolName(poolName)
        //.SetScope(ack ? ScopeType.DistributedAck : ScopeType.DistributedNoAck)
        .SetCachingEnabled(caching);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }

      region = regionFactory.Create(name);

      Assert.IsNotNull(region, "Region {0} was not created.", name);
      m_currRegion = region;
      Util.Log("Region {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static void DestroyRegion(string name, bool local, bool verify)
    {
      Region region;
      if (verify)
      {
        region = GetVerifyRegion(name);
      }
      else
      {
        region = GetRegion(name);
      }
      if (region != null)
      {
        if (local)
        {
          region.LocalDestroyRegion();
          Util.Log("Locally destroyed region {0}", name);
        }
        else
        {
          region.DestroyRegion();
          Util.Log("Destroyed region {0}", name);
        }
      }
    }

    public static void DestroyAllRegions(bool local)
    {
      if (m_cache != null && !m_cache.IsClosed)
      {
        Region[] regions = m_cache.RootRegions();
        if (regions != null)
        {
          foreach (Region region in regions)
          {
            if (local)
            {
              region.LocalDestroyRegion();
            }
            else
            {
              region.DestroyRegion();
            }
          }
        }
      }
    }

    public static void InvalidateRegion(string name, bool local, bool verify)
    {
      Region region;
      if (verify)
      {
        region = GetVerifyRegion(name);
        Util.Log("InvalidateRegion: GetVerifyRegion done for {0}", name);
      }
      else
      {
        region = GetRegion(name);
        Util.Log("InvalidateRegion: GetRegion done for {0}", name);
      }
      if (region != null)
      {
        if (local)
        {
          Util.Log("Locally invaliding region {0}", name);
          region.LocalInvalidateRegion();
          Util.Log("Locally invalidated region {0}", name);
        }
        else
        {
          Util.Log("Invalidating region {0}", name);
          region.InvalidateRegion();
          Util.Log("Invalidated region {0}", name);
        }
      }
    }

    #endregion

    #region Functions to obtain a region

    public static Region GetRegion(string path)
    {
      if (m_cache != null)
      {
        return m_cache.GetRegion(path);
      }
      return null;
    }

    public static Region GetRegionAD(string path)
    {
      if (m_cache != null)
      {
        return m_cache.GetRegion(path);
      }
      else if (DistributedSystem.IsConnected)
      {
        return CacheFactory.GetAnyInstance().GetRegion(path);
      }
      return null;
    }

    public static Region GetRegion(string path, Properties credentials)
    {
      if (m_cache != null)
      {
        Util.Log("GetRegion " + m_cacheForMultiUser);
        if (m_cacheForMultiUser == null)
        {
          Region region = GetRegion(path);
          Assert.IsNotNull(region, "Region [" + path + "] not found.");
          Assert.IsNotNull(region.Attributes.PoolName, "Region is created without pool.");

          Pool pool = PoolManager.Find(region.Attributes.PoolName);

          Assert.IsNotNull(pool, "Pool is null in GetVerifyRegion.");

          //m_cacheForMultiUser = pool.CreateSecureUserCache(credentials);
          m_cacheForMultiUser = m_cache.CreateAuthenticatedView(credentials, pool.Name);

          return m_cacheForMultiUser.GetRegion(path);
        }
        else
          return m_cacheForMultiUser.GetRegion(path);
      }
      return null;
    }

    public static IRegionService getMultiuserCache(Properties credentials)
    {
      if (m_cacheForMultiUser == null)
      {
        Pool pool = PoolManager.Find("__TESTPOOL1_");

        Assert.IsNotNull(pool, "Pool is null in getMultiuserCache.");
        Assert.IsTrue(!pool.Destroyed);

        //m_cacheForMultiUser = pool.CreateSecureUserCache(credentials);
        m_cacheForMultiUser = m_cache.CreateAuthenticatedView(credentials, pool.Name);

        return m_cacheForMultiUser;
      }
      else
        return m_cacheForMultiUser;
    }

    public static Region GetVerifyRegion(string path)
    {
      Region region = GetRegion(path);

      Assert.IsNotNull(region, "Region [" + path + "] not found.");
      Util.Log("Found region '{0}'", path);
      return region;
    }

    public static Region GetVerifyRegionAD(string path)
    {
      Region region = GetRegionAD(path);

      Assert.IsNotNull(region, "Region [" + path + "] not found.");
      Util.Log("Found region '{0}'", path);
      return region;
    }

    public static Region GetVerifyRegion(string path, Properties credentials)
    {
      Util.Log("GetVerifyRegion " + m_cacheForMultiUser);
      if (m_cacheForMultiUser == null)
      {
        Region region = GetRegion(path);
        Assert.IsNotNull(region, "Region [" + path + "] not found.");
        Assert.IsNotNull(region.Attributes.PoolName, "Region is created without pool.");

        Pool pool = PoolManager.Find(region.Attributes.PoolName);

        Assert.IsNotNull(pool, "Pool is null in GetVerifyRegion.");

        //m_cacheForMultiUser = pool.CreateSecureUserCache(credentials);
        m_cacheForMultiUser = m_cache.CreateAuthenticatedView(credentials, pool.Name);

        return m_cacheForMultiUser.GetRegion(path);
      }
      else
        return m_cacheForMultiUser.GetRegion(path);
    }

    public static void VerifyRegion(string path)
    {
      GetVerifyRegion(path);
    }

    public static void VerifyNoRegion(string path)
    {
      Region region = GetRegion(path);
      Assert.IsNull(region, "Region [" + path + "] should not exist.");
    }

    #endregion

    #region Functions to start/stop a java cacheserver for Thin Client regions

    public static void SetupJavaServers(params string[] cacheXmls)
    {
      SetupJavaServers(false, cacheXmls);
    }

    public static void setPorts(int s1, int s2, int s3, int l1, int l2)
    {
      HOST_PORT_1 = s1;
      HOST_PORT_2 = s1;
      HOST_PORT_3 = s3;
      LOCATOR_PORT_1 = l1;
      LOCATOR_PORT_2 = l2;
    }

    public static void SetupJavaServers(bool locators, params string[] cacheXmls)
    {
      createRandomPorts();
      m_cacheXmls = cacheXmls;
      m_gfeDir = Util.GetEnvironmentVariable("GFE_DIR");
      Assert.IsNotNull(m_gfeDir, "GFE_DIR is not set.");
      Assert.IsNotEmpty(m_gfeDir, "GFE_DIR is not set.");
      m_gfeLogLevel = Util.GetEnvironmentVariable("GFE_LOGLEVEL");
      m_gfeSecLogLevel = Util.GetEnvironmentVariable("GFE_SECLOGLEVEL");
      if (m_gfeLogLevel == null || m_gfeLogLevel.Length == 0)
      {
        m_gfeLogLevel = "config";
      }
      if (m_gfeSecLogLevel == null || m_gfeSecLogLevel.Length == 0)
      {
        m_gfeSecLogLevel = "config";
      }

      Match mt = Regex.Match(m_gfeDir, "^[^:]+:[0-9]+(,[^:]+:[0-9]+)*$");
      if (mt != null && mt.Length > 0)
      {
        // The GFE_DIR is for a remote server; contains an end-point list
        m_endpoints = m_gfeDir;
        m_localServer = false;
      }
      else if (cacheXmls != null)
      {
        // Assume the GFE_DIR is for a local server
        if (locators)
        {
          JavaMcastPort = 0;
        }
        else
        {
          JavaMcastPort = Util.Rand(2431, 31123);
        }

        for (int i = 0; i < cacheXmls.Length; i++)
        {
          string cacheXml = cacheXmls[i];
          Assert.IsNotNull(cacheXml, "cacheXml is not set for Java cacheserver.");
          Assert.IsNotEmpty(cacheXml, "cacheXml is not set for Java cacheserver.");
          string duplicateFile = "";
          // Assume the GFE_DIR is for a local server
          if (cacheXml.IndexOf(PathSep) < 0)
          {
            duplicateFile = Directory.GetCurrentDirectory() + PathSep + Util.Rand(2342350).ToString() + cacheXml;
            cacheXml = Directory.GetCurrentDirectory() + PathSep + cacheXml;
            createDuplicateXMLFile(cacheXml, duplicateFile);
            //Hitesh:create duplicate xml files
            cacheXmls[i] = duplicateFile;
          }

          //Console.WriteLine("hitesh filename:duplicateFile  " + duplicateFile);
          //Console.WriteLine("hitesh filename:cacheXml  " + cacheXml);
          // Find the port number from the given cache.xml
          XmlDocument xmlDoc = new XmlDocument();
          xmlDoc.Load(duplicateFile);
          XmlNode serverNode = xmlDoc.SelectSingleNode("/cache/bridge-server");
          if (m_endpoints == null)
          {
            m_endpoints = "localhost:" + serverNode.Attributes["port"].Value;
          }
          else
          {
            m_endpoints += ",localhost:" + serverNode.Attributes["port"].Value;
          }
        }
        Util.Log("JAVA server endpoints: " + m_endpoints);
      }
    }

    public static void createRandomPorts()
    {
      if (HOST_PORT_1 == 0)
      {
        HOST_PORT_1 = Util.RandPort(10000, 64000);
        HOST_PORT_2 = Util.RandPort(10000, 64000);
        HOST_PORT_3 = Util.RandPort(10000, 64000);
        HOST_PORT_4 = Util.RandPort(10000, 64000);
      }

      if (LOCATOR_PORT_1 == 0)
      {
        LOCATOR_PORT_1 = Util.RandPort(10000, 64000);
        LOCATOR_PORT_2 = Util.RandPort(10000, 64000);
        LOCATOR_PORT_3 = Util.RandPort(10000, 64000);
        LOCATOR_PORT_4 = Util.RandPort(10000, 64000);
      }
    }

    public static void createDuplicateXMLFile(string orignalFilename, string duplicateFilename)
    {
      string cachexmlstring = File.ReadAllText(orignalFilename);
      cachexmlstring = cachexmlstring.Replace("HOST_PORT1", HOST_PORT_1.ToString());
      cachexmlstring = cachexmlstring.Replace("HOST_PORT2", HOST_PORT_2.ToString());
      cachexmlstring = cachexmlstring.Replace("HOST_PORT3", HOST_PORT_3.ToString());
      cachexmlstring = cachexmlstring.Replace("HOST_PORT4", HOST_PORT_4.ToString());

      cachexmlstring = cachexmlstring.Replace("LOC_PORT1", LOCATOR_PORT_1.ToString());
      cachexmlstring = cachexmlstring.Replace("LOC_PORT2", LOCATOR_PORT_2.ToString());
      cachexmlstring = cachexmlstring.Replace("LOC_PORT3", LOCATOR_PORT_3.ToString());
      //cachexmlstring = cachexmlstring.Replace("LOC_PORT4", LOCATOR_PORT_4.ToString());

      File.Create(duplicateFilename).Close();
      File.WriteAllText(duplicateFilename, cachexmlstring);
    }

    public static void StartJavaLocator(int locatorNum, string startDir)
    {
      StartJavaLocator(locatorNum, startDir, null);
    }
    public static int getBaseLocatorPort()
    {
      return LocatorPort;
    }

    public static void StartJavaLocator(int locatorNum, string startDir,
      string extraLocatorArgs)
    {
      StartJavaLocator(locatorNum, startDir, extraLocatorArgs, false);
    }

    public static void StartJavaLocator(int locatorNum, string startDir,
      string extraLocatorArgs, bool ssl)
    {
      if (m_localServer)
      {
        Process javaProc;
        string locatorPath = m_gfeDir + PathSep + "bin" + PathSep + GemFireName;
        Util.Log("Starting locator {0} in directory {1}.", locatorNum, startDir);
        if (startDir != null)
        {
          startDir += Util.Rand(64687687);
          if (!Directory.Exists(startDir))
          {
            Directory.CreateDirectory(startDir);
          }
          try
          {
            TextWriter tw = new StreamWriter(Directory.GetCurrentDirectory() + "\\" + startDir + "\\gemfire.properties", false);
            tw.WriteLine("locators=localhost[{0}],localhost[{1}],localhost[{2}]", LOCATOR_PORT_1, LOCATOR_PORT_2, LOCATOR_PORT_3);
            if (ssl)
            {
              tw.WriteLine("ssl-enabled=true");
              tw.WriteLine("ssl-require-authentication=true");
              tw.WriteLine("ssl-ciphers=SSL_RSA_WITH_NULL_MD5");
              tw.WriteLine("mcast-port=0");
            }
            tw.Close();
          }
          catch (Exception ex)
          {
            Assert.Fail("Locator property file creation failed: {0}: {1}", ex.GetType().Name, ex.Message);
          }
          startDir = " -dir=" + startDir;
        }
        else
        {
          startDir = string.Empty;
        }

        string locatorPort = " -port=" + getLocatorPort(locatorNum);
        if (extraLocatorArgs != null)
        {
          extraLocatorArgs = ' ' + extraLocatorArgs + locatorPort;
        }
        else
        {
          extraLocatorArgs = locatorPort;
        }

        if (ssl)
        {
          string sslArgs = String.Empty;
          string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
          sslArgs += " -Djavax.net.ssl.keyStore=" + keystore + "/server_keystore.jks ";
          sslArgs += " -Djavax.net.ssl.keyStorePassword=gemstone ";
          sslArgs += " -Djavax.net.ssl.trustStore=" + keystore + "/server_truststore.jks  ";
          sslArgs += " -Djavax.net.ssl.trustStorePassword=gemstone ";
          extraLocatorArgs += sslArgs;
        }

        string locatorArgs = LocatorStartArgs + startDir + extraLocatorArgs;

        if (!Util.StartProcess(locatorPath, locatorArgs, false, null, true,
          false, false, true, out javaProc))
        {
          Assert.Fail("Failed to run the locator: {0}.",
            locatorPath);
        }

        StreamReader outSr = javaProc.StandardOutput;
        // Wait for cache server to start
        bool started = javaProc.WaitForExit(MaxWaitMillis);
        Util.Log("Output from '{0} {1}':{2}{3}", GemFireName, locatorArgs,
          Environment.NewLine, outSr.ReadToEnd());
        outSr.Close();
        if (!started)
        {
          javaProc.Kill();
        }
        Assert.IsTrue(started, "Timed out waiting for " +
          "Locator to start.{0}Please check the locator logs.",
          Environment.NewLine);
        m_runningLocators[locatorNum] = startDir;
        if (m_locators == null)
        {
          m_locators = "localhost:" + getLocatorPort(locatorNum);
        }
        else
        {
          m_locators += ",localhost:" + getLocatorPort(locatorNum);
        }
        Util.Log("JAVA locator endpoints: " + m_locators);
      }
    }

    static int getLocatorPort(int num)
    {
      switch (num)
      {
        case 1:
          return LOCATOR_PORT_1;
        case 2:
          return LOCATOR_PORT_2;
        case 3:
          return LOCATOR_PORT_3;
        default:
          return LOCATOR_PORT_1;
      }
    }

    public static void StartJavaServerWithLocators(int serverNum, string startDir, int numLocators)
    {
      StartJavaServerWithLocators(serverNum, startDir, numLocators, false);
    }

    public static void StartJavaServerWithLocators(int serverNum, string startDir, int numLocators, bool ssl)
    {
      string extraServerArgs = "locators=";
      for (int locator = 0; locator < numLocators; locator++)
      {
        if (locator > 0)
        {
          extraServerArgs += ",";
        }
        extraServerArgs += "localhost:" + getLocatorPort(locator + 1);
      }
      if (ssl)
      {
        string sslArgs = String.Empty;
        sslArgs += " ssl-enabled=true ssl-require-authentication=true ssl-ciphers=SSL_RSA_WITH_NULL_MD5 ";
        string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
        sslArgs += " -J-Djavax.net.ssl.keyStore=" + keystore + "/server_keystore.jks ";
        sslArgs += " -J-Djavax.net.ssl.keyStorePassword=gemstone ";
        sslArgs += " -J-Djavax.net.ssl.trustStore=" + keystore + "/server_truststore.jks  ";
        sslArgs += " -J-Djavax.net.ssl.trustStorePassword=gemstone ";
        extraServerArgs += sslArgs;
      }
      StartJavaServer(serverNum, startDir, extraServerArgs);
    }

    public static void StartJavaServer(int serverNum, string startDir)
    {
      StartJavaServer(serverNum, startDir, null);
    }

    public static void StartJavaServerWithLocators(int serverNum, string startDir,
      int numLocators, string extraServerArgs)
    {
      StartJavaServerWithLocators(serverNum, startDir, numLocators, extraServerArgs, false);
    }

    public static void StartJavaServerWithLocators(int serverNum, string startDir,
      int numLocators, string extraServerArgs, bool ssl)
    {
      extraServerArgs += " locators=";
      for (int locator = 0; locator < numLocators; locator++)
      {
        if (locator > 0)
        {
          extraServerArgs += ",";
        }
        extraServerArgs += "localhost:" + getLocatorPort(locator + 1);
      }
      if (ssl)
      {
        string sslArgs = String.Empty;
        sslArgs += " ssl-enabled=true ssl-require-authentication=true ssl-ciphers=SSL_RSA_WITH_NULL_MD5 ";
        string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
        sslArgs += " -J-Djavax.net.ssl.keyStore=" + keystore + "/server_keystore.jks ";
        sslArgs += " -J-Djavax.net.ssl.keyStorePassword=gemstone ";
        sslArgs += " -J-Djavax.net.ssl.trustStore=" + keystore + "/server_truststore.jks  ";
        sslArgs += " -J-Djavax.net.ssl.trustStorePassword=gemstone ";
        extraServerArgs += sslArgs;
      }
      StartJavaServer(serverNum, startDir, extraServerArgs);
    }

    public static void StartJavaServer(int serverNum, string startDir,
      string extraServerArgs)
    {
      if (m_localServer)
      {
        if (m_cacheXmls == null || serverNum > m_cacheXmls.Length)
        {
          Assert.Fail("SetupJavaServers called with incorrect parameters: " +
            "could not find cache.xml for server number {0}", serverNum);
        }
        string cacheXml = m_cacheXmls[serverNum - 1];
        Process javaProc;
        string javaServerPath = m_gfeDir + PathSep + "bin" + PathSep + JavaServerName;
        startDir += Util.Rand(372468723).ToString();
        Util.Log("Starting server {0} in directory {1}.", serverNum, startDir);
        if (startDir != null)
        {
          if (!Directory.Exists(startDir))
          {
            Directory.CreateDirectory(startDir);
          }
          startDir = " -dir=" + startDir;
        }
        else
        {
          startDir = string.Empty;
        }
        if (extraServerArgs != null)
        {
          extraServerArgs = ' ' + extraServerArgs;
        }
        string serverArgs = JavaServerStartArgs + cacheXml + " mcast-port=" +
          JavaMcastPort + " log-level=" + m_gfeLogLevel + startDir +
          " security-log-level=" + m_gfeSecLogLevel + extraServerArgs;
        if (!Util.StartProcess(javaServerPath, serverArgs, false, null, true,
          false, false, true, out javaProc))
        {
          Assert.Fail("Failed to run the java cacheserver executable: {0}.",
            javaServerPath);
        }

        StreamReader outSr = javaProc.StandardOutput;
        // Wait for cache server to start
        bool started = javaProc.WaitForExit(MaxWaitMillis);
        Util.Log("Output from '{0} {1}':{2}{3}", JavaServerName, serverArgs,
          Environment.NewLine, outSr.ReadToEnd());
        outSr.Close();
        if (!started)
        {
          javaProc.Kill();
        }
        Assert.IsTrue(started, "Timed out waiting for " +
          "Java cacheserver to start.{0}Please check the server logs.",
          Environment.NewLine);
        m_runningJavaServers[serverNum] = startDir;
      }
    }

    public static void StopJavaLocator(int locatorNum)
    {
      StopJavaLocator(locatorNum, true, false);
    }

    public static void StopJavaLocator(int locatorNum, bool verifyLocator)
    {
      StopJavaLocator(locatorNum, verifyLocator, false);
    }

    public static void StopJavaLocator(int locatorNum, bool verifyLocator, bool ssl)
    {
      if (m_localServer)
      {
        // Assume the GFE_DIR is for a local server
        string startDir;
        if (m_runningLocators.TryGetValue(locatorNum, out startDir))
        {
          Util.Log("Stopping locator {0} in directory {1}.", locatorNum, startDir);
          Process javaStopProc;
          string javaLocatorPath = m_gfeDir + PathSep + "bin" + PathSep + GemFireName;
          string locatorPort = " -port=" + getLocatorPort(locatorNum);
          string sslArgs = String.Empty;
          if (ssl)
          {
            string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
            sslArgs += " -Djavax.net.ssl.keyStore=" + keystore + "/server_keystore.jks ";
            sslArgs += " -Djavax.net.ssl.keyStorePassword=gemstone ";
            sslArgs += " -Djavax.net.ssl.trustStore=" + keystore + "/server_truststore.jks  ";
            sslArgs += " -Djavax.net.ssl.trustStorePassword=gemstone ";
            string propdir = startDir.Replace("-dir=", string.Empty).Trim();
            File.Copy(propdir + "/gemfire.properties", Directory.GetCurrentDirectory() + "/gemfire.properties", true);
          }
          if (!Util.StartProcess(javaLocatorPath, LocatorStopArgs + locatorPort + startDir + sslArgs,
            false, null, true, false, false, true, out javaStopProc))
          {
            Assert.Fail("Failed to run the executable: {0}.",
              javaLocatorPath);
          }

          StreamReader outSr = javaStopProc.StandardOutput;
          // Wait for cache server to stop
          bool stopped = javaStopProc.WaitForExit(MaxWaitMillis);
          Util.Log("Output from '{0} stop-locator':{1}{2}", GemFireName,
            Environment.NewLine, outSr.ReadToEnd());
          outSr.Close();
          if (!stopped)
          {
            javaStopProc.Kill();
          }
          if (ssl)
          {
            File.Delete(Directory.GetCurrentDirectory() + "/gemfire.properties");
          }
          Assert.IsTrue(stopped, "Timed out waiting for " +
            "Java locator to stop.{0}Please check the locator logs.",
            Environment.NewLine);
          m_runningLocators.Remove(locatorNum);
          Util.Log("Locator {0} in directory {1} stopped.", locatorNum,
            startDir.Replace("-dir=", string.Empty).Trim());
        }
        else
        {
          if (verifyLocator)
          {
            Assert.Fail("StopJavaLocator() invoked for a non-existing locator {0}",
              locatorNum);
          }
        }
      }
    }

    public static void StopJavaServer(int serverNum)
    {
      StopJavaServer(serverNum, true);
    }

    public static void StopJavaServer(int serverNum, bool verifyServer)
    {
      if (m_localServer)
      {
        // Assume the GFE_DIR is for a local server
        string startDir;
        if (m_runningJavaServers.TryGetValue(serverNum, out startDir))
        {
          Util.Log("Stopping server {0} in directory {1}.", serverNum, startDir);
          Process javaStopProc;
          string javaServerPath = m_gfeDir + PathSep + "bin" + PathSep + JavaServerName;
          if (!Util.StartProcess(javaServerPath, JavaServerStopArgs + startDir,
            false, null, true, false, false, true, out javaStopProc))
          {
            Assert.Fail("Failed to run the java cacheserver executable: {0}.",
              javaServerPath);
          }

          StreamReader outSr = javaStopProc.StandardOutput;
          // Wait for cache server to stop
          bool stopped = javaStopProc.WaitForExit(MaxWaitMillis);
          Util.Log("Output from '{0} stop':{1}{2}", JavaServerName,
            Environment.NewLine, outSr.ReadToEnd());
          outSr.Close();
          if (!stopped)
          {
            javaStopProc.Kill();
          }
          Assert.IsTrue(stopped, "Timed out waiting for " +
            "Java cacheserver to stop.{0}Please check the server logs.",
            Environment.NewLine);
          m_runningJavaServers.Remove(serverNum);
          Util.Log("Server {0} in directory {1} stopped.", serverNum,
            startDir.Replace("-dir=", string.Empty).Trim());
        }
        else
        {
          if (verifyServer)
          {
            Assert.Fail("StopJavaServer() invoked for a non-existing server {0}",
              serverNum);
          }
        }
      }
    }

    public static void StopJavaServers()
    {
      int[] runningServers = new int[m_runningJavaServers.Count];
      m_runningJavaServers.Keys.CopyTo(runningServers, 0);
      foreach (int serverNum in runningServers)
      {
        StopJavaServer(serverNum);
        Util.Log("Cacheserver {0} stopped.", serverNum);
      }
      m_runningJavaServers.Clear();
    }

    public static void StopJavaLocators()
    {
      int[] runningServers = new int[m_runningLocators.Count];
      m_runningLocators.Keys.CopyTo(runningServers, 0);
      foreach (int serverNum in runningServers)
      {
        StopJavaLocator(serverNum);
        Util.Log("Locator {0} stopped.", serverNum);
      }
      m_runningLocators.Clear();
    }

    public static void ClearEndpoints()
    {
      m_endpoints = null;
    }

    public static void ClearLocators()
    {
      m_locators = null;
    }

    public static void EndTest()
    {
      Util.Log("Cache Helper EndTest.");
      StopJavaServers();
      StopJavaLocators();
      ClearEndpoints();
      ClearLocators();

      Util.Log("Cache Helper EndTest completed.");
    }

    #endregion

    #region Utility functions

    public static void ShowKeys(ICacheableKey[] cKeys)
    {
      if (cKeys != null)
      {
        for (int i = 0; i < cKeys.Length; i++)
        {
          Util.Log("Key [{0}] = {1}", i, cKeys[i]);
        }
      }
    }

    public static void ShowValues(IGFSerializable[] cValues)
    {
      if (cValues != null)
      {
        for (int i = 0; i < cValues.Length; i++)
        {
          Util.Log("Value [{0}] = {1}", i, cValues[i]);
        }
      }
    }

    public static string RegionAttributesToString(RegionAttributes attrs)
    {
      string poolName = "RegionWithoutPool";

      if (attrs.PoolName != null)
        poolName = attrs.PoolName;

      StringBuilder attrsSB = new StringBuilder();
      attrsSB.Append(Environment.NewLine + "scope: " +
        attrs.Scope);
      attrsSB.Append(Environment.NewLine + "caching: " +
        attrs.CachingEnabled);
      attrsSB.Append(Environment.NewLine + "endpoints: " +
        attrs.Endpoints);
      attrsSB.Append(Environment.NewLine + "clientNotification: " +
        attrs.ClientNotificationEnabled);
      attrsSB.Append(Environment.NewLine + "initialCapacity: " +
        attrs.InitialCapacity);
      attrsSB.Append(Environment.NewLine + "loadFactor: " +
        attrs.LoadFactor);
      attrsSB.Append(Environment.NewLine + "concurrencyLevel: " +
        attrs.ConcurrencyLevel);
      attrsSB.Append(Environment.NewLine + "lruEntriesLimit: " +
        attrs.LruEntriesLimit);
      attrsSB.Append(Environment.NewLine + "lruEvictionAction: " +
        attrs.LruEvictionAction);
      attrsSB.Append(Environment.NewLine + "entryTimeToLive: " +
        attrs.EntryTimeToLive);
      attrsSB.Append(Environment.NewLine + "entryTimeToLiveAction: " +
        attrs.EntryTimeToLiveAction);
      attrsSB.Append(Environment.NewLine + "entryIdleTimeout: " +
        attrs.EntryIdleTimeout);
      attrsSB.Append(Environment.NewLine + "entryIdleTimeoutAction: " +
        attrs.EntryIdleTimeoutAction);
      attrsSB.Append(Environment.NewLine + "regionTimeToLive: " +
        attrs.RegionTimeToLive);
      attrsSB.Append(Environment.NewLine + "regionTimeToLiveAction: " +
        attrs.RegionTimeToLiveAction);
      attrsSB.Append(Environment.NewLine + "regionIdleTimeout: " +
        attrs.RegionIdleTimeout);
      attrsSB.Append(Environment.NewLine + "regionIdleTimeoutAction: " +
        attrs.RegionIdleTimeoutAction);
      attrsSB.Append(Environment.NewLine + "PoolName: " +
        poolName);
      attrsSB.Append(Environment.NewLine);
      return attrsSB.ToString();
    }

    #endregion
  }
}
