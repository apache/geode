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

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  using System.Management;

  public class PropsStringToObject
  {
    public PropsStringToObject(Properties<string, object> target)
    {
      m_target = target;
    }

    public void Visit(string key, string val)
    {
      if (key == "security-signature")
      {
        Util.Log("VJR: Got SIG as " + val);
        string[] stringbytes = val.Split(' ');
        byte[] credentialbytes = new byte[stringbytes.Length - 1];
        int position = 0;
        foreach (string item in stringbytes)
        {
          Util.Log("VJR: Parsing byte " + item);
          if (string.IsNullOrEmpty(item)) continue;
          credentialbytes[position++] = byte.Parse(item);
        }
        m_target.Insert(key, credentialbytes);
      }
      else
      {
        m_target.Insert(key, val);
      }
    }

    private Properties<string, object> m_target;
  }

  public class PutGetTestsAD : MarshalByRefObject
  {
    private static string m_regionName;
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
      CacheHelper.InvalidateRegion<object, object>(regionName, true, true);
    }

    public void DoRunQuery()
    {
      m_putGetTestInstance.DoRunQuery();
    }

    public void SetRegion(string regionName)
    {
      m_regionName = regionName;
      m_putGetTestInstance.SetRegion(regionName);
    }

    public void DoGets()
    {
      m_putGetTestInstance.DoGets();
    }

    public void pdxPutGet(bool caching, bool readPdxSerialized)
    {
      Serializable.RegisterPdxType(PdxTests.PdxType.CreateDeserializable);
      IRegion<object, object> reg = CacheHelper.GetRegion<object, object>(m_regionName);
      PdxTests.PdxType pt = new PdxTests.PdxType();
      reg["pi"] = pt;

      object pi = null;

      if (caching)
      {
        pi = reg.GetLocalView()["pi"];
      }
      else
      {
        pi = reg["pi"];
      }

      if (readPdxSerialized)
      {
        int iv = (int)((IPdxInstance)pi).GetField("m_int32");
        Assert.AreEqual(iv, pt.Int32);
      }
      else
      {
        Assert.AreEqual(pi, pt);
      }
    }

    public void pdxGetPut(bool caching, bool readPdxSerialized)
    {
      Serializable.RegisterPdxType(PdxTests.PdxType.CreateDeserializable);
      IRegion<object, object> reg = CacheHelper.GetRegion<object, object>(m_regionName);
      PdxTests.PdxType pt = new PdxTests.PdxType();

      object pi = null;

      if (caching)
      {
        pi = reg.GetLocalView()["pi"];
      }
      else
      {
        pi = reg["pi"];
      }

      if (readPdxSerialized)
      {
        int iv = (int)((IPdxInstance)pi).GetField("m_int32") + 10;

        IWritablePdxInstance wpi = ((IPdxInstance)pi).CreateWriter();

        wpi.SetField("m_int32", iv);

        reg["pi"] = wpi;

        if (caching)
        {
          pi = reg.GetLocalView()["pi"];
        }
        else
        {
          pi = reg["pi"];
        }

        iv = (int)((IPdxInstance)pi).GetField("m_int32");

        Assert.AreEqual(iv, pt.Int32 + 10);
      }
      else
      {
        Assert.AreEqual(pi, pt);
      }
    }
  }

  public class CacheHelperWrapper : MarshalByRefObject
  {
    public void CreateTCRegions_Pool_AD<TKey, TValue>(string[] regionNames,
      string locators, string poolName, bool clientNotification, bool ssl, bool caching, bool pdxReadSerialized)
    {
      try
      {
        Console.WriteLine("creating region1 " + pdxReadSerialized);
        CacheHelper.PdxReadSerialized = pdxReadSerialized;
        CacheHelper.CreateTCRegion_Pool_AD<TKey, TValue>(regionNames[0], true, caching,
          null, locators, poolName, clientNotification, ssl, false);
        //CacheHelper.CreateTCRegion_Pool(regionNames[1], false, true,
        //null, endpoints, locators, poolName, clientNotification, ssl, false);
        //     m_regionNames = regionNames;
        CacheHelper.PdxReadSerialized = false;
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
        //Console.WriteLine(" cakk CallDistrinbutedConnect");
        CacheHelper.DSConnectAD();
      }
      catch (Exception ex)
      {
        //Console.WriteLine(" cakk CallDistrinbutedConnect 33");
        Util.Log(" got AlreadyConnectedException " + ex.Message);
      }
    }

    public void RegisterBuiltins(long dtTime)
    {
      CacheableHelper.RegisterBuiltinsAD(dtTime);
    }

    public void InvalidateRegion(string regionName)
    {
      CacheHelper.InvalidateRegion<object, object>(regionName, true, true);
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

    public static bool PdxIgnoreUnreadFields = false;
    public static bool PdxReadSerialized = false;

    public static int LOCATOR_PORT_1;
    public static int LOCATOR_PORT_2;
    public static int LOCATOR_PORT_3;
    public static int LOCATOR_PORT_4;

    #region Private static members and constants

    private static DistributedSystem m_dsys = null;
    private static bool m_doDisconnect = false;
    private static Cache m_cache = null;
    private static IRegionService m_cacheForMultiUser = null;
    private static IRegion<object, object> m_currRegion = null;
    private static string m_gfeDir = null;
    private static string m_gfeLogLevel = null;
    private static string m_gfeSecLogLevel = null;
    private static string m_endpoints = null;
    private static string m_locators = null;
    private static string m_locatorFirst = null;
    private static string m_locatorSecond = null;
    private static string[] m_cacheXmls = null;
    private static bool m_localServer = true;
    private static string m_extraPropertiesFile = null;

    private const string DefaultDSName = "dstest";
    private const string DefaultCacheName = "cachetest";

    private const string JavaServerName = "gfsh.bat";
    private const string GemFireName = "gfsh.bat";
    private static int JavaMcastPort = -1;
    private const string JavaServerStartArgs =
      "start server --J=-Xmx512m --J=-Xms128m --J=-XX:+UseConcMarkSweepGC --J=-XX:+UseParNewGC --J=-Xss256k --cache-xml-file=";
    private const string JavaServerStopArgs = "stop server";
    private const string LocatorStartArgs = "start locator";
    private const string LocatorStopArgs = "stop locator";
    private const int LocatorPort = 34755;
    private const int MaxWaitMillis = 60000;
    private static char PathSep = Path.DirectorySeparatorChar;

    private static string m_testDir = null;
    private static Dictionary<int, string> m_runningJavaServers =
      new Dictionary<int, string>();
    private static Dictionary<int, string> m_runningLocators =
      new Dictionary<int, string>();
    private static CacheTransactionManager m_cstxManager = null;
    #endregion

    #region Public accessors

    public static IRegion<object, object> CurrentRegion
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
    public static CacheTransactionManager CSTXManager
    {
      get
      {
        return m_cstxManager;
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

    public static string Locators
    {
      get
      {
        return m_locators;
      }
    }

    public static string LocatorSecond
    {
      get
      {
        return m_locatorSecond;
      }
    }

    public static string LocatorFirst
    {
      get
      {
        return m_locatorFirst;
      }
    }

    public static string ExtraPropertiesFile
    {
      get
      {
        return m_extraPropertiesFile;
      }
    }

    /*
    public static QueryService QueryServiceInstance
    {
      get
      {
        return m_cache.GetQueryService();
      }
    }
     * */

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

    public static void DSConnectAD()
    {
      m_dsys = DistributedSystem.Connect("DSName", null);
    }

    private static void SetLogConfig(ref Properties<string, string> config)
    {
      if (Util.LogFile != null)
      {
        Log.Close();
        if (config == null)
        {
          config = new Properties<string, string>();
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

    private static void DSConnect(string dsName, Properties<string, string> config)
    {
      SetLogConfig(ref config);
      m_dsys = DistributedSystem.Connect(dsName, config);
    }

    public static void ConnectName(string dsName)
    {
      ConnectConfig(dsName, null);
    }

    public static void ConnectConfig(string dsName, Properties<string, string> config)
    {
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
      InitConfig(null, null);
    }

    public static void InitConfig(Properties<string, string> config)
    {
      InitConfig(config, null);
    }

    public static void InitConfigForDurable_Pool(string locators, int redundancyLevel,
      string durableClientId, int durableTimeout)
    {
      InitConfigForDurable_Pool(locators, redundancyLevel, durableClientId, durableTimeout, 1);
    }

    public static void InitConfigForDurable_Pool(string locators, int redundancyLevel,
      string durableClientId, int durableTimeout, int ackInterval)
    {
      Properties<string, string> config = new Properties<string, string>();
      config.Insert("durable-client-id", durableClientId);
      config.Insert("durable-timeout", durableTimeout.ToString());
      InitConfig(config, null);
      CreatePool<object, object>("__TESTPOOL1_", locators, (string)null, redundancyLevel, true,
        ackInterval, 300);
    }

    public static void InitConfigForDurable_Pool2(string locators, int redundancyLevel,
      string durableClientId, int durableTimeout, int ackInterval, string poolName)
    {
        Properties<string, string> config = new Properties<string, string>();
        config.Insert("durable-client-id", durableClientId);
        config.Insert("durable-timeout", durableTimeout.ToString());
        InitConfig(config, null);
        CreatePool<object, object>(poolName, locators, (string)null, redundancyLevel, true,
          ackInterval, 300);
    }

    public static void InitConfigForConflation(string durableClientId, string conflation)
    {
      Properties<string, string> config = new Properties<string, string>();
      config.Insert("durable-client-id", durableClientId);
      config.Insert("durable-timeout", "300");
      config.Insert("notify-ack-interval", "1");
      if (conflation != null && conflation.Length > 0)
      {
        config.Insert("conflate-events", conflation);
      }
      InitConfig(config, null);
    }

    static int m_heapLimit = -1;
    static int m_delta = -1;
    static public void SetHeapLimit(int maxheaplimit, int delta)
    {
      m_heapLimit = maxheaplimit;
      m_delta = delta;
    }

    static public void UnsetHeapLimit()
    {
      m_heapLimit = -1;
      m_delta = -1;
    }

    public static void InitConfigForConflation_Pool(string locators,
      string durableClientId, string conflation)
    {
      Properties<string, string> config = new Properties<string, string>();
      config.Insert("durable-client-id", durableClientId);
      config.Insert("durable-timeout", "300");
      config.Insert("notify-ack-interval", "1");
      if (conflation != null && conflation.Length > 0)
      {
        config.Insert("conflate-events", conflation);
      }
      InitConfig(config, null);
      CreatePool<object, object>("__TESTPOOL1_", locators, (string)null, 0, true);
    }

    public static void InitConfig(string cacheXml)
    {
      InitConfig(null, cacheXml);
    }

    public static void InitConfig(Properties<string, string> config, string cacheXml)
    {
      //Console.WriteLine(" in InitConfig1 " + System.AppDomain.CurrentDomain.Id);
      if (cacheXml != null)
      {
        string duplicateXMLFile = Util.Rand(3536776).ToString() + cacheXml;
        createDuplicateXMLFile(cacheXml, duplicateXMLFile);
        cacheXml = duplicateXMLFile;
      }
      if (config == null) {
        config = new Properties<string, string>();
      }
      if (m_extraPropertiesFile != null)
      {
        config.Load(m_extraPropertiesFile);
      }

      if (m_cache == null || m_cache.IsClosed)
      {

        try
        {
          CacheHelper.m_doDisconnect = false;
          config.Insert("enable-time-statistics", "true");
          SetLogConfig(ref config);

          if (m_heapLimit != -1)
            config.Insert("heap-lru-limit", m_heapLimit.ToString());
          if (m_delta != -1)
            config.Insert("heap-lru-delta", m_delta.ToString());
          config.Insert("enable-time-statistics", "true");

          CacheFactory cf = CacheFactory.CreateCacheFactory(config);

          if (cacheXml != null && cacheXml.Length > 0)
          {
            cf = cf.Set("cache-xml-file", cacheXml);
          }

          m_cache = cf
              .SetPdxIgnoreUnreadFields(PdxIgnoreUnreadFields)
              .SetPdxReadSerialized(PdxReadSerialized)
              .Create();

          PdxIgnoreUnreadFields = false; //reset so next test will have default value
          PdxReadSerialized = false;
        }
        catch (CacheExistsException)
        {
          m_cache = CacheFactory.GetAnyInstance();
        }
      }

      m_dsys = m_cache.DistributedSystem;
      m_cstxManager = m_cache.CacheTransactionManager;
    }

    public static void SetExtraPropertiesFile(string fName)
    {
      m_extraPropertiesFile = fName;
    }

    public static void InitClient()
    {
      CacheHelper.Close();
      Properties<string, string> config = new Properties<string, string>();
      config.Load("gfcpp.properties");
      CacheHelper.InitConfig(config);
    }

    public static void Close()
    {
      Util.Log("in cache close " + DistributedSystem.IsConnected + " : " + System.Threading.Thread.GetDomainID());
      //if (DistributedSystem.IsConnected)
      {
        CloseCache();
        if (m_doDisconnect)
        {
          //  DistributedSystem.Disconnect();
        }
      }
      m_dsys = null;
      m_cacheForMultiUser = null;
    }


    public static void CloseUserCache(bool keepAlive)
    {
      //TODO: need to look 
      m_cacheForMultiUser.Close();
    }

    public static void CloseCache()
    {
      Util.Log("A CloseCache " + (m_cache != null ? m_cache.IsClosed.ToString() : "cache is closed"));
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

    public static IRegion<TKey, TValue> CreateRegion<TKey, TValue>(string name, GemStone.GemFire.Cache.Generic.RegionAttributes<TKey, TValue> attrs)
    {
      Init();
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      //region = m_cache.CreateRegion(name, attrs);
      region = m_cache.CreateRegionFactory(RegionShortcut.LOCAL).Create<TKey, TValue>(name);
      Assert.IsNotNull(region, "IRegion<object, object> was not created.");
      m_currRegion = region as IRegion<object, object>;
      return region;
    }


    public static IRegion<TKey, TValue> CreateExpirationRegion<TKey, TValue>(
      string name, string poolname, ExpirationAction action, int entryTimeToLive)
    {
      Init();
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY)
        .SetEntryTimeToLive(action, (uint)entryTimeToLive).SetPoolName(poolname).Create<TKey, TValue>(name);
      Assert.IsNotNull(region, "IRegion<object, object> was not created.");
      m_currRegion = region as IRegion<object, object>;
      return region;
    }

    public static IRegion<TKey, TValue> CreateLocalRegionWithETTL<TKey, TValue>(
      string name, ExpirationAction action, int entryTimeToLive)
    {
      Init();
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.LOCAL)
        .SetEntryTimeToLive(action, (uint)entryTimeToLive).Create<TKey, TValue>(name);
      Assert.IsNotNull(region, "IRegion<object, object> was not created.");
      m_currRegion = region as IRegion<object, object>;
      return region;
    }

    public static void CreateDefaultRegion<TKey, TValue>()
    {
      CreatePlainRegion<TKey, TValue>(DefaultRegionName);
    }

    public static IRegion<TKey, TValue> CreatePlainRegion<TKey, TValue>(string name)
    {
      Init();
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.LOCAL).Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
      return region;
    }

    public static void CreateCachingRegion<TKey, TValue>(string name, bool caching)
    {
      Init();
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.PROXY).SetCachingEnabled(caching).Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
    }

    public static void CreateDistribRegion<TKey, TValue>(string name, bool ack,
      bool caching)
    {
      Init();
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetCachingEnabled(caching).SetInitialCapacity(100000).Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
    }

    public static IRegion<TKey, TValue> CreateDistRegion<TKey, TValue>(string rootName,
      string name, int size)
    {
      Init();
      CreateCachingRegion<TKey, TValue>(rootName, true);
      AttributesFactory<TKey, TValue> af = new AttributesFactory<TKey, TValue>();
      af.SetLruEntriesLimit(0);
      af.SetInitialCapacity(size);
      GemStone.GemFire.Cache.Generic.RegionAttributes<TKey, TValue> rattrs = af.CreateRegionAttributes();
      IRegion<TKey, TValue> region = ((Region<TKey, TValue>)m_currRegion).CreateSubRegion(name, rattrs);
      Assert.IsNotNull(region, "SubRegion {0} was not created.", name);
      return region;
    }

    public static IRegion<TKey, TValue> CreateILRegion<TKey, TValue>(string name, bool ack, bool caching,
      ICacheListener<TKey, TValue> listener)
    {
      Init();
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetInitialCapacity(100000)
        .SetCachingEnabled(caching);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }

      region = regionFactory.Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
      return region;
    }

    public static IRegion<TKey, TValue> CreateSizeRegion<TKey, TValue>(string name, int size, bool ack,
      bool caching)
    {
      Init();
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetLruEntriesLimit(0).SetInitialCapacity(size)
        .SetCachingEnabled(caching).Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
      return region;
    }

    public static IRegion<TKey, TValue> CreateLRURegion<TKey, TValue>(string name, uint size)
    {
      Init();
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      region = m_cache.CreateRegionFactory(RegionShortcut.LOCAL_ENTRY_LRU)
        .SetLruEntriesLimit(size).SetInitialCapacity((int)size)
        .Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
      return region;
    }


    public static IRegion<TradeKey, Object> CreateTCRegion2<TradeKey, Object>(string name, bool ack, bool caching,
      IPartitionResolver<TradeKey, Object> resolver, string locators, bool clientNotification)
    {
      Init();
      IRegion<TradeKey, Object> region = GetRegion<TradeKey, Object>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);

      regionFactory.SetInitialCapacity(100000);
      regionFactory.SetCachingEnabled(caching);

      if (resolver != null)
      {        
        Util.Log("resolver is attached {0}", resolver);
        regionFactory.SetPartitionResolver(resolver);
      }
      else
      {
        Util.Log("resolver is null {0}", resolver);
      }     

      PoolFactory/*<TKey, TVal>*/ poolFactory = PoolManager/*<TKey, TVal>*/.CreateFactory();

      if (locators != null)
      {
        string[] list = locators.Split(',');
        foreach (string item in list)
        {
          string[] parts = item.Split(':');
          poolFactory.AddLocator(parts[0], int.Parse(parts[1]));
          Util.Log("AddLocator parts[0] = {0} int.Parse(parts[1]) = {1} ", parts[0], int.Parse(parts[1]));
        }
      }
      else
      {
        Util.Log("No locators or servers specified for pool");
      }

      poolFactory.SetSubscriptionEnabled(clientNotification);
      poolFactory.Create("__TESTPOOL__");
      region = regionFactory.SetPoolName("__TESTPOOL__").Create<TradeKey, Object>(name);

      Assert.IsNotNull(region, "IRegion<TradeKey, Object> {0} was not created.", name);
      Util.Log("IRegion<TradeKey, Object> {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static Pool/*<TKey, TValue>*/ CreatePool<TKey, TValue>(string name, string locators, string serverGroup,
      int redundancy, bool subscription)
    {
      return CreatePool<TKey, TValue>(name, locators, serverGroup, redundancy, subscription, 5, 1, 300);
    }

    public static Pool/*<TKey, TValue>*/ CreatePool<TKey, TValue>(string name, string locators, string serverGroup,
      int redundancy, bool subscription, bool prSingleHop, bool threadLocal = false)
    {
      return CreatePool<TKey, TValue>(name, locators, serverGroup, redundancy, subscription, -1, 1, 300, false, prSingleHop, threadLocal);
    }

    public static Pool/*<TKey, TValue>*/ CreatePool<TKey, TValue>(string name, string locators, string serverGroup,
      int redundancy, bool subscription, int numConnections, bool isMultiuserMode)
    {
      return CreatePool<TKey, TValue>(name, locators, serverGroup, redundancy, subscription, numConnections, 1, 300, isMultiuserMode);
    }

    public static Pool/*<TKey, TValue>*/ CreatePool<TKey, TValue>(string name, string locators, string serverGroup,
      int redundancy, bool subscription, int numConnections)
    {
      return CreatePool<TKey, TValue>(name, locators, serverGroup, redundancy, subscription, numConnections, 1, 300);
    }

    public static Pool/*<TKey, TValue>*/ CreatePool<TKey, TValue>(string name, string locators, string serverGroup,
      int redundancy, bool subscription, int ackInterval, int dupCheckLife)
    {
      return CreatePool<TKey, TValue>(name, locators, serverGroup, redundancy, subscription,
        5, ackInterval, dupCheckLife);
    }

    public static Pool/*<TKey, TValue>*/ CreatePool<TKey, TValue>(string name, string locators, string serverGroup,
      int redundancy, bool subscription, int numConnections, int ackInterval, int dupCheckLife)
    {
      return CreatePool<TKey, TValue>(name, locators, serverGroup, redundancy, subscription, numConnections, ackInterval, 300, false);
    }

    public static Pool/*<TKey, TValue>*/ CreatePool<TKey, TValue>(string name, string locators, string serverGroup,
      int redundancy, bool subscription, int numConnections, int ackInterval, int dupCheckLife, bool isMultiuserMode, bool prSingleHop = true, bool threadLocal = false)
    {
      Init();

      Pool/*<TKey, TValue>*/ existing = PoolManager/*<TKey, TValue>*/.Find(name);

      if (existing == null)
      {
        PoolFactory/*<TKey, TValue>*/ fact = PoolManager/*<TKey, TValue>*/.CreateFactory();
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
        if (serverGroup != null)
        {
          fact.SetServerGroup(serverGroup);
        }
        fact.SetSubscriptionRedundancy(redundancy);
        fact.SetSubscriptionEnabled(subscription);
        fact.SetSubscriptionAckInterval(ackInterval);
        fact.SetSubscriptionMessageTrackingTimeout(dupCheckLife);
        fact.SetMultiuserAuthentication(isMultiuserMode);
        fact.SetPRSingleHopEnabled(prSingleHop);
        fact.SetThreadLocalConnections(threadLocal);
        Util.Log("SingleHop set to {0}" , prSingleHop );
        Util.Log("ThreadLocal = {0} ", threadLocal);
        Util.Log("numConnections set to {0}", numConnections);
        if (numConnections >= 0)
        {
          fact.SetMinConnections(numConnections);
          fact.SetMaxConnections(numConnections);
        }
        Pool/*<TKey, TValue>*/ pool = fact.Create(name);
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

    public static IRegion<TKey, TValue> CreateTCRegion_Pool<TKey, TValue>(string name, bool ack, bool caching,
      ICacheListener<TKey, TValue> listener, string locators, string poolName, bool clientNotification)
    {
      return CreateTCRegion_Pool(name, ack, caching, listener, locators, poolName,
        clientNotification, false, false);
    }

    public static void CreateTCRegion_Pool_AD1(string name, bool ack, bool caching,
       string locators, string poolName, bool clientNotification, bool cloningEnable)
    {
      CreateTCRegion_Pool_AD<object, object>(name, ack, caching, null, locators, poolName, clientNotification, false, cloningEnable);
    }

    public static IRegion<TKey, TValue> CreateTCRegion_Pool_AD<TKey, TValue>(string name, bool ack, bool caching,
      ICacheListener<TKey, TValue> listener, string locators, string poolName, bool clientNotification, bool ssl,
      bool cloningEnabled)
    {
      if (ssl)
      {
        Properties<string, string> sysProps = new Properties<string, string>();
        string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
        sysProps.Insert("ssl-enabled", "true");
        sysProps.Insert("ssl-keystore", keystore + "/client_keystore.pem");
        sysProps.Insert("ssl-truststore", keystore + "/client_truststore.pem");
        InitConfig(sysProps);
      }
      else
      {
        Properties<string, string> sysProps = new Properties<string, string>();
        sysProps.Insert("appdomain-enabled", "true");

        InitConfig(sysProps);
      }
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      if (PoolManager/*<TKey, TValue>*/.Find(poolName) == null)
      {
        PoolFactory/*<TKey, TValue>*/ fact = PoolManager/*<TKey, TValue>*/.CreateFactory();
        fact.SetSubscriptionEnabled(clientNotification);
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
        Pool/*<TKey, TValue>*/ pool = fact.Create(poolName);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
      }

      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetInitialCapacity(100000).SetPoolName(poolName).SetCloningEnabled(cloningEnabled)
        .SetCachingEnabled(caching);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }

      region = regionFactory.Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
      Util.Log("IRegion<object, object> {0} has been created with attributes:{1}",
        name, RegionAttributesToString<TKey, TValue>(region.Attributes));
      return region;
    }

    public static void CreateTCRegion_Pool_MDS(string name, bool ack, bool caching,
        string locators, string poolName, bool clientNotification, bool ssl,
       bool cloningEnabled)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(name, true, caching,
         null, locators, poolName, clientNotification, ssl, false);
    }

    public static IRegion<TKey, TValue> CreateTCRegion_Pool<TKey, TValue>(string name, bool ack, bool caching,
      ICacheListener<TKey, TValue> listener, string locators, string poolName, bool clientNotification, bool ssl,
      bool cloningEnabled)
    {
      if (ssl)
      {
        Properties<string, string> sysProps = new Properties<string, string>();
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
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }
      Pool pl = PoolManager/*<TKey, TValue>*/.Find(poolName);
      if (pl != null)
      {
        Util.Log("Pool is not closed " + poolName);
      }
      if (pl == null)
      {
        PoolFactory/*<TKey, TValue>*/ fact = PoolManager/*<TKey, TValue>*/.CreateFactory();
        fact.SetSubscriptionEnabled(clientNotification);
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
        Pool/*<TKey, TValue>*/ pool = fact.Create(poolName);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
      }
      Util.Log(" caching enable " + caching);
      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetInitialCapacity(100000).SetPoolName(poolName).SetCloningEnabled(cloningEnabled)
        .SetCachingEnabled(caching);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }

      region = regionFactory.SetPoolName(poolName).Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
      Util.Log("IRegion<object, object> {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static IRegion<TKey, TValue> CreateTCRegion_Pool2<TKey, TValue>(string name, bool ack, bool caching,
      ICacheListener<TKey, TValue> listener, string locators, string poolName, bool clientNotification, bool ssl,
      bool cloningEnabled, bool pr)
    {
      if (ssl)
      {
        Properties<string, string> sysProps = new Properties<string, string>();
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
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      if (PoolManager/*<TKey, TValue>*/.Find(poolName) == null)
      {
        PoolFactory/*<TKey, TValue>*/ fact = PoolManager/*<TKey, TValue>*/.CreateFactory();
        fact.SetSubscriptionEnabled(clientNotification);
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
        Pool/*<TKey, TValue>*/ pool = fact.Create(poolName);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
      }

      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetInitialCapacity(100000).SetPoolName(poolName).SetCloningEnabled(cloningEnabled)
        .SetCachingEnabled(caching);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }

      if (pr)
      {
        Util.Log("setting custom partition resolver");
        regionFactory.SetPartitionResolver(CustomPartitionResolver<object>.Create());
      }
      else
      {
        Util.Log("Resolver is null {0}", pr);
      }
      region = regionFactory.Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
      Util.Log("IRegion<object, object> {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static IRegion<TKey, TValue> CreateLRUTCRegion_Pool<TKey, TValue>(string name, bool ack, bool caching,
     ICacheListener<TKey, TValue> listener, string locators, string poolName, bool clientNotification, uint lru)
    {
      Init();
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      if (PoolManager/*<TKey, TValue>*/.Find(poolName) == null)
      {
        PoolFactory/*<TKey, TValue>*/ fact = PoolManager/*<TKey, TValue>*/.CreateFactory();
        fact.SetSubscriptionEnabled(clientNotification);
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
        Pool/*<TKey, TValue>*/ pool = fact.Create(poolName);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
      }

      Properties<string, string> sqLiteProps = Properties<string, string>.Create<string, string>();
      sqLiteProps.Insert("PageSize", "65536");
      sqLiteProps.Insert("MaxFileSize", "512000000");
      sqLiteProps.Insert("MaxPageCount", "1073741823");

      String sqlite_dir = "SqLiteRegionData" + Process.GetCurrentProcess().Id.ToString();
      sqLiteProps.Insert("PersistenceDirectory", sqlite_dir);

      RegionFactory regionFactory = m_cache
        .CreateRegionFactory(RegionShortcut.CACHING_PROXY_ENTRY_LRU)
        .SetDiskPolicy(DiskPolicyType.Overflows)
        .SetInitialCapacity(100000).SetPoolName(poolName)
        .SetCachingEnabled(caching).SetLruEntriesLimit(lru)
        .SetPersistenceManager("SqLiteImpl", "createSqLiteInstance", sqLiteProps);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }

      region = regionFactory.Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
      Util.Log("IRegion<object, object> {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static IRegion<TKey, TValue> CreateTCRegion_Pool<TKey, TValue>(string name, bool ack, bool caching,
      ICacheListener<TKey, TValue> listener, string locators, string poolName, bool clientNotification, string serverGroup)
    {
      Init();
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      if (PoolManager/*<TKey, TValue>*/.Find(poolName) == null)
      {
        PoolFactory/*<TKey, TValue>*/ fact = PoolManager/*<TKey, TValue>*/.CreateFactory();
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
        Pool/*<TKey, TValue>*/ pool = fact.Create(poolName);
        if (pool == null)
        {
          Util.Log("Pool creation failed");
        }
      }

      RegionFactory regionFactory = m_cache.CreateRegionFactory(RegionShortcut.PROXY)
        .SetInitialCapacity(100000).SetPoolName(poolName)
        .SetCachingEnabled(caching);

      if (listener != null)
      {
        regionFactory.SetCacheListener(listener);
      }
      else
      {
        Util.Log("Listener is null {0}", listener);
      }

      region = regionFactory.SetPoolName(poolName).SetInitialCapacity(100000).SetCachingEnabled(caching).SetCacheListener(listener).Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
      Util.Log("IRegion<object, object> {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static IRegion<TKey, TValue> CreateTCRegion_Pool1<TKey, TValue>(string name, bool ack, bool caching,
      ICacheListener<TKey, TValue> listener, string locators, string poolName, bool clientNotification, bool ssl,
      bool cloningEnabled, IPartitionResolver<int, TValue> pr)
    {
      if (ssl)
      {
        Properties<string, string> sysProps = new Properties<string, string>();
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
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(name);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", name);
      }

      if (PoolManager.Find(poolName) == null)
      {
        PoolFactory fact = PoolManager.CreateFactory();
        fact.SetSubscriptionEnabled(clientNotification);
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
        .SetInitialCapacity(100000).SetPoolName(poolName).SetCloningEnabled(cloningEnabled)
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
      region = regionFactory.Create<TKey, TValue>(name);

      Assert.IsNotNull(region, "IRegion<object, object> {0} was not created.", name);
      m_currRegion = region as IRegion<object, object>;
      Util.Log("IRegion<object, object> {0} has been created with attributes:{1}",
        name, RegionAttributesToString(region.Attributes));
      return region;
    }

    public static void DestroyRegion<TKey, TValue>(string name, bool local, bool verify)
    {
      IRegion<TKey, TValue> region;
      if (verify)
      {
        region = GetVerifyRegion<TKey, TValue>(name);
      }
      else
      {
        region = GetRegion<TKey, TValue>(name);
      }
      if (region != null)
      {
        if (local)
        {
          region.GetLocalView().DestroyRegion();
          Util.Log("Locally destroyed region {0}", name);
        }
        else
        {
          region.DestroyRegion();
          Util.Log("Destroyed region {0}", name);
        }
      }
    }

    public static void DestroyAllRegions<TKey, TValue>(bool local)
    {
      if (m_cache != null && !m_cache.IsClosed)
      {
        IRegion<TKey, TValue>[] regions = /*(Region<TKey, TValue>)*/m_cache.RootRegions<TKey, TValue>();
        if (regions != null)
        {
          foreach (IRegion<TKey, TValue> region in regions)
          {
            if (local)
            {
              region.GetLocalView().DestroyRegion();
            }
            else
            {
              region.DestroyRegion();
            }
          }
        }
      }
    }

    public static void InvalidateRegionNonGeneric(string name, bool local, bool verify)
    {
      InvalidateRegion<object, object>(name, local, verify);
    }
    public static void InvalidateRegion<TKey, TValue>(string name, bool local, bool verify)
    {
      IRegion<TKey, TValue> region;
      if (verify)
      {
        region = GetVerifyRegion<TKey, TValue>(name);
        Util.Log("InvalidateRegion: GetVerifyRegion done for {0}", name);
      }
      else
      {
        region = GetRegion<TKey, TValue>(name);
        Util.Log("InvalidateRegion: GetRegion done for {0}", name);
      }
      if (region != null)
      {
        if (local)
        {
          Util.Log("Locally invaliding region {0}", name);
          region.GetLocalView().InvalidateRegion();
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

    public static IRegion<TKey, TValue> GetRegion<TKey, TValue>(string path)
    {
      if (m_cache != null)
      {
        return m_cache.GetRegion<TKey, TValue>(path);
      }
      return null;
    }

    public static IRegion<TKey, TValue> GetRegionAD<TKey, TValue>(string path)
    {
      if (m_cache != null)
      {
        return m_cache.GetRegion<TKey, TValue>(path);
      }
      else if (DistributedSystem.IsConnected)
      {
        return CacheFactory.GetAnyInstance().GetRegion<TKey, TValue>(path);
      }
      return null;
    }

    public static Properties<string, object> GetPkcsCredentialsForMU(Properties<string, string> credentials)
    {
      if (credentials == null)
        return null;
      Properties<string, object> target = Properties<string, object>.Create<string, object>();
      PropsStringToObject psto = new PropsStringToObject(target);
      credentials.ForEach(new PropertyVisitorGeneric<string, string>(psto.Visit));
      return target;
    }

    public static IRegion<TKey, TValue> GetRegion<TKey, TValue>(string path, Properties<string, string> credentials)
    {
      if (m_cache != null)
      {
        Util.Log("GetRegion " + m_cacheForMultiUser);
        if (m_cacheForMultiUser == null)
        {
          IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(path);
          Assert.IsNotNull(region, "IRegion<object, object> [" + path + "] not found.");
          Assert.IsNotNull(region.Attributes.PoolName, "IRegion<object, object> is created without pool.");

          Pool/*<TKey, TValue>*/ pool = PoolManager/*<TKey, TValue>*/.Find(region.Attributes.PoolName);

          Assert.IsNotNull(pool, "Pool is null in GetVerifyRegion.");

          //m_cacheForMultiUser = pool.CreateSecureUserCache(credentials);
          m_cacheForMultiUser = m_cache.CreateAuthenticatedView(GetPkcsCredentialsForMU(credentials), pool.Name);

          return m_cacheForMultiUser.GetRegion<TKey, TValue>(path);
        }
        else
          return m_cacheForMultiUser.GetRegion<TKey, TValue>(path);
      }
      return null;
    }

    public static IRegionService getMultiuserCache(Properties<string, string> credentials)
    {
      if (m_cacheForMultiUser == null)
      {
        Pool/*<TKey, TValue>*/ pool = PoolManager/*<TKey, TValue>*/.Find("__TESTPOOL1_");

        Assert.IsNotNull(pool, "Pool is null in getMultiuserCache.");
        Assert.IsTrue(!pool.Destroyed);

        //m_cacheForMultiUser = pool.CreateSecureUserCache(credentials);
        m_cacheForMultiUser = m_cache.CreateAuthenticatedView(GetPkcsCredentialsForMU(credentials), pool.Name);

        return m_cacheForMultiUser;
      }
      else
        return m_cacheForMultiUser;
    }

    public static IRegion<TKey, TValue> GetVerifyRegion<TKey, TValue>(string path)
    {
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(path);

      Assert.IsNotNull(region, "IRegion<object, object> [" + path + "] not found.");
      Util.Log("Found region '{0}'", path);
      return region;
    }

    public static IRegion<TKey, TValue> GetVerifyRegionAD<TKey, TValue>(string path)
    {
      IRegion<TKey, TValue> region = GetRegionAD<TKey, TValue>(path);

      Assert.IsNotNull(region, "IRegion<object, object> [" + path + "] not found.");
      Util.Log("Found region '{0}'", path);
      return region;
    }

    public static IRegion<TKey, TValue> GetVerifyRegion<TKey, TValue>(string path, Properties<string, string> credentials)
    {
      Util.Log("GetVerifyRegion " + m_cacheForMultiUser);
      if (m_cacheForMultiUser == null)
      {
        IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(path);
        Assert.IsNotNull(region, "IRegion<object, object> [" + path + "] not found.");
        Assert.IsNotNull(region.Attributes.PoolName, "IRegion<object, object> is created without pool.");

        Pool/*<TKey, TValue>*/ pool = PoolManager/*<TKey, TValue>*/.Find(region.Attributes.PoolName);

        Assert.IsNotNull(pool, "Pool is null in GetVerifyRegion.");

        //m_cacheForMultiUser = pool.CreateSecureUserCache(credentials);
        m_cacheForMultiUser = m_cache.CreateAuthenticatedView(GetPkcsCredentialsForMU(credentials), pool.Name);

        return m_cacheForMultiUser.GetRegion<TKey, TValue>(path);
      }
      else
        return m_cacheForMultiUser.GetRegion<TKey, TValue>(path);
    }

    public static void VerifyRegion<TKey, TValue>(string path)
    {
      GetVerifyRegion<TKey, TValue>(path);
    }

    public static void VerifyNoRegion<TKey, TValue>(string path)
    {
      IRegion<TKey, TValue> region = GetRegion<TKey, TValue>(path);
      Assert.IsNull(region, "IRegion<object, object> [" + path + "] should not exist.");
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
            //:create duplicate xml files
            cacheXmls[i] = duplicateFile;
          }

          // Find the port number from the given cache.xml
          XmlDocument xmlDoc = new XmlDocument();
          xmlDoc.XmlResolver = null;
          xmlDoc.Load(duplicateFile);

          XmlNamespaceManager ns = new XmlNamespaceManager(xmlDoc.NameTable);
          ns.AddNamespace("geode", "http://geode.apache.org/schema/cache");
          XmlNodeList serverNodeList = xmlDoc.SelectNodes("//geode:cache-server", ns);

          if (m_endpoints == null)
          {
            m_endpoints = System.Net.Dns.GetHostEntry("localhost").HostName + ":" + serverNodeList[0].Attributes["port"].Value;
          }
          else
          {
            m_endpoints += "," + System.Net.Dns.GetHostEntry("localhost").HostName + ":" + serverNodeList[0].Attributes["port"].Value;
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
        string serverName = "Locator" + Util.Rand(64687687).ToString();
        if (startDir != null)
        {
          startDir += serverName;
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
          startDir = " --dir=" + startDir;
        }
        else
        {
          startDir = string.Empty;
        }

        string locatorPort = " --port=" + getLocatorPort(locatorNum);
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
          sslArgs += " --J=-Djavax.net.ssl.keyStore=" + keystore + "/server_keystore.jks ";
          sslArgs += " --J=-Djavax.net.ssl.keyStorePassword=gemstone ";
          sslArgs += " --J=-Djavax.net.ssl.trustStore=" + keystore + "/server_truststore.jks  ";
          sslArgs += " --J=-Djavax.net.ssl.trustStorePassword=gemstone ";
          extraLocatorArgs += sslArgs;
        }

        string locatorArgs = LocatorStartArgs + " --name=" + serverName + startDir + extraLocatorArgs;

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

    //this is for start locator independetly(will not see each other)
    public static void StartJavaLocator_MDS(int locatorNum, string startDir,
      string extraLocatorArgs, int dsId)
    {
      if (m_localServer)
      {
        Process javaProc;
        string locatorPath = m_gfeDir + PathSep + "bin" + PathSep + GemFireName;
        string serverName = "Locator" + Util.Rand(64687687).ToString();
        Util.Log("Starting locator {0} in directory {1}.", locatorNum, startDir);
        if (startDir != null)
        {
          startDir += serverName;
          if (!Directory.Exists(startDir))
          {
            Directory.CreateDirectory(startDir);
          }
          try
          {
            TextWriter tw = new StreamWriter(Directory.GetCurrentDirectory() + "\\" + startDir + "\\gemfire.properties", false);
            //tw.WriteLine("locators=localhost[{0}],localhost[{1}],localhost[{2}]", LOCATOR_PORT_1, LOCATOR_PORT_2, LOCATOR_PORT_3);
            tw.WriteLine("distributed-system-id=" + dsId);
            tw.WriteLine("mcast-port=0");
            tw.Close();
          }
          catch (Exception ex)
          {
            Assert.Fail("Locator property file creation failed: {0}: {1}", ex.GetType().Name, ex.Message);
          }
          startDir = " --dir=" + startDir;
        }
        else
        {
          startDir = string.Empty;
        }

        if (dsId == 1)
          m_locatorFirst = "localhost:" + getLocatorPort(locatorNum);
        else
          m_locatorSecond = "localhost:" + getLocatorPort(locatorNum);

        string locatorPort = " --port=" + getLocatorPort(locatorNum);
        if (extraLocatorArgs != null)
        {
          extraLocatorArgs = ' ' + extraLocatorArgs + locatorPort;
        }
        else
        {
          extraLocatorArgs = locatorPort;
        }
        string locatorArgs = LocatorStartArgs + " --name=" + serverName + startDir + extraLocatorArgs;

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
          m_locators = "localhost[" + getLocatorPort(locatorNum) + "]";
        }
        else
        {
          m_locators += ",localhost[" + getLocatorPort(locatorNum) + "]";
        }
        Util.Log("JAVA locator endpoints: " + m_locators);
      }
    }

    public static void StartJavaServerWithLocators(int serverNum, string startDir, int numLocators)
    {
      StartJavaServerWithLocators(serverNum, startDir, numLocators, false);
    }

    public static void StartJavaServerWithLocators(int serverNum, string startDir, int numLocators, bool ssl)
    {
      string extraServerArgs = "--locators=";
      for (int locator = 0; locator < numLocators; locator++)
      {
        if (locator > 0)
        {
          extraServerArgs += ",";
        }
        extraServerArgs += "localhost[" + getLocatorPort(locator + 1) + "]";
      }
      if (ssl)
      {
        string sslArgs = String.Empty;
        sslArgs += " ssl-enabled=true ssl-require-authentication=true ssl-ciphers=SSL_RSA_WITH_NULL_MD5 ";
        string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
        sslArgs += " -J=-Djavax.net.ssl.keyStore=" + keystore + "/server_keystore.jks ";
        sslArgs += " -J=-Djavax.net.ssl.keyStorePassword=gemstone ";
        sslArgs += " -J=-Djavax.net.ssl.trustStore=" + keystore + "/server_truststore.jks  ";
        sslArgs += " -J=-Djavax.net.ssl.trustStorePassword=gemstone ";
        extraServerArgs += sslArgs;
      }
      StartJavaServer(serverNum, startDir, extraServerArgs);
    }

    //this is to start multiple DS
    public static void StartJavaServerWithLocator_MDS(int serverNum, string startDir, int locatorNumber)
    {
      string extraServerArgs = "--locators=";
      extraServerArgs += "localhost[" + getLocatorPort(locatorNumber) +"]";

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
      extraServerArgs += " --locators=";
      for (int locator = 0; locator < numLocators; locator++)
      {
        if (locator > 0)
        {
          extraServerArgs += ",";
        }
        extraServerArgs += "localhost[" + getLocatorPort(locator + 1) + "]";
      }
      if (ssl)
      {
        string sslArgs = String.Empty;
        sslArgs += " ssl-enabled=true ssl-require-authentication=true ssl-ciphers=SSL_RSA_WITH_NULL_MD5 ";
        string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
        sslArgs += " --J=-Djavax.net.ssl.keyStore=" + keystore + "/server_keystore.jks ";
        sslArgs += " --J=-Djavax.net.ssl.keyStorePassword=gemstone ";
        sslArgs += " --J=-Djavax.net.ssl.trustStore=" + keystore + "/server_truststore.jks  ";
        sslArgs += " --J=-Djavax.net.ssl.trustStorePassword=gemstone ";
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
        string serverName = "Server" + Util.Rand(372468723).ToString();
        startDir += serverName;
        int port = 0;
        switch (serverNum)
        {
            case 1:
                port = HOST_PORT_1;
                break;
            case 2:
                port = HOST_PORT_2;
                break;
            case 3:
                port = HOST_PORT_3;
                break;
            case 4:
                port = HOST_PORT_4;
                break;
            default:
                throw new InvalidOperationException();
        }
        Util.Log("Starting server {0} in directory {1}.", serverNum, startDir);
        if (startDir != null)
        {
          if (!Directory.Exists(startDir))
          {
            Directory.CreateDirectory(startDir);
          }
          startDir = " --dir=" + startDir;
        }
        else
        {
          startDir = string.Empty;
        }
        if (extraServerArgs != null)
        {
          extraServerArgs = ' ' + extraServerArgs;
        }

        string classpath = Util.GetEnvironmentVariable("GF_CLASSPATH");

        string serverArgs = JavaServerStartArgs + cacheXml + " --name=" + serverName +
          " --server-port=" + port + " --classpath=" + classpath +
          " --log-level=" + m_gfeLogLevel + startDir +
          " --J=-Dsecurity-log-level=" + m_gfeSecLogLevel + extraServerArgs;
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
          string sslArgs = String.Empty;
          if (ssl)
          {
            string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
            sslArgs += " --J=-Djavax.net.ssl.keyStore=" + keystore + "/server_keystore.jks ";
            sslArgs += " --J=-Djavax.net.ssl.keyStorePassword=gemstone ";
            sslArgs += " --J=-Djavax.net.ssl.trustStore=" + keystore + "/server_truststore.jks  ";
            sslArgs += " --J=-Djavax.net.ssl.trustStorePassword=gemstone ";
            string propdir = startDir.Replace("--dir=", string.Empty).Trim();
            File.Copy(propdir + "/gemfire.properties", Directory.GetCurrentDirectory() + "/gemfire.properties", true);
          }
          if (!Util.StartProcess(javaLocatorPath, LocatorStopArgs + startDir + sslArgs,
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
            startDir.Replace("--dir=", string.Empty).Trim());
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
            startDir.Replace("--dir=", string.Empty).Trim());
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

    

    public static void KillJavaProcesses()
    {
      String myQuery = "select * from win32_process where Name ='java.exe' or Name = 'java.exe *32'";
      ObjectQuery objQuery = new ObjectQuery(myQuery);
      ManagementObjectSearcher objSearcher = new ManagementObjectSearcher(objQuery);
      ManagementObjectCollection processList = objSearcher.Get();

      foreach (ManagementObject item in processList)
      {
        try
        {
          int processId = Convert.ToInt32(item["ProcessId"].ToString());
          string commandline = item["CommandLine"].ToString();

          Util.Log("processId:{0} name:{1}", item["ProcessId"],item["Name"]);
          if (commandline.Contains("gemfire.jar"))
          {
            Util.Log("Killing gemfire process with id {0}", processId);
            Process proc = Process.GetProcessById(processId);
            proc.Kill();
            proc.WaitForExit();
          }
          else
          {
            Util.Log("Process with id {0} is not gemfire process", processId);
          }
        }
        catch (Exception e)
        {
          Console.WriteLine("Error: " + e);
        }
      }

    }

    public static void EndTest()
    {
      Util.Log("Cache Helper EndTest.");
      StopJavaServers();
      StopJavaLocators();
      ClearEndpoints();
      ClearLocators();
      KillJavaProcesses();
      Util.Log("Cache Helper EndTest completed.");
    }

    #endregion

    #region Utility functions

    public static void ShowKeys(ICollection<Object> cKeys)
    {
      if (cKeys != null)
      {
        for (int i = 0; i < cKeys.Count; i++)
        {
          //TODO ATTACH List TO THE COLLECTION AND UNCOMMENT BELOW LINE
          //Util.Log("Key [{0}] = {1}", i, cKeys[i]);
        }
      }
    }

    public static void ShowValues(ICollection<Object> cValues)
    {
      if (cValues != null)
      {
        for (int i = 0; i < cValues.Count; i++)
        {
          //TODO ATTACH List TO THE COLLECTION AND UNCOMMENT BELOW LINE
          //Util.Log("Value [{0}] = {1}", i, cValues[i]);
        }
      }
    }

    public static string RegionAttributesToString<TKey, TVal>(GemStone.GemFire.Cache.Generic.RegionAttributes<TKey, TVal> attrs)
    {
      string poolName = "RegionWithoutPool";

      if (attrs.PoolName != null)
        poolName = attrs.PoolName;

      StringBuilder attrsSB = new StringBuilder();
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
