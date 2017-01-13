//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Xml;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests.NewAPI;
  using GemStone.GemFire.Cache.Generic;
  using NEWAPI = GemStone.GemFire.Cache.Tests.NewAPI;
  //using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;


  /// <summary>
  /// Exception thrown when 'Call' is invoked on a client thread/process/...
  /// that has already exited (either due to some error/exception on the
  /// client side or due to its 'Dispose' function being called).
  /// </summary>
  [Serializable]
  public class FwkException : Exception
  {
    /// <summary>
    /// Constructor to create an exception object with empty message.
    /// </summary>
    public FwkException()
      : base()
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public FwkException(string message)
      : base(message)
    {
    }

    /// <summary>
    /// Constructor to create an exception object with the given message
    /// and with the given inner Exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner Exception object.</param>
    public FwkException(string message, Exception innerException)
      : base(message, innerException)
    {
    }

    /// <summary>
    /// Constructor to allow deserialization of this exception by .Net remoting
    /// </summary>
    public FwkException(SerializationInfo info, StreamingContext context)
      : base(info, context)
    {
    }
  }

  [Serializable]
  public struct FwkTaskData
  {
    #region Private members

    private string m_regionTag;
    private string m_name;
    private int m_numClients;
    private int m_numKeys;
    private int m_valueSize;
    private int m_numThreads;

    #endregion

    #region Constructor

    public FwkTaskData(string regionTag, string name, int numClients,
      int numKeys, int valueSize, int numThreads)
    {
      m_regionTag = regionTag;
      m_name = name;
      m_numClients = numClients;
      m_numKeys = numKeys;
      m_valueSize = valueSize;
      m_numThreads = numThreads;
    }

    #endregion

    #region Public methods and accessors

    public string RegionTag
    {
      get
      {
        return m_regionTag;
      }
    }

    public string Name
    {
      get
      {
        return m_name;
      }
    }

    public int NumClients
    {
      get
      {
        return m_numClients;
      }
    }

    public int NumKeys
    {
      get
      {
        return m_numKeys;
      }
    }

    public int ValueSize
    {
      get
      {
        return m_valueSize;
      }
    }

    public int NumThreads
    {
      get
      {
        return m_numThreads;
      }
    }

    public string GetLogString()
    {
      return string.Format("{0}-{1}-Clients-{2}-Keys-{3}-VSize-{4}-Threads-{5}",
        m_regionTag, m_name, m_numClients, m_numKeys, m_valueSize, m_numThreads);
    }

    public string GetCSVString()
    {
      return string.Format("{0},{1},{2},{3},{4},{5}",
        m_regionTag, m_name, m_numClients, m_numKeys, m_valueSize, m_numThreads);
    }

    #endregion
  }

  public abstract class FwkTest<TKey, TVal> : FwkReadData
  {
    #region Private members

    private static FwkTest<TKey, TVal> m_currentTest = null;
    private FwkTaskData m_taskData;
    private List<string> m_taskRecords;
    private const NEWAPI.CredentialGenerator.ClassCode DefaultSecurityCode =
      NEWAPI.CredentialGenerator.ClassCode.LDAP;

    #endregion

    #region Public accessors and constants

    public static FwkTest<TKey, TVal> CurrentTest
    {
      get
      {
        return m_currentTest;
      }
    }

    public const string JavaServerBB = "Cacheservers";
    public const string EndPointTag = "ENDPOINT:";
    public const string HeapLruLimitKey = "heapLruLimit";
    public const string RedundancyLevelKey = "redundancyLevel";
    public const string ConflateEventsKey = "conflateEvents";
    public const string SecurityParams = "securityParams";
    public const string SecurityScheme = "securityScheme";
    public const string JavaServerEPCountKey = "ServerEPCount";
    public const string EndPoints = "EndPoints";

    public static Properties<string, string> PER_CLIENT_FOR_MULTIUSER = null;
    #endregion

    #region Protected members

    protected FwkTaskData TaskData
    {
      get
      {
        return m_taskData;
      }
    }

    #endregion

    #region Public methods

    public FwkTest()
      : base()
    {
      m_currentTest = this;
      m_taskData = new FwkTaskData();
      m_taskRecords = new List<string>();
    }

    public virtual void FwkException(string message)
    {
      FwkSevere(message);
      throw new FwkException(message);
    }

    public virtual void FwkException(string fmt, params object[] paramList)
    {
      FwkException(string.Format(fmt, paramList));
    }

    public virtual void FwkSevere(string message)
    {
      Util.Log(Util.LogLevel.Error, "FWKLIB:: Task[{0}] Severe: {1}",
        TaskName, message);
    }

    public virtual void FwkSevere(string fmt, params object[] paramList)
    {
      FwkSevere(string.Format(fmt, paramList));
    }

    public virtual void FwkWarn(string message)
    {
      Util.Log(Util.LogLevel.Warning, "FWKLIB:: Task[{0}]: {1}",
        TaskName, message);
    }

    public virtual void FwkWarn(string fmt, params object[] paramList)
    {
      FwkWarn(string.Format(fmt, paramList));
    }

    public virtual void FwkInfo(string message)
    {
      Util.Log(Util.LogLevel.Info, "FWKLIB:: Task[{0}]: {1}",
        TaskName, message);
    }

    public virtual void FwkInfo(string fmt, params object[] paramList)
    {
      FwkInfo(string.Format(fmt, paramList));
    }

    public virtual void FwkAssert(bool condition, string message)
    {
      if (!condition)
      {
        FwkException(message);
      }
    }

    public virtual void FwkAssert(bool condition, string fmt,
      params object[] paramList)
    {
      if (!condition)
      {
        FwkException(fmt, paramList);
      }
    }

    public static void LogException(string message)
    {
      throw new FwkException(message);
    }

    public static void LogException(string fmt, params object[] paramList)
    {
      LogException(string.Format(fmt, paramList));
    }

    public static void LogSevere(string message)
    {
      Util.Log(Util.LogLevel.Error, "FWKLIB:: Severe: {0}", message);
    }

    public static void LogSevere(string fmt, params object[] paramList)
    {
      LogSevere(string.Format(fmt, paramList));
    }

    public static void LogWarn(string message)
    {
      Util.Log(Util.LogLevel.Warning, "FWKLIB:: {0}", message);
    }

    public static void LogWarn(string fmt, params object[] paramList)
    {
      LogWarn(string.Format(fmt, paramList));
    }

    public static void LogInfo(string message)
    {
      Util.Log(Util.LogLevel.Info, "FWKLIB:: {0}", message);
    }

    public static void LogInfo(string fmt, params object[] paramList)
    {
      LogInfo(string.Format(fmt, paramList));
    }

    public static void LogAssert(bool condition, string message)
    {
      if (!condition)
      {
        LogException(message);
      }
    }

    public static void LogAssert(bool condition, string fmt,
      params object[] paramList)
    {
      if (!condition)
      {
        LogException(fmt, paramList);
      }
    }

    public virtual IRegion<TKey, TVal> GetRootRegion()
    {
      string rootRegionData = GetStringValue("regionSpec");
      //rootRegionData = rootRegionData + "New";
      if (rootRegionData == null)
      {
        return null;
      }
      string rootRegionName = GetRegionName(rootRegionData);
      try
      {
        return CacheHelper<TKey, TVal>.GetVerifyRegion(rootRegionName);
      }
      catch
      {
        return null;
      }
    }
    
    public CredentialGenerator GetCredentialGenerator()
    {
      int schemeNumber;
      try
      {
        schemeNumber = (int)Util.BBGet(string.Empty,
          FwkReadData.TestRunNumKey);
      }
      catch (Exception)
      {
        schemeNumber = 1;
      }
      int schemeSkip = 1;
      string securityScheme;
      string bb = "GFE_BB";
      string key = "scheme";
      do
      {
        securityScheme = GetStringValue(SecurityScheme);
        Util.BBSet(bb, key, securityScheme);
      }
      while (++schemeSkip <= schemeNumber);
      NEWAPI.CredentialGenerator.ClassCode secCode;
      try
      {
        secCode = (NEWAPI.CredentialGenerator.ClassCode)Enum.Parse(typeof(
          NEWAPI.CredentialGenerator.ClassCode), securityScheme, true);
      }
      catch (Exception)
      {
        FwkWarn("Skipping unknown security scheme {0}. Using default " +
          "security scheme {1}.", securityScheme, DefaultSecurityCode);
        secCode = DefaultSecurityCode;
      }
      if (secCode == NEWAPI.CredentialGenerator.ClassCode.None)
      {
        return null;
      }
      NEWAPI.CredentialGenerator gen = NEWAPI.CredentialGenerator.Create(secCode, false);
      if (gen == null)
      {
        FwkWarn("Skipping security scheme {0} with no generator. Using " +
          "default security scheme.", secCode, DefaultSecurityCode);
        secCode = DefaultSecurityCode;
        gen = NEWAPI.CredentialGenerator.Create(secCode, false);
      }

      return gen;
    }

    public void GetClientSecurityProperties(ref Properties<string, string> props,
      string regionName)
    {
      string securityParams = GetStringValue(SecurityParams);
      NEWAPI.CredentialGenerator gen;//= GetCredentialGenerator();
      if (securityParams == null || securityParams.Length == 0 ||
        (gen = GetCredentialGenerator()) == null)
      {
        FwkInfo("Security is DISABLED.");
        return;
      }

      FwkInfo("Security params is: " + securityParams);
      FwkInfo("Security scheme: " + gen.GetClassCode());
      string dataDir = Util.GetFwkLogDir(Util.SystemType) + "/data";
      gen.Init(dataDir, dataDir);

      if (props == null)
      {
        props = new Properties<string, string>();
      }

      Properties<string, string> credentials;
      Random rnd = new Random();
      if (securityParams.Equals("valid"))
      {
        FwkInfo("Getting valid credentials");
        credentials = gen.GetValidCredentials(rnd.Next());
      }
      else if (securityParams.Equals("invalid"))
      {
        FwkInfo("Getting invalid credentials");
        credentials = gen.GetInvalidCredentials(rnd.Next());
      }
      else
      {
        FwkInfo("Getting credentials for a list of operations");
        List<OperationCode> opCodes = new List<OperationCode>();
        while (securityParams != null && securityParams.Length > 0)
        {
          securityParams = securityParams.ToLower().Replace("_", "");
          OperationCode opCode;
          if (securityParams == "create" || securityParams == "update")
          {
            opCode = OperationCode.Put;
          }
          else
          {
            opCode = (OperationCode)Enum.Parse(typeof(
              OperationCode), securityParams, true);
          }
          opCodes.Add(opCode);
          securityParams = GetStringValue(SecurityParams);
          FwkInfo("Next security params: {0}", securityParams);
        }
        // For now only XML based authorization is supported
        NEWAPI.AuthzCredentialGenerator authzGen = new NEWAPI.XmlAuthzCredentialGenerator();
        authzGen.Init(gen);
        List<string> regionNameList = new List<string>();
        if (regionName == null || regionName.Length == 0)
        {
          regionName = GetStringValue("regionPaths");
        }
        while (regionName != null && regionName.Length > 0)
        {
          regionNameList.Add(regionName);
          regionName = GetStringValue("regionPaths");
        }
        string[] regionNames = null;
        if (regionNameList.Count > 0)
        {
          regionNames = regionNameList.ToArray();
        }
        credentials = authzGen.GetAllowedCredentials(opCodes.ToArray(),
          regionNames, rnd.Next());
      }
      PER_CLIENT_FOR_MULTIUSER = credentials;
      NEWAPI.Utility.GetClientProperties(gen.AuthInit, credentials, ref props);
      FwkInfo("Security properties entries: {0}", props);
    }
    private string[] GetRoundRobinEP()
    {
      int contactnum = GetUIntValue("contactNum");
      string label = "EndPoints";
      int epCount = (int)Util.BBGet(JavaServerBB, JavaServerEPCountKey);
      //int epCount = (int)Util.BBGet("GFE_BB", "EP_COUNT");
      if (contactnum < 0)
        contactnum = epCount;
      string[] rbEP = new string[contactnum];
      string currEPKey = "CURRENTEP_COUNT";
      int currentEP = (int)Util.BBIncrement("GFERR_BB", currEPKey);
      for (int i = 0; i < contactnum; i++)
      {
        if (currentEP > epCount)
        {
          Util.BBSet("GFERR_BB", currEPKey, 0);
          currentEP = (int)Util.BBIncrement("GFERR_BB", currEPKey);
        }
        string key = label + "_" + currentEP.ToString();
        string ep = (string)Util.BBGet(JavaServerBB, key);
        rbEP[i] = ep;
        FwkInfo("GetRoundRobinEP = {0} key = {1} currentEP =  {2}", ep, key, currentEP);
        //  rbEP[i] = ep;
      }
      return rbEP;
    }
    private void XMLParseEndPoints(string ep, bool isServer, PoolFactory pf)
    {
      string[] eps = ep.Split(',');
      if (isServer)
      {
        bool disableShufflingEP = GetBoolValue("disableShufflingEP"); // smoke perf test
        if (disableShufflingEP)
        {
          string[] rbep = GetRoundRobinEP();
          for (int cnt = 0; cnt < rbep.Length; cnt++)
          {
            FwkInfo("round robin endpoint = {0}", rbep[cnt]);
            //string[] rep = rbep[cnt].Split(',');
            //foreach (string endpoint in eps)
            //{
              string hostName = rbep[cnt].Split(':')[0];
              int portNum = int.Parse(rbep[cnt].Split(':')[1]);
              pf.AddServer(hostName, portNum);
            //}
          }
        }
        else
        {
          foreach (string endpoint in eps)
          {
            string hostName = endpoint.Split(':')[0];
            int portNum = int.Parse(endpoint.Split(':')[1]);
            pf.AddServer(hostName, portNum);
          }
        }
      }
      else
      {
        foreach (string endpoint in eps)
        {
          string hostName = endpoint.Split(':')[0];
          int portNum = int.Parse(endpoint.Split(':')[1]);
          pf.AddLocator(hostName, portNum);
        }
      }
    }

     
      private void CreateCache()
    {
      Properties<string,string> dsProps = new Properties<string,string>();
      ResetKey("PdxReadSerialized");
      bool pdxReadSerialized = GetBoolValue("PdxReadSerialized");
      ResetKey("isDurable");
      bool isDC = GetBoolValue("isDurable");
      ResetKey("durableTimeout");
      int durableTimeout = 300;
      string durableClientId = "";
      string conflateEvents = GetStringValue(ConflateEventsKey);
      if (isDC)
      {
        durableTimeout = GetUIntValue("durableTimeout");
        bool isFeeder = GetBoolValue("isFeeder");
        if (isFeeder)
        {
          durableClientId = "Feeder";
          // VJR: Setting FeederKey because listener cannot read boolean isFeeder
          // FeederKey is used later on by Verify task to identify feeder's key in BB
          durableClientId = String.Format("ClientName_{0}_Count", Util.ClientNum);
       }
        else
        {
          durableClientId = String.Format("ClientName_{0}", Util.ClientNum);
        }
        //Util.BBSet("DURABLEBB", durableClientId,0);
        CacheHelper<TKey, TVal>.InitConfigForPoolDurable(durableClientId, durableTimeout, conflateEvents, false);
      }
      else if (pdxReadSerialized)
      {
          CacheHelper<TKey, TVal>.InitConfigPdxReadSerialized(dsProps, pdxReadSerialized);
      }
      else
          CacheHelper<TKey, TVal>.InitConfigPool(dsProps);
    }
    public void CreateCacheConnect()
    {
      CreateCache();
    }
    public virtual void CreatePool()
    {
      CreateCache();
      PoolFactory pf = PoolManager.CreateFactory();
      ResetKey("poolSpec");
      string poolRegionData = GetStringValue("poolSpec");
      FwkInfo("PoolSpec is :{0}", poolRegionData);
      string poolName = null;
      SetPoolAttributes(pf, poolRegionData, ref poolName);
      
      if (PoolManager.Find(poolName) == null)
      {
        Pool pool = pf.Create(poolName);
        FwkInfo("Pool attributes are {0}:", PoolAttributesToString(pool));
      }
    }
    public void SetPoolAttributes(PoolFactory pf, string spec, ref string poolName)
    {
      ReadXmlData(null, pf, spec, ref poolName);
    }
    private void SetThisPoolAttributes(PoolFactory pf,string key, string value)
    {
      switch (key)
      {
        case "free-connection-timeout":
          int fct = int.Parse(value);
           pf.SetFreeConnectionTimeout(fct);
           break;
        case "idle-timeout":
          int it = int.Parse(value);
            pf.SetIdleTimeout(it);
            break;
        case "load-conditioning-interval":
          int lci = int.Parse(value);
            pf.SetLoadConditioningInterval(lci);
          break;
        case "max-connections":
          int mxc = int.Parse(value);
            pf.SetMaxConnections(mxc);
            break;
        case "min-connections":
          int minc = int.Parse(value);
           pf.SetMinConnections(minc);
           break;
        case "ping-interval":
          int pi = int.Parse(value);
            pf.SetPingInterval(pi);
            break;
        case "read-timeout":
          int rt = int.Parse(value);
           pf.SetReadTimeout(rt);
           break;
        case "retry-attempts":
          int ra = int.Parse(value);
            pf.SetRetryAttempts(ra);
            break;
        case "server-group":
           pf.SetServerGroup(value);
          break;
        case "socket-buffer-size":
          int bs = int.Parse(value);
          pf.SetSocketBufferSize(bs);
          break;
        case "subscription-ack-interval":
           int acki = int.Parse(value);
            pf.SetSubscriptionAckInterval(acki);
            break;
        case "subscription-enabled":
           if (value == "true")
            {
              pf.SetSubscriptionEnabled(true);
            }
            else
            {
              pf.SetSubscriptionEnabled(false);
            }
          break;
        case "thread-local-connections":
          if (value == "true")
          {
              pf.SetThreadLocalConnections(true);
          }
          else
          {
              pf.SetThreadLocalConnections(false);
          }
          break;
        case "subscription-message-tracking-timeout":
            int smtt = int.Parse(value);
            pf.SetSubscriptionMessageTrackingTimeout(smtt);
            break;
        case "subscription-redundancy":
            int sr = int.Parse(value);
            pf.SetSubscriptionRedundancy(sr);
            break;
        case "locators":
          string locatorAddress = (string)Util.BBGet(string.Empty, "LOCATOR_ADDRESS_POOL");
            XMLParseEndPoints(locatorAddress, false, pf);
            break;
        case "servers":
            string ServerEndPoints = (string)Util.BBGet("Cacheservers", "ENDPOINT:");
            XMLParseEndPoints(ServerEndPoints, true, pf);
            break;
      }
    }
    /*
    public PoolFactory CreatePoolFactoryAndSetAttribute()
    {
      PoolFactory pf = PoolManager.CreateFactory();
      ResetKey("poolSpec");
      string poolRegionData = GetStringValue("poolSpec");
      //FwkInfo("PoolSpec is :{0}", poolRegionData);
      //Properties prop = GetNewPoolAttributes(poolRegionData);
      staing poolName = null;
      SetPoolAttributes(pf, poolRegionData,poolName);
      return pf;
    }*/
    private void ReadXmlData(RegionFactory af, PoolFactory pf,string spec, ref string poolname)
    {
      const string DriverNodeName = "test-driver";
      string xmlFile = Util.BBGet(string.Empty, "XMLFILE") as string;
      XmlNode node = XmlNodeReaderWriter.GetInstance(xmlFile).GetNode(
           '/' + DriverNodeName);
      XmlNodeList xmlNodes = node.SelectNodes("data");
      if (xmlNodes != null)
      {
        foreach (XmlNode xmlNode in xmlNodes)
        {
          XmlAttribute tmpattr = xmlNode.Attributes["name"];
          if(tmpattr.Value == spec)
          {
          if (xmlNode.FirstChild.Name == "snippet")
          {
            string regionName;
            if (xmlNode.FirstChild.FirstChild.Name == "region")
            {
              //Util.Log("rjk reading region xml data attri name in fwktest");
              XmlAttribute nameattr = xmlNode.FirstChild.FirstChild.Attributes["name"];
              regionName = nameattr.Value;
              XmlNode attrnode = xmlNode.FirstChild.FirstChild.FirstChild;
              //AttributesFactory af = new AttributesFactory();
              if (attrnode.Name == "region-attributes")
              {
                XmlAttributeCollection attrcoll = attrnode.Attributes;
                if (attrcoll != null)
                {
                  foreach (XmlAttribute eachattr in attrcoll)
                  {
                    //Util.Log("rjk fwktest region attri xml data attri name = {0} , attri value = {1}", eachattr.Name, eachattr.Value);
                    SetThisAttribute(eachattr.Name, eachattr, af, ref poolname);
                   }
                }
                if (attrnode.ChildNodes != null)
                {
                  foreach (XmlNode tmpnode in attrnode.ChildNodes)
                  {
                    //Util.Log("rjk fwktest region attri tmpnode xml data attri name = {0} , attri value = {1}", tmpnode.Name, tmpnode.Value);
                    SetThisAttribute(tmpnode.Name, tmpnode, af, ref poolname);
                  }
                }
                else
                {
                  throw new IllegalArgException("The xml file passed has an unknown format");
                }
              }
            }
            else if (xmlNode.FirstChild.FirstChild.Name == "pool")
            {
              XmlAttribute nameattr = xmlNode.FirstChild.FirstChild.Attributes["name"];
              poolname = nameattr.Value;
              // Now collect the pool atributes
              Properties<string,string> prop = new Properties<string,string>();
              XmlAttributeCollection attrcoll = xmlNode.FirstChild.FirstChild.Attributes;
              if (attrcoll != null)
              {
                foreach (XmlAttribute eachattr in attrcoll)
                {
                  //Util.Log("rjk fwktest xml data attri name = {0} , attri value = {1}", eachattr.Name, eachattr.Value);
                  SetThisPoolAttributes(pf, eachattr.Name, eachattr.Value);
                }
              }
              else
              {
                throw new IllegalArgException("The xml file passed has an unknown format");
              }
            }
          }
        }
        }
      }
    }
    private static ExpirationAction StrToExpirationAction(string str)
    {
      return (ExpirationAction)Enum.Parse(typeof(ExpirationAction),
        str.Replace("-", string.Empty), true);
    }
    public void SetRegionAttributes(RegionFactory af, string spec, ref string poolname)
    {
      ReadXmlData(af, null,spec, ref poolname);
    }
    public static void SetThisAttribute(string name, XmlNode node, RegionFactory af, ref string poolname)
    {
      string value = node.Value;
      switch (name)
      {
        case "caching-enabled":
          if (value == "true")
          {
            af.SetCachingEnabled(true);
          }
          else
          {
            af.SetCachingEnabled(false);
          }
          break;

        case "load-factor":
          float lf = float.Parse(value);
          af.SetLoadFactor(lf);
          break;

        case "concurrency-level":
          int cl = int.Parse(value);
          af.SetConcurrencyLevel(cl);
          break;

        case "lru-entries-limit":
          uint lel = uint.Parse(value);
          af.SetLruEntriesLimit(lel);
          break;

        case "initial-capacity":
          int ic = int.Parse(value);
          af.SetInitialCapacity(ic);
          break;

        case "disk-policy":
          if (value == "none")
          {
            af.SetDiskPolicy(DiskPolicyType.None);
          }
          else if (value == "overflows")
          {
            af.SetDiskPolicy(DiskPolicyType.Overflows);
          }
          else
          {
            throw new IllegalArgException("Unknown disk policy");
          }
          break;
      case "concurrency-checks-enabled":
          bool cce = bool.Parse(value);
          af.SetConcurrencyChecksEnabled(cce);
          break;
      case "pool-name":
          if (value.Length != 0)
          {
            af.SetPoolName(value);
          }
          else
          {
            af.SetPoolName(value);
          }
          poolname = value;
          break;
        
        case "region-time-to-live":
          XmlNode nlrttl = node.FirstChild;
          if (nlrttl.Name == "expiration-attributes")
          {
            XmlAttributeCollection exAttrColl = nlrttl.Attributes;
            ExpirationAction action = StrToExpirationAction(exAttrColl["action"].Value);
            string rttl = exAttrColl["timeout"].Value;
            af.SetRegionTimeToLive(action, uint.Parse(rttl));
          }
          else
          {
            throw new IllegalArgException("The xml file passed has an unknowk format");
          }
          break;

        case "region-idle-time":
          XmlNode nlrit = node.FirstChild;
          if (nlrit.Name == "expiration-attributes")
          {
            XmlAttributeCollection exAttrColl = nlrit.Attributes;
            ExpirationAction action = StrToExpirationAction(exAttrColl["action"].Value);
            string rit = exAttrColl["timeout"].Value;
            af.SetRegionIdleTimeout(action, uint.Parse(rit));
          }
          else
          {
            throw new IllegalArgException("The xml file passed has an unknowk format");
          }
          break;

        case "entry-time-to-live":
          XmlNode nlettl = node.FirstChild;
          if (nlettl.Name == "expiration-attributes")
          {
            XmlAttributeCollection exAttrColl = nlettl.Attributes;
            ExpirationAction action = StrToExpirationAction(exAttrColl["action"].Value);
            string ettl = exAttrColl["timeout"].Value;
            af.SetEntryTimeToLive(action, uint.Parse(ettl));
          }
          else
          {
            throw new IllegalArgException("The xml file passed has an unknowk format");
          }
          break;

        case "entry-idle-time":
          XmlNode nleit = node.FirstChild;
          if (nleit.Name == "expiration-attributes")
          {
            XmlAttributeCollection exAttrColl = nleit.Attributes;
            ExpirationAction action = StrToExpirationAction(exAttrColl["action"].Value);
            string eit = exAttrColl["timeout"].Value;
            af.SetEntryIdleTimeout(action, uint.Parse(eit));
          }
          else
          {
            throw new IllegalArgException("The xml file passed has an unknowk format");
          }
          break;

        case "cache-loader":
          XmlAttributeCollection loaderattrs = node.Attributes;
          string loaderlibrary = null;
          string loaderfunction = null;
          foreach (XmlAttribute tmpattr in loaderattrs)
          {
            if (tmpattr.Name == "library")
            {
              loaderlibrary = tmpattr.Value;
            }
            else if (tmpattr.Name == "function")
            {
              loaderfunction = tmpattr.Value;
            }
            else
            {
              throw new IllegalArgException("cahe-loader attributes in improper format");
            }
          }
          if (loaderlibrary != null && loaderfunction != null)
          {
            if (loaderfunction.IndexOf('.') < 0)
            {
              Type myType = typeof(FwkTest<TKey, TVal>);
              loaderfunction = myType.Namespace + '.' +
                loaderlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + loaderfunction;
              loaderlibrary = myType.Assembly.FullName;

              ICacheLoader<TKey, TVal> loader = null;

              String createCacheLoader = myType.Namespace + '.' +
                loaderlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createCacheLoader";

              if (String.Compare(loaderfunction, createCacheLoader, true) == 0) {                
                loader = new PerfCacheLoader<TKey, TVal>();
              }
              Util.Log(Util.LogLevel.Info, "Instantiated Loader = {0} ", loaderfunction);
              af.SetCacheLoader(loader);
            }
            //af.SetCacheLoader(loaderlibrary, loaderfunction);
          }
          break;

        case "cache-listener":
          XmlAttributeCollection listenerattrs = node.Attributes;
          string listenerlibrary = null;
          string listenerfunction = null;
          foreach (XmlAttribute tmpattr in listenerattrs)
          {
            if (tmpattr.Name == "library")
            {
              listenerlibrary = tmpattr.Value;
            }
            else if (tmpattr.Name == "function")
            {
              listenerfunction = tmpattr.Value;
            }
            else
            {
              throw new IllegalArgException("cahe-listener attributes in improper format");
            }
          }
          if (listenerlibrary != null && listenerfunction != null)
          {
            if (listenerfunction.IndexOf('.') < 0)
            {
              Type myType = typeof(FwkTest<TKey, TVal>);
              listenerfunction = myType.Namespace + '.' +
                listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + listenerfunction;
              //Util.Log(Util.LogLevel.Info, "rjk1 cache listener in fwktest: myType.Namespace {0} " +
              //          " listenerlibrary {1} listenerfunction {2}", myType.Namespace, listenerlibrary, listenerfunction);
              //listenerlibrary = myType.Assembly.FullName;
              //Util.Log(Util.LogLevel.Info, "rjk cache listener in fwktest inside if condition: listenerlibrary {0} listenerfunction {1}", listenerlibrary, listenerfunction);

              Util.Log(Util.LogLevel.Info, "listenerlibrary is {0} and listenerfunction is {1}", listenerlibrary, listenerfunction);
              ICacheListener<TKey, TVal> listener = null;

              String perfTestCacheListener = myType.Namespace + '.' +
                  listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createPerfTestCacheListener";
              String conflationTestCacheListener = myType.Namespace + '.' +
                  listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createConflationTestCacheListener";
              String latencyListener = myType.Namespace + '.' +
                  listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createLatencyListenerP";
              String dupChecker = myType.Namespace + '.' +
                  listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createDupChecker";
              String createDurableCacheListener = myType.Namespace + '.' +
                  listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createDurableCacheListener";
              String createConflationTestCacheListenerDC = myType.Namespace + '.' +
                  listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createConflationTestCacheListenerDC";
              String createDurablePerfListener = myType.Namespace + '.' +
                  listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createDurablePerfListener";              
              String CreateDurableCacheListenerSP = myType.Namespace + '.' +
                  listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createDurableCacheListenerSP";
              String createLatencyListener = myType.Namespace + '.' +
                  listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createLatencyListener";
              String createSilenceListener = myType.Namespace + '.' +
                  listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createSilenceListener";
              String createDeltaValidationCacheListener = myType.Namespace + '.' +
                  listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createDeltaValidationCacheListener";
              String createSilenceListenerPdx = myType.Namespace + '.' +
                                listenerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + "createSilenceListenerPdx";

              if (String.Compare(listenerfunction, perfTestCacheListener, true) == 0) {                
                listener = new PerfTestCacheListener<TKey, TVal>();
              }
              else if (String.Compare(listenerfunction, conflationTestCacheListener, true) == 0) {                
                listener = new ConflationTestCacheListener<TKey, TVal>();
              }
              else if (String.Compare(listenerfunction, latencyListener, true) == 0) {                
                listener = new LatencyListener<TKey, TVal>();
              }
              else if (String.Compare(listenerfunction, dupChecker, true) == 0) {                
                listener = new DupChecker<TKey, TVal>();
              }
              else if (String.Compare(listenerfunction, createDurableCacheListener, true) == 0) {                
                listener = new DurableListener<TKey, TVal>();
              }
              else if (String.Compare(listenerfunction, createConflationTestCacheListenerDC, true) == 0) {                
                listener = new ConflationTestCacheListenerDC<TKey, TVal>();
              }
              else if (String.Compare(listenerfunction, createDurablePerfListener, true) == 0) {                
                listener = new DurablePerfListener<TKey, TVal>();
              }              
              else if (String.Compare(listenerfunction, CreateDurableCacheListenerSP, true) == 0) {                
                listener = new DurableCacheListener<TKey, TVal>();
              }
              else if (String.Compare(listenerfunction, createLatencyListener, true) == 0) {                
                listener = new LatencyListeners<TKey, TVal>(InitPerfStat.perfstat[0]);
              }
              else if (String.Compare(listenerfunction, createSilenceListener, true) == 0)
              {
                listener = new SilenceListener<TKey, TVal>();
              }
              else if (String.Compare(listenerfunction, createSilenceListenerPdx, true) == 0)
              {
                  listener = new PDXSilenceListener<TKey, TVal>();
              }
              else if (String.Compare(listenerfunction, createDeltaValidationCacheListener, true) == 0)
              {
                listener = new DeltaClientValidationListener<TKey, TVal>();
              }
              Util.Log(Util.LogLevel.Info, "Instantiated Listener = {0} ", listenerfunction);
              af.SetCacheListener(listener);              
            }                       
            //af.SetCacheListener(listenerlibrary, listenerfunction);
          }
          break;

        case "cache-writer":
          XmlAttributeCollection writerattrs = node.Attributes;
          string writerlibrary = null;
          string writerfunction = null;
          foreach (XmlAttribute tmpattr in writerattrs)
          {
            if (tmpattr.Name == "library")
            {
              writerlibrary = tmpattr.Value;
            }
            else if (tmpattr.Name == "function")
            {
              writerfunction = tmpattr.Value;
            }
            else
            {
              throw new IllegalArgException("cahe-loader attributes in improper format");
            }
          }
          if (writerlibrary != null && writerfunction != null)
          {
            if (writerfunction.IndexOf('.') < 0)
            {
              Type myType = typeof(FwkTest<TKey, TVal>);
              writerfunction = myType.Namespace + '.' +
                writerlibrary + "<" + typeof(TKey) + "," + typeof(TVal) + ">." + writerfunction;
              writerlibrary = myType.Assembly.FullName;
            }
            af.SetCacheWriter(writerlibrary, writerfunction);
          }
          break;

        case "persistence-manager":
          string pmlibrary = null;
          string pmfunction = null;
          Properties<string, string> prop = new Properties<string, string>();
          XmlAttributeCollection pmattrs = node.Attributes;
          foreach (XmlAttribute attr in pmattrs)
          {
            if (attr.Name == "library")
            {
                pmlibrary = attr.Value;
            }
            else if (attr.Name == "function")
            {
                pmfunction = attr.Value;
            }
            else
            {
              throw new IllegalArgException("Persistence Manager attributes in wrong format: " + attr.Name);
            }
          }

          if (node.FirstChild.Name == "properties")
          {
            XmlNodeList pmpropnodes = node.FirstChild.ChildNodes;
            foreach (XmlNode propnode in pmpropnodes)
            {
              if (propnode.Name == "property")
              {
                XmlAttributeCollection keyval = propnode.Attributes;
                XmlAttribute keynode = keyval["name"];
                XmlAttribute valnode = keyval["value"];

                if (keynode.Value == "PersistenceDirectory" || keynode.Value == "EnvironmentDirectory")
                {
                  prop.Insert(keynode.Value, valnode.Value);
                }
                else if (keynode.Value == "CacheSizeGb" || keynode.Value == "CacheSizeMb"
                         || keynode.Value == "PageSize" || keynode.Value == "MaxFileSize")
                {
                  prop.Insert(keynode.Value, valnode.Value);
                }
              }
            }
          }
          af.SetPersistenceManager(pmlibrary, pmfunction, prop);
          break;
      }
    }
      private static string ConvertStringArrayToString(string[] array)
      {
          //
          // Concatenate all the elements into a StringBuilder.
          //
          StringBuilder builder = new StringBuilder();
          foreach (string value in array)
          {
              builder.Append(value);
              builder.Append('.');
          }
          return builder.ToString();
      }

      private string PoolAttributesToString(Pool attrs)
      {
          StringBuilder attrsSB = new StringBuilder();
          attrsSB.Append(Environment.NewLine + "poolName: " +
            attrs.Name);
          attrsSB.Append(Environment.NewLine + "FreeConnectionTimeout: " +
            attrs.FreeConnectionTimeout);
          attrsSB.Append(Environment.NewLine + "LoadConditioningInterval: " +
           attrs.LoadConditioningInterval);
          attrsSB.Append(Environment.NewLine + "SocketBufferSize: " +
            attrs.SocketBufferSize);
          attrsSB.Append(Environment.NewLine + "ReadTimeout: " +
            attrs.ReadTimeout);
          attrsSB.Append(Environment.NewLine + "MinConnections: " +
            attrs.MinConnections);
          attrsSB.Append(Environment.NewLine + "MaxConnections: " +
            attrs.MaxConnections);
          attrsSB.Append(Environment.NewLine + "StatisticInterval: " +
            attrs.StatisticInterval);
          attrsSB.Append(Environment.NewLine + "RetryAttempts: " +
            attrs.RetryAttempts);
          attrsSB.Append(Environment.NewLine + "SubscriptionEnabled: " +
            attrs.SubscriptionEnabled);
          attrsSB.Append(Environment.NewLine + "SubscriptionRedundancy: " +
            attrs.SubscriptionRedundancy);
          attrsSB.Append(Environment.NewLine + "SubscriptionAckInterval: " +
            attrs.SubscriptionAckInterval);
          attrsSB.Append(Environment.NewLine + "SubscriptionMessageTrackingTimeout: " +
            attrs.SubscriptionMessageTrackingTimeout);
          attrsSB.Append(Environment.NewLine + "ServerGroup: " +
            attrs.ServerGroup);
          attrsSB.Append(Environment.NewLine + "IdleTimeout: " +
            attrs.IdleTimeout);
          attrsSB.Append(Environment.NewLine + "PingInterval: " +
            attrs.PingInterval);
          attrsSB.Append(Environment.NewLine + "ThreadLocalConnections: " +
            attrs.ThreadLocalConnections);
          attrsSB.Append(Environment.NewLine + "MultiuserAuthentication: " +
            attrs.MultiuserAuthentication);
          attrsSB.Append(Environment.NewLine + "PRSingleHopEnabled: " +
            attrs.PRSingleHopEnabled);
          attrsSB.Append(Environment.NewLine + "Locators: " );
          if (attrs.Locators != null && attrs.Locators.Length > 0)
          {
              foreach (string value in attrs.Locators)
              {
                  attrsSB.Append(value);
                  attrsSB.Append(',');
              }
          }
          attrsSB.Append(Environment.NewLine + "Servers: " );
          if (attrs.Servers != null && attrs.Servers.Length > 0)
          {
              foreach (string value in attrs.Servers)
              {
                  attrsSB.Append(value);
                  attrsSB.Append(',');
              }
          }
          attrsSB.Append(Environment.NewLine);
          return attrsSB.ToString();
      }
    public virtual IRegion<TKey,TVal> CreateRootRegion()
    {
      return CreateRootRegion(null);
    }

    public virtual IRegion<TKey, TVal> CreateRootRegion(string regionName)
    {
      string rootRegionData = GetStringValue("regionSpec");
      return CreateRootRegion(regionName, rootRegionData);
    }

    public virtual IRegion<TKey, TVal> CreateRootRegion(string regionName,
      string rootRegionData)
    {
      string tagName = GetStringValue("TAG");
      string endpoints = Util.BBGet(JavaServerBB, EndPointTag + tagName)
        as string;
      return CreateRootRegion(regionName, rootRegionData, endpoints);
    }

    public virtual IRegion<TKey, TVal> CreateRootRegion(string regionName,
      string rootRegionData, string endpoints)
    {
      if (rootRegionData != null && rootRegionData.Length > 0)
      {
        string rootRegionName;
        if (regionName == null || regionName.Length == 0)
        {
          rootRegionName = GetRegionName(rootRegionData);
        }
        else
        {
          rootRegionName = regionName;
        }
        if (rootRegionName != null && rootRegionName.Length > 0)
        {
          IRegion<TKey, TVal> region;
          if ((region = CacheHelper<TKey, TVal>.GetRegion(rootRegionName)) == null)
          {
            Properties<string,string> dsProps = new Properties<string,string>();
            GetClientSecurityProperties(ref dsProps, rootRegionName); 
            
            // Check for any setting of heap LRU limit
            int heapLruLimit = GetUIntValue(HeapLruLimitKey);
            if (heapLruLimit > 0)
            {
              dsProps.Insert("heap-lru-limit", heapLruLimit.ToString());
            }
            string conflateEvents = GetStringValue(ConflateEventsKey);
            if (conflateEvents != null && conflateEvents.Length > 0)
            {
              dsProps.Insert("conflate-events", conflateEvents);
            }
            
            ResetKey("sslEnable");
            bool isSslEnable = GetBoolValue("sslEnable");
            if (isSslEnable)
            {
              dsProps.Insert("ssl-enabled", "true");
              string keyStorePath = Util.GetFwkLogDir(Util.SystemType) + "/data/keystore";
              string pubkey = keyStorePath + "/client_truststore.pem";
              string privkey = keyStorePath + "/client_keystore.pem";
              dsProps.Insert("ssl-keystore", privkey);
              dsProps.Insert("ssl-truststore", pubkey);
            }
            //Properties rootAttrs = GetNewRegionAttributes(rootRegionData);
            // Check if this is a thin-client region; if so set the endpoints
            RegionFactory rootAttrs = null; 
            //RegionFactory rootAttrs = CacheHelper.DCache.CreateRegionFactory(RegionShortcut.PROXY);
            string m_isPool = null;
              //SetRegionAttributes(rootAttrs, rootRegionData, ref m_isPool);
            int redundancyLevel = 0;
            redundancyLevel = GetUIntValue(RedundancyLevelKey);
            //string m_isPool = rootAttrs.Find("pool-name");
            string mode = Util.GetEnvironmentVariable("POOLOPT");
            if (endpoints != null && endpoints.Length > 0)
            {
              //redundancyLevel = GetUIntValue(RedundancyLevelKey);
              //if (redundancyLevel > 0)
              //{
                if (mode == "poolwithendpoints" || mode == "poolwithlocator" )//|| m_isPool != null)
                {
                  FwkInfo("Setting the pool-level configurations");
                  CacheHelper<TKey, TVal>.InitConfigPool(dsProps);
                }
                try
                {
                    rootAttrs = CacheHelper<TKey, TVal>.DCache.CreateRegionFactory(RegionShortcut.PROXY);
                }
                catch (Exception e)
                {
                    FwkException("GOT this {0}",e.Message);
                }
              FwkInfo("Creating region factory");
              SetRegionAttributes(rootAttrs, rootRegionData, ref m_isPool);
            
            }
            rootAttrs = CreatePool(rootAttrs, redundancyLevel);
            ResetKey("NumberOfRegion");
            int numRegions = GetUIntValue("NumberOfRegion");
            if (numRegions < 1)
              numRegions = 1;
            for (int i = 0; i < numRegions; i++)
            {
              if(i>0)
               region = CacheHelper<TKey, TVal>.CreateRegion(rootRegionName+"_"+i, rootAttrs);
              else
               region = CacheHelper<TKey, TVal>.CreateRegion(rootRegionName, rootAttrs);
            }
            GemStone.GemFire.Cache.Generic.RegionAttributes<TKey, TVal> regAttr = region.Attributes;
            FwkInfo("Region attributes for {0}: {1}", rootRegionName,
              CacheHelper<TKey, TVal>.RegionAttributesToString(regAttr));
          }
          Log.Info("<ExpectedException action=add>NotAuthorizedException" +
            "</ExpectedException>");
          return region;
        }
      }
      return null;
    }
    private void ParseEndPoints(string ep, bool isServer, int redundancyLevel)
    {
      string poolName = "_Test_Pool";
      PoolFactory pf = PoolManager.CreateFactory();
      string[] eps = ep.Split(',');
      foreach (string endpoint in eps)
      {
        string hostName = endpoint.Split(':')[0];
        int portNum = int.Parse(endpoint.Split(':')[1]);
        if (isServer)
        {
          FwkInfo("adding pool host port for server");
          pf.AddServer(hostName, portNum);
        }
        else
        {
          FwkInfo("adding pool host port for server");
          pf.AddLocator(hostName, portNum);
        }
      }

      pf.SetSubscriptionEnabled(true);

      ResetKey("multiUserMode");
      bool multiUserMode = GetBoolValue("multiUserMode");
      if (multiUserMode)
      {
        pf.SetMultiuserAuthentication(true);
        FwkInfo("MultiUser Mode is set to true");
      }
      else
      {
        pf.SetMultiuserAuthentication(false);
        FwkInfo("MultiUser Mode is set to false");
      }
      pf.SetFreeConnectionTimeout(180000);
      pf.SetReadTimeout(180000);
      pf.SetMinConnections(20);
      pf.SetMaxConnections(30);

      if (redundancyLevel > 0)
        pf.SetSubscriptionRedundancy(redundancyLevel);
      if (PoolManager.Find(poolName) == null)
      {
        Pool pool = pf.Create(poolName);
        FwkInfo("Pool attributes are {0}:", PoolAttributesToString(pool));
      }
      FwkInfo("Create Pool complete with poolName= {0}", poolName);
    }
   
    public virtual RegionFactory CreatePool(RegionFactory attr, int redundancyLevel)
    {
      string mode = Util.GetEnvironmentVariable("POOLOPT");
      if (mode == "poolwithendpoints")
      {
        string EndPoints = Util.BBGet(JavaServerBB, EndPointTag) as string;
        ParseEndPoints(EndPoints, true, redundancyLevel);
        attr = attr.SetPoolName("_Test_Pool");
      }
      else if (mode == "poolwithlocator")
      {
        string locatorAddress = (string)Util.BBGet(string.Empty, "LOCATOR_ADDRESS_POOL");
        ParseEndPoints(locatorAddress, false, redundancyLevel);
        attr = attr.SetPoolName("_Test_Pool");
      }
      return attr;
    }
   
    // TODO: format an appropriate line for logging.
    public virtual void SetTaskRunInfo(string regionTag, string taskName,
      int numKeys, int numClients, int valueSize, int numThreads)
    {
      m_taskData = new FwkTaskData(regionTag, taskName, numKeys, numClients,
        valueSize, numThreads);
    }

    public virtual void AddTaskRunRecord(int iters, TimeSpan elapsedTime)
    {
      double opsPerSec = iters / elapsedTime.TotalSeconds;
      double micros = elapsedTime.TotalMilliseconds * 1000;
      string recordStr = string.Format("{0} -- {1}ops/sec, {2} ops, {3} micros",
        m_taskData.GetLogString(), opsPerSec, iters, micros);
      lock (((ICollection)m_taskRecords).SyncRoot)
      {
        m_taskRecords.Add(recordStr);
      }
      Util.RawLog(string.Format("[PerfSuite] {0}{1}", recordStr,
        Environment.NewLine));
      Util.RawLog(string.Format("[PerfData],{0},{1},{2},{3},{4}{5}",
        m_taskData.GetCSVString(), opsPerSec, iters, micros,
        DateTime.Now.ToString("G"), Environment.NewLine));
    }

    public virtual void RunTask(ClientTask task, int numThreads,
      int iters, int timedInterval, int maxTime, object data)
    {
      ClientBase client = null;
      try
      {
        if (numThreads > 1)
        {
          client = ClientGroup.Create(UnitThread.Create, numThreads);
        }
        else
        {
          client = new UnitThread();
        }
        UnitFnMethod<UnitFnMethod<int, object>, int, object> taskDeleg =
          new UnitFnMethod<UnitFnMethod<int, object>, int, object>(
          client.Call<int, object>);
        task.StartRun();
        IAsyncResult taskRes = taskDeleg.BeginInvoke(
          task.DoTask, iters, data, null, null);
        if (timedInterval > 0)
        {
          System.Threading.Thread.Sleep(timedInterval);
          task.EndRun();
        }
        if (maxTime <= 0)
        {
          taskRes.AsyncWaitHandle.WaitOne();
        }
        else if (!taskRes.AsyncWaitHandle.WaitOne(maxTime, false))
        {
          throw new ClientTimeoutException("RunTask() timed out.");
        }
        taskDeleg.EndInvoke(taskRes);
        task.EndRun();
      }
      finally
      {
        if (client != null)
        {
          client.Dispose();
        }
      }
    }

    public virtual void EndTask()
    {
      lock (((ICollection)m_taskRecords).SyncRoot)
      {
        if (m_taskRecords.Count > 0)
        {
          StringBuilder summarySB = new StringBuilder();
          foreach (string taskRecord in m_taskRecords)
          {
            summarySB.Append(Environment.NewLine + '\t' + taskRecord);
          }
          FwkInfo("TIMINGS:: Summary: {0}", summarySB.ToString());
          m_taskRecords.Clear();
        }
      }
      ClearCachedKeys();
      PopTaskName();
    }

    public QueryService<TKey, object> CheckQueryService()
    {
      string mode = Util.GetEnvironmentVariable("POOLOPT");
      Pool/*<TKey, TVal>*/ pool = PoolManager/*<TKey, TVal>*/.Find("_Test_Pool");
      return pool.GetQueryService<TKey, object>();
    }

    #endregion
  }
}
