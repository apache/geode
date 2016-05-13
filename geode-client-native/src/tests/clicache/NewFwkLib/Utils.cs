//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{

  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests.NewAPI;
  using GemStone.GemFire.Cache.Generic;
  using NEWAPI = GemStone.GemFire.Cache.Tests.NewAPI;
  [Serializable]
  struct HostInfo
  {
    public bool Started;
    public string HostType;
    public string HostName;
    public string ExtraServerArgs;

    public HostInfo(bool started, string hostType, string hostName,
      string extraServerArgs)
    {
      Started = started;
      HostType = hostType;
      HostName = hostName;
      ExtraServerArgs = extraServerArgs;
    }
  }

  public class Utils<TKey, TVal> : FwkTest<TKey, TVal>
  {
    private const char PathSep = '/';
    private const int MaxWaitMillis = 1800000;
    private const string SetupJSName = "setupJavaServers";
    private const string StartJSName = "startJavaServers";
    private const string StopJSName = "stopJavaServers";
    private const string KillJSName = "killJavaServers";
    private const string SleepTime = "sleepTime";
    private const string MinServers = "minServers";
    private const string JavaServerCountKey = "ServerCount";
    private const string JavaServerMapKey = "ServerMap";
    private const string JavaServerName = "cacheserver.bat";
    private const string JavaServerOtherArgs =
      " statistic-sampling-enabled=true" +
      " statistic-archive-file=statArchive.gfs mcast-port=0";
    private const string JavaServerJavaArgs =
      " -J-Xmx1280m -J-Xms512m -J-DCacheClientProxy.MESSAGE_QUEUE_SIZE=100000";
    private const string JavaServerJavaArgsUnix =
      " -J-Xmx2048m -J-Xms1024m -J-XX:+HeapDumpOnOutOfMemoryError " +
      "-J-DCacheClientProxy.MESSAGE_QUEUE_SIZE=100000";
    private static string TempDir = Util.GetEnvironmentVariable("TMP");
    // Constants for status
    private const int GFNoError = 0;
    private const int GFError = 1;
    private const int GFTimeout = 2;

    private static Dictionary<string, HostInfo> m_javaServerHostMap =
      new Dictionary<string, HostInfo>();
    private static int m_numServers = 1;
    private static int locCount = 0;
    private static volatile bool m_exiting = false;
    private static bool m_firstRun = true;
    private static string m_locatorHost = null;
    private static string m_locatorType = null;

    #region Private methods

    private string[] ParseJavaServerArgs(string argStr, ref int numServers,
      out int argIndx, out string extraServerArgs)
    {
      argIndx = 0;
      extraServerArgs = string.Empty;
      if (argStr != null && argStr.Length > 0)
      {
        string[] args = argStr.Split(' ');
        while (args.Length > argIndx && args[argIndx][0] == '-')
        {
          FwkAssert(args.Length > (argIndx + 1),
            "JavaServer() value not provided after option '{0}'.",
            args[argIndx]);
          string argName = args[argIndx];
          string argValue = args[argIndx + 1];
          switch (argName)
          {
            case "-t":
              // Ignore the tagname; we now use the hostGroup name as the tag
              break;
            case "-N":
              break;
            case "-M":
              break;
            case "-X":
              break;
            case "-c":
              numServers = int.Parse(argValue);
              break;
            case "-e":
              // @TODO: this is C++ specific environment variables -- ignored for now
              break;
            case "-p":
              extraServerArgs += (" " + argValue.Replace("\\", "").
                Replace("\"", "").Replace("'", ""));
              break;
            default:
              FwkException("JavaServer() Unknown option '{0}'", argName);
              break;
          }
          argIndx += 2;
        }
        extraServerArgs += " ";
        return args;
      }
      return new string[0];
    }

    private string GetHostGroup()
    {
      string hostGroup;
      try
      {
        hostGroup = Util.BBGet(Util.ClientId,
          FwkReadData.HostGroupKey) as string;
      }
      catch
      {
        hostGroup = null;
      }
      return (hostGroup == null ? string.Empty : hostGroup);
    }

    private static HostInfo GetHostInfo(string serverId)
    {
      HostInfo hostInfo = new HostInfo();
      lock (((ICollection)m_javaServerHostMap).SyncRoot)
      {
        if (m_javaServerHostMap.Count == 0)
        {
          try
          {
            m_javaServerHostMap = Util.BBGet(JavaServerBB, JavaServerMapKey)
              as Dictionary<string, HostInfo>;
          }
          catch
          {
          }
        }
        if (m_javaServerHostMap.ContainsKey(serverId))
        {
          hostInfo = m_javaServerHostMap[serverId];
        }
      }
      return hostInfo;
    }

    private static string GetJavaStartDir(string serverId, string hostType)
    {
      return Util.GetLogBaseDir(hostType) + PathSep + "GFECS_" + serverId;
    }
    private static string GetLocatorStartDir(string locatorId, string hostType)
    {
      return Util.GetLogBaseDir(hostType) + PathSep + "GFELOC_" + locatorId;
    }
    private string GetJavaLocator()
    {
      try
      {
        return (string)Util.BBGet(string.Empty, "LOCATOR_ADDRESS");
      }
      catch
      {
        return null;
      }
    }
    private string GetJavaLocatorForPoolAttri()
    {
      try
      {
        return (string)Util.BBGet(string.Empty, "LOCATOR_ADDRESS_POOL");
      }
      catch
      {
        return null;
      }
    }
    private bool GetSetJavaLocator(string locatorHost,
      out int locatorPort)
    {
      locatorPort = 0;
      string locatorAddress = GetJavaLocator();
      string locatorAddressForPool = GetJavaLocatorForPoolAttri();
      if (locatorAddress == null || locatorAddress.Length == 0)
      {
        locatorPort = Util.Rand(31124, 54343);
        locatorAddress = locatorHost + '[' + locatorPort + ']';
        locatorAddressForPool = locatorHost + ':' + locatorPort;
        Util.BBSet(string.Empty, "LOCATOR_ADDRESS", locatorAddress);
        Util.BBSet(string.Empty, "LOCATOR_ADDRESS_POOL", locatorAddressForPool);
        locCount++;
        return true;
      }
      if (locatorAddress != null && locatorAddress.Length > 0 && locCount > 0)
      {
        locatorPort = Util.Rand(31124, 54343);
        locatorAddress += ',' + locatorHost + '[' + locatorPort + ']';
        locatorAddressForPool += ',' + locatorHost + ':' + locatorPort;
        Util.BBSet(string.Empty, "LOCATOR_ADDRESS", locatorAddress);
        Util.BBSet(string.Empty, "LOCATOR_ADDRESS_POOL", locatorAddressForPool);
        locCount++;
        return true;
      }
      return false;
    }

    private static int StartLocalGFExe(string exeName, string gfeDir,
      string exeArgs, out string outStr)
    {
      int status = GFNoError;
      Process javaProc;
      string javaExePath = gfeDir + PathSep + "bin" + PathSep + exeName;
      Util.ServerLog("Executing local GemFire command {0} {1}", javaExePath,
        exeArgs);
      if (!Util.StartProcess(javaExePath, exeArgs, false, TempDir,
        true, false, false, out javaProc))
      {
        outStr = null;
        return GFError;
      }

      StreamReader outSr = javaProc.StandardOutput;
      // Wait for process to start
      bool started = javaProc.WaitForExit(MaxWaitMillis);
      outStr = outSr.ReadLine();
      outSr.Close();
      if (!started)
      {
        try
        {
          javaProc.Kill();
        }
        catch
        {
        }
        status = GFTimeout;
      }
      else if (javaProc.ExitCode != 0)
      {
        status = GFError;
      }
      return status;
    }

    private static string StartRemoteGFExe(string host, string hostType,
      string exeName, string exeArgs)
    {
      string gfJavaDir = Util.GetEnvironmentVariable("GFE_DIR", hostType);
      string gfClassPath = Util.GetEnvironmentVariable("CLASSPATH", hostType);
      string gfJava = Util.GetEnvironmentVariable("GF_JAVA", hostType);

      string javaCmd = gfJavaDir + "/bin/" + exeName + ' ' + exeArgs;
      Dictionary<string, string> envVars = new Dictionary<string, string>();
      if (gfClassPath != null && gfClassPath.Length > 0)
      {
        envVars["CLASSPATH"] = gfClassPath;
      }
      if (gfJava != null && gfJava.Length > 0)
      {
        envVars["GF_JAVA"] = gfJava;
      }
      return Util.RunClientShellTask(Util.ClientId, host, javaCmd, envVars);
    }

    private string GetSlaveId(string serverNum)
    {
      return "slave." + serverNum;
    }

    private string CreateSlaveTaskSpecification(string progName,
      string serverNum, string extraArgs)
    {
      extraArgs = (extraArgs == null ? string.Empty : ' ' + extraArgs);
      return string.Format("<task name=\"Deleg{1}\" action=\"doRunProcess\" " +
        "container=\"utils\" waitTime=\"25m\">{0}<data name=\"program\">" +
        "{1}</data>{0}" + "<data name=\"arguments\">{2}{3}</data>{0}" +
        "<client-set name=\"{4}\">{0}<client name=\"{5}\"/>{0}" +
        "</client-set>{0}</task>", Environment.NewLine, progName, serverNum,
        extraArgs, Util.ClientId, GetSlaveId(serverNum));
    }
    private string GetSslProperty(string hostType, bool forServer,string startDir)
    {
      string sslCmdStr = null;
      //if (!File.Exists(startDir + PathSep + "gemfire.properties"))
      //{
        TextWriter sw = new StreamWriter(startDir + PathSep + "gemfire.properties", false);
        String locatorAddress = GetJavaLocator();
        sw.WriteLine("locators={0}", locatorAddress);
        ResetKey("sslEnable");
        bool isSslEnable = GetBoolValue("sslEnable");
        if (!isSslEnable)
        {
          sw.Close();
          return string.Empty;
        }
        FwkInfo("ssl is enable");
        if (!forServer)
        {
          //StreamWriter sw = new StreamWriter("gemfire.properties",false);
          sw.WriteLine("ssl-enabled=true");
          sw.WriteLine("ssl-require-authentication=true");
          sw.WriteLine("mcast-port=0");
          sw.WriteLine("ssl-ciphers=SSL_RSA_WITH_NULL_SHA");
        }

        sw.Close();

        string keyStorePath = Util.GetFwkLogDir(hostType) + "/data/keystore/sslcommon.jks";
        string trustStorePath = Util.GetFwkLogDir(hostType) + "/data/keystore/server_truststore.jks";
        sslCmdStr = " -Djavax.net.ssl.keyStore=" + keyStorePath +
          " -Djavax.net.ssl.keyStorePassword=gemstone -Djavax.net.ssl.trustStore=" + trustStorePath +
          " -Djavax.net.ssl.trustStorePassword=gemstone";
        //string securityParams = GetStringValue(SecurityParams);
        if (forServer)
        {
          sslCmdStr = " -J-Djavax.net.ssl.keyStore=" + keyStorePath +
            " -J-Djavax.net.ssl.keyStorePassword=gemstone -J-Djavax.net.ssl.trustStore=" + trustStorePath +
            " -J-Djavax.net.ssl.trustStorePassword=gemstone " +
            " ssl-enabled=true  ssl-require-authentication=true ssl-ciphers=SSL_RSA_WITH_NULL_SHA";
        }
      //}
      return sslCmdStr;
    }
    private string GetServerSecurityArgs(string hostType)
    {
      string securityParams = GetStringValue(SecurityParams);
      CredentialGenerator gen;
      // no security means security params is not applicable
      if (securityParams == null || securityParams.Length == 0 ||
        (gen = GetCredentialGenerator()) == null)
      {
        FwkInfo("Security is DISABLED.");
        return string.Empty;
      }
      string logDir = Util.GetFwkLogDir(hostType);
      if (logDir == null || logDir.Length == 0)
      {
        logDir = Util.GetLogBaseDir(hostType);
        logDir = logDir.Substring(0, logDir.LastIndexOf("/"));
        logDir = logDir.Substring(0, logDir.LastIndexOf("/"));
      }
      string dataDir = logDir + "/data";
      gen.Init(dataDir, dataDir);
      Properties<string,string> extraProps = new Properties<string,string>();
      if (gen.SystemProperties != null)
      {
        extraProps.AddAll(gen.SystemProperties);
      }
      // For now only XML based authorization is supported
      AuthzCredentialGenerator authzGen = new XmlAuthzCredentialGenerator();
      authzGen.Init(gen);
      if (authzGen.SystemProperties != null)
      {
        extraProps.AddAll(authzGen.SystemProperties);
      }
      return Utility.GetServerArgs(gen.Authenticator,
        authzGen.AccessControl, null, extraProps, gen.JavaProperties);
    }

    private void SetupJavaServers(string argStr)
    {
      int argIndx;
      string endpoints = string.Empty;
      m_numServers = 1;

      string gfeDir = Util.GetEnvironmentVariable("GFE_DIR");

      FwkAssert(gfeDir != null && gfeDir.Length != 0,
        "SetupJavaServers() GFE_DIR is not set.");

      string hostGroup = GetHostGroup();
      Match mt;

      if (argStr == "CPP")
      {
        // special string to denote that endpoints have to be obtained
        // from C++ framework
        string fwkBBPath = Util.AssemblyDir + "/../../bin/FwkBB";
        string fwkBBArgs = "getInt GFE_BB EP_COUNT";
        Process fwkBBProc;
        if (!Util.StartProcess(fwkBBPath, fwkBBArgs, false, null,
          true, false, false, out fwkBBProc))
        {
          FwkException("SetupJavaServers() Cannot start C++ FwkBB [{0}].",
            fwkBBPath);
        }
        int numEndpoints = int.Parse(fwkBBProc.StandardOutput.ReadToEnd());
        fwkBBProc.WaitForExit();
        fwkBBProc.StandardOutput.Close();
        for (int index = 1; index <= numEndpoints; index++)
        {
          fwkBBArgs = "get GFE_BB EndPoints_" + index;
          if (!Util.StartProcess(fwkBBPath, fwkBBArgs, false, null,
            true, false, false, out fwkBBProc))
          {
            FwkException("SetupJavaServers() Cannot start C++ FwkBB [{0}].",
              fwkBBPath);
          }
          string endpoint = fwkBBProc.StandardOutput.ReadToEnd();
          fwkBBProc.WaitForExit();
          fwkBBProc.StandardOutput.Close();
          if (endpoints.Length > 0)
          {
            endpoints += ',' + endpoint;
          }
          else
          {
            endpoints = endpoint;
          }
        }
      }
      else if ((mt = Regex.Match(gfeDir, "^[^:]+:[0-9]+(,[^:]+:[0-9]+)*$"))
        != null && mt.Length > 0)
      {
        // The GFE_DIR is for a remote server; contains an end-point list
        endpoints = gfeDir;
      }
      else
      {
        string extraServerArgs;
        string[] args = ParseJavaServerArgs(argStr, ref m_numServers,
          out argIndx, out extraServerArgs);
        Util.BBSet(JavaServerBB, FwkTest<TKey,TVal>.JavaServerEPCountKey, m_numServers);
        FwkAssert(args.Length == argIndx + 1, "SetupJavaServers() cache XML argument not correct");
        string cacheXml = args[argIndx];
        FwkAssert(cacheXml.Length > 0, "SetupJavaServers() cacheXml argument is empty.");

        string xmlDir = Util.GetEnvironmentVariable("GFBASE") + "/framework/xml";
        if (xmlDir != null && xmlDir.Length > 0)
        {
          cacheXml = Util.NormalizePath(xmlDir + PathSep + cacheXml);
        }

        int javaServerPort = Util.RandPort(21321, 29789);

        List<string> targetHosts = Util.BBGet(FwkReadData.HostGroupKey,
          hostGroup) as List<string>;
        for (int serverNum = 1; serverNum <= m_numServers; serverNum++)
        {
          if (m_exiting)
          {
            return;
          }
          string serverId = hostGroup + '_' + serverNum;
          string startDir = GetJavaStartDir(serverId, Util.SystemType);
          if (!Directory.Exists(startDir))
          {
            Directory.CreateDirectory(startDir);
          }

          string targetHost;
          int numHosts = (targetHosts == null ? 0 : targetHosts.Count);
          int lruMemSizeMb = 700;
          if (numHosts == 0)
          {
            targetHost = Util.HostName;
            lock (((IDictionary)m_javaServerHostMap).SyncRoot)
            {
              m_javaServerHostMap[serverId] = new HostInfo();
            }
          }
          else
          {
            int hostIndex;
            if (numHosts > 1)
            {
              hostIndex = ((serverNum - 1) % (numHosts - 1)) + 1;
            }
            else
            {
              hostIndex = 0;
            }
            targetHost = targetHosts[hostIndex];
            FwkInfo("Checking the type of host {0}.", targetHost);
            string hostType = Util.RunClientShellTask(Util.ClientId,
              targetHost, "uname", null);
            hostType = Util.GetHostType(hostType);
            FwkInfo("The host {0} is: {1}", targetHost, hostType);
            if (hostType != "WIN")
            {
              lruMemSizeMb = 1400;
            }
            lock (((IDictionary)m_javaServerHostMap).SyncRoot)
            {
              m_javaServerHostMap[serverId] = new HostInfo(false,
                hostType, targetHost, extraServerArgs);
            }
          }
          // Create the cache.xml with correct port
          javaServerPort++;
          StreamReader cacheXmlReader = new StreamReader(cacheXml);
          string cacheXmlContent = cacheXmlReader.ReadToEnd();
          cacheXmlReader.Close();
          cacheXmlContent = cacheXmlContent.Replace("$PORT_NUM",
            javaServerPort.ToString()).Replace("$LRU_MEM",
            lruMemSizeMb.ToString());
          StreamWriter cacheXmlWriter = new StreamWriter(startDir +
            PathSep + "cache.xml");
          cacheXmlWriter.Write(cacheXmlContent);
          cacheXmlWriter.Close();

          Util.ServerLog("SetupJavaServers() added '{0}' for host '{1}'",
            serverId, targetHost);
          FwkInfo("SetupJavaServers() added '{0}' for host '{1}'",
            serverId, targetHost);

          if (serverNum == 1)
          {
            endpoints = targetHost + ':' + javaServerPort;
          }
          else
          {
            endpoints += ',' + targetHost + ':' + javaServerPort;
          }
          Util.BBSet(JavaServerBB, FwkTest<TKey,TVal>.EndPoints + "_" + serverNum.ToString(), targetHost + ':' + javaServerPort);
        }


        int[] locatorPort = new int[2];
        int locatorPort1;
        HostInfo locatorInfo = GetHostInfo(hostGroup + "_1");
        string locatorHost = locatorInfo.HostName;
        string locatorType = locatorInfo.HostType;
        if (locatorHost == null)
        {
          locatorHost = Util.HostName;
          locatorType = Util.SystemType;
        }
        if (locatorType == Util.SystemType)
        {
          locatorHost = Util.HostName;
        }
        //ResetKey("multiLocator");
        //bool isMultiLocator = GetBoolValue("multiLocator");
        //if (isMultiLocator)
        //{
        for (int i = 0; i < 2; i++)
        {
          GetSetJavaLocator(locatorHost, out locatorPort1);
          locatorPort[i] = locatorPort1;
        }
        for (int i = 0; i < 2; i++)
        {
          //if (GetSetJavaLocator(locatorHost, out locatorPort))
          //{
          FwkInfo("SetupJavaServers() starting locator on host {0}",
            locatorHost);

          string locatorDir = GetLocatorStartDir((i + 1).ToString(), Util.SystemType);
          if (!Directory.Exists(locatorDir))
          {
            Directory.CreateDirectory(locatorDir);
          }
          ResetKey("sslEnable");
          bool isSslEnable = GetBoolValue("sslEnable");
          /*
          if (isSslEnable)
          {
            locatorDir = locatorDir + "/..";
          }
          */
          string sslArg = GetSslProperty(locatorType, false, locatorDir);
          locatorDir = GetLocatorStartDir((i + 1).ToString(), locatorType);
            FwkInfo("ssl arguments: {0} {1}", sslArg, locatorDir);
            string locatorArgs = "start-locator -port=" + locatorPort[i] +
              " -dir=" + locatorDir + sslArg;
            if (locatorType != Util.SystemType)
            {
              FwkInfo(StartRemoteGFExe(locatorHost, locatorType,
                "gemfire", locatorArgs));
            }
            else
            {
              string outStr;
              int status = StartLocalGFExe("gemfire.bat", gfeDir,
                locatorArgs, out outStr);
              if (status == GFTimeout)
              {
                FwkException("SetupJavaServers() Timed out while starting " +
                  "locator. Please check the logs in {0}", locatorDir);
              }
              else if (status != GFNoError)
              {
                FwkException("SetupJavaServers() Error while starting " +
                  "locator. Please check the logs in {0}", locatorDir);
              }
              FwkInfo(outStr);
            }

            if (isSslEnable)
            {
              Util.RegisterTestCompleteDelegate(TestCompleteWithSSL);
            }
            else
            {
              Util.RegisterTestCompleteDelegate(TestCompleteWithoutSSL);
            }
            m_locatorHost = locatorHost;
            m_locatorType = locatorType;
            FwkInfo("SetupJavaServers() started locator on host {0}.",
              locatorHost);
          //}
        }
        //}
      }
      FwkInfo("SetupJavaServers() endpoints: {0}", endpoints);
      // Write the endpoints for both the tag and the cumulative one
      string globalEndPoints;
      try
      {
        globalEndPoints = Util.BBGet(string.Empty,
          FwkTest<TKey, TVal>.EndPointTag) as string;
      }
      catch (GemStone.GemFire.DUnitFramework.KeyNotFoundException)
      {
        globalEndPoints = null;
      }
      if (globalEndPoints != null && globalEndPoints.Length > 0 &&
        endpoints != null && endpoints.Length > 0)
      {
        globalEndPoints += ',' + endpoints;
      }
      else
      {
        globalEndPoints = endpoints;
      }
      Util.BBSet(JavaServerBB, FwkTest<TKey, TVal>.EndPointTag, globalEndPoints);
      Util.BBSet(JavaServerBB, FwkTest<TKey, TVal>.EndPointTag + hostGroup, endpoints);
      lock (((IDictionary)m_javaServerHostMap).SyncRoot)
      {
        Util.BBSet(JavaServerBB, JavaServerMapKey, m_javaServerHostMap);
      }
      FwkInfo("SetupJavaServers() completed.");
    }

    private void StartJavaServers(string argStr)
    {
      int numServers = -1;
      int argIndx;
      string extraServerArgs;
      string[] args = ParseJavaServerArgs(argStr, ref numServers,
        out argIndx, out extraServerArgs);

      string gfeDir = Util.GetEnvironmentVariable("GFE_DIR");
      string endpoints = string.Empty;
      string locatorAddress = GetJavaLocator();

      FwkAssert(gfeDir != null && gfeDir.Length != 0,
        "StartJavaServers() GFE_DIR is not set.");
      FwkAssert(locatorAddress != null && locatorAddress.Length > 0,
        "StartJavaServers() LOCATOR_ADDRESS is not set.");

      string hostGroup = GetHostGroup();

      Match mt = Regex.Match(gfeDir, "^[^:]+:[0-9]+(,[^:]+:[0-9]+)*$");
      if (mt == null || mt.Length == 0)
      {
        int startServer = 1;
        int endServer;
        if (args.Length == (argIndx + 1))
        {
          startServer = int.Parse(args[argIndx]);
          endServer = (numServers == -1 ? startServer :
            (startServer + numServers - 1));
        }
        else
        {
          endServer = (numServers == -1 ? m_numServers : numServers);
        }
        for (int serverNum = startServer; serverNum <= endServer; serverNum++)
        {
          if (m_exiting)
          {
            break;
          }
          string serverId = hostGroup + '_' + serverNum;
          HostInfo hostInfo = GetHostInfo(serverId);
          string targetHost = hostInfo.HostName;
          string hostType = hostInfo.HostType;
          string startDir = GetJavaStartDir(serverId, hostType);
          extraServerArgs = hostInfo.ExtraServerArgs + ' ' + extraServerArgs;
          string sslArg = GetSslProperty(hostType, true, startDir);
          FwkInfo("ssl arguments for server: {0} ",sslArg);
          string javaServerOtherArgs = GetServerSecurityArgs(hostType) + ' ' + sslArg;
          if (javaServerOtherArgs != null && javaServerOtherArgs.Length > 0)
          {
            FwkInfo("StartJavaServers() Using security server args: {0}",
              javaServerOtherArgs);
          }
          javaServerOtherArgs = JavaServerOtherArgs + ' ' + javaServerOtherArgs;
          if (targetHost == null || targetHost.Length == 0 ||
            Util.IsHostMyself(targetHost))
          {
            string cacheXml = startDir + PathSep + "cache.xml";
            string javaServerArgs = "start" + JavaServerJavaArgs + " -dir=" +
              startDir + " cache-xml-file=" + cacheXml + " locators=" +
              locatorAddress + extraServerArgs + javaServerOtherArgs ;

            // Assume the GFE_DIR is for starting a local server
            FwkInfo("StartJavaServers() starting {0} with GFE_DIR {1} " +
              "and arguments: {2}", JavaServerName, gfeDir, javaServerArgs);
            string outStr;
            int status = StartLocalGFExe(JavaServerName, gfeDir,
              javaServerArgs, out outStr);
            if (status == GFTimeout)
            {
              FwkException("StartJavaServers() Timed out waiting for Java " +
                "cacheserver to start. Please check the server log in {0}.",
                startDir);
            }
            else if (status != GFNoError)
            {
              FwkSevere("StartJavaServers() Error in starting Java " +
                "cacheserver. Please check the server log in {0}.", startDir);
              Thread.Sleep(60000);
            }
            FwkInfo("StartJavaServers() output from start script: {0}",
              outStr);
          }
          else if (hostType != Util.SystemType)
          {
            FwkInfo("StartJavaServers() starting '{0}' on remote host " +
              "'{1}' of type {2}", serverId, targetHost, hostType);

            string javaCSArgs = "start" +
              JavaServerJavaArgsUnix + " -dir=" + startDir +
              " cache-xml-file=cache.xml locators=" + locatorAddress +
              extraServerArgs + javaServerOtherArgs;
            FwkInfo(StartRemoteGFExe(targetHost, hostType,
              "cacheserver", javaCSArgs));
          }
          else
          {
            string taskSpec = CreateSlaveTaskSpecification(
              "startJavaServers", serverNum.ToString(), null);
            FwkInfo("StartJavaServers() starting '{0}' on host '{1}'",
              serverId, targetHost);
            Util.BBSet(Util.ClientId + '.' + GetSlaveId(
              serverNum.ToString()), FwkReadData.HostGroupKey,
              hostGroup);
            if (!Util.RunClientWinTask(Util.ClientId, targetHost, taskSpec))
            {
              FwkException("StartJavaServers() failed to start '{0}' on host '{1}'",
                serverId, targetHost);
            }
          }
          lock (((IDictionary)m_javaServerHostMap).SyncRoot)
          {
            hostInfo.Started = true;
            m_javaServerHostMap[serverId] = hostInfo;
          }
          Util.ServerLog("StartJavaServers() started '{0}' on host '{1}'",
            serverId, targetHost);
        }
      }
      ResetKey("sslEnable");
      bool isSslEnabled = GetBoolValue("sslEnable");
      if (isSslEnabled)
      {
        Util.RegisterTestCompleteDelegate(TestCompleteWithSSL);
      }
      else
      {
        Util.RegisterTestCompleteDelegate(TestCompleteWithoutSSL);
      }
      FwkInfo("StartJavaServers() completed.");
    }

    private void StopJavaServers(string argStr)
    {
      string gfeDir = Util.GetEnvironmentVariable("GFE_DIR");

      FwkAssert(gfeDir != null && gfeDir.Length != 0,
        "StopJavaServers() GFE_DIR is not set.");

      Match mt = Regex.Match(gfeDir, "^[^:]+:[0-9]+(,[^:]+:[0-9]+)*$");
      if (mt == null || mt.Length == 0)
      {
        int numServers = -1;
        int argIndx;
        string extraServerArgs;
        string[] args = ParseJavaServerArgs(argStr, ref numServers,
          out argIndx, out extraServerArgs);

        string hostGroup = GetHostGroup();
        string javaServerPath = gfeDir + PathSep + "bin" +
          PathSep + JavaServerName;
        // Assume the GFE_DIR is for stopping a local server
        int startServer = 1;
        int endServer;
        if (args.Length == (argIndx + 1))
        {
          startServer = int.Parse(args[argIndx]);
          endServer = (numServers == -1 ? startServer :
            (startServer + numServers - 1));
        }
        else
        {
          endServer = (numServers == -1 ? m_numServers : numServers);
        }
        for (int serverNum = startServer; serverNum <= endServer; serverNum++)
        {
          if (m_exiting)
          {
            break;
          }
          string serverId = hostGroup + '_' + serverNum;
          HostInfo hostInfo = GetHostInfo(serverId);
          string targetHost = hostInfo.HostName;
          string hostType = hostInfo.HostType;
          string startDir = GetJavaStartDir(serverId, hostType);
          string javaServerArgs = "stop -dir=" + startDir;
          if (targetHost == null || targetHost.Length == 0 ||
            Util.IsHostMyself(targetHost))
          {
            FwkInfo("StopJavaServers() stopping {0} with GFE_DIR {1} " +
              "and arguments: {2}", JavaServerName, gfeDir, javaServerArgs);

            string outStr;
            int status = StartLocalGFExe(JavaServerName, gfeDir,
              javaServerArgs, out outStr);
            if (status != GFNoError)
            {
              if (status == GFTimeout)
              {
                FwkSevere("StopJavaServers() Timed out waiting for Java " +
                  "cacheserver to stop. Please check the server log in {0}.",
                  startDir);
              }
              else
              {
                Thread.Sleep(20000);
              }
              KillLocalJavaServer(serverId, "-9");
            }
          }
          else if (hostType != Util.SystemType)
          {
            FwkInfo("StopJavaServers() stopping '{0}' on remote host " +
              "'{1}' of type {2}", serverId, targetHost, hostType);
            FwkInfo(StartRemoteGFExe(targetHost, hostType,
              "cacheserver", javaServerArgs));
          }
          else
          {
            string taskSpec = CreateSlaveTaskSpecification(
              "stopJavaServers", serverNum.ToString(), null);
            FwkInfo("StopJavaServers() stopping '{0}' on host '{1}'",
              serverId, targetHost);
            if (!Util.RunClientWinTask(Util.ClientId, targetHost, taskSpec))
            {
              FwkSevere("StopJavaServers() failed to stop '{0}' on host '{1}'",
                serverId, targetHost);
            }
            FwkInfo("StopJavaServers() stopped '{0}' on host '{1}'",
              serverId, targetHost);
          }
          lock (((ICollection)m_javaServerHostMap).SyncRoot)
          {
            hostInfo.Started = false;
            m_javaServerHostMap[serverId] = hostInfo;
          }
          Util.ServerLog("StopJavaServers() stopped '{0}' on host '{1}'",
            serverId, targetHost);
        }
      }
      FwkInfo("StopJavaServers() completed.");
    }

    private void KillJavaServers(string argStr)
    {
      string gfeDir = Util.GetEnvironmentVariable("GFE_DIR");

      FwkAssert(gfeDir != null && gfeDir.Length != 0,
        "KillJavaServers() GFE_DIR is not set.");

      Match mt = Regex.Match(gfeDir, "^[^:]+:[0-9]+(,[^:]+:[0-9]+)*$");
      if (mt == null || mt.Length == 0)
      {
        int numServers = -1;
        int argIndx;
        string extraServerArgs;
        string[] args = ParseJavaServerArgs(argStr, ref numServers,
          out argIndx, out extraServerArgs);

        string hostGroup = GetHostGroup();
        string javaServerPath = gfeDir + PathSep + "bin" +
          PathSep + JavaServerName;
        // Assume the GFE_DIR is for stopping a local server
        int startServer = 1;
        int endServer;
        string signal = "15";
        if (args.Length >= (argIndx + 1))
        {
          startServer = int.Parse(args[argIndx++]);
          endServer = (numServers == -1 ? startServer :
            (startServer + numServers - 1));
          if (args.Length >= (argIndx + 1))
          {
            signal = args[argIndx];
          }
        }
        else
        {
          endServer = (numServers == -1 ? m_numServers : numServers);
        }
        for (int serverNum = startServer; serverNum <= endServer; serverNum++)
        {
          if (m_exiting)
          {
            break;
          }
          string serverId = hostGroup + '_' + serverNum;
          HostInfo hostInfo = GetHostInfo(serverId);
          string targetHost = hostInfo.HostName;
          string hostType = hostInfo.HostType;
          if (targetHost == null || targetHost.Length == 0 ||
            Util.IsHostMyself(targetHost))
          {
            FwkInfo("KillJavaServers() killing '{0}' on localhost", serverId);
            KillLocalJavaServer(serverId, '-' + signal);
          }
          else if (hostType != Util.SystemType)
          {
            FwkInfo("KillJavaServers() killing '{0}' on remote host " +
              "'{1}' of type {2}", serverId, targetHost, hostType);
            string startDir = GetJavaStartDir(serverId, hostType);
            string killCmd = '"' + Util.GetFwkLogDir(hostType) +
              "/data/killJavaServer.sh\" " + signal + " \"" + startDir + '"';
            FwkInfo(Util.RunClientShellTask(Util.ClientId, targetHost,
              killCmd, null));
          }
          else
          {
            string taskSpec = CreateSlaveTaskSpecification(
              "killJavaServers", serverNum.ToString(), signal);
            FwkInfo("KillJavaServers() killing '{0}' on host '{1}'",
              serverId, targetHost);
            if (!Util.RunClientWinTask(Util.ClientId, targetHost, taskSpec))
            {
              FwkSevere("KillJavaServers() failed to kill '{0}' on host '{1}'",
                serverId, targetHost);
            }
          }
          lock (((ICollection)m_javaServerHostMap).SyncRoot)
          {
            hostInfo.Started = false;
            m_javaServerHostMap[serverId] = hostInfo;
          }
          Util.ServerLog("KillJavaServers() killed '{0}' on host '{1}'",
            serverId, targetHost);
        }
      }
      FwkInfo("KillJavaServers() completed.");
    }

    private static string GetGFJavaPID(string javaLog)
    {
      string javaPID = null;
      bool islocator = javaLog.Contains("locator.log");
      using (FileStream fs = new FileStream(javaLog, FileMode.Open,
        FileAccess.Read, FileShare.ReadWrite))
      {
        StreamReader sr = new StreamReader(fs);
        Regex pidRE = new Regex(@"Process ID: [\s]*(?<PID>[^\s]*)",
          RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline);
        while (!sr.EndOfStream)
        {
          Match mt = pidRE.Match(sr.ReadLine());
          if (mt != null && mt.Length > 0)
          {
            javaPID = mt.Groups["PID"].Value;
            if(!islocator)
              break;
          }
        }
        sr.Close();
        fs.Close();
      }
      return javaPID;
    }

    private static string GetJavaServerPID(string serverId)
    {
      string startDir = GetJavaStartDir(serverId, Util.SystemType);
      string javaLog = startDir + "/cacheserver.log";
      return GetGFJavaPID(javaLog);
    }

    private static bool KillLocalGFJava(string javaPID, string signal)
    {
      try
      {
        Process.GetProcessById(int.Parse(javaPID)).Kill();
        return true;
      }
      catch (Exception excp)
      {
        LogException("KillLocalGFJava: {0}: {1}", excp.GetType().Name, excp.Message);
        return false;
      }
    }

    private static bool KillLocalGFJava_disabled(string javaPID, string signal)
    {
      bool success = false;
      if (javaPID != null)
      {
        Process javaProc;
        if (!Util.StartProcess("/bin/kill", "-f " + signal + ' ' + javaPID,
          false, TempDir, false, false, false, out javaProc))
        {
          LogException("KillLocalGFJava(): unable to run 'kill'");
        }

        // Wait for java process to stop
        bool stopped = javaProc.WaitForExit(MaxWaitMillis);
        if (!stopped)
        {
          try
          {
            javaProc.Kill();
          }
          catch
          {
          }
          LogSevere("KillLocalGFJava() Could not execute " +
            "kill successfully.");
        }
        int numTries = 30;
        while (numTries-- > 0)
        {
          if (!Util.StartProcess("/bin/kill", "-f -0 " + javaPID,
            false, TempDir, false, false, false, out javaProc))
          {
            LogException("KillLocalGFJava(): unable to run 'kill'");
          }
          stopped = javaProc.WaitForExit(MaxWaitMillis);
          if (stopped && javaProc.ExitCode == 0)
          {
            success = true;
            break;
          }
          Thread.Sleep(10000);
        }
      }
      return success;
    }

    private static void KillLocalJavaServer(string serverId, string signal)
    {
      string javaPID = GetJavaServerPID(serverId);
      LogInfo("KillLocalJavaServer() killing '{0}' with PID '{1}' using " +
        "signal '{2}'", serverId, javaPID, signal);
      if (KillLocalGFJava(javaPID, signal))
      {
        string startDir = GetJavaStartDir(serverId, Util.SystemType);
        File.Delete(startDir + "/.cacheserver.ser");
      }
      else
      {
        LogException("KillLocalJavaServer() Timed out waiting for " +
          "Java cacheserver to stop.");
      }
    }

    #endregion

    #region Public methods loaded by XMLs

    /// <summary>
    /// Will run a process using Cygwin bash
    /// </summary>
    public void DoRunProcess()
    {
      string progName = GetStringValue("program");
      
      //bool driverUsingpsexec = false;
      bool hostIsWindows = false;
      try
      {
        //driverUsingpsexec = (bool)Util.BBGet(string.Empty, "UsePsexec");
        hostIsWindows = (bool)Util.BBGet(string.Empty, Util.HostName + ".IsWindows");
      }
      catch
      {
      }
      
      if (progName == null)
      {
        FwkException("DoRunProcess(): program not specified for task {0}.",
          TaskName);
      }
      string args = GetStringValue("arguments");
      
      if (progName == "cp") // for smpke perf xml
      {
        if (hostIsWindows )//&& driverUsingpsexec)
          progName = "copy";
        
        string[] argStr = args.Split(' ');
        int i = argStr[0].IndexOf("/framework/xml/");
        string gfBaseDir = Util.GetEnvironmentVariable("GFBASE");
        string sharePath = Util.NormalizePath(gfBaseDir);
        string specName = argStr[0].Substring(i);
        string perfStatictic = sharePath + specName;
        args = perfStatictic + ' ' + (string)Util.BBGet(string.Empty,"XMLLOGDIR") + '/' + argStr[1];
      }
      string fullProg = progName + ' ' + args;
      fullProg = fullProg.Trim();

      // Special treatment for java server scripts since they are C++ specific
      // (e.g. the environment variables they require, the FwkBBClient program,
      //  the auto-ssh ...)
      string[] progs = fullProg.Split(';');
      for (int index = 0; index < progs.Length; index++)
      {
        if (m_exiting)
        {
          break;
        }
        string prog = progs[index].Trim();
        int javaIndx;
        if ((javaIndx = prog.IndexOf(SetupJSName)) >= 0)
        {
          args = prog.Substring(javaIndx + SetupJSName.Length).Trim();
          SetupJavaServers(args);
        }
        else if ((javaIndx = prog.IndexOf(StartJSName)) >= 0)
        {
          args = prog.Substring(javaIndx + StartJSName.Length).Trim();
          StartJavaServers(args);
        }
        else if ((javaIndx = prog.IndexOf(StopJSName)) >= 0)
        {
          args = prog.Substring(javaIndx + StopJSName.Length).Trim();
          StopJavaServers(args);
        }
        else if ((javaIndx = prog.IndexOf(KillJSName)) >= 0)
        {
          args = prog.Substring(javaIndx + KillJSName.Length).Trim();
          KillJavaServers(args);
        }
        else
        {
          FwkInfo("DoRunProcess() starting '{0}' using bash", prog);
          
          Process runProc = new Process();
          ProcessStartInfo startInfo; 
          
          if (hostIsWindows)// && driverUsingpsexec)
          {
            prog = prog.Replace('/', '\\');
            startInfo = new ProcessStartInfo("cmd",
               string.Format("/C \"{0}\"", prog));
          }
          else
            startInfo = new ProcessStartInfo("bash",
              string.Format("-c \"{0}\"", prog));
              
          startInfo.CreateNoWindow = true;
          startInfo.UseShellExecute = false;
          startInfo.RedirectStandardOutput = true;
          startInfo.RedirectStandardError = true;
          runProc.StartInfo = startInfo;

          if (!runProc.Start())
          {
            FwkException("DoRunProcess() unable to run '{0}' using bash", prog);
          }
          StreamReader outSr = runProc.StandardOutput;
          StreamReader errSr = runProc.StandardError;
          string outStr = outSr.ReadToEnd();
          string errStr = errSr.ReadToEnd();
          runProc.WaitForExit();
          errSr.Close();
          outSr.Close();

          if (outStr != null && outStr.Length > 0)
          {
            FwkInfo("DoRunProcess() Output from executing '{0}':" +
              "{1}{2}{3}{4}", prog, Environment.NewLine, outStr,
              Environment.NewLine, Util.MarkerString);
          }
          if (errStr != null && errStr.Length > 0)
          {
            FwkSevere("DoRunProcess() Error output from executing '{0}':" +
              "{1}{2}{3}{4}", prog, Environment.NewLine, errStr,
              Environment.NewLine, Util.MarkerString);
          }
          FwkInfo("DoRunProcess() completed '{0}'.", prog);
        }
      }
    }

    public void DoSleep()
    {
      int secs = GetUIntValue(SleepTime);
      if (secs < 1)
      {
        secs = 30;
      }
      FwkInfo("DoSleep() called for task: '{0}', sleeping for {1} seconds.",
        TaskName, secs);
      Thread.Sleep(secs * 1000);
    }

    public void DoRunProcessAndSleep()
    {
      DoRunProcess();
      if (!m_exiting)
      {
        int secs = GetUIntValue(SleepTime);
        if (secs > 0)
        {
          Thread.Sleep(secs * 1000);
        }
      }
    }

    public void DoStopStartServer()
    {
      if (m_firstRun)
      {
        m_firstRun = false;
        Util.BBIncrement(JavaServerBB, JavaServerCountKey);
      }

      string op = GetStringValue("operation");
      if (op == null || op.Length == 0)
      {
        FwkException("DoStopStartServer(): operation not specified.");
      }
      string serverId = GetStringValue("ServerId");
      if (serverId == null || serverId.Length == 0)
      {
        FwkException("DoStopStartServer(): server id not specified.");
      }
      FwkInfo("DoStopStartServer(): stopping and starting server {0}.",
        serverId);

      UnitFnMethod<string> stopDeleg = null;
      string stopArg = null;
      if (op == "stop")
      {
        stopDeleg = StopJavaServers;
        stopArg = serverId;
      }
      else if (op == "term")
      {
        stopDeleg = KillJavaServers;
        stopArg = serverId;
      }
      else if (op == "kill")
      {
        stopDeleg = KillJavaServers;
        stopArg = serverId + " 9";
      }
      else
      {
        FwkException("DoStopStartServer(): unknown operation {0}.", op);
      }

      int secs = GetUIntValue(SleepTime);
      int minServers = GetUIntValue(MinServers);
      minServers = (minServers <= 0) ? 1 : minServers;
      FwkInfo("DoStopStartServer(): using minServers: {0}", minServers);
      bool done = false;
      while (!done)
      {
        int numServers = Util.BBDecrement(JavaServerBB, JavaServerCountKey);
        if (numServers >= minServers)
        {
          stopDeleg(stopArg);
          Thread.Sleep(60000);
          StartJavaServers(serverId);
          Thread.Sleep(60000);
          Util.BBIncrement(JavaServerBB, JavaServerCountKey);
          done = true;
        }
        else
        {
          Util.BBIncrement(JavaServerBB, JavaServerCountKey);
          Thread.Sleep(1000);
        }
      }
      if (secs > 0)
      {
        Thread.Sleep(secs * 1000);
      }
    }

    #endregion

    public static void TestCompleteWithSSL()
    {
      TestComplete(true);
    }

    public static void TestCompleteWithoutSSL()
    {
      TestComplete(false);
    }

    public static void TestComplete(bool ssl)
    {
      m_exiting = true;
      lock (((ICollection)m_javaServerHostMap).SyncRoot)
      {
        if (m_javaServerHostMap.Count == 0)
        {
          try
          {
            m_javaServerHostMap = Util.BBGet(JavaServerBB, JavaServerMapKey)
              as Dictionary<string, HostInfo>;
          }
          catch
          {
          }
        }
        if (m_javaServerHostMap.Count > 0)
        {
          // Stop all the remaining java servers here.
          string gfeDir = Util.GetEnvironmentVariable("GFE_DIR");

          LogAssert(gfeDir != null, "ClientExit() GFE_DIR is not set.");
          LogAssert(gfeDir.Length != 0, "ClientExit() GFE_DIR is not set.");

          Match mt = Regex.Match(gfeDir, "^[^:]+:[0-9]+(,[^:]+:[0-9]+)*$");
          if (mt == null || mt.Length == 0)
          {
            foreach (KeyValuePair<string, HostInfo> serverHostPair
              in m_javaServerHostMap)
            {
              string serverId = serverHostPair.Key;
              string targetHost = serverHostPair.Value.HostName;
              string hostType = serverHostPair.Value.HostType;
              string startDir = GetJavaStartDir(serverId, hostType);
              string javaServerArgs = "stop -dir=" + startDir;
              if (serverHostPair.Value.Started)
              {
                if (targetHost == null || targetHost.Length == 0 ||
                    Util.IsHostMyself(targetHost))
                {
                  LogInfo("ClientExit() stopping {0} with GFE_DIR {1} and " +
                    "arguments: {2}", JavaServerName, gfeDir, javaServerArgs);

                  string outStr;
                  int status = StartLocalGFExe(JavaServerName, gfeDir,
                    javaServerArgs, out outStr);
                  if (status != GFNoError)
                  {
                    if (status == GFTimeout)
                    {
                      LogSevere("ClientExit() Timed out waiting for Java " +
                        "cacheserver to stop. Please check the server log " +
                        "in {0}.", startDir);
                    }
                    KillLocalJavaServer(serverId, "-9");
                  }
                }
                else if (hostType != Util.SystemType)
                {
                  // Stop the remote host
                  LogInfo("ClientExit() stopping '{0}' on remote host " +
                    "'{1}' of type {2}", serverId, targetHost, hostType);
                  LogInfo(StartRemoteGFExe(targetHost, hostType,
                    "cacheserver", javaServerArgs));
                }
              }
            }
          }
          m_javaServerHostMap.Clear();
        }
        // Stop the locator here.
        if (m_locatorHost != null && m_locatorType != null)
        {
          LogInfo("ClientExit() stopping locator on host {0}", m_locatorHost);
          for (int i = 0; i < locCount; i++)
          {
            string locatorDir = GetLocatorStartDir((i + 1).ToString(), Util.SystemType);
            /* if (ssl)
            {
              locatorDir = locatorDir + "/..";
            }
            */
          string locatorPID = GetGFJavaPID(locatorDir +
            PathSep + "locator.log");
          if (locatorPID != null && locatorPID.Length > 0)
          {
            if (m_locatorType != Util.SystemType)
            {
              string killCmd = "kill -15 " + locatorPID;
              LogInfo(Util.RunClientShellTask(Util.ClientId, m_locatorHost,
                killCmd, null));
              Thread.Sleep(3000);
              killCmd = "kill -9 " + locatorPID;
              LogInfo(Util.RunClientShellTask(Util.ClientId, m_locatorHost,
                killCmd, null));
              LogInfo("ClientExit() successfully stopped locator PID {0} on host {1}",
                locatorPID, m_locatorHost);
            }
            else
            {
              if (!KillLocalGFJava(locatorPID, "-15") &&
                  !KillLocalGFJava(locatorPID, "-9"))
              {
                LogSevere("ClientExit() Error while stopping " +
                  "locator. Please check the logs in {0}", locatorDir);
              }
              else
              {
                LogInfo("ClientExit() successfully stopped locator on host {0}",
                  m_locatorHost);
              }
            }
          }
        }
        }
      }
      locCount = 0;
      m_locatorHost = null;
      m_locatorType = null;
      Util.BBRemove(string.Empty, "LOCATOR_ADDRESS");
      Util.BBRemove(string.Empty, "LOCATOR_ADDRESS_POOL");
      m_exiting = false;
    }
  }
}
