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
using System.Net;
using System.Runtime.InteropServices;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Ipc;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading;
using System.Xml;

namespace GemStone.GemFire.Cache.FwkDriver
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;
  using GemStone.GemFire.Cache.FwkLib;

  static class FwkDriver
  {
    #region Private constants

    private const string DriverNodeName = "test-driver";
    private const string TestNodeName = "test";
    private const string TaskNodeName = "task";
    private const string NameAttrib = "name";
    private const string DescAttrib = "description";
    private const string WaitTimeAttrib = "waitTime";
    private const string TimesToRunAttrib = "timesToRun";

    private static Dictionary<string, string> m_sshArgs =
      new Dictionary<string, string>();
    private static Dictionary<string, string> m_psexecArgs =
      new Dictionary<string, string>();
       
    private const string SlaveIPC = "tempSlaveIPC";
    private const string SlaveService = "slave";
    private const int MaxWaitMillis = 120000;
    

    #endregion

    #region Private static members

    private static Dictionary<string, List<string>> m_hostGroupToHostMap;
    private static List<string> m_hosts;
    private static Dictionary<string, bool> m_hostIsWindows = new Dictionary<string, bool>();
    private static Dictionary<string, string> m_envs =
      new Dictionary<string, string>();
    private static string m_sshPath;
    private static bool m_requirePassword;
    private static string m_sshArgsFmt;
    private static bool m_usepsexec = false;
    private static string m_psexecArgsFmt;
    private static string m_psexecPath;
    private static string m_sharePath;
    private static string m_shareName;
    private static string m_fwkOutDir;
    private static string m_fwkOutName;
    private static string m_currentXmlName;
    private static string m_currentXmlDir;
    private static string m_currentTestName;
    private static int m_currentClientNum = 0;
    private static IChannel m_driverChannel;
    private static volatile bool m_exiting = false;

    private static int m_totalTasks;
    private static int m_runTasks;
    private static int m_succeededTasks;
    private static int m_failedTasks;
    private static DateTime now;
    private static string xmlLogDir;
    private static string latestPropFile;
    private static string PropFile;
    private static bool m_poolOpt;
    private static int m_launcherPort = 0;
    
    private static List<string> m_xmlFiles = new List<string>();
    private static List<string> m_clientNames = new List<string>();
    private static List<string> m_xmlLogDir = new List<string>();
    private static Dictionary<string, UnitProcess> m_launcherProcesses =
      new Dictionary<string, UnitProcess>();
    #endregion

    static int Main(string[] args)
    {
      DateTime atTime;
      bool slave;
      now = DateTime.Now;
      int exitStatus = 0;
      
      #region Read the environment variables from run.env and gfcsharp.env

      string[] envFiles = { Util.AssemblyDir + "/run.env", "run.env.extra",
        "gfcsharp.env" };
      foreach (string envFile in envFiles)
      {
        if (File.Exists(envFile))
        {
          using (StreamReader envReader = new StreamReader(envFile))
          {
            while (!envReader.EndOfStream)
            {
              string envLine = envReader.ReadLine();
              if (envLine.Length > 0 && envLine[0] != '#')
              {
                string[] envStr = envLine.Split('=');
                if (envStr != null && envStr.Length == 2)
                {
                  string envVar = envStr[0];
                  string envValue = envStr[1].Replace('\\', '/');
                  envValue = envValue.Trim('"', '\'');
                  m_envs[envVar] = envValue;
                  int lastIndx = envVar.Length - 1;
                  if (envVar[lastIndx] == '+')
                  {
                    envVar = envVar.Substring(0, lastIndx);
                    envValue = envValue + Util.GetEnvironmentVariable(envVar);
                  }
                  Util.SetEnvironmentVariable(envVar, envValue);
                }
              }
            }
            envReader.Close();
          }
        }
      }

      #endregion

      bool autoSsh;
      bool usePlink;
      bool skipReport;
      string bbServer;
      bool bbPasswd;
      int driverPort;
      string windowsUser = "";
      ParseArgs(args, out atTime, out autoSsh, out skipReport, out slave,
        out m_hostGroupToHostMap, out m_hosts, out bbServer, out bbPasswd,
        out driverPort, out usePlink, out windowsUser, out m_launcherPort);
      if (autoSsh)
      {
        m_sshPath = "ssh";
        m_requirePassword = false;
        m_sshArgsFmt = "-o StrictHostKeyChecking=no -l {0}";
      }
      else if (usePlink)
      {
        m_sshPath = "plink";
        m_requirePassword = true;
        m_sshArgsFmt = "-l {0} -pw {1}";
      }
      else
      {
        if (string.IsNullOrEmpty(windowsUser))
        {
          windowsUser = Util.GetEnvironmentVariable("USER");
          string domainName = Util.GetEnvironmentVariable("USERDOMAIN");
          if (!string.IsNullOrEmpty(domainName))
            windowsUser = domainName + "\\" + windowsUser;
        }
        m_psexecPath = "psexec";
        m_requirePassword = true;
        m_psexecArgsFmt = " -u {0} -p {1} ";
        m_sshPath = "ssh";
        m_sshArgsFmt = "-o StrictHostKeyChecking=no -l {0}";
        m_usepsexec = true;
      }
      if (driverPort > 0)
      {
        Util.DriverPort = driverPort;
      }
      Util.DriverComm = new DriverComm();
      UnitProcess.Init();
      if (bbServer != null)
      {
        try
        {
          Util.ExternalBBServer = "tcp://" + bbServer + '/' +
            CommConstants.BBService;
          Util.BBComm = ServerConnection<IBBComm>.Connect(
            Util.ExternalBBServer);
          Util.Log("Using external Blackboard at TCP address {0}.", bbServer);
        }
        catch (Exception ex)
        {
          Console.WriteLine(ex.Message);
          Environment.Exit(1);
        }
      }
      else
      {
        Util.BBComm = new BBComm();
        Util.Log("Starting Blackboard on port {0}.", Util.DriverPort);
      }
      int numHosts;
      
      Util.BBSet(string.Empty, "UsePsexec", m_usepsexec);
      
      if (slave)
      {
        #region Start off the slave if required and wait for the time to arrive

        SlaveComm slaveComm = ServerConnection<SlaveComm>.Connect(
          "ipc://" + SlaveIPC + '/' + SlaveService);
        m_hostGroupToHostMap = slaveComm.GetHostGroupToHostMap();
        m_hosts = slaveComm.GetHosts();
        m_hostIsWindows = slaveComm.GetHostIsWindows();
        m_sshArgs = slaveComm.GetSshArgs();
        numHosts = m_hosts.Count;
        try
        {
          slaveComm.Exit();
        }
        catch (Exception)
        {
          // Ignore any exception in communication while the driver exits
        }

        TimeSpan sleep = atTime - now;
        if (sleep > TimeSpan.Zero)
        {
          Thread.Sleep(sleep);
        }

        #endregion
      }
      else
      {
        #region Check the hosts from the command-line

        Util.LogOnConsole = true;
        numHosts = m_hosts.Count;
        string userName = Util.GetEnvironmentVariable("USER");
        if (numHosts > 0)
        {
          List<string> removeHosts = new List<string>();
          if (bbPasswd)
          {
            string passwd = string.Empty;
            foreach (string hostName in m_hosts)
            {
              try
              {
                m_hostIsWindows[hostName] = bool.Parse(Util.BBGet("CONFIG",
                   "hostType." + hostName) as string);
                if (m_usepsexec && m_hostIsWindows[hostName])
                {
                  passwd = Util.BBGet("CONFIG", "host." + hostName) as string;
                  m_psexecArgs[hostName] = string.Format(m_psexecArgsFmt,
                    userName, passwd);
                }
                else if (m_requirePassword)
                {
                  passwd = Util.BBGet("CONFIG", "host." + hostName) as string;
                  m_sshArgs[hostName] = string.Format(m_sshArgsFmt,
                  userName, passwd);
                    
                }
                else
                {
                  m_sshArgs[hostName] = string.Format(m_sshArgsFmt, userName);
                }
                
              }
              catch
              {
                removeHosts.Add(hostName);
              }
            }
          }
          else
          {
            string passwd = string.Empty;
            int defaultMaxTries = 1;
            if (m_requirePassword)
            {
              defaultMaxTries = 3;
              Console.Write("Please enter the password: ");
              passwd = ConsoleControl.GetPassword();
            }
            
            string psexecArgs = null;
            if (m_usepsexec)
              psexecArgs = string.Format(m_psexecArgsFmt, windowsUser, passwd);
              
            // Check if login works for the given hosts.
            Process sshProc;
            foreach (string hostName in m_hosts)
            {
              int maxTries = defaultMaxTries;
              string hostType = string.Empty;
              if (m_usepsexec && m_hostIsWindows[hostName])
              {
                maxTries = 0;
                hostType = "1";
                m_psexecArgs[hostName] = psexecArgs;
              }
              while (maxTries-- > 0)
              {
                Util.Log("Checking login on {0}", hostName);
                string sshArgs = string.Format(m_sshArgsFmt, userName, passwd);
                Util.StartProcess(m_sshPath, sshArgs + ' ' + hostName +
                  " \"uname\"", false, null, true, false, false, out sshProc);
                StreamReader outSr = sshProc.StandardOutput;
                bool status = sshProc.WaitForExit(MaxWaitMillis);
                if (!status)
                {
                  Console.WriteLine("Timed out waiting for {0} on server " +
                    "{1}; removing from hosts list.", m_sshPath, hostName);
                  removeHosts.Add(hostName);
                  break;
                }
                else
                {
                  hostType = Util.GetHostType(outSr.ReadToEnd());
                  outSr.Close();
                  if (hostType.Length > 0)
                  {
                    m_sshArgs[hostName] = sshArgs;
                    m_hostIsWindows[hostName] = hostType.Equals("WIN");
                    break;
                  }
                  else
                  {
                    if (m_requirePassword)
                    {
                      Console.Write("Incorrect password for server {0}. " +
                        "Password: ", hostName);
                      Thread.Sleep(3000);
                      passwd = ConsoleControl.GetPassword();
                    }
                  }
                }
              }
              if (hostType.Length == 0)
              {
                Console.WriteLine("Failed login to server {0}; removing " +
                  "from hosts list", hostName);
                removeHosts.Add(hostName);
              }
            }

          }
          foreach (string hostName in removeHosts)
          {
            m_hosts.Remove(hostName);
            foreach (List<string> hostList in m_hostGroupToHostMap.Values)
            {
              hostList.Remove(hostName);
            }
          }
          numHosts = m_hosts.Count;
        }
        
        #endregion

        #region Start off the client and delegate the task to it if the command is to be executed later

        if (atTime != DateTime.MinValue && atTime > now)
        {
          Util.Log("Will run the tests at: {0}", atTime);
          BinaryServerFormatterSinkProvider serverProvider =
            new BinaryServerFormatterSinkProvider();
          serverProvider.TypeFilterLevel = TypeFilterLevel.Full;
          BinaryClientFormatterSinkProvider clientProvider =
            new BinaryClientFormatterSinkProvider();
          Dictionary<string, string> properties = new Dictionary<string, string>();
          properties["portName"] = SlaveIPC;
          m_driverChannel = new IpcChannel(properties, clientProvider, serverProvider);
          ChannelServices.RegisterChannel(m_driverChannel, false);
          RemotingConfiguration.RegisterWellKnownServiceType(typeof(SlaveComm),
            SlaveService, WellKnownObjectMode.SingleCall);

          Process slaveProc;
          string xmlFileStr = string.Empty;
          foreach (string xmlFile in m_xmlFiles)
          {
            xmlFileStr += " --xml=\"" + xmlFile + '"';
          }
          Util.StartProcess(Util.ProcessName, "--slave --at=\"" + atTime +
            '"' + xmlFileStr, false, null, false, false, false, out slaveProc);

          // Just wait for slave since the callback from slave will exit this
          slaveProc.WaitForExit();
          return 0;
        }

        #endregion
      }

      #region Set the environment variables for the driver and clients

      now = DateTime.Now;

      string gfBaseDir = Util.GetEnvironmentVariable("GFBASE");
      if (gfBaseDir != null && gfBaseDir.Length > 0)
      {
        m_sharePath = Util.NormalizePath(gfBaseDir);
        if (!Directory.Exists(m_sharePath))
        {
          FatalError("Cannot find GFBASE path: {0}", m_sharePath);
        }
        string nowStr = "FwkCS_" + now.ToString("yy-MM-dd_HH-mm-ss");
        string fwkOutBase = Util.GetEnvironmentVariable("FWK_LOGDIR");
        if (fwkOutBase != null && fwkOutBase.Length > 0)
        {
          fwkOutBase = Util.NormalizePath(fwkOutBase);
        }
        else
        {
          fwkOutBase = Util.NormalizePath(Environment.CurrentDirectory);
        }
        m_fwkOutDir = fwkOutBase + '/' + nowStr;
        if (Directory.Exists(m_fwkOutDir))
        {
          Directory.Delete(m_fwkOutDir, true);
        }
        Directory.CreateDirectory(m_fwkOutDir);

        // TODO: Set the log-level depending on the command-line arguments
        Util.CurrentLogLevel = Util.LogLevel.Info;
        Util.LogFile = m_fwkOutDir + "/Driver.log";

        string latestOutDir = fwkOutBase + "/latest";
        Process lnProc;
        Util.StartProcess("sh", "-c 'rm -rf \"" + latestOutDir +
          "\" && ln -sf " + nowStr + " \"" + latestOutDir + "\"'",
          false, null, false, false, false, out lnProc);

        m_shareName = m_sharePath;
        if (numHosts > 0)
        {
          #region Make the directory as shared to enable sharing with clients

          m_shareName = Util.ShareDirectory(m_sharePath, "FwkShare_",
            "Framework share for access across hosts");
          if (m_shareName == null)
          {
            FatalError("Cannot create or find a share for: {0}",
              m_sharePath);
          }
          Util.BBSet("SharedPath", "sharedDir", m_shareName);
          Util.Log("Using share path {0} for framework.", m_shareName);

          #endregion

          #region Share the GFE_DIR directory

          string gfeDir = m_envs["GFE_DIR"];
          if (gfeDir != null && gfeDir.Length > 0)
          {
            string javaShareName = Util.ShareDirectory(
              Path.GetDirectoryName(gfeDir), "FwkShareJava_",
              "Framework Java server share for access across hosts");
            if (javaShareName != null)
            {
              javaShareName = javaShareName + '/' + Path.GetFileName(gfeDir);
              m_envs["GFE_DIR"] = javaShareName;
              Util.Log("Using share path {0} for java server.", javaShareName);

              #region Replace all occurances of GFE_DIR by its share name

              Dictionary<string, string> newEnvs =
                new Dictionary<string, string>();
              foreach (KeyValuePair<string, string> envPair in m_envs)
              {
                newEnvs[envPair.Key] = envPair.Value.Replace(gfeDir,
                  javaShareName);
              }
              m_envs = newEnvs;

              #endregion
            }
          }

          #endregion
        }

        #region Copy data files (e.g. XMLs, keystore files etc.)

        Directory.CreateDirectory(m_fwkOutDir + "/data");
        Util.CopyFiles(m_sharePath + "/framework/data", "*",
          m_fwkOutDir + "/data", true);
        Util.CopyFiles(m_sharePath + "/framework", "rcsinfo",
          m_fwkOutDir, false);
        Util.CopyFiles(m_sharePath + "/framework/csharp/bin", "*.sh",
          m_fwkOutDir + "/data", false);

        #endregion

        if (!fwkOutBase.StartsWith("//"))
        {
          if (m_fwkOutDir.StartsWith(m_sharePath))
          {
            m_fwkOutName = m_fwkOutDir.Replace(m_sharePath, m_shareName);
          }
          else
          {
            m_fwkOutName = Util.ShareDirectory(fwkOutBase, "FwkOut_",
              "Framework output share") + '/' + nowStr;
          }
        }
        else
        {
          m_fwkOutName = m_fwkOutDir;
        }
        Util.Log("Using share path {0} for output logs.", m_fwkOutName);
      }
      Util.Log("Started Driver service on port {0}.", Util.DriverPort);

      #endregion

      if (driverPort <= 0)
      {
        // Catch the console events
        ConsoleControl.CtrlCleanup(new ControlEventHandler(EndClients));
      }
      // Register the delegate that shall be invoked to terminate the driver
      DriverComm.TermDeleg = new UnitFnMethod(TerminateDriver);

      // Register the delegate which shall be used to process client request
      // to run a task on another host.
      DriverComm.WinTaskDeleg = RunClientWinTask;
      DriverComm.ShellTaskDeleg = RunClientShellTask;

      // Increase the threadpool size to large size required for large number of clients.
      if (!ThreadPool.SetMaxThreads(512, 1000))
      {
        int defMaxWorkers, defMaxIO;
        ThreadPool.GetMaxThreads(out defMaxWorkers, out defMaxIO);
        Util.Log(Util.LogLevel.Error,
          "Could not set the number of threads in ThreadPool to 512. " +
          "Using default of {0} worker threads -- expect problems with " +
          "tests having large (> 20) number of clients.", defMaxWorkers);
      }
      Util.Log("Commandline: {0}", Environment.CommandLine);
      Util.Log("Starting the tests.");
      foreach (string xmlFile in m_xmlFiles)
      {
        if (m_exiting)
        {
          break;
        }
        try
        {
          XmlNode driverNode = XmlNodeReaderWriter.GetInstance(xmlFile).GetNode(
            '/' + DriverNodeName);
          int i = xmlFile.IndexOf("/framework/xml/");
          string xmlfilename = m_shareName + xmlFile.Substring(i);
          Util.BBSet(string.Empty, "XMLFILE", xmlfilename);
          if (driverNode == null)
          {
            throw new IllegalArgException(string.Format(
              "Could not find top-level '{0}' node in xml file '{1}'",
              DriverNodeName, xmlFile));
          }
          now = DateTime.Now;
          m_currentXmlName = Path.GetFileName(xmlFile);
          m_currentXmlDir = GetXMLLogDir(Path.GetDirectoryName(xmlFile));
          xmlLogDir = GetBaseLogDir();
          m_xmlLogDir.Add(GetXMLLogPart());
          latestPropFile = xmlLogDir + "/latest.prop";
          // Driver.log should be in every test directory for perf generation via hydra perffrwk
          Util.LogFile = xmlLogDir + "/Driver.log";
          Util.Log("Loading XML test file {0}", xmlFile);
          PropFile = xmlLogDir + "/" + Path.GetFileNameWithoutExtension(m_currentXmlName) + ".prop";
          Util.BBSet(string.Empty, "XMLDIRPATH", m_currentXmlDir);
          Util.BBSet(string.Empty ,"XMLLOGDIR", xmlLogDir);
          Util.BBSet("SharedPath", "sharedDir", m_shareName);
          Util.Log("Using share path {0} for framework.", m_shareName);
          if (!Directory.Exists(xmlLogDir))
          {
            Directory.CreateDirectory(xmlLogDir);
          }
          Util.Log("Current directory is: {0}", Directory.GetCurrentDirectory());
          if (File.Exists("gfcsharp.properties"))
          {
            Util.Log("Copying gfcsharp.properties as gfcpp.properties " +
              "to {0}", xmlLogDir);
            File.Copy("gfcsharp.properties", xmlLogDir + "/gfcpp.properties");
          }
          else if (File.Exists("gfcpp.properties"))
          {
            Util.Log("Copying gfcpp.properties to {0}", xmlLogDir);
            File.Copy("gfcpp.properties", xmlLogDir + "/gfcpp.properties");
          }
          Util.Log("Copying XML to {0}", xmlLogDir);
          File.Copy(xmlFile, xmlLogDir + '/' + m_currentXmlName);

          #region Load the global data

          Dictionary<string, FwkData> globalData =
            FwkData.ReadDataNodes(driverNode);
          if (globalData != null)
          {
            foreach (KeyValuePair<string, FwkData> dataPair in globalData)
            {
              Util.BBSet(string.Empty, dataPair.Key, dataPair.Value);
            }
          }

          #endregion

          #region Get the clients from XML and create them
          m_clientNames = FwkClient.ReadClientNames(driverNode, true);
          CreateClients();

          #endregion

          RunTests(driverNode);
          Util.BBComm.Clear();
          FwkTask.GlobalTaskNames.Clear();
        }
        catch (Exception ex)
        {
          if (!m_exiting)
          {
            LogError("FATAL Exception [{0}] caught: {1}",
              ex.GetType(), ex.Message);
          }
          exitStatus = 1;
        }
        finally
        {
          XmlNodeReaderWriter.RemoveInstance(xmlFile);
          EndClients();
        }
      }
      EndLaunchers();
      if (!skipReport)
      {
        GenerateTestReport();
      }
      Util.DeleteShareName(Path.GetFileName(m_shareName));
      return exitStatus;
    }
    public static void latestprop(string currentTestDescription)
    {
      if (Directory.Exists(xmlLogDir))
      {
        string hosts = null;
        foreach (string host in m_hosts)
        {
          hosts += host + " ";
        }
        StreamWriter sw = new StreamWriter(latestPropFile);
        StreamWriter propfile = new StreamWriter(PropFile);
        propfile.WriteLine("perffmwk.comparisonKey={0}", Path.GetFileNameWithoutExtension(m_currentXmlName));
        propfile.Close();
        sw.WriteLine("nativeClient.TestType=C#");
        sw.WriteLine("nativeClient.Hosts={0}", hosts);
        //Set PERFTEST=true if running smoketest/perf tests
        string perftest = Util.GetEnvironmentVariable("PERFTEST");
        string testname = null;
        if (perftest != null && perftest.Length > 0)
        {
          testname = m_currentXmlDir + "/" + Path.GetFileNameWithoutExtension(m_currentXmlName) + ".conf";
        }
        else
        {
          testname = m_currentXmlDir + "/" + m_currentXmlName;
        }
        sw.WriteLine("TestName={0}", testname);
        sw.WriteLine("nativeClient.version={0}", Util.GetEnvironmentVariable("NATIVECLIENTVERSION"));
        sw.WriteLine("nativeClient.revision={0}", Util.GetEnvironmentVariable("GEMFIRE_SOURCE_REVISION"));
        sw.WriteLine("nativeClient.repository={0}", Util.GetEnvironmentVariable("GEMFIRE_SOURCE_REPOSITORY"));
        sw.WriteLine("hydra.Prms-testDescription={0}", currentTestDescription);
        sw.Close();
      }
    }
    public static void FatalError(string message)
    {
      LogError("Error: " + message);
      Environment.Exit(1);
    }

    public static void FatalError(string fmt, params object[] args)
    {
      LogError("Error: " + fmt, args);
      Environment.Exit(1);
    }

    private static void LogError(string message)
    {
      Util.Log(Util.LogLevel.Error, message);
    }

    private static void LogError(string fmt, params object[] args)
    {
      Util.Log(Util.LogLevel.Error, fmt, args);
    }

    private static string GetFullXMLPath(string xmlFile, string xmlDir)
    {
      if (!File.Exists(xmlFile))
      {
        if (xmlDir != null)
        {
          xmlFile = xmlDir + '/' + xmlFile;
        }
      }
      if (!File.Exists(xmlFile))
      {
        Console.WriteLine("FATAL: Could not find XML file '{0}'", xmlFile);
        ShowUsage();
      }
      return xmlFile;
    }

    private static void ParseArgs(string[] args, out DateTime atTime,
      out bool autoSsh, out bool skipReport, out bool slave,
      out Dictionary<string, List<string>> hostGroupToHostMap,
      out List<string> hosts, out string bbServer, out bool bbPasswd,
      out int driverPort, out bool usePlink, out string userName,
      out int launcherPort)
    {
      int argLen = 0;
      if (args == null || (argLen = args.Length) == 0)
      {
        ShowUsage();
      }
      string XMLOption = "--xml=";
      string XMLListOption = "--list=";
      string AtOption = "--at=";
      string AutoSshOption = "--auto-ssh";
      string PlinkOption= "--plink";
      string SkipReportOption = "--skip-report";
      string BBServerOption = "--bbServer=";
      string BBPasswdOption = "--bbPasswd";
      string DriverPortOption = "--driverPort=";
      string SlaveOption = "--slave";
      string DatabaseOption = "--database";
      string PoolOption = "--pool=";
      string PoolInMultiuserMode = "--poolinmultiusermode=";
      string WindowsUser = "--windowsuser=";
      string WindowsDomain = "--windowsdomain=";
      string PortOfRunningLauncher= "--portOfRunningLaunchers=";
      
      List<string> xmlFileList = new List<string>();
      atTime = DateTime.MinValue;
      autoSsh = false;
      skipReport = false;
      bbServer = null;
      userName = "";
      bbPasswd = false;
      slave = false;
      usePlink = false;
      driverPort = 0;
      launcherPort = 0;
      
      string xmlDir = Util.GetEnvironmentVariable("GFXMLBASE");

      if (xmlDir == null || xmlDir.Length == 0)
      {
        xmlDir = Util.GetEnvironmentVariable("GFBASE") + "/framework/xml";
      }
      int argIndx = 0;
     // m_poolOpt = true;
      while (argIndx <= (argLen - 1) && args[argIndx].StartsWith("--"))
      {
        string arg = args[argIndx];
        if (arg.StartsWith(XMLOption))
        {
          m_xmlFiles.Add(GetFullXMLPath(arg.Substring(XMLOption.Length), xmlDir));
        }
        else if (arg.StartsWith(XMLListOption))
        {
          xmlFileList.Add(arg.Substring(XMLListOption.Length));
        }
        else if (arg.StartsWith(AtOption))
        {
          atTime = DateTime.Parse(arg.Substring(AtOption.Length));
        }
        else if (arg.Equals(AutoSshOption))
        {
          if (usePlink)
          {
            Console.WriteLine("Options {0} and {1} cannot be specified together.", AutoSshOption, PlinkOption);
            ShowUsage();
          }
          autoSsh = true;
        }
        else if (arg.Equals(PlinkOption))
        {
          if (autoSsh)
          {
            Console.WriteLine("Options {0} and {1} cannot be specified together.", AutoSshOption, PlinkOption);
            ShowUsage();
          }
          usePlink = true;
        }
        else if (arg.Equals(SkipReportOption))
        {
          skipReport = true;
        }
        else if (arg.StartsWith(BBServerOption))
        {
          bbServer = arg.Substring(BBServerOption.Length);
        }
        else if (arg.StartsWith(PoolOption))
        {
          if(arg.Substring(PoolOption.Length) == "poolwithlocator" || arg.Substring(PoolOption.Length) == "poolwithendpoints") 
          {
            m_poolOpt = true;
            m_envs["POOLOPT"] = arg.Substring(PoolOption.Length);
          } else {
            Console.WriteLine("Unknown option: {0}", arg.Substring(PoolOption.Length));
            ShowUsage();
          }
        }
        else if (arg.StartsWith(PoolInMultiuserMode))
        {
          if (arg.Substring(PoolInMultiuserMode.Length) == "true" || arg.Substring(PoolInMultiuserMode.Length) == "false")
          {
            m_envs["POOLINMULTIUSERMODE"] = arg.Substring(PoolInMultiuserMode.Length);
          }
          else
          {
            Console.WriteLine("Unknown option: {0}", arg.Substring(PoolOption.Length));
            ShowUsage();
          }
        }
        else if (arg.Equals(BBPasswdOption))
        {
          if (bbServer == null)
          {
            Console.WriteLine(
              "bbServer option should be specified before bbPasswd.");
            ShowUsage();
          }
          bbPasswd = true;
        }
        else if (arg.StartsWith(DriverPortOption))
        {
          driverPort = int.Parse(arg.Substring(DriverPortOption.Length));
        }
        else if (arg.Equals(SlaveOption))
        {
          slave = true;
        }
        else if (arg.Equals(DatabaseOption))
        {
          Util.SetEnvironmentVariable("DBOPTION", "true");
        }
        else if (arg.StartsWith(WindowsUser))
        {
          userName = userName + arg.Substring(WindowsUser.Length);
        }
        else if (arg.StartsWith(WindowsDomain))
        {
          userName = arg.Substring(WindowsDomain.Length) + "\\" + userName;
        }
        else if (arg.StartsWith(PortOfRunningLauncher))
        {
          string port = arg.Substring(PortOfRunningLauncher.Length);
          try
          {
            launcherPort = int.Parse(port);
          }
          catch
          {
            Console.WriteLine("Port number should be an integer: {0}", port);
            ShowUsage();
          }
        }
        else
        {
          Console.WriteLine("Unknown option: {0}", arg);
          ShowUsage();
        }
        argIndx++;
      }
      foreach (string xmlList in xmlFileList)
      {
        try
        {
          using (StreamReader sr = new StreamReader(xmlList))
          {
            while (!sr.EndOfStream)
            {
              string xmlName = System.Text.RegularExpressions.Regex.Replace(
                sr.ReadLine(), "#.*$", "").Trim();
              if (xmlName.Length > 0)
              {
                m_xmlFiles.Add(GetFullXMLPath(xmlName, xmlDir));
              }
            }
            sr.Close();
          }
        }
        catch (IOException ex)
        {
          Console.WriteLine(ex.Message);
          ShowUsage();
        }
      }
      if (m_xmlFiles.Count == 0)
      {
        Console.WriteLine("No XML files provided.");
        ShowUsage();
      }
      if ((autoSsh || usePlink) && launcherPort != 0)
      {
        Console.WriteLine("Option {0} can be provided only when using launcher to launch clients", PortOfRunningLauncher);
        ShowUsage();
      }
      int argIndxToIterate = argIndx;
     
      
      hostGroupToHostMap = new Dictionary<string, List<string>>();
      hosts = new List<string>();
      while (argIndx < argLen)
      {
        string host = args[argIndx++];
        
        string hostType;
        int hostTypeIndex;
        bool hostIsWindows = true;
        if ((hostTypeIndex = host.IndexOf(':')) >= 0)
        {
          hostType = host.Substring(0, hostTypeIndex);
          if (hostType == "LIN")
          {
            host = host.Substring(hostTypeIndex + 1);
            hostIsWindows = false;
          }
        }
        
        string hostGroup;
        int hostGroupIndex;
        if ((hostGroupIndex = host.IndexOf(':')) >= 0)
        {
          hostGroup = host.Substring(0, hostGroupIndex);
          if (hostGroup == "*")
          {
            Console.WriteLine("Cannot use '*' as the hostGroup");
            ShowUsage();
          }
          host = host.Substring(hostGroupIndex + 1);
        }
        else
        {
          hostGroup = string.Empty;
        }
        if (!hosts.Contains(host))
        {
          hosts.Add(host);
        }
        
        if (!m_hostIsWindows.ContainsKey(host))
          m_hostIsWindows[host] = hostIsWindows;
          
        List<string> hostList;
        if (hostGroupToHostMap.TryGetValue(hostGroup, out hostList))
        {
          if (!hostList.Contains(host))
          {
            hostList.Add(host);
          }
        }
        else
        {
          hostList = new List<string>();
          hostList.Add(host);
          hostGroupToHostMap.Add(hostGroup, hostList);
        }
      }
      hostGroupToHostMap.Add("*", hosts);
    }

    private static void ShowUsage()
    {
      string procName = Util.ProcessName;
      Console.WriteLine("Usage: " + procName + " [OPTION] [<host1> ...]");
      Console.WriteLine(" By default, FwkLauncher is used to launch clients on the hosts. FwkLauncher is started on the remote host using psexec.");
      Console.WriteLine(" <host1> ... are (optional) hostnames of the machines on which to distribute the clients");
      Console.WriteLine(" When using FwkLauncher, hosts that are non-windows should have 'LIN:' prefixed to them.");
      Console.WriteLine(" Each host can optionally have the hostGroup name prefixed to it with ':'");
      Console.WriteLine("   e.g. LIN:CS:host1 CS:host2; the hostGroup should not be '*' which implicitly stands for all hosts");
      Console.WriteLine("Options are:");
      Console.WriteLine("  --auto-ssh \t\t Specify this option to use ssh, instead of FwkLauncher, configured for passwordless login to launch clients");
      Console.WriteLine("  --plink \t\t Specify this option to use plink, instead of FwkLauncher, to launch clients");
      Console.WriteLine("  --portOfRunningLaunchers=PORT \t\t Specify this option if FwkLauncher is already running on all the remote hosts on the same port specified here. ");
      Console.WriteLine("  --windowsuser=USER \t\t Optional. Windows user that is used by psexec to launch FwkLauncher on the remote host.");
      Console.WriteLine("  --windowsdomain=DOMAIN \t\t Optional. Domain of the windows user that is used by psexec to launch FwkLauncher on the remote host."); 
      Console.WriteLine("  --xml=FILE \t\t An XML file having the test to be executed.");
      Console.WriteLine("\t\t\t This option can be specified multiple times");
      Console.WriteLine("  --list=FILE \t\t A file containing a list XML files having the tests to be executed");
      Console.WriteLine("  --at=TIME \t\t The time at which to execute the regression test");
      Console.WriteLine("  --pool=POOLOPTION \t\t where POOLOPTIONs are poolwithendpoints/poolwithlocator");
      Console.WriteLine("\t\t\t Note that this shall only work if sshd is running as a domain user having");
      Console.WriteLine("\t\t\t permissions to access network shares and other resources on given hosts.");
      Console.WriteLine("  --skip-report \t Skip error.report generation");
      Console.WriteLine("  --bbServer=HOST:PORT \t The <host>:<port> to use for connecting to external TCP BlackBoard server.");
      Console.WriteLine("\t\t\t This is to be used when an external BBserver is being used instead of the internal one.");
      Console.WriteLine("  --bbPasswd \t\t Use the external BBserver to get the passwords.");
      Console.WriteLine("  --driverPort=PORT \t The port to use for running the Driver service.");
      Console.WriteLine("  --slave \t\t Indicate that this is a slave process to run in background (INTERNAL USE ONLY)");
      Console.WriteLine("  --database \t\t This to be used for upload the data in database for regression history. Need to be mentioned at every regression run.");
      Console.WriteLine("At least one XML file should be specified using one of --xml or --list options");
      Console.WriteLine("Both options can be provided in which case all the specified tests in both will be run.");
      Environment.Exit(1);
    }

    private static void CreateClients()
    {
      int numHosts = m_hosts.Count;
      
      foreach (string hostname in m_hosts)
        Util.BBSet(string.Empty, hostname + ".IsWindows", m_hostIsWindows[hostname]);
        
      lock (((ICollection)FwkClient.GlobalHostGroups).SyncRoot)
      {
        if (m_clientNames.Count == 0)
        {
          if (numHosts == 0)
          {
            m_clientNames.Add("Client_1");
            FwkClient.GlobalHostGroups.Add("Client_1", "*");
          }
          else
          {
            int index = 1;
            foreach (string host in m_hosts)
            {
              if (m_hostIsWindows[host])
              {
                m_clientNames.Add("Client_" + index);
                FwkClient.GlobalHostGroups.Add("Client_" + index, "*");
                index++;
              }
            }
          }
        }
      }

      lock (((ICollection)FwkClient.GlobalClients).SyncRoot)
      {
        if (numHosts > 0)
        {
          // Group clients by hostname
          List<string> removedHosts = new List<string>();
          bool clientsFailed;
          do
          {
            clientsFailed = false;
            Dictionary<string, List<string>> hostClientMap =
              new Dictionary<string, List<string>>();
            foreach (KeyValuePair<string, List<string>> hostGroupPair in
              m_hostGroupToHostMap)
            {
              string hostGroup = hostGroupPair.Key;
              List<string> hostList = hostGroupPair.Value;
              foreach (string removedHost in removedHosts)
              {
                hostList.Remove(removedHost);
              }
              if (hostList.Count == 0)
              {
                throw new ApplicationException("No hosts available to create "
                  + "the clients in hostGroup '" + hostGroup + "'.");
              }
              int hostIndex = 0;
              List<string> hostListCopy = new List<string>(hostList);
              foreach (string clientName in m_clientNames)
              {
                string clientGroup;
                string hostName;
                if (FwkClient.GlobalHostGroups.TryGetValue(clientName,
                    out clientGroup) && (clientGroup == hostGroup))
                {
                  while (hostListCopy.Count > 0)
                  {
                    hostIndex = hostIndex % hostListCopy.Count;
                    hostName = hostListCopy[hostIndex];
                    if (m_hostIsWindows[hostName])
                    {
                      List<string> currentClientList;
                      if (hostClientMap.TryGetValue(hostName,
                        out currentClientList))
                      {
                        currentClientList.Add(clientName);
                      }
                      else
                      {
                        currentClientList = new List<string>();
                        currentClientList.Add(clientName);
                        hostClientMap.Add(hostName, currentClientList);
                      }
                      hostIndex++;
                      break;
                    }
                    else
                    {
                      hostListCopy.RemoveAt(hostIndex);
                    }
                  }
                  if (hostListCopy.Count == 0)
                  {
                    throw new ApplicationException("No hosts available to "
                      + "create the client [" + clientName +
                      "] in hostGroup [" + clientGroup + "].");
                  }
                  clientGroup = null;
                }
              }
              Util.BBSet(FwkReadData.HostGroupKey, hostGroup, hostList);
            }
            foreach (KeyValuePair<string, List<string>> hostClientPair in
              hostClientMap)
            {
              if (m_exiting)
              {
                return;
              }
              string hostName = hostClientPair.Key;
              List<string> clientNames = hostClientPair.Value;
              if (clientNames.Count > 0)
              {
                string clientStr = string.Empty;
                foreach (string clientName in clientNames)
                {
                  clientStr += (clientName + ' ');
                }
                clientStr = clientStr.Trim();
                Util.Log("Creating new clients: {0}; on host {1}.",
                  clientStr, hostName);
                List<UnitProcess> newClients;
                try
                {
                    if (m_usepsexec && m_hostIsWindows[hostName])
					          newClients = UnitProcess.CreateClientsUsingLauncher(clientNames,
                      m_currentClientNum, m_psexecPath, m_psexecArgs[hostName],
                      hostName, m_sharePath, m_shareName, GetBaseLogDir(), m_envs,
                      m_launcherPort, m_launcherProcesses, m_fwkOutDir);
                  else 
				            newClients = UnitProcess.Create(clientNames,
                      m_currentClientNum, m_sshPath, m_sshArgs[hostName],
					            hostName, m_sharePath, m_shareName, GetBaseLogDir(), m_envs);
                }
                catch (Exception ex)
                {
                  Util.Log("Failed to create clients on host {0}: {1}", hostName, ex);
                  Util.Log("Removing the host and trying again " +
                    "with remaining hosts.");
                  clientsFailed = true;
                  foreach (UnitProcess client in FwkClient.GlobalClients.Values)
                  {
                    Util.Log("Ending client {0}.", client.ID);
                    client.Dispose();
                    Util.Log("\t...client {0} ended.", client.ID);
                  }
                  removedHosts.Add(hostName);
                  break;
                }
                Util.Log("Created new clients: {0}; on host {1}.",
                  clientStr, hostName);
                foreach (UnitProcess newClient in newClients)
                {
                  FwkClient.GlobalClients[newClient.ID] = newClient;

                  // Set the values for the hostGroups in the BB
                  Util.BBSet(newClient.ID, FwkReadData.HostGroupKey,
                    FwkClient.GlobalHostGroups[newClient.ID]);
                }
                m_currentClientNum += clientNames.Count;
                clientNames.Clear();
              }
            }
          } while (clientsFailed);
          // Check that all clients were created successfully.
          foreach (string clientName in m_clientNames)
          {
            if (!FwkClient.GlobalClients.ContainsKey(clientName))
            {
              throw new ApplicationException("Could not create client '"
                + clientName + "'.");
            }
          }
        }
        else
        {
          ClientBase newClient;
          foreach (string clientName in m_clientNames)
          {
            if (m_exiting)
            {
              return;
            }
            newClient = new UnitProcess(clientName, GetBaseLogDir());
            Util.Log("Created a new client {0} on localhost.", clientName);
            FwkClient.GlobalClients[clientName] = newClient;
          }
        }
      }
    }
    
    private static void EndLaunchers()
    {
      if (m_usepsexec && m_launcherPort == 0)
      {
        Util.Log("Cleaning up the launchers...");
        foreach (KeyValuePair<string, UnitProcess> launcherProcessPair in m_launcherProcesses)
        {
          Util.Log("Ending launcher on host {0}", launcherProcessPair.Key);
          try
          {
            launcherProcessPair.Value.Dispose();
            Util.Log("Ended launcher on host {0}", launcherProcessPair.Key);
          }
          catch
          {
          }
        }
        // Clear this because this function can be called multiple times. 
        m_launcherProcesses.Clear();
      }
    }
    
    private static void EndClients()
    {
      lock (((ICollection)FwkClient.GlobalClients).SyncRoot)
      {
        if (FwkClient.GlobalClients.Count > 0)
        {
          Util.Log("Cleaning up the clients ...");
          foreach (ClientBase client in FwkClient.GlobalClients.Values)
          {
            Util.Log("Ending client {0}.", client.ID);
            try
            {
              client.Dispose();
              Util.Log("\t...client {0} ended.", client.ID);
            }
            catch
            {
            }
          }
          FwkClient.GlobalClients.Clear();
          m_clientNames.Clear();
          FwkClient.GlobalHostGroups.Clear();
          Util.Log("--------------------------------------------------" +
            "-------------------------{0}", Environment.NewLine);
        }
      }
    }

    private static void GenerateTestReport()
    {
      if (m_fwkOutDir != null && m_fwkOutDir.Length > 0 &&
        m_xmlLogDir.Count > 0)
      {
        Util.Log("Generating tests summary...");
        string xmlFilesStr = string.Empty;
        foreach (string xmlFile in m_xmlLogDir)
        {
          xmlFilesStr += " '" + xmlFile + '\'';
        }
        DateTime now = DateTime.Now;
        Process genReportProc;
        if (Util.StartProcess("bash", Util.AssemblyDir + '/' +
          "genCSFwkReport.sh '" + m_fwkOutDir + '\'' +  " " + now.ToString("yy-MM-dd_HH-mm-ss") + 
          " " + (m_poolOpt == true ? m_envs["POOLOPT"] : "0") + " " + xmlFilesStr, false,
          null, false, false, false, out genReportProc))  
        {
          genReportProc.WaitForExit();
        }
      }
    }

    private static void EndClients(ConsoleEvent ev)
    {
      m_exiting = true;
      Util.Log("INTERRUPT:: received interrupt event {0}. Terminating...", ev);
      EndClients();
      EndLaunchers();
    }

    private static void TerminateDriver()
    {
      EndClients(ConsoleEvent.CTRL_CLOSE);
    }

    private static bool WaitParallelTasks(List<IAsyncResult> taskResults,
      UnitFnMethodR<bool, FwkTask, string> taskDeleg)
    {
      bool success = true;
      foreach (IAsyncResult taskResult in taskResults)
      {
        taskResult.AsyncWaitHandle.WaitOne();
        FwkTask pTask = taskResult.AsyncState as FwkTask;
        if (taskDeleg.EndInvoke(taskResult))
        {
          Util.Log("TaskResult:: SUCCESS :: Parallel task [{0}] " +
            "succeeded in test: {1}", pTask.Name, m_currentTestName);
        }
        else
        {
          if (pTask.ContinueOnError)
          {
            LogError("TaskResult:: FAILURE :: Parallel task [{0}] " +
              "failed in test: {1}.", pTask.Name, m_currentTestName);
          }
          else
          {
            LogError("TaskResult:: FAILURE :: Parallel task [{0}] " +
              "failed in test: {1}. Cannot continue.", pTask.Name,
              m_currentTestName);
            success = false;
          }
        }
      }
      return success;
    }

    private static void HandleTaskStarted(FwkTask task, ClientBase client)
    {
      if (!m_exiting)
      {
        Interlocked.Increment(ref m_runTasks);
        Util.Log("ASSIGNED:: Task [{0}] assigned to client [{1}]",
          task.Name, client.ID);
      }
    }

    private static void HandleTaskDone(FwkTask task, ClientBase client,
      Exception ex)
    {
      if (!m_exiting)
      {
        if (ex == null)
        {
          Interlocked.Increment(ref m_succeededTasks);
          Util.Log("SUCCESS:: Task [{0}] succeeded on client [{1}]",
            task.Name, client.ID);
        }
        else
        {
          Interlocked.Increment(ref m_failedTasks);
          if (!(ex is ClientTimeoutException))
          {
            LogError("Client {0} threw an exception: {1}: {2}",
              client.ID, ex.Message, ex.InnerException);
            LogError("------------------------------------------------------------");
          }
          if (task.ContinueOnError)
          {
            FwkClient.NewClient(client.ID, client.ID);
          }
          LogError("FAILURE:: Task [{0}] failed on client [{1}]",
            task.Name, client.ID);
        }
      }
    }

    private static void RunTests(XmlNode driverNode)
    {
      #region Step through all the tests

      XmlNodeList testNodes = driverNode.SelectNodes(TestNodeName);
      if (testNodes != null)
      {
        XmlAttributeCollection xmlAttribs;
        foreach (XmlNode testNode in testNodes)
        {
          if (m_exiting)
          {
            return;
          }
          m_totalTasks = 0;
          m_runTasks = 0;
          m_succeededTasks = 0;
          m_failedTasks = 0;
          try
          {
            m_currentTestName = null;
            string currentTestDesc = null;
            int currentTestTimeout = -1;
            int currentTimesToRun = 1;

            xmlAttribs = testNode.Attributes;
            if (xmlAttribs != null)
            {
              foreach (XmlAttribute xmlAttrib in xmlAttribs)
              {
                switch (xmlAttrib.Name)
                {
                  case NameAttrib:
                    m_currentTestName = xmlAttrib.Value.Replace(' ', '_');
                    break;
                  case DescAttrib:
                    currentTestDesc = xmlAttrib.Value;
                    break;
                  case WaitTimeAttrib:
                    currentTestTimeout = XmlNodeReaderWriter.String2Int(xmlAttrib.Value, -1);
                    break;
                  case TimesToRunAttrib:
                    currentTimesToRun = XmlNodeReaderWriter.String2Int(xmlAttrib.Value, -1);
                    break;
                  default:
                    throw new IllegalArgException("Unknown attribute '" +
                      xmlAttrib.Name + "' found in '" + TestNodeName +
                      "' node.");
                }
              }
            }
            latestprop(currentTestDesc);
            if (m_currentTestName != null && m_currentTestName.Length > 0)
            {
              Util.LogPrefix = "TEST:" + GetXMLLogPart();
            }
            XmlNodeList taskNodes = testNode.SelectNodes(TaskNodeName);
            List<FwkTask> allTasks = new List<FwkTask>();
            if (taskNodes != null)
            {
              foreach (XmlNode taskNode in taskNodes)
              {
                FwkTask task = new FwkTask(taskNode, m_currentTestName,
                  HandleTaskStarted, HandleTaskDone);
                m_totalTasks += task.ClientNames.Count * task.TimesToRun *
                  currentTimesToRun;
                allTasks.Add(task);
              }
            }
            for (int testRunNum = 1; testRunNum <= currentTimesToRun; ++testRunNum)
            {
              if (m_exiting)
              {
                return;
              }
              Util.Log("Starting test [{0}] for {1} of {2} times", currentTestDesc,
                testRunNum, currentTimesToRun);
              Util.BBSet(string.Empty, FwkReadData.TestRunNumKey, testRunNum);
              UnitFnMethodR<bool, FwkTask, string> taskDeleg = RunTask;
              List<IAsyncResult> taskResults = new List<IAsyncResult>();
              foreach (FwkTask task in allTasks)
              {
                if (m_exiting)
                {
                  return;
                }
                if (task.Parallel)
                {
                  Util.Log("Starting parallel task {0} ...", task.Name);
                  taskResults.Add(taskDeleg.BeginInvoke(task, m_currentTestName,
                    null, task));
                }
                else
                {
                  if (!WaitParallelTasks(taskResults, taskDeleg))
                  {
                    taskResults.Clear();
                    break;
                  }
                  taskResults.Clear();
                  Util.Log("Starting sequential task {0} ...", task.Name);
                  if (RunTask(task, m_currentTestName))
                  {
                    Util.Log("TaskResult:: SUCCESS :: Sequential task [{0}] " +
                      "succeeded in test: {1}", task.Name, m_currentTestName);
                  }
                  else
                  {
                    if (task.ContinueOnError)
                    {
                      LogError("TaskResult:: FAILURE :: Sequential task [{0}] " +
                        "failed in test: {1}.", task.Name, m_currentTestName);
                    }
                    else
                    {
                      LogError("TaskResult:: FAILURE :: Sequential task [{0}] " +
                        "failed in test: {1}. Cannot continue.", task.Name,
                        m_currentTestName);
                      taskResults.Clear();
                      break;
                    }
                  }
                }
              }
              WaitParallelTasks(taskResults, taskDeleg);
              taskResults.Clear();
              foreach (ClientBase client in FwkClient.GlobalClients.Values)
              {
                client.TestCleanup();
              }
            }
            foreach (FwkTask task in allTasks)
            {
              task.ClearData();
            }
          }
          finally
          {
            lock (((ICollection)FwkClient.GlobalClients).SyncRoot)
            {
              // Write the test summary
              Util.Log(string.Empty);
              Util.Log("===========================================================================");
              Util.Log("TEST SUMMARY:: for {0} in XML {1}/{2}",
                m_currentTestName, m_currentXmlDir, m_currentXmlName);
              Util.Log("TEST SUMMARY:: Total number of tasks: {0}", m_totalTasks);
              Util.Log("TEST SUMMARY:: Number of tasks run: {0}", m_runTasks);
              Util.Log("TEST SUMMARY:: Number of tasks passed: {0}", m_succeededTasks);
              Util.Log("TEST SUMMARY:: Number of tasks failed: {0}", m_failedTasks);
              Util.Log("===========================================================================");
              Util.LogPrefix = null;
              if (m_succeededTasks < m_totalTasks)
              {
                StreamWriter sw = new StreamWriter(xmlLogDir + "/failure.txt");
                sw.Close();
              }
            }
          }
        }
      }

      #endregion
    }

    private static string GetLogDir(string testName)
    {
      if (testName != null)
      {
        return GetBaseLogDir() + '/' + testName + '/';
      }
      return null;
    }

    private static string GetBaseLogDir()
    {
      return m_fwkOutName + '/' + GetXMLLogPart();
    }

    private static string GetXMLLogDir(string xmlDir)
    {
      const string endIndicator = "framework/xml/";
      xmlDir = Util.NormalizePath(xmlDir);
      int endPos = xmlDir.LastIndexOf(endIndicator);
      if (endPos >= 0)
      {
        xmlDir = xmlDir.Substring(endPos + endIndicator.Length);
      }
      else
      {
        xmlDir = Path.GetFileName(xmlDir);
      }
      return xmlDir;
    }

    private static string GetDirNameFromPath(string path)
    {
      return path.Replace('/', '_').Replace('\\', '_');
    }
// Added log directory path same as c++ log path
    private static string GetXMLLogPart()
    {
      return Path.GetFileNameWithoutExtension(m_currentXmlName) + "-" + now.ToString("MMdd") + "-" + now.ToString("HHmmss");
    }

    private static string GetXMLLogPart(string xmlFile)
    {
      string xmlDir = Path.GetDirectoryName(xmlFile);
      xmlDir = GetDirNameFromPath(GetXMLLogDir(xmlDir));
      return Path.GetFileNameWithoutExtension(xmlFile) + "-" + now.ToString("MMdd") + "-" + now.ToString("HHmmss");
    }
/*
    private static string GetXMLLogPart()
    {
      return GetDirNameFromPath(m_currentXmlDir) + '_' +
        Path.GetFileNameWithoutExtension(m_currentXmlName);
    }

    private static string GetXMLLogPart(string xmlFile)
    {
      string xmlDir = Path.GetDirectoryName(xmlFile);
      xmlDir = GetDirNameFromPath(GetXMLLogDir(xmlDir));
      return xmlDir + '_' + Path.GetFileNameWithoutExtension(xmlFile);
    }
*/
    private static bool RunTask(FwkTask task, string testName)
    {
      // TODO: Set the log-level depending on the command-line arguments
      task.SetLogLevel(Util.LogLevel.Config);

      string testDir = GetLogDir(testName);
      if (testDir != null)
      {
        task.SetLogFile(testDir + "Client");
      }
      bool exitStatus = true;
      for (int i = 1; i <= task.TimesToRun; i++)
      {
        if (m_exiting)
        {
          return false;
        }
        try
        {
          string logmsg = "Executing task " + task.Name + " for " + i + " of " +
            task.TimesToRun + " number of times with timeout " + task.Timeout +
            "ms on clients: ";
          string clientNames = null;
          foreach (string clientName in task.ClientNames)
          {
            clientNames += clientName + " ";
          }
          logmsg += clientNames;
          Util.Log(logmsg);
          exitStatus = task.Execute();
        }
        finally
        {
          try
          {
            task.EndTask();
          }
          catch
          {
            exitStatus = false;
          }
        }
      }
      return exitStatus;
    }

    private static bool RunClientWinTask(string clientId, string hostName, string taskSpec)
    {
      bool success = true;
      if (clientId != null && clientId.Length > 0 &&
        hostName != null && hostName.Length > 0 &&
        taskSpec != null && taskSpec.Length > 0 && !m_exiting)
      {
        Util.Log("Server got a delegate windows task from client {0} for host {1}",
          clientId, hostName);
        XmlNode taskSpecNode = XmlNodeReaderWriter.CreateXmlNode(taskSpec);
        try
        {
          FwkTask task = new FwkTask(taskSpecNode, m_currentTestName, null, null);
          ClientBase newProc = null;
          List<string> newClientIds = task.ClientNames;
          string newClientId;
          if (newClientIds != null && newClientIds.Count == 1)
          {
            newClientId = newClientIds[0];
          }
          else
          {
            newClientId = clientId + ".slave";
          }
          lock (((ICollection)FwkClient.GlobalClients).SyncRoot)
          {
            // Linear search since the clients are not too many (maybe upto 200).
            foreach (ClientBase unitProc in FwkClient.GlobalClients.Values)
            {
              if (unitProc != null && unitProc.HostName == hostName)
              {
                // Use this process itself to send the commands to avoid
                // hitting the limit of 10 users on Windows XP
                newProc = unitProc;
                Util.Log("Using an existing client {0} on host {1}.",
                  unitProc.ID, hostName);
                // Set the hostGroup for this client.
                string hostGroup = null;
                try
                {
                  hostGroup = Util.BBGet(newClientId,
                    FwkReadData.HostGroupKey) as string;
                }
                catch
                {
                  hostGroup = null;
                }
                if (hostGroup != null)
                {
                  Util.BBSet(newProc.ID, FwkReadData.HostGroupKey,
                    hostGroup);
                }
                break;
              }
            }
            if (newProc == null)
            {
              // Use the same ssh args as for the client that called us.
              if (FwkClient.GlobalClients.ContainsKey(clientId))
              {
                UnitProcess requestProc = FwkClient.GlobalClients[clientId] as UnitProcess;
                
                if (m_usepsexec && m_hostIsWindows[hostName])
                  newProc = new UnitProcess(newClientId, m_currentClientNum,
                    m_psexecPath, requestProc.PsexecArgs, hostName, m_sharePath,
                    m_shareName, GetBaseLogDir(), m_envs, m_launcherPort, m_launcherProcesses, 
                    m_fwkOutDir);
                else
                  newProc = new UnitProcess(newClientId, m_currentClientNum,
                    m_sshPath, requestProc.SshArgs, hostName, m_sharePath,
                    m_shareName, GetBaseLogDir(), m_envs);
                FwkClient.GlobalClients[newClientId] = newProc;
                m_currentClientNum++;
              }
            }
            if (newProc == null)
            {
              LogError("Failed to create client {0} on host {1}.",
                clientId, hostName);
              return false;
            }
          }
          task.SetClients(newProc);
          if (!RunTask(task, m_currentTestName))
          {
            success = false;
            LogError("ClientTask failed from client {0} on host {1} " +
              "with specification:{2}{3}", clientId, hostName,
              Environment.NewLine, taskSpecNode.OuterXml);
          }
          if (FwkTask.GlobalTaskNames.ContainsKey(task.Name))
          {
            FwkTask.GlobalTaskNames.Remove(task.Name);
          }
        }
        catch (Exception ex)
        {
          success = false;
          LogError("Exception in ClientTask from client {0} on host {1} " +
            "with specification:{2}{3}: {4}", clientId, hostName,
            Environment.NewLine, taskSpecNode.OuterXml, ex);
        }
      }
      return success;
    }

    private static string RunClientShellTask(string clientId, string hostName,
      string shellCmd, Dictionary<string, string> envVars)
    {
      if (clientId != null && clientId.Length > 0 &&
        hostName != null && hostName.Length > 0 &&
        shellCmd != null && shellCmd.Length > 0)
      {
        Util.Log("Server got a delegate shell command '{0}' from client {1} for host {2}",
          shellCmd, clientId, hostName);
        if (m_usepsexec && m_hostIsWindows[hostName])
        {
          Util.Log("Delegate shell command '{0}' from client {1} for host {2} will not be executed as the host is a windows machine."
            ,shellCmd, clientId, hostName);
          return null;
        }
        try
        {
          if (FwkClient.GlobalClients.ContainsKey(clientId))
          {
            string envStr = string.Empty;
            string sshArgs = null;
            if (envVars != null && envVars.Count > 0)
            {
              foreach (KeyValuePair<string, string> envPair in envVars)
              {
                envStr += ("export " + envPair.Key + "=\"" + envPair.Value +
                  "\"; ");
              }
            }
            if (!m_sshArgs.TryGetValue(hostName, out sshArgs))
            {
              // Host does not exist;
              // use the same ssh args as the client that called us.
              UnitProcess requestProc = FwkClient.GlobalClients[clientId] as UnitProcess;
              sshArgs = requestProc.SshArgs;
            }
            Process proc;
            string cmdArgs = sshArgs + ' ' + hostName + " \"bash -c '" +
              envStr + shellCmd + "'\"";
            if (Util.StartProcess(m_sshPath, cmdArgs, false, null, true,
              false, false, out proc))
            {
              StreamReader sr = proc.StandardOutput;
              proc.WaitForExit();
              string outStr = sr.ReadToEnd();
              sr.Close();
              return outStr;
            }
            else
            {
              LogError("Failed to create client {0} on host {1}.",
                clientId, hostName);
            }
          }
        }
        catch (Exception ex)
        {
          LogError("Exception in ClientTask from client {0} on host {1} in " +
            "with cmdline:{2}{3}: {4}", clientId, hostName,
            Environment.NewLine, shellCmd, ex);
        }
      }
      return null;
    }

    public class SlaveComm : MarshalByRefObject
    {
      public Dictionary<string, List<string>> GetHostGroupToHostMap()
      {
        return m_hostGroupToHostMap;
      }

      public List<string> GetHosts()
      {
        return m_hosts;
      }

      public Dictionary<string, bool> GetHostIsWindows()
      {
        return m_hostIsWindows;
      }

      public Dictionary<string, string> GetSshArgs()
      {
        return m_sshArgs;
      }

      public void Exit()
      {
        try
        {
          if (m_driverChannel != null)
          {
            ChannelServices.UnregisterChannel(m_driverChannel);
          }
        }
        finally
        {
          Environment.Exit(0);
        }
      }
    }
  }

  enum ConsoleEvent
  {
    CTRL_C = 0,  // From wincom.h
    CTRL_BREAK = 1,
    CTRL_CLOSE = 2,
    CTRL_LOGOFF = 5,
    CTRL_SHUTDOWN = 6
  }

  delegate void ControlEventHandler(ConsoleEvent consoleEvent);

  class ConsoleControl
  {
    [DllImport("kernel32", SetLastError = true)]
    static extern IntPtr GetStdHandle(IntPtr whichHandle);
    [DllImport("kernel32", SetLastError = true)]
    static extern bool GetConsoleMode(IntPtr handle, out uint mode);
    [DllImport("kernel32", SetLastError = true)]
    static extern bool SetConsoleMode(IntPtr handle, uint mode);

    [DllImport("kernel32.dll")]
    static extern bool SetConsoleCtrlHandler(ControlEventHandler e, bool add);

    static readonly IntPtr STD_INPUT_HANDLE = new IntPtr(-10);
    const uint ENABLE_LINE_INPUT = 2;
    const uint ENABLE_ECHO_INPUT = 4;
    static ControlEventHandler m_eventHandler;

    /// <summary>
    /// Returns an int of the key pressed at the console.
    /// This returns when key is hit, so does not require Enter as Console.Read().
    /// </summary>
    /// <returns></returns>
    public static int GetChar()
    {
      // Turn off console echo.
      IntPtr hConsole = GetStdHandle(STD_INPUT_HANDLE);
      uint oldMode;
      if (!GetConsoleMode(hConsole, out oldMode))
      {
        FwkDriver.FatalError("GetConsoleMode failed");
      }
      uint newMode = oldMode & ~(ENABLE_LINE_INPUT | ENABLE_ECHO_INPUT);
      if (!SetConsoleMode(hConsole, newMode))
      {
        FwkDriver.FatalError("SetConsoleMode failed");
      }
      int i;
      try
      {
        i = Console.Read();
        return i;
      }
      finally
      {
        // Restore console echo and line input mode.
        if (!SetConsoleMode(hConsole, oldMode))
        {
          FwkDriver.FatalError("SetConsoleMode failed");
        }
      }
    }

    public static string GetPassword()
    {
      Process proc;
      string secret = null;
      if (Util.StartProcess("bash", "-c \"stty -echo; read passwd; stty " +
        "echo; echo -n ${passwd}\"", false, null, true, false, false, out proc))
      {
        StreamReader outSr = proc.StandardOutput;
        secret = outSr.ReadToEnd();
        Console.WriteLine();
        proc.WaitForExit();
        outSr.Close();
      }
      else
      {
        FwkDriver.FatalError("Could not start bash.");
      }
      return secret;
    }

    public static string GetPassword2()
    {
      // turn off console echo
      IntPtr hConsole = GetStdHandle(STD_INPUT_HANDLE);
      uint oldMode;
      if (!GetConsoleMode(hConsole, out oldMode))
      {
        FwkDriver.FatalError("GetConsoleMode failed");
      }
      uint newMode = oldMode & ~(ENABLE_LINE_INPUT | ENABLE_ECHO_INPUT);
      if (!SetConsoleMode(hConsole, newMode))
      {
        FwkDriver.FatalError("SetConsoleMode failed");
      }
      int i;
      StringBuilder secret = new StringBuilder();
      try
      {
        while ((i = Console.Read()) != 13)
        {
          secret.Append((char)i);
          Console.Write('*');
        }
      }
      finally
      {
        // Restore console echo and line input mode.
        if (!SetConsoleMode(hConsole, oldMode))
        {
          FwkDriver.FatalError("SetConsoleMode failed");
        }
        Console.WriteLine();
      }
      return secret.ToString();
    }

    public static void CtrlCleanup(ControlEventHandler handler)
    {
      m_eventHandler = handler; // To avoid GC'ing of the handler after SetConsoleCtrlHandler returns
      SetConsoleCtrlHandler(m_eventHandler, true);
    }
  }
}
