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
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Ipc;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading;

namespace GemStone.GemFire.DUnitFramework
{
  using NUnit.Framework;

  public class UnitProcess : ClientBase
  {
    #region Public constants and accessors

    public const string ClientProcName = "FwkClient.exe";

    public string SshArgs
    {
      get
      {
        return m_sshArgs;
      }
    }
    public string PsexecArgs
    {
      get
      {
        return m_psexecArgs;
      }
    }

    public override string StartDir
    {
      get
      {
        return m_startDir;
      }
    }

    #endregion

    #region Private members

    private static int m_clientId = 0;
    private static int m_clientPort = Util.RandPort(20000, 40000) - 1;
    
    internal static Dictionary<string, ManualResetEvent> ProcessIDMap =
      new Dictionary<string, ManualResetEvent>();

    public const int MaxStartWaitMillis = 300000;
    public const int MaxEndWaitMillis = 3000000;

    private Process m_process;
    private string m_sshPath;
    private string m_sshArgs;
    private string m_psexecPath;
    private string m_psexecArgs;
    private string m_hostName;
    private string m_sharePath;
    private string m_shareName;
    private string m_startDir;
    private Dictionary<string, string> m_envs;
    private IClientComm m_clientComm;
    private bool m_timeout;
    private volatile bool m_exiting;
    private ManualResetEvent m_clientEvent = new ManualResetEvent(false);

    #endregion

    // Using static constructor since it is guaranteed
    // to be called on first access.
    static UnitProcess()
    {
      // Create the server side channels (required only once)

      // NOTE: This is required so that remote client receives custom exceptions
      RemotingConfiguration.CustomErrorsMode = CustomErrorsModes.Off;

      RegisterChannel(true);
      RegisterChannel(false);

      RemotingConfiguration.RegisterWellKnownServiceType(typeof(DriverComm),
        CommConstants.DriverService, WellKnownObjectMode.SingleCall);
      if (Util.ExternalBBServer == null)
      {
        RemotingConfiguration.RegisterWellKnownServiceType(typeof(BBComm),
          CommConstants.BBService, WellKnownObjectMode.SingleCall);
        Util.SetEnvironmentVariable(CommConstants.BBAddrEnvVar,
          Util.IPAddressString + ':' + Util.DriverPort.ToString());
      }
    }

    #region Private functions

    private static void RegisterChannel(bool ipc)
    {
      BinaryServerFormatterSinkProvider serverProvider =
        new BinaryServerFormatterSinkProvider();
      serverProvider.TypeFilterLevel = TypeFilterLevel.Full;
      BinaryClientFormatterSinkProvider clientProvider =
        new BinaryClientFormatterSinkProvider();
      Dictionary<string, string> properties = new Dictionary<string, string>();
      IChannel channel;

            if (ipc)
            {
                properties["portName"] = CommConstants.ServerIPC + Util.DriverPort.ToString();
                properties["name"] = "GFIpcChannel";
                channel = new IpcChannel(properties, clientProvider, serverProvider);
            }
            else
            {
                properties["port"] = Util.DriverPort.ToString();
                properties["name"] = "GFTcpChannel";
                channel = new TcpChannel(properties, clientProvider, serverProvider);
            }
            ChannelServices.RegisterChannel(channel, false);
    }

    private void ExitClient(int waitMillis, bool force)
    {
      if (m_clientComm != null)
      {
        Thread killThread = new Thread(ExitClient);
        killThread.Start();
        if (waitMillis > 0)
        {
          if (!killThread.Join(waitMillis))
          {
            Util.Log(Util.LogLevel.Error,
              "Timed out waiting for client {0} to exit.", ID);
          }
        }
        if (m_process != null && !m_process.HasExited)
        {
          if (waitMillis > 0)
          {
            m_process.WaitForExit(waitMillis);
          }
        }
        if (m_process != null && !m_process.HasExited)
        {
          try
          {
            m_process.Kill();
          }
          catch
          {
          }
          if (waitMillis > 0)
          {
            m_process.WaitForExit(waitMillis);
          }
        }
        m_clientComm = null;
      }
    }

    private void ExitClient()
    {
      ExitClient(false);
    }

    private void ExitClient(bool force)
    {
      try
      {
        m_clientComm.Exit(force);
      }
      catch
      {
        // Client cannot be reached or already forcibly closed.
        // Nothing required to be done.
      }
    }

    #endregion

    #region Public non-overriding methods

    public static void Init()
    {
      // Do nothing; just ensures that the static constructor is invoked.
    }

    public static int GetClientId()
    {
      Interlocked.Increment(ref m_clientId);
      return m_clientId;
    }

    public static int GetClientPort()
    {
      Interlocked.Increment(ref m_clientPort);
      return m_clientPort;
    }

    private UnitProcess(string clientId)
    {
      if (clientId == null)
      {
        clientId = GetClientId().ToString();
      }
      this.ID = clientId;
    }

    public UnitProcess()
      : this(null, null)
    {
    }

    public UnitProcess(string clientId, string startDir)
    {
      string clientIPC = CommConstants.ClientIPC + GetClientPort().ToString();
      if (clientId == null)
      {
        clientId = GetClientId().ToString();
      }
      this.ID = clientId;
      string localArgs = "--id=" + clientId + " --driver=ipc://" +
        CommConstants.ServerIPC + Util.DriverPort + '/' +
        CommConstants.DriverService + " --bbServer=";
      if (Util.ExternalBBServer != null)
      {
        localArgs += Util.ExternalBBServer + ' ' + clientIPC;
      }
      else
      {
        localArgs += "ipc://" + CommConstants.ServerIPC + Util.DriverPort +
          '/' + CommConstants.BBService + ' ' + clientIPC;
      }
      lock (((ICollection)ProcessIDMap).SyncRoot)
      {
        ProcessIDMap[clientId] = m_clientEvent;
      }
      bool procStarted;
      bool hasCoverage = "true".Equals(Environment.GetEnvironmentVariable(
        "COVERAGE_ENABLED"));
      if (hasCoverage)
      {
        string coveragePrefix = "coverage-" + clientId;
        procStarted = Util.StartProcess("ncover.console.exe", "//x "
          + coveragePrefix + ".xml " + ClientProcName + " " + localArgs,
          Util.LogFile == null, startDir, true, true, true, out m_process);
      }
      else
      {
        procStarted = Util.StartProcess(ClientProcName, localArgs,
          Util.LogFile == null, startDir, false, false, false, true, out m_process);
      }
      if (!procStarted)
      {
        throw new AssertionException("FATAL: Could not start client: " +
          ClientProcName);
      }

      m_clientComm = ServerConnection<IClientComm>.Connect("ipc://" +
        clientIPC + '/' + CommConstants.ClientService);
      m_timeout = false;
      m_exiting = false;
      m_startDir = startDir;
    }

    public UnitProcess(string clientId, int clientNum, string sshPath,
      string sshArgs, string hostName, string sharePath, string shareName,
      string startDir, Dictionary<string, string> envs)
    {
      string clientProcPath = ClientProcName;
      string remoteArgs;
      string runDir = null;

      if (sharePath != null)
      {
        runDir = Util.AssemblyDir.Replace(sharePath.ToLower(), shareName);
        clientProcPath = runDir + Path.DirectorySeparatorChar + ClientProcName;
        clientProcPath = clientProcPath.Replace('\\', '/');
      }

      int clientPort = GetClientPort();
      if (clientId == null)
      {
        clientId = GetClientId().ToString();
      }
      this.ID = clientId;
      string envStr = string.Empty;

      if (envs != null)
      {
        m_envs = envs;
        foreach (KeyValuePair<string, string> envNameValue in envs)
        {
          string envValue = envNameValue.Value.Replace(sharePath, shareName);
          envStr += " '--env:" + envNameValue.Key + '=' + envValue + '\'';
        }
      }
      string bbServer;
      if (Util.ExternalBBServer != null)
      {
        bbServer = Util.ExternalBBServer;
      }
      else
      {
        bbServer = "tcp://" + Util.IPAddressString + ':' + Util.DriverPort +
          '/' + CommConstants.BBService;
      }
      remoteArgs = sshArgs + ' ' + hostName + " '" + clientProcPath +
        "' --id=" + clientId + " --num=" + clientNum + envStr +
        " '--startdir=" + (startDir == null ? runDir : startDir) +
        "' '--driver=tcp://" + Util.IPAddressString + ':' + Util.DriverPort +
        '/' + CommConstants.DriverService + "' '--bbServer=" + bbServer +
        "' " + clientPort;
      try
      {
        lock (((ICollection)ProcessIDMap).SyncRoot)
        {
          ProcessIDMap[clientId] = m_clientEvent;
        }
        if (!Util.StartProcess(sshPath, remoteArgs, Util.LogFile == null,
          null, false, false, false, out m_process))
        {
          throw new AssertionException("Failed to start the client.");
        }

        if (!m_clientEvent.WaitOne(MaxStartWaitMillis, false))
        {
          throw new AssertionException("Timed out waiting for client to start.");
        }
        m_clientComm = ServerConnection<IClientComm>.Connect("tcp://" +
          hostName + ':' + clientPort + '/' + CommConstants.ClientService);
        m_process = m_clientComm.GetClientProcess();
        m_sshPath = sshPath;
        m_sshArgs = sshArgs;
        m_hostName = hostName;
        m_timeout = false;
        m_exiting = false;
        m_sharePath = sharePath;
        m_shareName = shareName;
        m_startDir = startDir;
      }
      catch (Exception ex)
      {
        throw new AssertionException(string.Format("FATAL: Could not start " +
          "client: {1}{0}\ton host: {2}{0}\tusing remote shell: {3}{0}" +
          "\tException: {4}", Environment.NewLine, clientProcPath, hostName,
          sshPath, ex));
      }
    }
    
    /// <summary>
    /// Create a UnitProcess object by launching a client using a launcher. The 
    /// launcher is launched on the remote host using psexec.
    /// </summary>
    public UnitProcess(string clientId, int clientNum, string psexecPath,
          string psexecArgs, string hostName, string sharePath, string shareName,
          string startDir, Dictionary<string, string> envs,
          int launcherPort, Dictionary<string, UnitProcess> launcherProcesses, string logPath)
    {
      UnitProcess launcherProcess = null;
      if (!launcherProcesses.ContainsKey(hostName))
      {
        launcherProcess = CreateLauncherUsingPsexec(GetClientId().ToString(),
          psexecPath, psexecArgs, hostName, sharePath, shareName, startDir,
          launcherPort, logPath);
        launcherProcesses.Add(hostName, launcherProcess);
      }
      else
      {
        launcherProcess = launcherProcesses[hostName];
      }

      string clientProcPath = ClientProcName;
      string remoteArgs;
      string runDir = null;

      if (sharePath != null)
      {
        runDir = Util.AssemblyDir.Replace(sharePath.ToLower(), shareName);
        clientProcPath = runDir + Path.DirectorySeparatorChar + ClientProcName;
        clientProcPath = clientProcPath.Replace('/', '\\');
      }

      int clientPort = GetClientPort();
      if (clientId == null)
      {
        clientId = GetClientId().ToString();
      }
      this.ID = clientId;
      string envStr = string.Empty;

      if (envs != null)
      {
        m_envs = envs;
        foreach (KeyValuePair<string, string> envNameValue in envs)
        {
          string envValue = envNameValue.Value.Replace(sharePath, shareName);
          envStr += " --env:" + envNameValue.Key + "=\"" + envValue + '\"';
        }
      }
      string bbServer;
      if (Util.ExternalBBServer != null)
      {
        bbServer = Util.ExternalBBServer;
      }
      else
      {
        bbServer = "tcp://" + Util.IPAddressString + ':' + Util.DriverPort +
          '/' + CommConstants.BBService;
      }
      remoteArgs = " --id=" + clientId + " --num=" + clientNum + envStr +
        " --startdir=\"" + (startDir == null ? runDir : startDir) +
        "\" --driver=tcp://" + Util.IPAddressString + ':' + Util.DriverPort +
        '/' + CommConstants.DriverService + " --bbServer=\"" + bbServer +
        "\" " + clientPort;
      try
      {
        IClientCommV2 clientComm = launcherProcess.m_clientComm as IClientCommV2;
        Process proc;
        if (!clientComm.LaunchNewClient(clientProcPath, remoteArgs, out proc))
        {
          throw new AssertionException("Failed to start client: " +
            clientId);
        }
        
        m_process = proc;
        lock (((ICollection)ProcessIDMap).SyncRoot)
        {
          ProcessIDMap[clientId] = m_clientEvent;
        }
        
        if (!m_clientEvent.WaitOne(MaxStartWaitMillis, false))
        {
          throw new AssertionException("Timed out waiting for client to start.");
        }
        
        m_clientComm = ServerConnection<IClientComm>.Connect("tcp://" +
          hostName + ':' + clientPort + '/' + CommConstants.ClientService);
        m_process = m_clientComm.GetClientProcess();
        
        m_psexecPath = psexecPath;
        m_psexecArgs = psexecArgs;
        m_hostName = hostName;
        m_timeout = false;
        m_exiting = false;
        m_sharePath = sharePath;
        m_shareName = shareName;
        m_startDir = startDir;
      }
      catch (Exception ex)
      {
        throw new AssertionException(string.Format("FATAL: Could not start " +
          "client: {1}{0}\ton host: {2}{0}\tusing: FwkLauncher{0}" +
          "\tException: {3}", Environment.NewLine, clientProcPath, hostName,
          ex));
      }
    }
    public static UnitProcess Create()
    {
      return new UnitProcess();
    }

    public static UnitProcess Create(string clientId, string startDir)
    {
      return new UnitProcess(clientId, startDir);
    }

    public static UnitProcess Create(string clientId, int clientNum,
      string sshPath, string sshArgs, string hostName, string sharePath,
      string shareName, string startDir, Dictionary<string, string> envs)
    {
      return new UnitProcess(clientId, clientNum, sshPath, sshArgs, hostName,
        sharePath, shareName, startDir, envs);
    }

    public static List<UnitProcess> Create(List<string> clientIds,
      int startClientNum, string sshPath, string sshArgs, string hostName,
      string sharePath, string shareName, string startDir,
      Dictionary<string, string> envs)
    {
      if (clientIds != null && clientIds.Count > 0)
      {
        string clientProcPath = ClientProcName;
        string clientsCmd = string.Empty;
        string runDir = null;
        string clientId;

        List<UnitProcess> unitProcs = new List<UnitProcess>();
        UnitProcess unitProc;

        if (sharePath != null)
        {
          runDir = Util.AssemblyDir.Replace(sharePath.ToLower(), shareName);
          clientProcPath = runDir + Path.DirectorySeparatorChar + ClientProcName;
          clientProcPath = clientProcPath.Replace('\\', '/');
        }

        string envStr = string.Empty;

        if (envs != null)
        {
          foreach (KeyValuePair<string, string> envNameValue in envs)
          {
            string envValue = envNameValue.Value.Replace(sharePath, shareName);
            envStr += " '--env:" + envNameValue.Key + '=' + envValue + '\'';
          }
        }
        List<int> clientPorts = new List<int>();
        lock (((ICollection)ProcessIDMap).SyncRoot)
        {
          for (int i = 0; i < clientIds.Count; i++, startClientNum++)
          {
            int clientPort = GetClientPort();
            clientId = clientIds[i];
            if (clientId == null)
            {
              clientId = GetClientId().ToString();
              clientIds[i] = clientId;
            }
            string bbServer;
            if (Util.ExternalBBServer != null)
            {
              bbServer = Util.ExternalBBServer;
            }
            else
            {
              bbServer = "tcp://" + Util.IPAddressString + ':' + Util.DriverPort +
                '/' + CommConstants.BBService;
            }
            clientsCmd += '\'' + clientProcPath + "' --bg --id=" + clientId +
              " --num=" + startClientNum + envStr + " '--startdir=" +
              (startDir == null ? runDir : startDir) + "' '--driver=tcp://" +
              Util.IPAddressString + ':' + Util.DriverPort + '/' +
              CommConstants.DriverService + "' '--bbServer=" + bbServer +
              "' " + clientPort + " ; ";
            clientPorts.Add(clientPort);

            unitProc = new UnitProcess(clientId);
            ProcessIDMap[clientId] = unitProc.m_clientEvent;
            unitProcs.Add(unitProc);
          }
        }
        try
        {
          Process proc;
          //string sshAllArgs = sshArgs + ' ' + hostName +
          //  " \"" + " NCover.Console.exe //x Coverage.xml //at coverage.trend " + clientsCmd + '"';
          string sshAllArgs = sshArgs + ' ' + hostName +
            " \"" + clientsCmd + '"';
          Util.RawLog(string.Format("Running remote command: {0} {1}{2}",
            sshPath, Util.HideSshPassword(sshAllArgs), Environment.NewLine));
          if (!Util.StartProcess(sshPath, sshAllArgs, Util.LogFile == null, null,
            false, false, false, out proc))
          {
            throw new AssertionException("Failed to start the clients.");
          }

          for (int i = 0; i < clientIds.Count; i++)
          {
            clientId = clientIds[i];
            unitProc = (UnitProcess)unitProcs[i];
            if (!unitProc.m_clientEvent.WaitOne(MaxStartWaitMillis, false))
            {
              throw new AssertionException("Timed out waiting for client: " +
                clientId);
            }

            unitProc.m_clientComm = ServerConnection<IClientComm>.Connect("tcp://" +
                hostName + ':' + clientPorts[i] + '/' + CommConstants.ClientService);
            unitProc.m_process = unitProc.m_clientComm.GetClientProcess();
            unitProc.m_sshPath = sshPath;
            unitProc.m_sshArgs = sshArgs;
            unitProc.m_hostName = hostName;
            unitProc.m_timeout = false;
            unitProc.m_exiting = false;
            unitProc.m_sharePath = sharePath;
            unitProc.m_shareName = shareName;
            unitProc.m_startDir = startDir;
            unitProc.m_envs = envs;
          }
        }
        catch (Exception ex)
        {
          unitProcs.Clear();
          unitProcs = null;
          clientPorts.Clear();
          clientPorts = null;
          throw new AssertionException(string.Format("FATAL: Could not start " +
            "client: {1}{0}\ton host: {2}{0}\tusing remote shell: {3}{0}" +
            "\tException: {4}", Environment.NewLine, clientProcPath, hostName,
            sshPath, ex));
        }
        return unitProcs;
      }
      return null;
    }

    /// <summary>
    /// Create clients on the remote host using FwkLauncher. 
    /// </summary>
    public static List<UnitProcess> CreateClientsUsingLauncher(List<string> clientIds,
     int startClientNum, string psexecPath, string psexecArgs, string hostName,
     string sharePath, string shareName, string startDir,
     Dictionary<string, string> envs, int launcherPort, 
     Dictionary<string, UnitProcess>  launcherProcesses, string logPath)
    {
      if (clientIds != null && clientIds.Count > 0)
      {
        UnitProcess launcherProcess = null;
        if (!launcherProcesses.ContainsKey(hostName))
        {
          launcherProcess = CreateLauncherUsingPsexec(GetClientId().ToString(), psexecPath, psexecArgs,
            hostName, sharePath, shareName, startDir, launcherPort, logPath);
          launcherProcesses.Add(hostName, launcherProcess);
        }
        else
        {
          launcherProcess = launcherProcesses[hostName];
        }

        
        string clientProcPath = ClientProcName;
        string runDir = null;
        string clientId;

        List<UnitProcess> unitProcs = new List<UnitProcess>();
        
        if (sharePath != null)
        {
          runDir = Util.AssemblyDir.Replace(sharePath.ToLower(), shareName);
          clientProcPath = runDir + Path.DirectorySeparatorChar + ClientProcName;
          clientProcPath = clientProcPath.Replace('\\', '/');
        }

        string envStr = string.Empty;

        if (envs != null)
        {
          foreach (KeyValuePair<string, string> envNameValue in envs)
          {
            string envValue = envNameValue.Value.Replace(sharePath, shareName);
            envStr += " --env:" + envNameValue.Key + "=\"" + envValue + '\"';
          }
        }
        List<int> clientPorts = new List<int>();
        
          
        try
        {
          for (int i = 0; i < clientIds.Count; i++, startClientNum++)
          {
            string clientsCmd = string.Empty;
            int clientPort = GetClientPort();
            clientId = clientIds[i];
            if (clientId == null)
            {
              clientId = GetClientId().ToString();
              clientIds[i] = clientId;
            }
            string bbServer;
            if (Util.ExternalBBServer != null)
            {
              bbServer = Util.ExternalBBServer;
            }
            else
            {
              bbServer = "tcp://" + Util.IPAddressString + ':' + Util.DriverPort +
                '/' + CommConstants.BBService;
            }
            clientsCmd = "  --id=" + clientId +
              " --num=" + startClientNum + envStr + " --startdir=\"" +
              (startDir == null ? runDir : startDir) + "\" --driver=\"tcp://" +
              Util.IPAddressString + ':' + Util.DriverPort + '/' +
              CommConstants.DriverService + "\" --bbServer=\"" + bbServer +
              "\" " + clientPort;
            clientPorts.Add(clientPort);
            IClientCommV2 clientComm = launcherProcess.m_clientComm as IClientCommV2;
            Process proc;
            UnitProcess unitProc;
            unitProc = new UnitProcess(clientId);
            lock (((ICollection)ProcessIDMap).SyncRoot)
            {
                ProcessIDMap[clientId] = unitProc.m_clientEvent;
                unitProcs.Add(unitProc);
            }
            if (!clientComm.LaunchNewClient(clientProcPath, clientsCmd, out proc))
            {
              throw new AssertionException("Failed to start client: " +
                clientId);
            }
            unitProc.m_process = proc;
            
          }
          
          for (int i = 0; i < clientIds.Count; i++)
          {
            clientId = clientIds[i];
            UnitProcess unitProc = (UnitProcess)unitProcs[i];
            if (!unitProc.m_clientEvent.WaitOne(MaxStartWaitMillis, false))
            {
              throw new AssertionException("Timed out waiting for client: " +
                clientId);
            }

            unitProc.m_clientComm = ServerConnection<IClientComm>.Connect("tcp://" +
                hostName + ':' + clientPorts[i] + '/' + CommConstants.ClientService);
            unitProc.m_process = unitProc.m_clientComm.GetClientProcess();
            unitProc.m_psexecPath = psexecPath;
            unitProc.m_psexecArgs = psexecArgs;
            unitProc.m_hostName = hostName;
            unitProc.m_timeout = false;
            unitProc.m_exiting = false;
            unitProc.m_sharePath = sharePath;
            unitProc.m_shareName = shareName;
            unitProc.m_startDir = startDir;
            unitProc.m_envs = envs;
          }
        }
        catch (Exception ex)
        {
          unitProcs.Clear();
          unitProcs = null;
          clientPorts.Clear();
          clientPorts = null;
          throw new AssertionException(string.Format("FATAL: Could not start " +
            "client: {1}{0}\ton host: {2}{0}\tusing: FwkLauncher{0}" +
            "\tException: {3}", Environment.NewLine, clientProcPath, hostName,
            ex));
        }
        return unitProcs;
      }
      
      return null;
    }
    
    /// <summary>
    /// If not already created, Create a launcher process on the remote host using 
    /// psexec. This launcher is used to launch the FwkClients on the host. 
    /// </summary>
    private static UnitProcess CreateLauncherUsingPsexec(string clientId,
          string psexecPath, string psexecArgs, string hostName,
          string sharePath, string shareName, string startDir,
          int launcherPort, string logPath)
    {
      if (clientId != null)
      {
        string clientProcPath = "FwkLauncher.exe";
        string clientsCmd = string.Empty;
        string runDir = null;
        bool newProcCreated = false;  
        UnitProcess unitProc;
        Process proc = null;
            
        try
        {
          lock (((ICollection)ProcessIDMap).SyncRoot)
          {
            unitProc = new UnitProcess(clientId);
            ProcessIDMap[clientId] = unitProc.m_clientEvent;
          }
          if (launcherPort == 0) // i.e. launcher is not running
          {
            launcherPort = GetClientPort();
        
            if (sharePath != null)
            {
              runDir = Util.AssemblyDir.Replace(sharePath.ToLower(), shareName);
              clientProcPath = runDir + Path.DirectorySeparatorChar + clientProcPath;
              clientProcPath = clientProcPath.Replace('/', '\\');
            }
            clientsCmd = " cmd.exe /C \"\"" + clientProcPath + "\" --id=" + clientId + 
              " --bg  --port=" + launcherPort + " --driver=\"tcp://" + Util.IPAddressString + 
              ':' + Util.DriverPort + '/' + CommConstants.DriverService + "\"\"";

            
            
            string psexecAllArgs = psexecArgs + "\\\\" + hostName +
              " " + clientsCmd;
            Util.Log(string.Format("Running remote command: {0} {1}{2}",
              psexecPath, Util.HidePsexecPassword(psexecAllArgs), Environment.NewLine));
            if (!Util.StartProcess(psexecPath, psexecAllArgs, Util.LogFile == null, null,
              false, false, false, out proc))
            {
              throw new AssertionException("Failed to start the launcher.");
            }
            if (!unitProc.m_clientEvent.WaitOne(90000, false))
            {
              throw new AssertionException("Timed out waiting for launcher: " +
                clientId);
            }
            newProcCreated = true;
          }
          
          unitProc.m_clientComm = (IClientComm)ServerConnection<IClientCommV2>.Connect("tcp://" +
              hostName + ':' + launcherPort + '/' + CommConstants.ClientService);

          unitProc.m_process = unitProc.m_clientComm.GetClientProcess();
          
          unitProc.m_psexecPath = psexecPath;
          unitProc.m_psexecArgs = psexecArgs;
          unitProc.m_hostName = hostName;
          unitProc.m_timeout = false;
          unitProc.m_exiting = false;
          unitProc.m_sharePath = sharePath;
          unitProc.m_shareName = shareName;
          unitProc.m_startDir = startDir;
          if (newProcCreated)
          {
              logPath += "/" + "Launcher" + "_" + hostName + "_" + clientId + ".log";
              Util.Log("Launcher log path {0}", logPath);
                    
              unitProc.SetLogLevel(Util.CurrentLogLevel);
              unitProc.SetLogFile(logPath);
          }
        }
        catch (Exception ex)
        {
          throw new AssertionException(string.Format("FATAL: Could not start " +
            "launcher: {1}{0}\ton host: {2}{0}" +
            "\tException: {3}", Environment.NewLine, clientProcPath, hostName, 
            ex));
        }
        return unitProc;
      }
      return null;
    }

    #endregion

    
    #region ClientBase Members
    
    public override void RemoveObjectID(int objectID)
    {
      if (m_clientComm != null)
      {
        m_clientComm.RemoveObjectID(objectID);
      }
    }

    public override void CallFn(Delegate deleg, object[] paramList)
    {
      string assemblyName, typeName, methodName;
      int objectId;

      ClientBase.GetDelegateInfo(deleg, out assemblyName,
        out typeName, out methodName, out objectId);
      Exception callEx = null;
      for (int numTries = 1; numTries <= 5; numTries++)
      {
        if (m_clientComm != null)
        {
          try
          {
            m_clientComm.Call(objectId,
              assemblyName, typeName, methodName, paramList);
            return;
          }
          catch (Exception ex)
          {
            callEx = ex;
            if (m_exiting)
            {
              throw new ClientExitedException("Process exited while calling '." +
                deleg.Method.ToString() + '\'');
            }
            else if (m_timeout)
            {
              throw new ClientTimeoutException("Timeout occured while calling '" +
                deleg.Method.ToString() + '\'');
            }
            else if (ex is RemotingException ||
              ex is System.Net.Sockets.SocketException)
            {
              Thread.Sleep(numTries * 1000);
            }
            else
            {
              break;
            }
          }
        }
      }
      if (callEx != null)
      {
        throw callEx;
      }
    }

    public override object CallFnR(Delegate deleg, object[] paramList)
    {
      string assemblyName, typeName, methodName;
      int objectId;

      ClientBase.GetDelegateInfo(deleg, out assemblyName,
        out typeName, out methodName, out objectId);
      Exception callEx = null;
      for (int numTries = 1; numTries <= 5; numTries++)
      {
        if (m_clientComm != null)
        {
          try
          {
            return m_clientComm.CallR(objectId,
              assemblyName, typeName, methodName, paramList);
          }
          catch (Exception ex)
          {
            callEx = ex;
            if (m_exiting)
            {
              throw new ClientExitedException("Process exited while calling '." +
                deleg.Method.ToString() + '\'');
            }
            else if (m_timeout)
            {
              throw new ClientTimeoutException("Timeout occured while calling '" +
                deleg.Method.ToString() + '\'');
            }
            else if (ex is RemotingException ||
              ex is System.Net.Sockets.SocketException)
            {
              Thread.Sleep(numTries * 1000);
            }
            else
            {
              break;
            }
          }
        }
      }
      if (callEx != null)
      {
        throw callEx;
      }
      return null;
    }

    public override string HostName
    {
      get
      {
        return (m_hostName == null ? "localhost" : m_hostName);
      }
    }

    /// <summary>
    /// Set the log file for the client.
    /// </summary>
    /// <param name="logPath">The path of the log file.</param>
    public override void SetLogFile(string logPath)
    {
      if (m_clientComm != null)
      {
        m_clientComm.SetLogFile(logPath);
      }
    }


    /// <summary>
    /// Set logging level for the client.
    /// </summary>
    /// <param name="logLevel">The logging level to set.</param>
    public override void SetLogLevel(Util.LogLevel logLevel)
    {
      if (m_clientComm != null)
      {
        m_clientComm.SetLogLevel(logLevel);
      }
    }

    public override void DumpStackTrace()
    {
      if (m_clientComm != null)
      {
        m_clientComm.DumpStackTrace();
      }
    }

    public override void ForceKill(int waitMillis)
    {
      if (m_clientComm != null)
      {
        m_timeout = true;
        Util.Log("Forcing kill for client {0}", this.ID);
        ExitClient(waitMillis, true);
        Util.Log("Done kill for client {0}", this.ID);
      }
    }

    public override ClientBase CreateNew(string clientId)
    {
      UnitProcess newProc;
      if (clientId == null)
      {
        clientId = GetClientId().ToString();
      }
      if (m_sshPath != null)
      {
        int clientPort = GetClientPort();
        m_clientEvent.Reset();
        lock (((ICollection)ProcessIDMap).SyncRoot)
        {
          ProcessIDMap[clientId] = m_clientEvent;
        }
        if (!m_clientComm.CreateNew(clientId, clientPort))
        {
          throw new AssertionException("FATAL: Could not start client: " +
            ClientProcName);
        }

        if (!m_clientEvent.WaitOne(MaxStartWaitMillis, false))
        {
          throw new AssertionException("FATAL: Timed out waiting for client to start: " +
            ClientProcName);
        }
        newProc = new UnitProcess(clientId);
        newProc.m_sshPath = m_sshPath;
        newProc.m_sshArgs = m_sshArgs;
        newProc.m_hostName = m_hostName;
        newProc.m_clientComm = ServerConnection<IClientComm>.Connect("tcp://" +
          m_hostName + ':' + clientPort + '/' + CommConstants.ClientService);
        newProc.m_process = newProc.m_clientComm.GetClientProcess();
        newProc.m_timeout = false;
        newProc.m_exiting = false;
        newProc.m_sharePath = m_sharePath;
        newProc.m_shareName = m_shareName;
        newProc.m_startDir = m_startDir;
        newProc.m_envs = m_envs;
      }
      else
      {
        newProc = new UnitProcess(clientId, m_startDir);
      }
      return newProc;
    }

    public override void TestCleanup()
    {
      if (m_clientComm != null)
      {
        m_clientComm.TestCleanup();
      }
    }

    public override void Exit()
    {
      if (m_clientComm != null)
      {
        m_exiting = true;
        ExitClient(MaxEndWaitMillis, false);
      }
    }

    #endregion
  }
}
