//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Ipc;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;

namespace GemStone.GemFire.Cache.FwkClient
{
  using GemStone.GemFire.DUnitFramework;
  
  class ClientProcess
  {
    public static IChannel clientChannel = null;
    public static string bbUrl;
    public static string logFile = null;
    static void Main(string[] args)
    {
      string myId = "0";
      try
      {
        int myNum;
        string driverUrl;
        object clientPort;

        ParseArguments(args, out driverUrl, out bbUrl, out clientPort,
          out myId, out myNum, out logFile);

        // NOTE: This is required so that remote client receive custom exceptions
        RemotingConfiguration.CustomErrorsMode = CustomErrorsModes.Off;

        BinaryServerFormatterSinkProvider serverProvider =
          new BinaryServerFormatterSinkProvider();
        serverProvider.TypeFilterLevel = TypeFilterLevel.Full;
        BinaryClientFormatterSinkProvider clientProvider =
          new BinaryClientFormatterSinkProvider();
        Dictionary<string, string> properties;

        #region Create the communication channel to receive commands from server

        properties = new Dictionary<string, string>();
        if (clientPort.GetType() == typeof(int))
        {
          properties["port"] = clientPort.ToString();
          clientChannel = new TcpChannel(properties, clientProvider, serverProvider);
          //Util.Log("Registering TCP channel: " + clientPort);
        }
        else
        {
          properties["portName"] = clientPort.ToString();
          clientChannel = new IpcChannel(properties, clientProvider, serverProvider);
          //Util.Log("Registering IPC channel: " + clientPort);
        }
        ChannelServices.RegisterChannel(clientChannel, false);

        RemotingConfiguration.RegisterWellKnownServiceType(typeof(ClientComm),
          CommConstants.ClientService, WellKnownObjectMode.SingleCall);

        #endregion

        Util.ClientId = myId;
        Util.ClientNum = myNum;
        Util.LogFile = logFile;
        Util.DriverComm = ServerConnection<IDriverComm>.Connect(driverUrl);
        Util.BBComm = ServerConnection<IBBComm>.Connect(bbUrl);
        Util.ClientListening();
      }
      catch (Exception ex)
      {
        Util.Log("FATAL: Client {0}, Exception caught: {1}", myId, ex);
      }
      System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite);
    }

    private static void ShowUsage(string[] args)
    {
      if (args != null)
      {
        Util.Log("Args: ");
        foreach (string arg in args)
        {
          Util.Log("\t{0}", arg);
        }
      }
      string procName = Util.ProcessName;
      Util.Log("Usage: " + procName + " [OPTION] <client port>");
      Util.Log("If <client port> is a string then IPC is used else TCP at the given port number");
      Util.Log("Options are:");
      Util.Log("  --id=ID \t\t ID of the client; process ID is used when not provided");
      Util.Log("  --driver=URL \t The URL (e.g. tcp://<host>:<port>/<service>) of the Driver");
      Util.Log("  --bbServer=URL \t The URL (e.g. tcp://<host>:<port>/<service>) of the BlackBoard server");
      Util.Log("  --num=NUM \t\t An optional number for the process");
      Util.Log("  --log=LOGFILE \t The name of the logfile; standard output is used when not provided");
      Util.Log("  --env:VAR=VALUE \t Set environment variable VAR with given VALUE");
      Util.Log("                  \t If VAR is the PATH variable then it is prepended to the existing PATH");
      Util.Log("  --startdir=DIR \t Start in the given directory");
      Util.Log("  --bg \t Start in background");
      Environment.Exit(1);
    }

    private static void ParseArguments(string[] args, out string driverUrl,
      out string bbUrl, out object clientPort, out string myId, out int myNum,
      out string logFile)
    {
      if (args == null)
      {
        ShowUsage(args);
      }
      string IDOption = "--id=";
      string DriverOption = "--driver=";
      string BBServerOption = "--bbServer=";
      string NumOption = "--num=";
      string LogOption = "--log=";
      string EnvOption = "--env:";
      string StartDirOption = "--startdir=";
      string BGOption = "--bg";

      myId = Util.PID.ToString();
      driverUrl = null;
      bbUrl = null;
      myNum = 0;
      logFile = null;

      int argIndx = 0;
      while (argIndx <= (args.Length - 1) && args[argIndx].StartsWith("--"))
      {
        string arg = args[argIndx];
        if (arg.StartsWith(IDOption))
        {
          myId = arg.Substring(IDOption.Length);
        }
        else if (arg.StartsWith(DriverOption))
        {
          driverUrl = arg.Substring(DriverOption.Length);
        }
        else if (arg.StartsWith(BBServerOption))
        {
          bbUrl = arg.Substring(BBServerOption.Length);
        }
        else if (arg.StartsWith(NumOption))
        {
          try
          {
            myNum = int.Parse(arg.Substring(NumOption.Length));
          }
          catch (Exception ex)
          {
            Util.Log("Exception while reading --num: {0}", ex.Message);
            ShowUsage(args);
          }
        }
        else if (arg.StartsWith(LogOption))
        {
          logFile = arg.Substring(LogOption.Length);
        }
        else if (arg.StartsWith(EnvOption))
        {
          string[] envVarValue = arg.Substring(EnvOption.Length).Split('=');
          if (envVarValue.Length != 2)
          {
            ShowUsage(args);
          }
          string envVar = envVarValue[0];
          string envValue = envVarValue[1];
          int lastIndx = envVar.Length - 1;
          if (envVar[lastIndx] == '+')
          {
            envVar = envVar.Substring(0, lastIndx);
            envValue = envValue + Util.GetEnvironmentVariable(envVar);
          }
          Util.SetEnvironmentVariable(envVar, envValue);
        }
        else if (arg.StartsWith(StartDirOption))
        {
          string startDir = arg.Substring(StartDirOption.Length);
          if (startDir.Length > 0)
          {
            Environment.CurrentDirectory = startDir;
          }
        }
        else if (arg == BGOption)
        {
          string procArgs = string.Empty;
          foreach (string newArg in args)
          {
            if (newArg != BGOption)
            {
              procArgs += '"' + newArg + "\" ";
            }
          }
          procArgs = procArgs.Trim();
          System.Diagnostics.Process bgProc;
          if (!Util.StartProcess(Environment.GetCommandLineArgs()[0],
            procArgs, false, null, false, false, false, out bgProc))
          {
            Util.Log("Failed to start background process with args: {0}",
              procArgs);
            Environment.Exit(1);
          }
          Environment.Exit(0);
        }
        else
        {
          Util.Log("Unknown option: {0}", arg);
          ShowUsage(args);
        }
        argIndx++;
      }

      if (driverUrl == null || driverUrl.Length == 0 ||
        bbUrl == null || bbUrl.Length == 0)
      {
        ShowUsage(args);
      }
      if (args.Length != (argIndx + 1))
      {
        Util.Log("Incorrect number of arguments: {0}",
          (args.Length - argIndx));
        ShowUsage(args);
      }
      try
      {
        clientPort = int.Parse(args[argIndx]);
      }
      catch
      {
        clientPort = args[argIndx];
      }
    }
  }

  public class CreateBBServerConnection : MarshalByRefObject
  {
    public void OpenBBServerConnection(string urlPath)
    {
      Util.BBComm = ServerConnection<IBBComm>.Connect(urlPath);
    }

    public void SetLogFile(string logfile)
    {
      Util.LogFile = logfile;
    }
  }

}
