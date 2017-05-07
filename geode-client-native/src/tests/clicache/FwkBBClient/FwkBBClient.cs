//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace GemStone.GemFire.Cache.FwkBBClient
{
  using GemStone.GemFire.DUnitFramework;

  static class FwkBBClient
  {
    static int Main(string[] args)
    {
      string bbName;
      string key;
      string value;
      bool isGet;
      bool isQuit;
      bool isTermDriver;

      bool status = ParseArgs(args, out bbName, out key, out value,
        out isGet, out isQuit, out isTermDriver);
      if (!status)
      {
        ShowUsage();
        return 1;
      }
      string serverAddr = null;
      try
      {
        if (isTermDriver)
        {
          serverAddr = GetServerAddress(CommConstants.DriverAddrEnvVar);
          Util.DriverComm = ServerConnection<IDriverComm>.Connect("tcp://" +
            serverAddr + '/' + CommConstants.DriverService);
        }
        else
        {
          serverAddr = GetServerAddress(CommConstants.BBAddrEnvVar);
          Util.BBComm = ServerConnection<IBBComm>.Connect("tcp://" +
            serverAddr + '/' + CommConstants.BBService);
        }
      }
      catch
      {
        Console.WriteLine("{0}: Could not connect to server {1}",
          Util.ProcessName, serverAddr);
        return 3;
      }
      if (isTermDriver)
      {
        try
        {
          Util.DriverComm.Term();
        }
        catch
        {
          return 2;
        }
      }
      else if (isQuit)
      {
        try
        {
          Util.BBComm.Exit();
        }
        catch
        {
          return 2;
        }
      }
      else if (isGet)
      {
        try
        {
          object result = Util.BBGet(bbName, key);
          if (result != null)
          {
            Console.Write(result.ToString());
          }
        }
        catch
        {
          return 2;
        }
      }
      else
      {
        try
        {
          Util.BBSet(bbName, key, value);
        }
        catch
        {
          return 2;
        }
      }

      return 0;
    }

    static void ShowUsage()
    {
      string procName = Util.ProcessName;
      Console.WriteLine("Usage: " + procName + " [get|set|quit|termDriver] <BlackBoard name> <key> [value]");
      Console.WriteLine(" The blackboard name and key must be provided as first two args for 'get' or 'set'.");
      Console.WriteLine(" The 'set' operation sets the given value for the key on the given blackboard.");
      Console.WriteLine(" The 'get' operation gets the value for the key from the given blackboard.");
      Console.WriteLine(" The 'quit' operation sends the Exit() command to the blackboard server.");
      Console.WriteLine(" The 'termDriver' operation sends the Term() command to the driver.");
      Console.WriteLine("Environment variable {0} must be set to the <host>:<port>",
        CommConstants.BBAddrEnvVar);
      Console.WriteLine(" of blackboard server for 'get', 'set' or 'quit'.");
      Console.WriteLine("Environment variable {0} must be set to the <host>:<port>",
        CommConstants.DriverAddrEnvVar);
      Console.WriteLine(" of the driver for 'termDriver'");
    }

    static bool ParseArgs(string[] args, out string bbName, out string key,
      out string value, out bool isGet, out bool isQuit, out bool isTermDriver)
    {
      bbName = null;
      key = null;
      value = null;
      isGet = false;
      isQuit = false;
      isTermDriver = false;

      bool success = false;
      if (args != null && args.Length >= 1)
      {
        if (args[0] == "get")
        {
          isGet = true;
          if (args.Length == 3)
          {
            bbName = args[1];
            key = args[2];
            success = true;
          }
        }
        else if (args[0] == "set")
        {
          isGet = false;
          if (args.Length == 4)
          {
            bbName = args[1];
            key = args[2];
            value = args[3];
            success = true;
          }
        }
        else if (args[0] == "quit")
        {
          isQuit = true;
          success = true;
        }
        else if (args[0] == "termDriver")
        {
          isTermDriver = true;
          success = true;
        }
      }
      return success;
    }

    static string GetServerAddress(string envVar)
    {
      string serverAddr = Util.GetEnvironmentVariable(envVar);
      if (serverAddr == null || serverAddr.Length == 0)
      {
        Console.WriteLine("{0}: {1} not set", Util.ProcessName, envVar);
        Console.WriteLine();
        ShowUsage();
        Environment.Exit(1);
      }
      return serverAddr;
    }
  }
}
