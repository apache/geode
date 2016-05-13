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
using System.Reflection;
using System.Runtime.Remoting.Channels;
using System.Text;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLauncher
{
  using GemStone.GemFire.DUnitFramework;

  class LauncherComm : MarshalByRefObject, IClientCommV2
  {
    #region IClientComm Members
    
    public void SetLogFile(string logFile)
    {
      Util.LogFile = logFile;
    }

    public void SetLogLevel(Util.LogLevel logLevel)
    {
      Util.CurrentLogLevel = logLevel;
    }

    
    public void DumpStackTrace()
    {
      StringBuilder dumpStr = new StringBuilder();
      try
      {
        dumpStr.Append(Util.MarkerString);
        dumpStr.Append("Dumping managed stack for process [" + Util.PID + "]:");
        dumpStr.Append(Util.MarkerString);
        lock (((ICollection)UnitThread.GlobalUnitThreads).SyncRoot)
        {
          foreach (Thread thread in UnitThread.GlobalUnitThreads.Keys)
          {
            dumpStr.Append(UnitThread.GetStackTrace(thread));
            dumpStr.Append(Environment.NewLine + Environment.NewLine);
          }
        }
        dumpStr.Append(Util.MarkerString);
        Util.Log(dumpStr.ToString());
        dumpStr.Length = 0;
        dumpStr.Append("Dumping native stack for process [" + Util.PID + "]:");
        dumpStr.Append(Util.MarkerString);
        Process cdbProc;
        // Expect to find 'cdb.pl' in the same dir as FwkClient.exe
        if (Util.StartProcess("perl", Util.AssemblyDir + "/cdb.pl " + Util.PID,
          false, null, true, false, true, out cdbProc))
        {
          StreamReader cdbRead = cdbProc.StandardOutput;
          StreamReader cdbReadErr = cdbProc.StandardError;
          dumpStr.Append(cdbRead.ReadToEnd());
          dumpStr.Append(cdbReadErr.ReadToEnd());
          cdbProc.WaitForExit();
          cdbRead.Close();
          cdbReadErr.Close();
        }
        else
        {
          dumpStr.Append("Failed to start: perl ");
          dumpStr.Append(Util.AssemblyDir + "/cdb.pl " + Util.PID);
        }
      }
      catch (Exception ex)
      {
        dumpStr.Append("Exception while invoking cdb.pl: ");
        dumpStr.Append(ex.ToString());
      }
      dumpStr.Append(Util.MarkerString + Environment.NewLine);
      Util.Log(dumpStr.ToString());
    }

    public void Exit(bool force)
    {
      Environment.Exit(0);
      Util.Process.Kill();
    }
    
    public void TestCleanup()
    {
      throw new NotImplementedException();
    }
    public void Call(int objectId, string assemblyName, string typeName, string methodName, params object[] paramList)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public object CallR(int objectId, string assemblyName, string typeName, string methodName, params object[] paramList)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }

    public void RemoveObjectID(int objectId)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }
    
    public bool CreateNew(string clientId, int port)
    {
      throw new NotImplementedException("The method or operation is not implemented.");
    }
    #endregion


    #region IClientCommV2 Members

    public bool LaunchNewClient(string clientPath, string args, out Process proc)
    {
      proc = null;
      if (!clientPath.EndsWith("FwkClient.exe"))
      {
        Util.Log("Launcher cannot launch the process {0}", clientPath);
        return false;
      }
      
      Util.Log("Starting client {0} with args {1}.", clientPath, args);
      if (!Util.StartProcess(clientPath, args, false, null,
        false, false, false, out proc))
      {
        Util.Log("Failed to start client");
        return false;
      }
      Util.Log("Started client {0}", clientPath);
      return true;
    }
    #endregion

    public Process GetClientProcess()
    {
      return Util.Process;
    }
  }
}
