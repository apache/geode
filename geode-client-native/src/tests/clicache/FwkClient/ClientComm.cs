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

namespace GemStone.GemFire.Cache.FwkClient
{
  using GemStone.GemFire.DUnitFramework;

  class ClientComm : MarshalByRefObject, IClientComm
  {
    private static Dictionary<string, object> m_InstanceMap =
      new Dictionary<string, object>();
    private static Dictionary<string, AppDomain> m_appDomainMap =
      new Dictionary<string, AppDomain>();
    private static volatile bool m_exiting = false;

    private void CallP(int objectId, string assemblyName, string typeName,
      string methodName, object[] paramList, ref object result)
    {
      if (m_exiting) {
        return;
      }
      string objID = objectId.ToString();
      object typeInst = null;
      string appDomain = null;
      // assume first argument to be the name of AppDomain
      if (paramList != null && paramList.Length > 0) {
        object param = paramList[paramList.Length - 1];
        if (param is string && (appDomain = (string)param).StartsWith(
            Util.AppDomainPrefix)) {
          appDomain = appDomain.Substring(Util.AppDomainPrefix.Length);
          if (appDomain.Length > 0) {
            objID = Util.AppDomainPrefix + ":" + appDomain + objID;
            object[] newParams = new object[paramList.Length - 1];
            Array.Copy(paramList, 0, newParams, 0, paramList.Length - 1);
            paramList = newParams;
          }
          else {
            appDomain = null;
          }
        }
        else {
          appDomain = null;
        }
      }
      lock (((ICollection)m_InstanceMap).SyncRoot) {
        if (m_InstanceMap.ContainsKey(objID)) {
          typeInst = m_InstanceMap[objID];
        }
        else if (appDomain != null)
        {
          AppDomain domain;
          if (!m_appDomainMap.ContainsKey(appDomain)) {
            domain = AppDomain.CreateDomain(appDomain);
            CreateBBServerConnection urlConn = (CreateBBServerConnection)domain.
              CreateInstanceAndUnwrap("FwkClient",
              "GemStone.GemFire.Cache.FwkClient.CreateBBServerConnection");
            urlConn.OpenBBServerConnection(ClientProcess.bbUrl);
            urlConn.SetLogFile(Util.LogFile);
            Util.Log("Created app domain {0}",domain);
            m_appDomainMap.Add(appDomain, domain);
          }
          else {
            domain = m_appDomainMap[appDomain];
            CreateBBServerConnection urlConn = (CreateBBServerConnection)domain.
              CreateInstanceAndUnwrap("FwkClient",
              "GemStone.GemFire.Cache.FwkClient.CreateBBServerConnection");
            urlConn.SetLogFile(Util.LogFile);
          }
          try {
            typeInst = domain.CreateInstanceAndUnwrap(assemblyName, typeName);
          } catch (Exception e) {
            Util.Log("Exception thrown for app domain CreateInstanceAndUnwrap : {0} ", e.Message);
          }
          if (typeInst != null) {
            m_InstanceMap[objID] = typeInst;
          }
          else {
            throw new IllegalArgException(
              "FATAL: Could not load class with name: " + typeName);
          }
        }
        else {
          Assembly assmb = null;
          try {
            assmb = Assembly.Load(assemblyName);
          } catch (Exception) {
            throw new IllegalArgException(
              "FATAL: Could not load assembly: " + assemblyName);
          }
          typeInst = assmb.CreateInstance(typeName);
          if (typeInst != null) {
            m_InstanceMap[objID] = typeInst;
          }
          else {
            throw new IllegalArgException(
              "FATAL: Could not load class with name: " + typeName);
          }
        }
      }
      MethodInfo[] mInfos = typeInst.GetType().GetMethods(
        BindingFlags.IgnoreCase | BindingFlags.Instance | BindingFlags.Public
        | BindingFlags.NonPublic | BindingFlags.Static
        | BindingFlags.FlattenHierarchy);
      MethodInfo mInfo = null;
      foreach (MethodInfo method in mInfos) {
        if (method.Name.Equals(methodName,
          StringComparison.CurrentCultureIgnoreCase)) {
          ParameterInfo[] prms = method.GetParameters();
          int paramLen = (paramList != null ? paramList.Length : 0);
          if (paramLen == prms.Length) {
            mInfo = method;
          }
        }
      }
      if (mInfo != null) {
        Thread currentThread = Thread.CurrentThread;
        lock (((ICollection)UnitThread.GlobalUnitThreads).SyncRoot) {
          if (!UnitThread.GlobalUnitThreads.ContainsKey(currentThread)) {
            UnitThread.GlobalUnitThreads.Add(currentThread, true);
          }
        }
        if (result == null) {
          mInfo.Invoke(typeInst, paramList);
        }
        else {
          result = mInfo.Invoke(typeInst, paramList);
        }
        lock (((ICollection)UnitThread.GlobalUnitThreads).SyncRoot) {
          if (!UnitThread.GlobalUnitThreads.ContainsKey(currentThread)) {
            UnitThread.GlobalUnitThreads.Remove(currentThread);
          }
        }
      }
      else {
        throw new IllegalArgException("FATAL: Could not load function with name: " +
          methodName + ", in class: " + typeInst.GetType());
      }
    }

    #region IClientComm Members

    public void Call(int objectId, string assemblyName, string typeName,
      string methodName, params object[] paramList)
    {
      object obj = null;
      CallP(objectId, assemblyName, typeName, methodName, paramList, ref obj);
    }

    public object CallR(int objectId, string assemblyName, string typeName,
      string methodName, params object[] paramList)
    {
      object obj = 0;
      CallP(objectId, assemblyName, typeName, methodName, paramList, ref obj);
      return obj;
    }

    public void RemoveObjectID(int objectId)
    {
      lock (((ICollection)m_InstanceMap).SyncRoot)
      {
        if (m_InstanceMap.ContainsKey(objectId.ToString()))
        {
          m_InstanceMap.Remove(objectId.ToString());
        }
      }
    }

    public void SetLogFile(string logFile)
    {
      Util.LogFile = logFile;
    }

    public void SetLogLevel(Util.LogLevel logLevel)
    {
      Util.CurrentLogLevel = logLevel;
    }

    public bool CreateNew(string clientId, int port)
    {
      string clientPath = Assembly.GetExecutingAssembly().Location;
      string[] args = Environment.GetCommandLineArgs();
      string argStr = string.Empty;
      for (int i = 1; i < args.Length - 1; i++)
      {
        string arg = args[i];
        if (arg.StartsWith("--id="))
        {
          argStr += ("\"--id=" + clientId + "\" ");
        }
        else
        {
          argStr += ('"' + arg + "\" ");
        }
      }
      argStr += port.ToString();
      Process proc;
      return Util.StartProcess(clientPath, argStr, false, null,
        false, false, false, out proc);
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

    public void TestCleanup()
    {
      foreach (UnitFnMethod deleg in Util.RegisteredTestCompleteDelegates)
      {
        try
        {
          Util.Log("Cleaning test on client [{0}]: Calling {1} :: {2}",
            Util.ClientId, deleg.Method.DeclaringType, deleg.Method);
          deleg();
        }
        catch (Exception ex)
        {
          Util.Log(Util.LogLevel.Error, ex.ToString());
        }
      }
    }

    public void Exit(bool force)
    {
      try
      {
        m_exiting = true;
        if (!force)
        {
          foreach (UnitFnMethod deleg in Util.RegisteredTestCompleteDelegates)
          {
            try
            {
              Util.Log("Exiting client [{0}]: Calling {1} :: {2}", Util.ClientId,
                deleg.Method.DeclaringType, deleg.Method);
              deleg();
            }
            catch (Exception ex)
            {
              Util.Log(Util.LogLevel.Error, ex.ToString());
            }
          }
        }
      }
      finally
      {
        Environment.Exit(0);
        Util.Process.Kill();
      }
    }

    public Process GetClientProcess()
    {
      return Util.Process;
    }

    #endregion
  }
}
