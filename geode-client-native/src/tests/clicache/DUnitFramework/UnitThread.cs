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
using System.Threading;

#pragma warning disable 618

namespace GemStone.GemFire.DUnitFramework
{
  using NUnit.Framework;

  public class UnitThread : ClientBase
  {
    #region Private members

    private ManualResetEvent m_start;
    private ManualResetEvent m_end;
    private Thread m_thread;
    private volatile bool m_endThread;
    private Delegate m_currDeleg;
    private object[] m_currParams;
    private object m_result;
    private Exception m_exception;

    private const string _m_disposeExceptionStr = "Thread has exited since the object has been Disposed";

    #endregion

    /// <summary>
    /// A global dictionary of threads maintained so that the list of managed
    /// threads (at least created using <c>UnitThread</c> can be obtained.
    /// </summary>
    public static Dictionary<Thread, bool> GlobalUnitThreads
      = new Dictionary<Thread, bool>();

    public UnitThread()
    {
      m_start = new ManualResetEvent(false);
      m_end = new ManualResetEvent(true);
      m_endThread = false;
      m_currDeleg = null;
      m_thread = new Thread(new ThreadStart(ThreadMain));
      m_thread.Start();
      lock (((ICollection)GlobalUnitThreads).SyncRoot)
      {
        GlobalUnitThreads.Add(m_thread, true);
      }
    }

    public static UnitThread Create()
    {
      return new UnitThread();
    }

    private void ThreadMain()
    {
      while (true)
      {
        m_start.WaitOne();
        if (m_endThread)
        {
          lock (((ICollection)GlobalUnitThreads).SyncRoot)
          {
            GlobalUnitThreads.Remove(m_thread);
          }
          return;
        }
        m_start.Reset();
        m_exception = null;
        if (m_currDeleg != null)
        {
          try
          {
            if (m_result != null)
            {
              m_result = m_currDeleg.DynamicInvoke(m_currParams);
            }
            else
            {
              m_currDeleg.DynamicInvoke(m_currParams);
            }
          }
          catch (Exception ex)
          {
            m_exception = ex;
          }
        }
        m_end.Set();
      }
    }

    #region ClientBase Members

    public override void RemoveObjectID(int objectID)
    {
      // TODO: Nothing for now.
    }

    public override void CallFn(Delegate deleg, params object[] paramList)
    {
      object result = null;
      CallFnP(deleg, paramList, ref result);
    }

    public override object CallFnR(Delegate deleg, params object[] paramList)
    {
      object result = true;
      CallFnP(deleg, paramList, ref result);
      return result;
    }

    public override string HostName
    {
      get
      {
        return "localhost";
      }
    }

    public override void SetLogFile(string logFile)
    {
      // Nothing for now since this shall be done anyway at the process level
      //Util.LogFile = logFile;
    }

    public override void SetLogLevel(Util.LogLevel logLevel)
    {
      // Nothing for now since this shall be done anyway at the process level
      //Util.CurrentLogLevel = logLevel;
    }

    public override void DumpStackTrace()
    {
      string trace = GetStackTrace();
      if (trace != null)
      {
        Util.Log(trace);
      }
    }

    public static string GetStackTrace(Thread thread)
    {
      string traceStr = null;
      try
      {
        thread.Suspend();
      }
      catch
      {
      }
      StackTrace trace = new StackTrace(thread, true);
      traceStr = string.Format("Dumping stack for managed thread [{0}]:{1}{2}",
        thread.ManagedThreadId, Environment.NewLine, trace.ToString());
      try
      {
        thread.Resume();
      }
      catch
      {
      }
      return traceStr;
    }

    public string GetStackTrace()
    {
      if (!m_endThread)
      {
        return GetStackTrace(m_thread);
      }
      return null;
    }

    public override void ForceKill(int waitMillis)
    {
      if (!m_endThread)
      {
        bool failed = true;
        m_endThread = true;
        m_start.Set();
        if (waitMillis > 0 && m_thread.Join(waitMillis))
        {
          failed = false;
        }
        if (failed)
        {
          m_exception = new ClientTimeoutException(
            "Timeout occurred in calling '" + m_currDeleg.Method.ToString() +
            '\'');
          m_end.Set();
        }
      }
    }

    public override ClientBase CreateNew(string clientId)
    {
      ClientBase newClient = new UnitThread();
      newClient.ID = clientId;
      return newClient;
    }

    public override void TestCleanup()
    {
    }

    public override void Exit()
    {
      if (!m_endThread)
      {
        m_endThread = true;
        m_start.Set();
      }
    }

    public override string ID
    {
      get
      {
        return base.ID;
      }
      set
      {
        base.ID = value;
        m_thread.Name = value;
      }
    }

    #endregion

    private void CallFnP(Delegate deleg, object[] paramList, ref object result)
    {
      lock (m_end)
      {
        if (m_endThread)
        {
          throw new ClientExitedException(_m_disposeExceptionStr);
        }
        m_end.WaitOne();
        m_end.Reset();
        m_currDeleg = deleg;
        m_currParams = paramList;
        m_result = result;
        m_start.Set();
      }
      m_end.WaitOne();
      if (m_exception != null)
      {
        throw m_exception;
      }
      if (result != null)
      {
        result = m_result;
      }
    }
  }
}
