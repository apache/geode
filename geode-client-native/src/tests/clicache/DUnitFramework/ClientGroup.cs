//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;

namespace GemStone.GemFire.DUnitFramework
{
  public delegate object ClientResultAggregator(List<object> objList);

  public class ClientGroup : ClientBase
  {
    #region Public accessors

    public bool Managed
    {
      get
      {
        return m_managed;
      }
      set
      {
        m_managed = value;
      }
    }

    public List<ClientBase> Clients
    {
      get
      {
        return m_clients;
      }
    }

    #endregion

    #region Public events

    public event UnitFnMethod<ClientBase> ClientTaskStarted;
    public event UnitFnMethod<ClientBase, Exception> ClientTaskDone;

    #endregion

    private List<ClientBase> m_clients;
    private bool m_managed;
    private ClientResultAggregator m_aggregator;
    private const int ClientWaitLoopMillis = 100;

    public ClientGroup(bool managed)
    {
      m_clients = new List<ClientBase>();
      m_managed = managed;
      m_aggregator = null;
    }

    public static ClientGroup Create(bool managed)
    {
      return new ClientGroup(managed);
    }

    public static ClientGroup Create(
      UnitFnMethodR<ClientBase> clientCreateDeleg, int num)
    {
      ClientGroup group = new ClientGroup(true);
      group.AddP(clientCreateDeleg, null, num);
      return group;
    }

    public void Add(params ClientBase[] clients)
    {
      if (clients != null)
      {
        foreach (ClientBase client in clients)
        {
          m_clients.Add(client);
        }
      }
    }

    public void Add(List<ClientBase> clients)
    {
      if (clients != null)
      {
        m_clients.AddRange(clients);
      }
    }

    public void Add(UnitFnMethodR<ClientBase> clientCreateDeleg, int num)
    {
      AddP(clientCreateDeleg, null, num);
    }

    public void Add<T1>(UnitFnMethodR<ClientBase, T1> clientCreateDeleg,
      int num, T1 param1)
    {
      AddP(clientCreateDeleg, new object[] { param1 }, num);
    }

    public void Add<T1, T2>(UnitFnMethodR<ClientBase, T1, T2> clientCreateDeleg,
      int num, T1 param1, T2 param2)
    {
      AddP(clientCreateDeleg, new object[] { param1, param2 }, num);
    }

    public void Add<T1, T2, T3>(UnitFnMethodR<ClientBase, T1, T2, T3> clientCreateDeleg,
      int num, T1 param1, T2 param2, T3 param3)
    {
      AddP(clientCreateDeleg, new object[] { param1, param2, param3 }, num);
    }

    public void Add<T1, T2, T3, T4>(UnitFnMethodR<ClientBase, T1, T2, T3, T4> clientCreateDeleg,
      int num, T1 param1, T2 param2, T3 param3, T4 param4)
    {
      AddP(clientCreateDeleg,
        new object[] { param1, param2, param3, param4 }, num);
    }

    public void Add<T1, T2, T3, T4, T5>(UnitFnMethodR<ClientBase, T1, T2, T3, T4, T5> clientCreateDeleg,
      int num, T1 param1, T2 param2, T3 param3, T4 param4, T5 param5)
    {
      AddP(clientCreateDeleg,
        new object[] { param1, param2, param3, param4, param5 }, num);
    }

    public void AddTaskHandlers(UnitFnMethod<ClientBase> taskStartedHandler,
      UnitFnMethod<ClientBase, Exception> taskDoneHandler)
    {
      ClientTaskStarted += taskStartedHandler;
      ClientTaskDone += taskDoneHandler;
    }

    public void RemoveTaskHandlers(UnitFnMethod<ClientBase> taskStartedHandler,
      UnitFnMethod<ClientBase, Exception> taskDoneHandler)
    {
      ClientTaskStarted -= taskStartedHandler;
      ClientTaskDone -= taskDoneHandler;
    }

    public void SetAggregator(ClientResultAggregator aggregator)
    {
      m_aggregator = aggregator;
    }

    #region Private methods

    private void AddP(Delegate clientCreateDeleg, object[] paramList, int num)
    {
      lock (((ICollection)m_clients).SyncRoot)
      {
        for (int i = 1; i <= num; i++)
        {
          m_clients.Add(
            (ClientBase)clientCreateDeleg.DynamicInvoke(paramList));
        }
      }
    }

    #endregion

    #region ClientBase Members

    public override string ID
    {
      get
      {
        string baseId = base.ID;
        if (baseId != null && baseId.Length > 0) {
          return baseId;
        }
        StringBuilder sb = new StringBuilder();
        foreach (ClientBase client in m_clients) {
          if (sb.Length > 0) {
            sb.Append(',');
          }
          sb.Append(client.ID);
        }
        return sb.ToString();
      }
      set
      {
        base.ID = value;
      }
    }

    public override void RemoveObjectID(int objectID)
    {
      foreach (ClientBase client in m_clients)
      {
        client.RemoveObjectID(objectID);
      }
    }

    public override void CallFn(Delegate deleg, object[] paramList)
    {
      if (m_clients.Count > 1)
      {
        UnitFnMethod<Delegate, object[]> invokeDeleg;
        IAsyncResult asyncResult;
        List<IAsyncResult> invokeList = new List<IAsyncResult>();
        List<UnitFnMethod<Delegate, object[]>> invokeDelegList =
          new List<UnitFnMethod<Delegate, object[]>>();
        foreach (ClientBase client in m_clients)
        {
          invokeDeleg = client.CallFn;
          asyncResult = invokeDeleg.BeginInvoke(deleg, paramList, null, null);
          client.IncrementTasksRunning();
          if (ClientTaskStarted != null)
          {
            ClientTaskStarted.Invoke(client);
          }
          invokeDelegList.Add(invokeDeleg);
          invokeList.Add(asyncResult);
        }
        bool clientsRunning = true;
        Dictionary<ClientBase, bool> doneTasks =
          new Dictionary<ClientBase, bool>();
        while (clientsRunning)
        {
          clientsRunning = false;
          for (int i = 0; i < m_clients.Count; i++)
          {
            ClientBase client = m_clients[i];
            if (!doneTasks.ContainsKey(client))
            {
              clientsRunning = true;
              invokeDeleg = invokeDelegList[i];
              asyncResult = invokeList[i];
              try
              {
                if (asyncResult.AsyncWaitHandle.WaitOne(
                  ClientWaitLoopMillis, false))
                {
                  doneTasks.Add(client, true);
                  client.DecrementTasksRunning();
                  invokeDeleg.EndInvoke(asyncResult);
                  if (ClientTaskDone != null)
                  {
                    ClientTaskDone.Invoke(client, null);
                  }
                }
              }
              catch (Exception ex)
              {
                if (ClientTaskDone != null)
                {
                  ClientTaskDone.Invoke(client, ex);
                }
              }
            }
          }
        }
      }
      else if (m_clients.Count == 1)
      {
        ClientBase client = m_clients[0];
        try
        {
          client.IncrementTasksRunning();
          if (ClientTaskStarted != null)
          {
            ClientTaskStarted.Invoke(client);
          }
          client.CallFn(deleg, paramList);
          if (ClientTaskDone != null)
          {
            ClientTaskDone.Invoke(client, null);
          }
        }
        catch (Exception ex)
        {
          if (ClientTaskDone != null)
          {
            ClientTaskDone.Invoke(client, ex);
          }
        }
        client.DecrementTasksRunning();
      }
    }

    public override object CallFnR(Delegate deleg, object[] paramList)
    {
      List<object> resultList = new List<object>();

      if (m_clients.Count > 1)
      {
        UnitFnMethodR<object, Delegate, object[]> invokeDeleg;
        IAsyncResult asyncResult;
        List<IAsyncResult> invokeList = new List<IAsyncResult>();
        List<UnitFnMethodR<object, Delegate, object[]>> invokeDelegList =
          new List<UnitFnMethodR<object, Delegate, object[]>>();
        foreach (ClientBase client in m_clients)
        {
          invokeDeleg = client.CallFnR;
          asyncResult = invokeDeleg.BeginInvoke(deleg, paramList, null, null);
          client.IncrementTasksRunning();
          if (ClientTaskStarted != null)
          {
            ClientTaskStarted.Invoke(client);
          }
          invokeDelegList.Add(invokeDeleg);
          invokeList.Add(asyncResult);
        }
        bool clientsRunning = true;
        Dictionary<ClientBase, bool> doneTasks =
          new Dictionary<ClientBase, bool>();
        while (clientsRunning)
        {
          clientsRunning = false;
          for (int i = 0; i < m_clients.Count; i++)
          {
            ClientBase client = m_clients[i];
            if (!doneTasks.ContainsKey(client))
            {
              clientsRunning = true;
              invokeDeleg = invokeDelegList[i];
              asyncResult = invokeList[i];
              try
              {
                if (asyncResult.AsyncWaitHandle.WaitOne(
                  ClientWaitLoopMillis, false))
                {
                  doneTasks.Add(client, true);
                  client.DecrementTasksRunning();
                  resultList.Add(invokeDeleg.EndInvoke(asyncResult));
                  if (ClientTaskDone != null)
                  {
                    ClientTaskDone.Invoke(client, null);
                  }
                }
              }
              catch (Exception ex)
              {
                if (ClientTaskDone != null)
                {
                  ClientTaskDone.Invoke(client, ex);
                }
              }
            }
          }
        }
      }
      else if (m_clients.Count == 1)
      {
        ClientBase client = m_clients[0];
        try
        {
          client.IncrementTasksRunning();
          if (ClientTaskStarted != null)
          {
            ClientTaskStarted.Invoke(client);
          }
          resultList.Add(client.CallFnR(deleg, paramList));
          if (ClientTaskDone != null)
          {
            ClientTaskDone.Invoke(client, null);
          }
        }
        catch (Exception ex)
        {
          if (ClientTaskDone != null)
          {
            ClientTaskDone.Invoke(client, ex);
          }
        }
        client.DecrementTasksRunning();
      }

      if (m_aggregator != null)
      {
        return m_aggregator(resultList);
      }
      else
      {
        return resultList[0];
      }
    }

    public override string HostName
    {
      get
      {
        return "localhost";
      }
    }

    public override void SetLogFile(string logPath)
    {
      foreach (ClientBase client in m_clients)
      {
        client.SetLogFile(logPath);
      }
    }

    public override void SetLogLevel(Util.LogLevel logLevel)
    {
      foreach (ClientBase client in m_clients)
      {
        client.SetLogLevel(logLevel);
      }
    }

    /// <summary>
    /// Dump the stack trace for all the clients that are stuck
    /// i.e. a CallFn/CallFnR is executing on them.
    /// To get the dump of all the clients use <see cref="DumpStackTraceAll"/>.
    /// </summary>
    public override void DumpStackTrace()
    {
      foreach (ClientBase client in m_clients)
      {
        if (!TaskRunning || client.TaskRunning)
        {
          client.DumpStackTrace();
        }
      }
    }

    public override void ForceKill(int waitMillis)
    {
      lock (((ICollection)m_clients).SyncRoot)
      {
        if (waitMillis > 0)
        {
          UnitFnMethod<int> timeoutDeleg;
          IAsyncResult asyncResult;
          List<IAsyncResult> timeoutList = new List<IAsyncResult>();
          List<UnitFnMethod<int>> timeoutDelegList =
            new List<UnitFnMethod<int>>();

          foreach (ClientBase client in m_clients)
          {
            timeoutDeleg = client.ForceKill;
            asyncResult = timeoutDeleg.BeginInvoke(waitMillis, null, null);
            timeoutDelegList.Add(timeoutDeleg);
            timeoutList.Add(asyncResult);
          }
          for (int i = 0; i < m_clients.Count; i++)
          {
            timeoutDeleg = timeoutDelegList[i];
            asyncResult = timeoutList[i];
            try
            {
              asyncResult.AsyncWaitHandle.WaitOne();
              timeoutDeleg.EndInvoke(asyncResult);
            }
            catch (Exception)
            {
              // Some exception in invoking the kill -- just ignore.
            }
          }
        }
        else
        {
          foreach (ClientBase client in m_clients)
          {
            client.ForceKill(0);
          }
        }
      }
    }

    public override ClientBase CreateNew(string clientId)
    {
      ClientGroup newGroup = new ClientGroup(true);
      foreach (ClientBase client in m_clients)
      {
        newGroup.m_clients.Add(client.CreateNew(clientId));
      }
      return newGroup;
    }

    public override void TestCleanup()
    {
      lock (((ICollection)m_clients).SyncRoot)
      {
        if (m_managed)
        {
          foreach (ClientBase client in m_clients)
          {
            client.TestCleanup();
          }
        }
      }
    }

    public override void Exit()
    {
      lock (((ICollection)m_clients).SyncRoot)
      {
        if (m_managed)
        {
          foreach (ClientBase client in m_clients)
          {
            client.Exit();
          }
        }
        m_clients.Clear();
      }
    }

    #endregion

    /// <summary>
    /// Dump stack trace for all the clients;
    /// the <see cref="DumpStackTrace"/> function only dumps the stacks for
    /// clients that are stuck.
    /// </summary>
    public void DumpStackTraceAll()
    {
      foreach (ClientBase client in m_clients)
      {
        client.DumpStackTrace();
      }
    }

    /// <summary>
    /// Sets the log file name with client name appended to each file.
    /// </summary>
    /// <param name="logPath">
    /// The path of the log file.
    /// The actual log file name shall be of the form {logPath}_{ClientId}.log
    /// </param>
    public void SetNameLogFile(string logPath)
    {
      foreach (ClientBase client in m_clients)
      {
        client.SetLogFile(logPath + '_' + client.ID + ".log");
      }
    }
  }
}
