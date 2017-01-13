//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Xml;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;

  public class FwkTask
  {
    #region Private members

    private Delegate m_delegate;
    private Delegate m_pushTaskNameDelegate;
    private Delegate m_endDelegate;
    private List<string> m_clients;
    private TimeBomb m_timeBomb;
    private string m_class;
    private int m_timeoutMillis;
    private int m_timesToRun;
    private int m_threadCount;
    private bool m_parallel;
    private bool m_continueOnError;
    private ClientGroup m_clientGroup;
    private List<string> m_localDataNames;
    private UnitFnMethod<FwkTask, ClientBase> m_taskStartedHandler;
    private UnitFnMethod<FwkTask, ClientBase, Exception> m_taskDoneHandler;
    private bool m_taskStatus;

    #endregion

    #region Private constants and static members

    private const string PushTaskNameMethod = "PushTaskName";
    private const string EndTaskMethod = "EndTask";
    private const int DefaultTimeoutMillis = 600000;
    private static string[] ThreadCountKeys = { "threadCount", "numThreads" };
    private static Dictionary<string, int> m_GlobalTaskNames =
      new Dictionary<string, int>();
    private static Dictionary<string, Assembly> m_AssemblyMap =
      new Dictionary<string, Assembly>();
    private const string ClientCountKey = "clientCount";

    private const string NameAttrib = "name";
    private const string MethodAttrib = "action";
    private const string ClassAttrib = "class";
    private const string ContainerAttrib = "container";
    private const string WaitTimeAttrib = "waitTime";
    private const string TimesToRunAttrib = "timesToRun";
    private const string ThreadCountAttrib = "threadCount";
    private const string ParallelAttrib = "parallel";
    private const string ContinueAttrib = "continueOnError";
    private const string ArgTypes = "argTypes";

    #endregion

    #region Public accessors

    public static Dictionary<string, int> GlobalTaskNames
    {
      get
      {
        return m_GlobalTaskNames;
      }
    }

    public List<string> ClientNames
    {
      get
      {
        return m_clients;
      }
    }

    public string Name
    {
      get
      {
        return (m_timeBomb == null ? null : m_timeBomb.TaskName);
      }
    }

    public string Class
    {
      get
      {
        return m_class;
      }
    }

    public int Timeout
    {
      get
      {
        return m_timeoutMillis;
      }
    }

    public int TimesToRun
    {
      get
      {
        return m_timesToRun;
      }
    }

    public int ThreadCount
    {
      get
      {
        return m_threadCount;
      }
    }

    public bool Parallel
    {
      get
      {
        return m_parallel;
      }
    }

    public bool ContinueOnError
    {
      get
      {
        return m_continueOnError;
      }
    }

    #endregion

    #region Private methods

    private void HandleTaskStarted(ClientBase client)
    {
      if (m_taskStartedHandler != null)
      {
        m_taskStartedHandler(this, client);
      }
    }

    private void HandleTaskDone(ClientBase client, Exception ex)
    {
      if (m_taskDoneHandler != null)
      {
        m_taskStatus = (ex == null);
        m_taskDoneHandler(this, client, ex);
      }
    }

    #endregion

    #region Public methods

    public FwkTask(XmlNode node, string testName,
      UnitFnMethod<FwkTask, ClientBase> taskStartedHandler,
      UnitFnMethod<FwkTask, ClientBase, Exception> taskDoneHandler)
    {
      if (node == null)
      {
        throw new IllegalArgException("Null node for FwkTask constructor.");
      }

      #region Read the attributes

      string taskName = string.Empty;
      string containerStr = null;
      string methodName = null;
      string typeAttrib = null;
      m_timesToRun = 1;
      m_threadCount = 1;
      m_parallel = false;
      m_timeoutMillis = DefaultTimeoutMillis;
      m_continueOnError = false;
      m_class = null;

      XmlAttributeCollection xmlAttribs = node.Attributes;
      if (xmlAttribs != null)
      {
        foreach (XmlAttribute xmlAttrib in xmlAttribs)
        {
          switch (xmlAttrib.Name)
          {
            case NameAttrib:
              taskName = xmlAttrib.Value;
              break;
            case ArgTypes:
              typeAttrib = xmlAttrib.Value;
              break;
            case ContainerAttrib:
              containerStr = xmlAttrib.Value;
              break;
            case MethodAttrib:
              methodName = xmlAttrib.Value;
              break;
            case TimesToRunAttrib:
              m_timesToRun = XmlNodeReaderWriter.String2Int(xmlAttrib.Value, 1);
              break;
            case ThreadCountAttrib:
              m_threadCount = XmlNodeReaderWriter.String2Int(xmlAttrib.Value, 1);
              break;
            case ParallelAttrib:
              m_parallel = XmlNodeReaderWriter.String2Bool(xmlAttrib.Value, false);
              break;
            case WaitTimeAttrib:
              m_timeoutMillis = XmlNodeReaderWriter.String2Seconds(xmlAttrib.Value,
                DefaultTimeoutMillis) * 1000;
              break;
            case ContinueAttrib:
              m_continueOnError = XmlNodeReaderWriter.String2Bool(xmlAttrib.Value, false);
              break;
            case ClassAttrib:
              m_class = xmlAttrib.Value;
              break;
            default:
              throw new IllegalArgException("Unknown attribute '" +
                xmlAttrib.Name + "' found in '" + node.Name +
                "' node.");
          }
        }
      }
      int taskNum = 1;
      if (m_GlobalTaskNames.ContainsKey(taskName))
      {
        taskNum = m_GlobalTaskNames[taskName] + 1;
      }
      m_GlobalTaskNames[taskName] = taskNum;
      taskName += '_' + taskNum.ToString();

      #endregion

      m_timeBomb = new TimeBomb();
      m_timeBomb.TaskName = testName + '.' + taskName;

      #region Create a delegate by loading the assembly

      Assembly loadAssmb = null;
      string typeName = null;
      if (containerStr != null && methodName != null)
      {
        object inst = null;
        int dotIndx;
        if ((dotIndx = containerStr.IndexOf('.')) < 0)
        {
          Type myType = this.GetType();
          loadAssmb = myType.Assembly;
          typeName = myType.Namespace + '.' + containerStr;
          Util.Log(Util.LogLevel.Info, "Assembly {0} loaded and typename is {1}", loadAssmb, typeName);
        }
        else
        {
          string assmbName = containerStr.Substring(0, dotIndx);
          if (!m_AssemblyMap.TryGetValue(assmbName, out loadAssmb))
          {
            try
            {
              loadAssmb = Assembly.Load(assmbName);
              Util.Log(Util.LogLevel.Info, "Assembly {0} loaded ", assmbName);
            }
            catch (Exception e)
            {
              throw new IllegalArgException("Cannot load assembly '" +
                assmbName + "' for task: " + m_timeBomb.TaskName +
                " exception: " + e);
            }
            m_AssemblyMap.Add(assmbName, loadAssmb);
          }
          typeName = containerStr.Substring(dotIndx + 1);
        }
        //string typeAttrib;
        if (loadAssmb != null)
        {
            if (typeAttrib == null)
            {
              inst = loadAssmb.CreateInstance(typeName, true);
            }
            else
            {
              //typeAttrib = "GemStone.GemFire.Cache.Tests.ArrayOfByte,GemStone.GemFire.Cache.Tests.ArrayOfByte";
              //typeAttrib = "System.int,System.Int32";
              string[] typeNames = typeAttrib.Split(',');
              string mangledName = typeName + "`" + typeNames.Length.ToString();

              //Type type = loadAssmb.GetType(mangledName, true, true);
              Type[] types = new Type[typeNames.Length];
             for (int index = 0; index < typeNames.Length; ++index)
              {
                string typName = typeNames[index].Trim();
                if (typName == "int" || typName == "Int32" || typName == "string" ||
                  typName == "String" || typName == "byte[]" || typName == "Byte[]"
                  || typName == "string[]" || typName == "String[]" || typName == "Object" 
                  || typName == "object")
                {
                  if (typName.Equals("int"))
                    typName = "Int32";
                  else if (typName.Equals("string"))
                    typName = "String";
                  else if (typName.Equals("string[]"))
                    typName = "String[]";
                  else if (typName.Equals("byte[]"))
                    typName = "Byte[]";
                  else if (typName.Equals("object"))
                    typName = "Object";
                  typName = "System." + typName;
                  //Util.Log("rjk: FwkTask: typeAttrib 33 argname {0}", typName);
                  types[index] = Type.GetType(typName.Trim());
                  //Util.Log("rjk: FwkTask: typeAttrib 34 argname {0}", typName);
                }
                else
                {
                  typName = "GemStone.GemFire.Cache.Tests.NewAPI." + typName;
                  types[index] = loadAssmb.GetType(typName.Trim(), true, true);
                  //Util.Log("rjk: FwkTask: typeAttrib for userobject 34 argname {0}", typName);
                }
              }

              Type type = loadAssmb.GetType(mangledName, true, true).MakeGenericType(types);
              inst = type.GetConstructor(System.Type.EmptyTypes).Invoke(null);
              
            }
          
        }
        if (inst != null)
        {
          try
          {
            MethodInfo mInfo = inst.GetType().GetMethod(methodName,
              BindingFlags.IgnoreCase | BindingFlags.Public |
              BindingFlags.Static | BindingFlags.FlattenHierarchy |
              BindingFlags.Instance);
            m_delegate = Delegate.CreateDelegate(typeof(UnitFnMethod), inst,
              mInfo, true);
          }
          catch (Exception ex)
          {
            throw new IllegalArgException(
              "Exception while creating delegate [" + methodName + "]: " + ex);
          }
          m_pushTaskNameDelegate = Delegate.CreateDelegate(
            typeof(UnitFnMethod<string>), inst, PushTaskNameMethod, true);
          m_endDelegate = Delegate.CreateDelegate(
            typeof(UnitFnMethod), inst, EndTaskMethod, true);
        }
      }
      if (m_delegate == null)
      {
        throw new IllegalArgException("Cannot create delegate '" +
          methodName + "' for task: " + m_timeBomb.TaskName);
      }

      #endregion

      #region Add the clients

      m_clients = new List<string>();
      m_clientGroup = new ClientGroup(false);
      m_clients = FwkClient.ReadClientNames(node, false);
      List<ClientBase> clients = FwkClient.GetClients(m_clients);
      m_clientGroup.Add(clients);
      m_timeBomb.AddClients(new ClientBase[] { m_clientGroup });
      m_taskStartedHandler = taskStartedHandler;
      m_taskDoneHandler = taskDoneHandler;
      int clientCount = m_clients.Count;

      #endregion

      #region Add any data

      m_localDataNames = new List<string>();
      Dictionary<string, FwkData> data = FwkData.ReadDataNodes(node);
      // Task specific data is written as <taskname>.<key> to avoid
      // overwriting the global data, since that needs to be preserved
      // across tasks.
      if (m_threadCount > 1)
      {
        // We shall treat 'threadCount' and 'numThreads' as equivalent,
        // i.e. if 'threadCount' is defined for a task then 'numThreads'
        // shall also be written as data.
        foreach (string threadKey in ThreadCountKeys)
        {
          m_localDataNames.Add(threadKey);
          Util.BBSet(Name, threadKey,
            new FwkData(m_threadCount.ToString(), null, DataKind.String));
        }
      }
      if (clientCount > 0)
      {
        // Overwrite the clientCount (if any) with the actual value.
        Util.BBSet(Name, ClientCountKey,
        new FwkData(clientCount.ToString(), null, DataKind.String));
      }
      foreach (KeyValuePair<string, FwkData> pair in data)
      {
        m_localDataNames.Add(pair.Key);
        Util.BBSet(Name, pair.Key, pair.Value);
      }

      #endregion
    }

    public void AddClients(params ClientBase[] clients)
    {
      m_clientGroup.Add(clients);
    }

    public void SetClients(params ClientBase[] clients)
    {
      m_clientGroup.Clients.Clear();
      m_clientGroup.Add(clients);
    }

    public bool Execute()
    {
      m_taskStatus = true;
      m_timeBomb.Start(m_timeoutMillis);
      try
      {
        if (m_pushTaskNameDelegate != null)
        {
          m_clientGroup.CallFn(m_pushTaskNameDelegate,
              new object[] { m_timeBomb.TaskName });
        }
        if (m_taskStartedHandler != null || m_taskDoneHandler != null)
        {
          m_clientGroup.AddTaskHandlers(HandleTaskStarted, HandleTaskDone);
        }
        FwkReadData read = new FwkReadData();
        read.PushTaskName(Name);
        string appDomain = read.GetStringValue("appdomain");
        if (appDomain != null && appDomain.Length > 0) {
          m_clientGroup.CallFn(m_delegate, new object[] { Util.AppDomainPrefix
          + appDomain });
        }
        else {
          m_clientGroup.CallFn(m_delegate, null);
        }
        if (m_taskStartedHandler != null || m_taskDoneHandler != null)
        {
          m_clientGroup.RemoveTaskHandlers(HandleTaskStarted, HandleTaskDone);
        }
      }
      finally
      {
        m_timeBomb.Diffuse();
      }
      return m_taskStatus;
    }

    public void ClearData()
    {
      foreach (string dataName in m_localDataNames)
      {
        Util.BBRemove(Name, dataName);
      }
    }

    public void EndTask()
    {
      if (m_endDelegate != null)
      {
        m_clientGroup.CallFn(m_endDelegate, null);
      }
      m_clientGroup.RemoveObject(m_delegate.Target);
    }

    public void SetLogFile(string logPath)
    {
      m_clientGroup.SetNameLogFile(logPath);
    }

    public void SetLogLevel(Util.LogLevel logLevel)
    {
      m_clientGroup.SetLogLevel(logLevel);
    }

    public string Dump()
    {
      StringBuilder dumpStr = new StringBuilder();

      dumpStr.Append(Environment.NewLine + "The task name is: " +
        m_timeBomb.TaskName);
      dumpStr.Append(string.Format("{0}The function to be executed: " +
        "'{1}' in class '{2}'", Environment.NewLine, m_delegate.Method.Name,
        m_delegate.Method.ReflectedType.FullName));
      dumpStr.Append(Environment.NewLine + "The class of the task: " +
        m_class);
      dumpStr.Append(Environment.NewLine + "The threadCount is: " +
        m_threadCount);
      dumpStr.Append(Environment.NewLine + "Parallel attribute is: " +
        m_parallel);
      dumpStr.Append(Environment.NewLine + "Times to run is: " +
        m_timesToRun);
      dumpStr.Append(Environment.NewLine + "The clients are: ");
      foreach (string clientName in m_clients)
      {
        dumpStr.Append(Environment.NewLine + "\t" + clientName);
      }
      dumpStr.Append(Environment.NewLine + "Continue on error is: " +
        m_continueOnError);
      dumpStr.Append(Environment.NewLine + "The local data names are: ");
      foreach (string dataName in m_localDataNames)
      {
        dumpStr.Append(Environment.NewLine + '\t' + dataName);
      }
      dumpStr.Append(Environment.NewLine + "The timeout is: " +
        m_timeoutMillis);
      return dumpStr.ToString();
    }

    #endregion
  }
}
