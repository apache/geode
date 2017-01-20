/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Xml;

namespace Apache.Geode.DUnitFramework
{
  public class DriverComm : MarshalByRefObject, IDriverComm
  {
    public static UnitFnMethodR<bool, string, string, string>
      WinTaskDeleg = null;
    public static UnitFnMethodR<string, string, string, string,
      Dictionary<string, string>> ShellTaskDeleg = null;
    public static UnitFnMethod TermDeleg = null;

    #region IServerComm Members

    public void Log(string clientId, string prefix, string message)
    {
      string logLine = Util.GetLogLine("CLIENT:" + prefix, message,
        "Client:" + clientId);
      Util.RawLog(logLine);
    }

    public void ClientListening(string clientId)
    {
      lock (((ICollection)UnitProcess.ProcessIDMap).SyncRoot)
      {
        System.Threading.ManualResetEvent clientEvent;
        if (UnitProcess.ProcessIDMap.TryGetValue(clientId, out clientEvent))
        {
          clientEvent.Set();
        }
      }
    }

    public bool RunWinTask(string clientId, string hostName, string taskSpec)
    {
      if (WinTaskDeleg != null)
      {
        return WinTaskDeleg(clientId, hostName, taskSpec);
      }
      return false;
    }

    public string RunShellTask(string clientId, string hostName,
      string shellCmd, Dictionary<string, string> envVars)
    {
      if (ShellTaskDeleg != null)
      {
        return ShellTaskDeleg(clientId, hostName, shellCmd, envVars);
      }
      return null;
    }

    public void Term()
    {
      if (TermDeleg != null)
      {
        TermDeleg();
      }
    }

    #endregion
  }

  public class BBComm : MarshalByRefObject, IBBComm
  {
    public static Dictionary<string, object> KeyValueMap =
      new Dictionary<string, object>();

    #region IBBComm Members

    public void WriteObject(string key, object value)
    {
      lock (((ICollection)KeyValueMap).SyncRoot)
      {
        KeyValueMap[key] = value;
      }
    }

    public int AddInt(string key, int incValue)
    {
      lock (((ICollection)KeyValueMap).SyncRoot)
      {
        object result;
        if (KeyValueMap.TryGetValue(key, out result))
        {
          try
          {
            int iResult = (int)result;
            iResult += incValue;
            KeyValueMap[key] = iResult;
            return iResult;
          }
          catch (InvalidCastException)
          {
            throw new KeyNotFoundException("The value for key '" + key +
              "' is not an integer.");
          }
        }
        else
        {
          throw new KeyNotFoundException("The key '" + key +
            "' does not exist.");
        }
      }
    }

    public int AddOrSetInt(string key, int val)
    {
      int iResult = val;
      lock (((ICollection)KeyValueMap).SyncRoot)
      {
        object result;
        if (KeyValueMap.TryGetValue(key, out result))
        {
          try
          {
            int origVal = (int)result;
            iResult += origVal;
          }
          catch (InvalidCastException)
          {
          }
        }
        KeyValueMap[key] = iResult;
      }
      return iResult;
    }

    public object ReadObject(string key)
    {
      lock (((ICollection)KeyValueMap).SyncRoot)
      {
        object result;
        if (KeyValueMap.TryGetValue(key, out result))
        {
          return result;
        }
        else
        {
          throw new KeyNotFoundException("The key '" + key +
            "' does not exist.");
        }
      }
    }

    public void RemoveObject(string key)
    {
      lock (((ICollection)KeyValueMap).SyncRoot)
      {
        if (KeyValueMap.ContainsKey(key))
        {
          KeyValueMap.Remove(key);
        }
      }
    }

    public void Clear()
    {
      lock (((ICollection)KeyValueMap).SyncRoot)
      {
        // to print the BB data in Driver log
        /*
        foreach (KeyValuePair<string, object> bbMap in KeyValueMap)
        {
          Util.Log("BB dump Key = {0} , Value = {1} :", bbMap.Key, bbMap.Value);
        }
        */
        KeyValueMap.Clear();
      }
    }

    public void Exit()
    {
      Util.Log("Ending the BBServer.");
      Environment.Exit(0);
      Util.Process.Kill();
    }

    #endregion
  }
}
