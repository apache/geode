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
using System.Collections.Generic;
using System.Threading;

namespace Apache.Geode.Client.FwkLib
{
  using Apache.Geode.DUnitFramework;
  using Apache.Geode.Client;
  public class MyResultCollector<TResult> : Client.IResultCollector<TResult>
  {
    #region Private members
    private bool m_resultReady = false;
    //private CacheableVector m_results = null;
    ICollection<TResult> m_results = null;
    private int m_addResultCount = 0;
    private int m_getResultCount = 0;
    private int m_endResultCount = 0;
    #endregion
    public int GetAddResultCount()
    {
      return m_addResultCount;
    }
    public int GetGetResultCount()
    {
      return m_getResultCount;
    }
    public int GetEndResultCount()
    {
      return m_endResultCount;
    }
    public MyResultCollector()
    {
      m_results = new List<TResult>();
    }
    public void AddResult(TResult result)
    {
      m_addResultCount++;
      //CacheableArrayList rs = result as CacheableArrayList;
      //List<Object> rs = result as List<Object>;
      //for (int i = 0; i < rs.Count; i++)
      //{
      //  m_results.Add(rs[i]);
      //}
      m_results.Add(result);
    }
    public ICollection<TResult> GetResult()
    {
      return GetResult(50);
    }
    public ICollection<TResult> GetResult(UInt32 timeout)
    {
      m_getResultCount++;
      if (m_resultReady == true)
      {
        return m_results;
      }
      else
      {
        for (int i = 0; i < timeout; i++)
        {
          Thread.Sleep(1000);
          if (m_resultReady == true)
          {
            return m_results;
          }
        }
        throw new FunctionExecutionException(
                   "Result is not ready, endResults callback is called before invoking getResult() method");

      }
    }
    public void EndResults()
    {
      m_endResultCount++;
      m_resultReady = true;
    }
    public void ClearResults(/*bool unused*/)
    {
      m_results.Clear();
    }

  }
  public class MyResultCollectorHA<TResult> : Client.IResultCollector<TResult>
  {
    #region Private members
    private bool m_resultReady = false;
    //private CacheableVector m_results = null;
    ICollection<TResult> m_results = null;
    private int m_addResultCount = 0;
    private int m_getResultCount = 0;
    private int m_endResultCount = 0;
    private int m_clearResultCount = 0;
    #endregion
    public int GetAddResultCount()
    {
      return m_addResultCount;
    }
    public int GetGetResultCount()
    {
      return m_getResultCount;
    }
    public int GetEndResultCount()
    {
      return m_endResultCount;
    }
    public int GetClearResultCount()
    {
      return m_clearResultCount;
    }
    public MyResultCollectorHA()
    {
      m_results = new List<TResult>();
    }
    public void AddResult(TResult result)
    {
      //m_addResultCount++;
      ////CacheableString rs = result as CacheableString;
      //string rs = result.ToString();
      m_results.Add(result);
    }
    public ICollection<TResult> GetResult()
    {
      return GetResult(50);
    }
    public ICollection<TResult> GetResult(UInt32 timeout)
    {
      m_getResultCount++;
      if (m_resultReady == true)
      {
        return m_results;
      }
      else
      {
        for (int i = 0; i < timeout; i++)
        {
          Thread.Sleep(1000);
          if (m_resultReady == true)
          {
            return m_results;
          }
        }
        throw new FunctionExecutionException(
                   "Result is not ready, endResults callback is called before invoking getResult() method");

      }
    }
    public void ClearResults(/*bool unused*/)
    {
      m_clearResultCount++;
      m_results.Clear();
    }
    public void EndResults()
    {
      m_endResultCount++;
      m_resultReady = true;
    }

  }
}