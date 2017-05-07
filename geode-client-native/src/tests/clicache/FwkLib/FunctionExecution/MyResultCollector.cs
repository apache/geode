using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  public class MyResultCollector : IResultCollector
  {
    #region Private members
    private bool m_resultReady = false;
    private CacheableVector m_results = null;
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
      m_results = new CacheableVector();
    }
    public void AddResult(IGFSerializable result)
    {
      m_addResultCount++;
      CacheableArrayList rs = result as CacheableArrayList;
      for (int i = 0; i < rs.Count; i++)
      {
        m_results.Add(rs[i]);
      }
    }
    public IGFSerializable[] GetResult()
    {
      return GetResult(50);
    }
    public IGFSerializable[] GetResult(UInt32 timeout)
    {
      m_getResultCount++;
      if (m_resultReady == true)
      {
        return m_results.ToArray();
      }
      else
      {
        for (int i = 0; i < timeout; i++)
        {
          Thread.Sleep(1000);
          if (m_resultReady == true)
          {
            return m_results.ToArray();
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
    public void ClearResults()
    {
      m_results.Clear();
    }

  }
  public class MyResultCollectorHA : IResultCollector
  {
    #region Private members
    private bool m_resultReady = false;
    private CacheableVector m_results = null;
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
      m_results = new CacheableVector();
    }
    public void AddResult(IGFSerializable result)
    {
      m_addResultCount++;
      CacheableString rs = result as CacheableString;
      m_results.Add(rs);
    }
    public IGFSerializable[] GetResult()
    {
      return GetResult(50);
    }
    public IGFSerializable[] GetResult(UInt32 timeout)
    {
      m_getResultCount++;
      if (m_resultReady == true)
      {
        return m_results.ToArray();
      }
      else
      {
        for (int i = 0; i < timeout; i++)
        {
          Thread.Sleep(1000);
          if (m_resultReady == true)
          {
            return m_results.ToArray();
          }
        }
        throw new FunctionExecutionException(
                   "Result is not ready, endResults callback is called before invoking getResult() method");

      }
    }
    public void ClearResults()
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