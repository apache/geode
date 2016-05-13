using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  public class MyResultCollector<TResult> : Generic.IResultCollector<TResult>
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
  public class MyResultCollectorHA<TResult> : Generic.IResultCollector<TResult>
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