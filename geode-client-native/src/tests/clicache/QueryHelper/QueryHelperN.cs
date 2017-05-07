//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  //using Region = GemStone.GemFire.Cache.Generic.IRegion<TKey, TVal>;

  /// <summary>
  /// Helper class to populate regions and verify query results.
  /// This class is intentionally not thread-safe.
  /// </summary>
  public class QueryHelper<TKey,TVal>
  {

    #region Private members

    private int m_portfolioSetSize;
    private int m_portfolioNumSets;
    private int m_positionSetSize;
    private int m_positionNumSets;

    private static QueryHelper<TKey, TVal> m_instance = null;

    #endregion

    #region Public accessors

    public virtual int PortfolioSetSize
    {
      get
      {
        return m_portfolioSetSize;
      }
    }

    public virtual int PortfolioNumSets
    {
      get
      {
        return m_portfolioNumSets;
      }
    }

    public virtual int PositionSetSize
    {
      get
      {
        return m_positionSetSize;
      }
    }

    public virtual int PositionNumSets
    {
      get
      {
        return m_positionNumSets;
      }
    }

    #endregion

    private QueryHelper()
    {
      m_portfolioSetSize = 20;
      m_portfolioNumSets = 1;
      m_positionSetSize = 20;
      m_positionNumSets = 1;
    }

    public static QueryHelper<TKey, TVal> GetHelper()
    {
      if (m_instance == null)
      {
        m_instance = new QueryHelper<TKey,TVal>();
      }
      return m_instance;
    }

    public virtual void PopulatePortfolioData(IRegion<TKey, TVal> region, int setSize,
      int numSets)
    {
      PopulatePortfolioData(region, setSize, numSets, 1);
    }

    public virtual void PopulatePortfolioData(IRegion<TKey, TVal> region, int setSize,
      int numSets, int objSize)
    {
      PopulatePortfolioData(region, setSize, numSets, objSize, null);
    }
    public virtual void PopulatePortfolioPdxData(IRegion<TKey, TVal> region, int setSize,
     int numSets)
    {
      PopulatePortfolioPdxData(region, setSize, numSets, 1);
    }

    public virtual void PopulatePortfolioPdxData(IRegion<TKey, TVal> region, int setSize,
      int numSets, int objSize)
    {
      PopulatePortfolioPdxData(region, setSize, numSets, objSize, null);
    }
    public virtual void PopulatePortfolioData(IRegion<TKey, TVal> region, int setSize,
      int numSets, int objSize, string[] names)
    {
      Util.Log("QueryHelper.PopulatePortfolioData: putting for setSize={0}, numSets={1}, objSize={2}",setSize, numSets, objSize);
      if(names == null)
      {
         Util.Log("QueryHelper.PopulatePortfolioData: names is null");
      } else 
      {
	      for(int i =0 ; i < names.Length; i++)
	      {
          Util.Log("QueryHelper.PopulatePortfolioData: names[{0}]={1}", i, names[i]);
	      }
      }

      Position.Count = 0;

      for (int set = 1; set <= numSets; set++)
      {
        for (int current = 1; current <= setSize; current++)
        {
          Portfolio portfolio = new Portfolio(current, objSize, names);

          TKey portfolioName = (TKey)(object)string.Format("port{0}-{1}", set, current);
          //Util.Log("QueryHelper.PopulatePortfolioData: creating key = {0} and"
          //+ " puting data.", portfolioName);
          //Put operation
          region[portfolioName] = (TVal)(object)portfolio;
        }
      }

      //m_portfolioSetSize = setSize;
      //m_portfolioNumSets = numSets;
      //m_objectSize = objSize;

      Util.Log("QueryHelper.PopulatePortfolioData: all puts done.");
    }
    public virtual void PopulatePortfolioPdxData(IRegion<TKey, TVal> region, int setSize,
     int numSets, int objSize, string[] names)
    {
      Util.Log("QueryHelper.PopulatePortfolioPdxData: putting for setSize={0}, numSets={1}, objSize={2}", setSize, numSets, objSize);
      if (names == null)
      {
        Util.Log("QueryHelper.PopulatePortfolioPdxData: names is null");
      }
      else
      {
        for (int i = 0; i < names.Length; i++)
        {
          Util.Log("QueryHelper.PopulatePortfolioPdxData: names[{0}]={1}", i, names[i]);
        }
      }

      PositionPdx.Count = 0;

      for (int set = 1; set <= numSets; set++)
      {
        for (int current = 1; current <= setSize; current++)
        {
          PortfolioPdx portfolio = new PortfolioPdx(current, objSize, names);

          TKey portfolioName = (TKey)(object)string.Format("port{0}-{1}", set, current);
          //Util.Log("QueryHelper.PopulatePortfolioData: creating key = {0} and"
          //+ " puting data.", portfolioName);
          //Put operation
          region[portfolioName] = (TVal)(object)portfolio;
        }
      }

      //m_portfolioSetSize = setSize;
      //m_portfolioNumSets = numSets;
      //m_objectSize = objSize;

      Util.Log("QueryHelper.PopulatePortfolioPdxData: all puts done.");
    }
    public virtual void PopulatePositionData(IRegion<TKey, TVal> region, int setSize,
      int numSets)
    {
      Util.Log("QueryHelper.PopulatePositionData: putting for setSize={0}, numSets={1}",
        setSize, numSets);

      string[] secIds = Portfolio.SecIds;
      int numSecIds = secIds.Length;

      for (int set = 1; set <= numSets; set++)
      {
        for (int current = 1; current <= setSize; current++)
        {
          Position pos = new Position(secIds[current % numSecIds],
            current * 100);

          TKey posName = (TKey)(object)string.Format("pos{0}-{1}", set, current);
          //Put Operation
          region[posName] = (TVal)(object)pos;
        }
      }
      //m_positionSetSize = setSize;
      //m_positionNumSets = numSets;

      Util.Log("QueryHelper.PopulatePositionData: all puts done.");
    }
    public virtual void PopulatePositionPdxData(IRegion<TKey, TVal> region, int setSize,
      int numSets)
    {
      Util.Log("QueryHelper.PopulatePositionPdxData: putting for setSize={0}, numSets={1}",
        setSize, numSets);

      string[] secIds = PortfolioPdx.SecIds;
      int numSecIds = secIds.Length;

      for (int set = 1; set <= numSets; set++)
      {
        for (int current = 1; current <= setSize; current++)
        {
          PositionPdx pos = new PositionPdx(secIds[current % numSecIds],
            current * 100);

          TKey posName = (TKey)(object)string.Format("pos{0}-{1}", set, current);
          //Util.Log("QueryHelper.PopulatePositionPdxData: creating key = {0} and"
          //+ " puting data.", posName);
          //Put Operation
          region[posName] = (TVal)(object)pos;
        }
      }
      //m_positionSetSize = setSize;
      //m_positionNumSets = numSets;

      Util.Log("QueryHelper.PopulatePositionPdxData: all puts done.");
    }
    public virtual void DestroyPortfolioOrPositionData(IRegion<TKey, TVal> region,
      int setSize, int numSets, string objectType)
    {
      string prefix = string.Empty;

      if (objectType.Equals("Portfolio") || objectType.Equals("PortfolioPdx"))
      {
        prefix = "port";
      }
      else // if (objectType.Equals("Position"))
      {
        prefix = "pos";
      }

      for (int set = 1; set <= numSets; set++)
      {
        for (int current = 1; current <= setSize; current++)
        {
          string keyname = string.Empty;
          keyname = prefix + set + "-" + current; // "port1-1" or "pos1-1" for example
          TKey key = (TKey)(object)keyname;
          region.Remove(key);
        }
      }
    }

    public virtual void InvalidatePortfolioOrPositionData(IRegion<TKey, TVal> region,
      int setSize, int numSets, string objectType)
    {
      string prefix = string.Empty;

      if (objectType.Equals("Portfolio") || objectType.Equals("PortfolioPdx"))
      {
        prefix = "port";
      }
      else // if (objectType.Equals("Position"))
      {
        prefix = "pos";
      }

      for (int set = 1; set <= numSets; set++)
      {
        for (int current = 1; current <= setSize; current++)
        {
          string keyname = string.Empty;
          keyname = prefix + set + "-" + current; // "port1-1" or "pos1-1" for example
          TKey key = (TKey)(object)keyname;
          region.Invalidate(key);
        }
      }
    }

    public virtual bool VerifyRS(ISelectResults<object> resultset, int expectedRows)
    {
      if (resultset == null) return false;

      int foundRows = 0;

      SelectResultsIterator<object> sr = resultset.GetIterator();
      while (sr.HasNext)  
      {
       //TVal ser = (TVal)sr.Next();
        Object ser = sr.Next();
        if (ser == null)
        {
          Util.Log("QueryHelper.VerifyRS: Object is null.");
          return false;
        }
        
        foundRows++;
      }
      Util.Log("QueryHelper.VerifyRS: found rows {0}, expected {1}",
        foundRows, expectedRows);
      return (foundRows == expectedRows);
    }

    public virtual bool VerifySS(ISelectResults<object> structset, int expectedRows,
      int expectedFields)
    {
      if (structset == null)
      {
        if (expectedRows == 0 && expectedFields == 0)
          return true; //quite possible we got a null set back.
        return false;
      }

      int foundRows = 0;
      foreach (Struct si in structset)
      {
        foundRows++;

        if (si == null)
        {
          Util.Log("QueryHelper.VerifySS: Struct is null.");
          return false;
        }

        int foundFields = 0;
        for (uint cols = 0; cols < si.Length; cols++)
        {
          //IGFSerializable field = si[cols];
          object field = (object)si[cols];
          foundFields++;
        }

        if (foundFields != expectedFields)
        {
          Util.Log("QueryHelper.VerifySS: found fields {0}, expected"
            + " fields {1}.", foundFields, expectedFields);
          return false;
        }
      }
      if (foundRows != expectedRows)
      {
        Util.Log("QueryHelper.VerifySS: rows fields {0}, expected rows {1}.",
          foundRows, expectedRows);
        return false;
      }
      return true;
    }

    public bool IsExpectedRowsConstantRS(int queryindex)
    {
      foreach (int constantIndex in QueryStatics.ConstantExpectedRowsRS)
      {
        if (queryindex == constantIndex)
        {
          Util.Log("Index {0} is having constant rows.", constantIndex);
          return true;
        }
      }
      return false;
    }

    public bool IsExpectedRowsConstantPQRS(int queryindex)
    {
      foreach (int constantIndex in QueryStatics.ConstantExpectedRowsPQRS)
      {
        if (queryindex == constantIndex)
        {
          Util.Log("Index {0} is having constant rows.", constantIndex);
          return true;
        }
      }
      return false;
    }

    public bool IsExpectedRowsConstantSS(int queryindex)
    {
      foreach (int constantIndex in QueryStatics.ConstantExpectedRowsSS)
      {
        if (queryindex == constantIndex)
        {
          Util.Log("Index {0} is having constant rows.", constantIndex);
          return true;
        }
      }
      return false;
    }

    public bool IsExpectedRowsConstantPQSS(int queryindex)
    {
      foreach (int constantIndex in QueryStatics.ConstantExpectedRowsPQSS)
      {
        if (queryindex == constantIndex)
        {
          Util.Log("Index {0} is having constant rows.", constantIndex);
          return true;
        }
      }
      return false;
    }

    public bool IsExpectedRowsConstantCQRS(int queryindex)
    {
      foreach (int constantIndex in QueryStatics.ConstantExpectedRowsCQRS)
        if (queryindex == constantIndex)
        {
          Util.Log("Index {0} is having constant rows.", constantIndex);
          return true;
        }

      return false;
    }

    public void PopulateRangePositionData(IRegion<TKey, TVal> region, int start, int end)
    {
      for (int i = start; i <= end; i++)
      {
        TVal pos = (TVal)(object)new Position(i);
        TKey key = (TKey)(object)string.Format("pos{0}", i);
        region[key] = pos;
      }
    }
    public void PopulateRangePositionPdxData(IRegion<TKey, TVal> region, int start, int end)
    {
      for (int i = start; i <= end; i++)
      {
        TVal pos = (TVal)(object)new PositionPdx(i);
        TKey key = (TKey)(object)string.Format("pos{0}", i);
        region[key] = pos;
      }
    }
    public bool CompareTwoPositionObjects(TVal pos1, TVal pos2)
    {
      Position p1 = pos1 as Position;
      Position p2 = pos2 as Position;

      if (p1 == null || p2 == null)
      {
        Util.Log("The object(s) passed are not of Position type");
        return false;
      }

      DataOutput o1 = new DataOutput();
      DataOutput o2 = new DataOutput();

      p1.ToData(o1);
      p2.ToData(o2);

      uint len1 = o1.BufferLength;
      uint len2 = o2.BufferLength;

      if (len1 != len2)
      {
        return false;
      }

      byte[] ptr1 = o1.GetBuffer();
      byte[] ptr2 = o2.GetBuffer();

      if (ptr1.Length != ptr2.Length)
      {
        return false;
      }

      for (int i = ptr1.Length; i < ptr1.Length; i++)
      {
        if (ptr1[i] != ptr2[i])
        {
          return false;
        }
      }

      return true;
    }

    public TVal GetExactPositionObject(int iForExactPosObject)
    {
      return (TVal)(object)new Position(iForExactPosObject);
    }
    public TVal GetExactPositionPdxObject(int iForExactPosObject)
    {
      return (TVal)(object)new PositionPdx(iForExactPosObject);
    }
    public TVal GetCachedPositionObject(IRegion<TKey, TVal> region, int iForExactPosObject)
    {
      TKey key = (TKey)(object)string.Format("pos{0}", iForExactPosObject);
      return region[key];
    }

    public void PutExactPositionObject(IRegion<TKey, TVal> region, int iForExactPosObject)
    {
      TKey key = (TKey)(object)string.Format("pos{0}", iForExactPosObject);
      region[key] = (TVal)(object)new Position(iForExactPosObject);
    }
    public void PutExactPositionPdxObject(IRegion<TKey, TVal> region, int iForExactPosObject)
    {
      TKey key = (TKey)(object)string.Format("pos{0}", iForExactPosObject);
      region[key] = (TVal)(object)new PositionPdx(iForExactPosObject);
    }
  }
}
