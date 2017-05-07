//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  //using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;
  //using IntRegion = GemStone.GemFire.Cache.Generic.IRegion<int, byte[]>;
  //using StringRegion = GemStone.GemFire.Cache.Generic.IRegion<string, byte[]>;

  public class PutsTask<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private TVal[] m_values;

    #endregion

    #region Public accessors

    public TVal[] Values
    {
      get
      {
        return m_values;
      }
      set
      {
        m_values = value;
      }
    }

    #endregion

    public PutsTask(IRegion<TKey, TVal> region, TKey[] keys,
      TVal[] values)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_keys = keys as TKey[];
      m_values = values as TVal[];
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        Util.Log("PutsTask::DoTask: starting {0} iterations.", iters);
        while (Running && (iters-- != 0))
        {
          int idx = count % numKeys;
          try
          {
            m_region[m_keys[idx]] = m_values[idx];
            //Util.Log("rjk: puttask ---- idx = {0} key is {1} and value is {2} size of {3}", idx, m_keys[idx], m_values[idx].ToString(), m_values[idx].ToString().Length);
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while putting key[{0}] for region {1} in iteration " +
              "{2}: {3}", idx, m_region.Name, (count - offset), ex);
            throw;
          }
          count++;
          //if ((count % 1000) == 0)
          //{
          //  Util.Log("PutsTask::DoTask: Intermediate: Ran for 1000 iterations.");
          //}
        }
        //Util.Log("PutsTask::DoTask: Ran for {0} iterations.", count);
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class LatencyPutsTask<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private TVal[] m_values;
    private int m_opsSec;

    #endregion

    #region Public accessors

    public TVal[] Values
    {
      get
      {
        return m_values;
      }
      set
      {
        m_values = value;
      }
    }

    #endregion

    public const uint LatMark = 0x55667788;

    public LatencyPutsTask(IRegion<TKey, TVal> region, TKey[] keys,
      TVal[] values, int opsSec)
      : base()
    {
      m_region = region;
      m_keys = keys;
      m_values = values;
      m_opsSec = opsSec;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        TKey key = m_keys[0];
        TVal buffer = m_values[0];
        //TVal[] buffer = value;
        int count = 0;
        PaceMeter pm = new PaceMeter(m_opsSec);
        while (Running && (iters-- != 0))
        {
          /*
          if (buffer.Length >= (int)(sizeof(int) + sizeof(long)))
          {
            BitConverter.GetBytes(LatMark).CopyTo(buffer, 0);
            BitConverter.GetBytes(DateTime.Now.Ticks).CopyTo(buffer, (int)(sizeof(int)));
          }
          */
          try
          {
            m_region[key] = buffer;
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while putting key[{0}] for region {1} in iteration " +
              "{2}: {3}", 0, m_region.Name, count, ex);
            throw;
          }
          count++;
          pm.CheckPace();
        }
        Interlocked.Add(ref m_iters, count);
      }
    }
  }

  public class MeteredPutsTask<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private TVal[] m_values;
    private int m_opsSec;

    #endregion

    #region Public accessors

    public TVal[] Values
    {
      get
      {
        return m_values;
      }
      set
      {
        m_values = value;
      }
    }

    #endregion

    public MeteredPutsTask(IRegion<TKey, TVal> region, TKey[] keys, TVal[] values, int opsSec)
      : base()
    {
      m_region = region;
      m_keys = keys;
      m_values = values;
      m_opsSec = opsSec;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        int idx;
        PaceMeter pm = new PaceMeter(m_opsSec);
        while (Running && (iters-- != 0))
        {
          idx = count % numKeys;
          try
          {
            m_region[m_keys[idx]] = m_values[idx];
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while putting key[{0}] for region {1} in iteration " +
              "{2}: {3}", idx, m_region.Name, (count - offset), ex);
            throw;
          }
          count++;
          pm.CheckPace();
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class GetsTask<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;

    #endregion

    public GetsTask(IRegion<TKey, TVal> region, TKey[] keys)
      : base()
    {
      m_region = region;
      m_keys = keys;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        while (Running && (iters-- != 0))
        {
          TVal val = default(TVal); ;
          int idx = count % numKeys;
          try
          {
            val = m_region[m_keys[idx]];
            //Util.Log("rjk: ---- idx = {0} key is {1} and value is {2} size of {3}", idx, m_keys[idx], val.ToString(),val.ToString().Length) ;
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while getting key[{0}] for region {1} in iteration " +
              "{2}: {3}", idx, m_region.Name, (count - offset), ex);
            throw;
          }
          if (val == null)
          {
            string exStr = string.Format("Key[{0}] not found in region {1}",
              m_keys[idx], m_region.Name);
            Util.Log(Util.LogLevel.Error, exStr);
            throw new EntryNotFoundException(exStr);
          }
          count++;
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class DestroyTask<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;

    #endregion

    public DestroyTask(IRegion<TKey, TVal> region, TKey[] keys)
      : base()
    {
      m_region = region;
      m_keys = keys;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        int i = 0;
        int numKeys = m_keys.Length;
        while (Running && i < numKeys)
        {
          try
          {
            m_region.Remove(m_keys[i++]);
          }
          catch (EntryNotFoundException)
          {
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while destroying key[{0}] for region {1}: {2}",
              i, m_region.Name, ex);
            throw;
          }
        }
        Interlocked.Add(ref m_iters, i);
      }
    }
  }
}
