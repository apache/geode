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
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using GemStone.GemFire.DUnitFramework;

  using GemStone.GemFire.Cache;
  using GemStone.GemFire.Cache.Generic;

  class TallyLoader<TKey, TVal> : ICacheLoader<TKey, TVal>
  {
    #region Private members

    private int m_loads = 0;

    #endregion

    #region Public accessors

    public int Loads
    {
      get
      {
        return m_loads;
      }
    }

    #endregion

    public int ExpectLoads(int expected)
    {
      int tries = 0;
      while ((m_loads < expected) && (tries < 200))
      {
        Thread.Sleep(100);
        tries++;
      }
      return m_loads;
    }

    public void Reset()
    {
      m_loads = 0;
    }

    public void ShowTallies()
    {
      Util.Log("TallyLoader state: (loads = {0})", Loads);
    }

    public static TallyLoader<TKey, TVal> Create()
    {
      return new TallyLoader<TKey, TVal>();
    }

    public virtual int GetLoadCount()
    {
      return m_loads;
    }

    #region ICacheLoader<TKey, TVal> Members

    public virtual TVal Load(IRegion<TKey, TVal> region, TKey key, object helper)
    {
      m_loads++;
      Util.Log("TallyLoader Load: (m_loads = {0}) TYPEOF key={1} val={2} for region {3}",
        m_loads, typeof(TKey), typeof(TVal), region.Name);
      if (typeof(TVal) == typeof(string))
      {
        return (TVal) (object) m_loads.ToString();
      }
      if (typeof(TVal) == typeof(int))
      {
        return (TVal)(object) m_loads;
      }
      return default(TVal);
    }

    public virtual void Close(IRegion<TKey, TVal> region)
    {
      Util.Log("TallyLoader<TKey, TVal>::Close");
    }

    #endregion
  }
}
