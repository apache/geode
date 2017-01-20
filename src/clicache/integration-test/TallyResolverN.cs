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

namespace Apache.Geode.Client.UnitTests.NewAPI
{
  using Apache.Geode.DUnitFramework;

  using Apache.Geode.Client;
  using Apache.Geode.Client.Generic;

  class TallyResolver<TKey, TVal> : IPartitionResolver<TKey, TVal>
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
      Util.Log("TallyResolver state: (loads = {0})", Loads);
    }

    public static TallyResolver<TKey, TVal> Create()
    {
      return new TallyResolver<TKey, TVal>();
    }

    public virtual int GetLoadCount()
    {
      return m_loads;
    }

    #region IPartitionResolver<TKey, TVal> Members

    public virtual Object GetRoutingObject(EntryEvent<TKey, TVal> ev)
    {
      m_loads++;
      Util.Log("TallyResolver Load: (m_loads = {0}) TYPEOF key={1} val={2} for region {3}",
        m_loads, typeof(TKey), typeof(TVal), ev.Region.Name);
      return ev.Key;
    }

    public virtual string GetName()
    {
      Util.Log("TallyResolver<TKey, TVal>::GetName");
	    return "TallyResolver";
    }

    #endregion
  }
}
