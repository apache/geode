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
//using Apache.Geode.Client;
using Apache.Geode.Client;

namespace Apache.Geode.Client.QuickStart
{
  /// <summary>
  /// Capture and display cache events.
  /// </summary>
  class DurableCacheListener<TKey, TVal> : ICacheListener<TKey, TVal>
  {
    #region ICacheListener<TKey, TVal> Members

    public void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("DurableCacheListener: Received AfterCreate event for: {0}", ev.Key);
    }

    public void AfterDestroy(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("DurableCacheListener: Received AfterDestroy event for: {0}", ev.Key);
    }

    public void AfterInvalidate(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("DurableCacheListener: Received AfterInvalidate event for: {0}", ev.Key);
    }

    public void AfterRegionDestroy(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("DurableCacheListener: Received AfterRegionDestroy event of region: {0}", ev.Region.Name);
    }

    public void AfterRegionClear(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("DurableCacheListener: Received AfterRegionClear event of region: {0}", ev.Region.Name);
    }

    public void AfterRegionInvalidate(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("DurableCacheListener: Received AfterRegionInvalidate event of region: {0}", ev.Region.Name);
    }

    public void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("DurableCacheListener: Received AfterUpdate event of: {0}", ev.Key);
    }

    public void Close(IRegion<TKey, TVal> region)
    {
      Console.WriteLine("DurableCacheListener: Received Close event of region: {0}", region.Name);
    }

    public void AfterRegionLive(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("DurableCacheListener: Received AfterRegionLive event of region: {0}", ev.Region.Name);
    }

    public void AfterRegionDisconnected(IRegion<TKey, TVal> region)
    {
      Console.WriteLine("DurableCacheListener: Received AfterRegionDisconnected event of region: {0}", region.Name);
    }

    #endregion
  }
}
