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
using Apache.Geode.Client;

namespace Apache.Geode.Client.Tests
{
  using Apache.Geode.DUnitFramework;
  /// <summary>
  /// Capture and display cache events.
  /// </summary>
  class SimpleCacheListener<TKey, TValue> : ICacheListener<TKey, TValue>
  {
    #region ICacheListener Members
    public static bool isSuccess = true;
    public void AfterCreate(EntryEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterCreate event for: {0}", ev.Key);
    }

    public void AfterDestroy(EntryEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterDestroy event for: {0}", ev.Key);
    }

    public void AfterInvalidate(EntryEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterInvalidate event for: {0}", ev.Key);
    }

    public void AfterRegionDestroy(RegionEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterRegionDestroy event of region: {0}", ev.Region.Name);
    }

    public void AfterRegionInvalidate(RegionEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterRegionInvalidate event of region: {0}", ev.Region.Name);
    }

    public void AfterUpdate(EntryEvent<TKey, TValue> ev)
    {
      TValue newval = ev.NewValue;
      if (newval == null && (newval as DeltaEx) == null)
      {
        isSuccess = false;
      }
      else
      {
        isSuccess = true;
      }
    }

    public void Close(IRegion<TKey, TValue> region)
    {
      Util.Log("SimpleCacheListener: Received Close event of region: {0}", region.Name);
    }

    public void AfterRegionLive(RegionEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterRegionLive event of region: {0}", ev.Region.Name);
    }

    public void AfterRegionDisconnected(IRegion<TKey, TValue> region)
    {
      Util.Log("SimpleCacheListener: Received AfterRegionDisconnected event of region: {0}", region.Name);
    }

    public void AfterRegionClear(RegionEvent<TKey, TValue> ev)
    {
      // Do nothing.
    }

    #endregion
  }
}
