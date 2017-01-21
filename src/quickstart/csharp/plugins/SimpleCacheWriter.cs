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

namespace Apache.Geode.Client.QuickStart
{
  /// <summary>
  /// Capture and display cache events.
  /// </summary>
  class SimpleCacheWriter<TKey, TVal> : ICacheWriter<TKey, TVal>
  {
    #region ICacheWriter<TKey, TVal> Members

    public bool BeforeUpdate(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheWriter: Received BeforeUpdate event for: {0}", ev.Key);
      return true;
    }

    public bool BeforeCreate(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheWriter: Received BeforeCreate event for: {0}", ev.Key);
      return true;
    }

    public bool BeforeDestroy(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheWriter: Received BeforeDestroy event for: {0}", ev.Key);
      return true;
    }

    public bool BeforeRegionClear(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheWriter: Received BeforeRegionClear event of region: {0}", ev.Region.Name);
      return true;
    }

    public bool BeforeRegionDestroy(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheWriter: Received BeforeRegionDestroy event of region: {0}", ev.Region.Name);
      return true;
    }

    public void Close(IRegion<TKey, TVal> region)
    {
      Console.WriteLine("SimpleCacheWriter: Received Close event of region: {0}", region.Name);
    }

    #endregion
  }
}
