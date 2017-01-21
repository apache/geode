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

namespace Apache.Geode.Client.Examples
{
  /// <summary>
  /// Capture and display cache events.
  /// </summary>
  class ExampleCacheListenerCallback : ICacheListener
  {
    public static ICacheListener Create()
    {
      return new ExampleCacheListenerCallback();
    }

    #region ICacheListener Members

    public void AfterCreate(EntryEvent ev)
    {
      Console.WriteLine("{0}--- Received afterCreate event of: {1}",
        Environment.NewLine, ev.Key);
    }

    public void AfterDestroy(EntryEvent ev)
    {
      Console.WriteLine("{0}--- Received afterDestroy event of: {1}",
        Environment.NewLine, ev.Key);
    }

    public void AfterInvalidate(EntryEvent ev)
    {
      Console.WriteLine("{0}--- Received afterInvalidate event of: {1}",
        Environment.NewLine, ev.Key);
    }

    public void AfterRegionClear(RegionEvent ev)
    {
      Console.WriteLine("{0}--- Received afterRegionClear event of region: {1}",
        Environment.NewLine, ev.Region.Name);
    }

    public void AfterRegionDestroy(RegionEvent ev)
    {
      Console.WriteLine("{0}--- Received afterRegionDestroy event of region: {1}",
        Environment.NewLine, ev.Region.Name);
    }

    public void AfterRegionInvalidate(RegionEvent ev)
    {
      Console.WriteLine("{0}--- Received afterRegionInvalidate event of region: {1}",
        Environment.NewLine, ev.Region.Name);
    }

    public void AfterRegionLive(RegionEvent ev)
    {
      Console.WriteLine("{0}--- Received afterRegionLive event of region: {1}",
        Environment.NewLine, ev.Region.Name);
    }

    public void AfterUpdate(EntryEvent ev)
    {
      Console.WriteLine("{0}--- Received afterUpdate event of: {1}",
        Environment.NewLine, ev.Key);
    }
    
    public void Close(Region region)
    {
      Console.WriteLine("{0}--- Received close event of region: {1}",
        Environment.NewLine, region.Name);
    }
    public void AfterRegionDisconnected(Region region)
    {
      Console.WriteLine("{0}--- Received disconnected event of region: {1}",
        Environment.NewLine, region.Name);
    }
    #endregion
  }
}
