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

namespace Apache.Geode.Client.FwkLib
{

  using Apache.Geode.DUnitFramework;

  public class ETCacheListener : ICacheListener
  {
    public static ICacheListener Create()
    {
      return new ETCacheListener();
    }

    #region ICacheListener Members

    public void AfterCreate(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_CREATE_COUNT");
    }

    public void AfterDestroy(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_DESTROY_COUNT");
    }

    public void AfterInvalidate(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_INVALIDATE_COUNT");
    }

    public void AfterRegionDestroy(RegionEvent ev)
    {
      //Util.BBIncrement("AFTER_REGION_DESTROY_COUNT");
    }

    public void AfterRegionInvalidate(RegionEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_REGION_INVALIDATE_COUNT");
    }
    public void AfterRegionClear(RegionEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_REGION_CLEAR_COUNT");
    }


    public void AfterUpdate(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_UPDATE_COUNT");
    }

    public virtual void AfterRegionLive(RegionEvent ev)
    {
      //Util.BBIncrement("AFTER_REGION_LIVE_COUNT");
    }

    public void Close(Region region)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "CLOSE_COUNT");
    }
    public void AfterRegionDisconnected(Region region)
    {
    }
    #endregion
  }
}
