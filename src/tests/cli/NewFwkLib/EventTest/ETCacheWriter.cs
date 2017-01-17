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

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;

  public class ETCacheWriter : ICacheWriter
  {
    public static ICacheWriter Create()
    {
      return new ETCacheWriter();
    }

    #region ICacheWriter Members

    public bool BeforeUpdate(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "BEFORE_UPDATE_COUNT");
      return true;
    }

    public bool BeforeCreate(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "BEFORE_CREATE_COUNT");
      return true;
    }

    public bool BeforeDestroy(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "BEFORE_DESTROY_COUNT");
      return true;
    }
    public bool BeforeRegionClear(RegionEvent rv)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "BEFORE_REGION_CLEAR_COUNT");
      return true;
    }

    public bool BeforeRegionDestroy(RegionEvent rv)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "BEFORE_REGION_DESTROY_COUNT");
      return true;
    }

    public void Close(Region rv)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "CLOSE_COUNT");
    }

    #endregion
  }
}
