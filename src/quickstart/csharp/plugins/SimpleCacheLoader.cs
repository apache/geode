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
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  /// <summary>
  /// Capture and display cache events.
  /// </summary>
  class SimpleCacheLoader<TKey, TVal> : ICacheLoader<TKey, TVal>
  {
    #region ICacheLoader Members

    public TVal Load(IRegion<TKey, TVal> region, TKey key, object helper)
    {
      Console.WriteLine("SimpleCacheLoader: Received Load event for region: {0} and key: {1}", region.Name, key);
      return default(TVal);
    }

    public void Close(IRegion<TKey, TVal> region)
    {
      Console.WriteLine("SimpleCacheLoader: Received Close event of region: {0}", region.Name);
    }

    #endregion
  }
}
