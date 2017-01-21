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

#pragma once

#include "gf_defs.hpp"
#include "ICacheWriter.hpp"


namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      /// <summary>
      /// Utility class that implements all methods in <c>ICacheWriter</c>
      /// with empty implementations. Applications can subclass this class
      /// and only override the methods for the events of interest.
      /// </summary>
      generic<class TKey, class TValue>
      public ref class CacheWriterAdapter
        : public ICacheWriter<TKey, TValue>
      {
      public:
        virtual bool BeforeUpdate(EntryEvent<TKey, TValue>^ ev)
        {
          return true;
        }

        virtual bool BeforeCreate(EntryEvent<TKey, TValue>^ ev)
        {
          return true;
        }

        virtual bool BeforeDestroy(EntryEvent<TKey, TValue>^ ev)
        {
          return true;
        }

        virtual bool BeforeRegionDestroy(RegionEvent<TKey, TValue>^ ev)
        {
          return true;
        }

        virtual bool BeforeRegionClear(RegionEvent<TKey, TValue>^ ev)
        {
          return true;
        }

        virtual void Close(IRegion<TKey, TValue>^ region)
        {
        }
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

