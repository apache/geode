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

//#include "../gf_includes.hpp"
//#include "../../../IPartitionResolver.hpp"
#include "../IPartitionResolver.hpp"
#include "../Region.hpp"
#include "SafeConvert.hpp"
#include "ManagedString.hpp"
//#include "../../../Region.hpp"
//#include "../../../Cache.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      public interface class IPartitionResolverProxy
      {
      public:
        gemfire::CacheableKeyPtr getRoutingObject(const gemfire::EntryEvent& ev);
        const char * getName();
      };

      generic<class TKey, class TValue>
      public ref class PartitionResolverGeneric : IPartitionResolverProxy
      {
        private:

          IPartitionResolver<TKey, TValue>^ m_resolver;

        public:

          void SetPartitionResolver(IPartitionResolver<TKey, TValue>^ resolver)
          {
            m_resolver = resolver;
          }

          virtual gemfire::CacheableKeyPtr getRoutingObject(const gemfire::EntryEvent& ev)
          {
            EntryEvent<TKey, TValue> gevent(&ev);
						Object^ groutingobject = m_resolver->GetRoutingObject(%gevent);
            return Serializable::GetUnmanagedValueGeneric<Object^>(groutingobject);
          }

          virtual const char * getName()
          {
            ManagedString mg_name(m_resolver->GetName());
            return mg_name.CharPtr;
          }
      };
    }
    }
  }
}
