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
#include "../IFixedPartitionResolver.hpp"
#include "../Region.hpp"
#include "ManagedString.hpp"
#include "SafeConvert.hpp"

using namespace System;
using namespace System::Collections::Generic;
using namespace System::Threading;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      public interface class IFixedPartitionResolverProxy
      {
      public:
        apache::geode::client::CacheableKeyPtr getRoutingObject(const apache::geode::client::EntryEvent& ev);
        const char * getName();
        const char* getPartitionName(const apache::geode::client::EntryEvent& opDetails);       
      };

      generic<class TKey, class TValue>
      public ref class FixedPartitionResolverGeneric : IFixedPartitionResolverProxy
      {
        private:

          IPartitionResolver<TKey, TValue>^ m_resolver;
          IFixedPartitionResolver<TKey, TValue>^ m_fixedResolver;
          Dictionary<String^, ManagedString^> ^m_strList;
        public:

          void SetPartitionResolver(IPartitionResolver<TKey, TValue>^ resolver)
          {            
            m_resolver = resolver;
            m_fixedResolver = dynamic_cast<IFixedPartitionResolver<TKey, TValue>^>(resolver);
            m_strList = gcnew Dictionary<String^, ManagedString^>();
          }

          virtual apache::geode::client::CacheableKeyPtr getRoutingObject(const apache::geode::client::EntryEvent& ev)
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

          virtual const char* getPartitionName(const apache::geode::client::EntryEvent& opDetails)
          {
            if (m_fixedResolver == nullptr)
            {
              throw apache::geode::client::IllegalStateException("GetPartitionName() called on non fixed partition resolver.");
            }

            EntryEvent<TKey, TValue> gevent(&opDetails);                        
            String^ str = m_fixedResolver->GetPartitionName(%gevent);
            ManagedString ^mnStr = nullptr;
            try
            {
              Monitor::Enter( m_strList );
              if(!m_strList->TryGetValue(str,mnStr))
              {
                mnStr= gcnew ManagedString(str);
                m_strList->Add(str,mnStr);
              }
            }
            finally
            { 
              Monitor::Exit( m_strList );
            }
            
            return mnStr->CharPtr;            
          }
      };
    }
    }
  }
}
