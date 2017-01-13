/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
        gemfire::CacheableKeyPtr getRoutingObject(const gemfire::EntryEvent& ev);
        const char * getName();
        const char* getPartitionName(const gemfire::EntryEvent& opDetails);       
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

          virtual const char* getPartitionName(const gemfire::EntryEvent& opDetails)
          {
            if (m_fixedResolver == nullptr)
            {
              throw gemfire::IllegalStateException("GetPartitionName() called on non fixed partition resolver.");
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
