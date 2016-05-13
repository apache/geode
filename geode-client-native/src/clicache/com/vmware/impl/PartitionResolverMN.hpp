/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#pragma once

//#include "../gf_includesN.hpp"
//#include "../../../IPartitionResolver.hpp"
#include "../IPartitionResolverN.hpp"
#include "../RegionMN.hpp"
#include "SafeConvertN.hpp"
#include "ManagedStringN.hpp"
//#include "../../../RegionM.hpp"
//#include "../../../CacheM.hpp"

using namespace System;
using namespace GemStone::GemFire::Cache::Generic;

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
