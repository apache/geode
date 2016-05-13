/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "PoolM.hpp"
#include "QueryServiceM.hpp"
#include "CacheableStringM.hpp"
#include "CacheM.hpp"
#include "PropertiesM.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      String^ Pool::Name::get( )
      {
        return ManagedString::Get( NativePtr->getName( ) );
      }

      Int32 Pool::FreeConnectionTimeout::get()
      {
        return NativePtr->getFreeConnectionTimeout();
      }

      Int32 Pool::LoadConditioningInterval::get()
      {
        return NativePtr->getLoadConditioningInterval();
      }

      Int32 Pool::SocketBufferSize::get()
      {
        return NativePtr->getSocketBufferSize();
      }

      Int32 Pool::ReadTimeout::get()
      {
        return NativePtr->getReadTimeout();
      }

      Int32 Pool::MinConnections::get()
      {
        return NativePtr->getMinConnections();
      }

      Int32 Pool::MaxConnections::get()
      {
        return NativePtr->getMaxConnections();
      }

      Int32 Pool::IdleTimeout::get()
      {
        return NativePtr->getIdleTimeout();
      }

      Int32 Pool::PingInterval::get()
      {
        return NativePtr->getPingInterval();
      }

      Int32 Pool::StatisticInterval::get()
      {
        return NativePtr->getStatisticInterval();
      }

      Int32 Pool::RetryAttempts::get()
      {
        return NativePtr->getRetryAttempts();
      }

      Boolean Pool::SubscriptionEnabled::get()
      {
        return NativePtr->getSubscriptionEnabled();
      }

      Boolean Pool::PRSingleHopEnabled::get()
      {
        return NativePtr->getPRSingleHopEnabled();
      }

      Int32 Pool::SubscriptionRedundancy::get()
      {
        return NativePtr->getSubscriptionRedundancy();
      }

      Int32 Pool::SubscriptionMessageTrackingTimeout::get()
      {
        return NativePtr->getSubscriptionMessageTrackingTimeout();
      }

      Int32 Pool::SubscriptionAckInterval::get()
      {
        return NativePtr->getSubscriptionAckInterval();
      }

      String^ Pool::ServerGroup::get( )
      {
        return ManagedString::Get( NativePtr->getServerGroup( ) );
      }

      array<String^>^ Pool::Locators::get()
      {
        gemfire::CacheableStringArrayPtr locators = NativePtr->getLocators();
        int length = locators->length();
        if (length > 0)
        {
          array<String^>^ result = gcnew array<String^>(length);
          for (int item = 0; item < length; item++)
          {
            result[item] = CacheableString::GetString(locators[item].ptr());
          }
          return result;
        }
        else
        {
          return nullptr;
        }
      }

      array<String^>^ Pool::Servers::get()
      {
        gemfire::CacheableStringArrayPtr servers = NativePtr->getServers();
        int length = servers->length();
        if (length > 0)
        {
          array<String^>^ result = gcnew array<String^>(length);
          for (int item = 0; item < length; item++)
          {
            result[item] = CacheableString::GetString(servers[item].ptr());
          }
          return result;
        }
        else
        {
          return nullptr;
        }
      }

      Boolean Pool::ThreadLocalConnections::get()
      {
        return NativePtr->getThreadLocalConnections();
      }

      bool Pool::MultiuserAuthentication::get()
      {
        return NativePtr->getMultiuserAuthentication();    
      }
      
      void Pool::Destroy(Boolean KeepAlive)
      {
        NativePtr->destroy(KeepAlive);
      }

      void Pool::Destroy()
      {
        NativePtr->destroy();
      }

      Boolean Pool::Destroyed::get()
      {
        return NativePtr->isDestroyed();
      }

      QueryService^ Pool::GetQueryService()
      {
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::QueryService::Create( NativePtr->getQueryService( ).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }
    }
  }
}
