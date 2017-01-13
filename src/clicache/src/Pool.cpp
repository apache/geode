/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "Pool.hpp"
#include "QueryService.hpp"
#include "CacheableString.hpp"
#include "Cache.hpp"
#include "Properties.hpp"
#include "impl/ManagedString.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      //generic<class TKey, class TValue>
      String^ Pool/*<TKey, TValue>*/::Name::get( )
      {
        return ManagedString::Get( NativePtr->getName( ) );
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::FreeConnectionTimeout::get()
      {
        return NativePtr->getFreeConnectionTimeout();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::LoadConditioningInterval::get()
      {
        return NativePtr->getLoadConditioningInterval();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::SocketBufferSize::get()
      {
        return NativePtr->getSocketBufferSize();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::ReadTimeout::get()
      {
        return NativePtr->getReadTimeout();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::MinConnections::get()
      {
        return NativePtr->getMinConnections();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::MaxConnections::get()
      {
        return NativePtr->getMaxConnections();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::IdleTimeout::get()
      {
        return NativePtr->getIdleTimeout();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::PingInterval::get()
      {
        return NativePtr->getPingInterval();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::UpdateLocatorListInterval::get()
      {
        return NativePtr->getUpdateLocatorListInterval();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::StatisticInterval::get()
      {
        return NativePtr->getStatisticInterval();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::RetryAttempts::get()
      {
        return NativePtr->getRetryAttempts();
      }

      //generic<class TKey, class TValue>
      Boolean Pool/*<TKey, TValue>*/::SubscriptionEnabled::get()
      {
        return NativePtr->getSubscriptionEnabled();
      }

      //generic<class TKey, class TValue>
      Boolean Pool/*<TKey, TValue>*/::PRSingleHopEnabled::get()
      {
        return NativePtr->getPRSingleHopEnabled();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::SubscriptionRedundancy::get()
      {
        return NativePtr->getSubscriptionRedundancy();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::SubscriptionMessageTrackingTimeout::get()
      {
        return NativePtr->getSubscriptionMessageTrackingTimeout();
      }

      //generic<class TKey, class TValue>
      Int32 Pool/*<TKey, TValue>*/::SubscriptionAckInterval::get()
      {
        return NativePtr->getSubscriptionAckInterval();
      }

      //generic<class TKey, class TValue>
      String^ Pool/*<TKey, TValue>*/::ServerGroup::get( )
      {
        return ManagedString::Get( NativePtr->getServerGroup( ) );
      }

      //generic<class TKey, class TValue>
      array<String^>^ Pool/*<TKey, TValue>*/::Locators::get()
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

      //generic<class TKey, class TValue>
      array<String^>^ Pool/*<TKey, TValue>*/::Servers::get()
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

	  //generic<class TKey, class TValue>
      Boolean Pool/*<TKey, TValue>*/::ThreadLocalConnections::get()
      {
        return NativePtr->getThreadLocalConnections();
      }

      //generic<class TKey, class TValue>
      bool Pool/*<TKey, TValue>*/::MultiuserAuthentication::get()
      {
        return NativePtr->getMultiuserAuthentication();    
      }
      
      //generic<class TKey, class TValue>
      void Pool/*<TKey, TValue>*/::Destroy(Boolean KeepAlive)
      {
        NativePtr->destroy(KeepAlive);
      }

      void Pool::ReleaseThreadLocalConnection()
      {
        NativePtr->releaseThreadLocalConnection();
      }

      //generic<class TKey, class TValue>
      void Pool/*<TKey, TValue>*/::Destroy()
      {
        NativePtr->destroy();
      }

      //generic<class TKey, class TValue>
      Boolean Pool/*<TKey, TValue>*/::Destroyed::get()
      {
        return NativePtr->isDestroyed();
      }

      generic<class TKey, class TResult>
      QueryService<TKey, TResult>^ Pool/*<TKey, TValue>*/::GetQueryService()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return QueryService<TKey, TResult>::Create( NativePtr->getQueryService( ).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }
      
      Int32 Pool::PendingEventCount::get()
      {
        _GF_MG_EXCEPTION_TRY2

        return NativePtr->getPendingEventCount();

        _GF_MG_EXCEPTION_CATCH_ALL2
      }
    }
  }
}
 } //namespace 
