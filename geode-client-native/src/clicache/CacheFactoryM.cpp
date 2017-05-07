/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "CacheFactoryM.hpp"
#include "CacheM.hpp"
#include "CacheAttributesM.hpp"
#include "DistributedSystemM.hpp"
#include "SystemPropertiesM.hpp"
//#pragma warning(disable:4091)
//#include <msclr/lock.h>
//#pragma warning(disable:4091)


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      CacheFactory^ CacheFactory::CreateCacheFactory()
      {
        return CacheFactory::CreateCacheFactory(Properties::Create());
      }

      CacheFactory^ CacheFactory::CreateCacheFactory(Properties^ dsProps)
      {
        _GF_MG_EXCEPTION_TRY

           gemfire::PropertiesPtr nativepropsptr(
            GetNativePtr<gemfire::Properties>(dsProps));

          gemfire::CacheFactoryPtr& nativeptr( gemfire::CacheFactory::createCacheFactory( nativepropsptr) );         
          if (nativeptr.ptr() != nullptr)
            return gcnew CacheFactory( nativeptr.ptr(), dsProps );
          return nullptr;

        _GF_MG_EXCEPTION_CATCH_ALL        
      }

      Cache^ CacheFactory::Create()
      {
        _GF_MG_EXCEPTION_TRY
          //msclr::lock lockInstance(m_singletonSync);
          DistributedSystem::acquireDisconnectLock();
    
          if(!m_connected)
          {
             gemfire::PropertiesPtr nativepropsptr(
               GetNativePtr<gemfire::Properties>(m_dsProps));
            DistributedSystem::AppDomainInstanceInitialization(nativepropsptr);      
          }

          gemfire::CachePtr& nativeptr( NativePtr->create( ) );

          bool appDomainEnable = DistributedSystem::SystemProperties->AppDomainEnabled;
          SafeConvertClass::SetAppDomainEnabled(appDomainEnable);

           if(!m_connected)
           {
             //it registers types in unmanage layer, so should be once only 
             DistributedSystem::ManagedPostConnect();
             DistributedSystem::AppDomainInstancePostInitialization();
             DistributedSystem::connectInstance();
           }
          
           m_connected = true;
           
           return Cache::Create( nativeptr.ptr( ) );
        _GF_MG_EXCEPTION_CATCH_ALL
          finally {
          DistributedSystem::releaseDisconnectLock();
        }
      }

      Cache^ CacheFactory::Create( String^ name, DistributedSystem^ system )
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_name( name );
          gemfire::DistributedSystemPtr systemptr(
            GetNativePtr<gemfire::DistributedSystem>( system ) );

          gemfire::CachePtr& nativeptr( gemfire::CacheFactory::create(
            mg_name.CharPtr, systemptr ) );
          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Cache^ CacheFactory::Create( String^ name, DistributedSystem^ system,
        String^ cacheXml )
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_name( name );
          gemfire::DistributedSystemPtr systemptr(
            GetNativePtr<gemfire::DistributedSystem>( system ) );
          ManagedString mg_cacheXml( cacheXml );

          gemfire::CachePtr& nativeptr( gemfire::CacheFactory::create(
            mg_name.CharPtr, systemptr, mg_cacheXml.CharPtr ) );
          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Cache^ CacheFactory::Create( String^ name, DistributedSystem^ system,
        CacheAttributes^ attributes )
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_name( name );
          gemfire::DistributedSystemPtr systemptr(
            GetNativePtr<gemfire::DistributedSystem>( system ) );
          gemfire::CacheAttributesPtr attrsPtr(
            GetNativePtr<gemfire::CacheAttributes>(attributes));

          gemfire::CachePtr& nativeptr( gemfire::CacheFactory::create(
            mg_name.CharPtr, systemptr, attrsPtr ) );
          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Cache^ CacheFactory::Create( String^ name, DistributedSystem^ system,
        String^ cacheXml, CacheAttributes^ attributes )
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_name( name );
          gemfire::DistributedSystemPtr systemptr(
            GetNativePtr<gemfire::DistributedSystem>( system ) );
          ManagedString mg_cacheXml( cacheXml );
          gemfire::CacheAttributesPtr attrsPtr(
            GetNativePtr<gemfire::CacheAttributes>(attributes));

          gemfire::CachePtr& nativeptr( gemfire::CacheFactory::create(
            mg_name.CharPtr, systemptr, mg_cacheXml.CharPtr, attrsPtr ) );
          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Cache^ CacheFactory::GetInstance( DistributedSystem^ system )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::DistributedSystemPtr p_system(
            GetNativePtr<gemfire::DistributedSystem>( system ) );
          gemfire::CachePtr& nativeptr(
            gemfire::CacheFactory::getInstance( p_system ) );

          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Cache^ CacheFactory::GetInstanceCloseOk( DistributedSystem^ system )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::DistributedSystemPtr p_system(
            GetNativePtr<gemfire::DistributedSystem>( system ) );
          gemfire::CachePtr& nativeptr(
            gemfire::CacheFactory::getInstanceCloseOk( p_system ) );

          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Cache^ CacheFactory::GetAnyInstance( )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CachePtr& nativeptr(
            gemfire::CacheFactory::getAnyInstance( ) );
          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      String^ CacheFactory::Version::get( )
      {
        return ManagedString::Get( gemfire::CacheFactory::getVersion( ) );
      }

      String^ CacheFactory::ProductDescription::get( )
      {
        return ManagedString::Get(
          gemfire::CacheFactory::getProductDescription( ) );
      }


      CacheFactory^ CacheFactory::SetFreeConnectionTimeout( Int32 connectionTimeout )
		  {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setFreeConnectionTimeout( connectionTimeout );

        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::SetLoadConditioningInterval( Int32 loadConditioningInterval )
		  {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setLoadConditioningInterval( loadConditioningInterval );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::SetSocketBufferSize( Int32 bufferSize )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setSocketBufferSize( bufferSize );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::SetReadTimeout( Int32 timeout )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setReadTimeout( timeout );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::SetMinConnections( Int32 minConnections )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setMinConnections( minConnections );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::SetMaxConnections( Int32 maxConnections )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setMaxConnections( maxConnections );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::SetIdleTimeout( Int32 idleTimeout )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setIdleTimeout( idleTimeout );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::SetRetryAttempts( Int32 retryAttempts )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setRetryAttempts( retryAttempts );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::SetPingInterval( Int32 pingInterval )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setPingInterval( pingInterval );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

      CacheFactory^ CacheFactory::SetStatisticInterval( Int32 statisticInterval )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setStatisticInterval( statisticInterval );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

      CacheFactory^ CacheFactory::SetServerGroup( String^ group )
      {
			  _GF_MG_EXCEPTION_TRY

        ManagedString mg_servergroup( group );
        NativePtr->setServerGroup( mg_servergroup.CharPtr );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::AddLocator( String^ host, Int32 port )
      {
			  _GF_MG_EXCEPTION_TRY

        ManagedString mg_host( host );
        NativePtr->addLocator( mg_host.CharPtr, port );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

      CacheFactory^ CacheFactory::AddServer( String^ host, Int32 port )
      {
			  _GF_MG_EXCEPTION_TRY

			  ManagedString mg_host( host );
        NativePtr->addServer( mg_host.CharPtr, port );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::SetSubscriptionEnabled( Boolean enabled )
      {
			  _GF_MG_EXCEPTION_TRY

			  NativePtr->setSubscriptionEnabled( enabled );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

      CacheFactory^ CacheFactory::SetPRSingleHopEnabled( Boolean enabled )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->setPRSingleHopEnabled(enabled);
          return this;

         _GF_MG_EXCEPTION_CATCH_ALL
      }

		  CacheFactory^ CacheFactory::SetSubscriptionRedundancy( Int32 redundancy )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setSubscriptionRedundancy( redundancy );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::SetSubscriptionMessageTrackingTimeout( Int32 messageTrackingTimeout )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setSubscriptionMessageTrackingTimeout( messageTrackingTimeout );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

		  CacheFactory^ CacheFactory::SetSubscriptionAckInterval( Int32 ackInterval )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setSubscriptionAckInterval( ackInterval );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
		  }

      CacheFactory^ CacheFactory::SetMultiuserAuthentication( bool multiuserAuthentication )
      {
			  _GF_MG_EXCEPTION_TRY

          NativePtr->setMultiuserAuthentication( multiuserAuthentication );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
	   }

      CacheFactory^ CacheFactory::Set(String^ name, String^ value)
      {
        _GF_MG_EXCEPTION_TRY
          ManagedString mg_name( name );
          ManagedString mg_value( value );
          NativePtr->set( mg_name.CharPtr, mg_value.CharPtr );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL
      }

    }
  }
}
