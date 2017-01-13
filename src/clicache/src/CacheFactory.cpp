/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"

#include "ExceptionTypes.hpp"

#include "CacheFactory.hpp"
#include "Cache.hpp"
#include "DistributedSystem.hpp"
#include "SystemProperties.hpp"
#include "impl/SafeConvert.hpp"
#include "impl/PdxTypeRegistry.hpp"
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
      namespace Generic
      {
      CacheFactory^ CacheFactory::CreateCacheFactory()
      {
        return CacheFactory::CreateCacheFactory(Properties<String^, String^>::Create<String^, String^>());
      }

      CacheFactory^ CacheFactory::CreateCacheFactory(Properties<String^, String^>^ dsProps)
      {
        _GF_MG_EXCEPTION_TRY2

           gemfire::PropertiesPtr nativepropsptr(
            GetNativePtr<gemfire::Properties>(dsProps));

          gemfire::CacheFactoryPtr& nativeptr( gemfire::CacheFactory::createCacheFactory( nativepropsptr) );         
          if (nativeptr.ptr() != nullptr)
            return gcnew CacheFactory( nativeptr.ptr(), dsProps );
            
          return nullptr;

        _GF_MG_EXCEPTION_CATCH_ALL2        
      }

      Cache^ CacheFactory::Create()
      {
				bool pdxIgnoreUnreadFields = false;
        bool pdxReadSerialized = false;
				bool appDomainEnable = false; 
        _GF_MG_EXCEPTION_TRY2
          //msclr::lock lockInstance(m_singletonSync);
          DistributedSystem::acquireDisconnectLock();
    
          if(!m_connected)
          {
             gemfire::PropertiesPtr nativepropsptr(
               GetNativePtr<gemfire::Properties>(m_dsProps));
            DistributedSystem::AppDomainInstanceInitialization(nativepropsptr);                  
          }

          gemfire::CachePtr& nativeptr( NativePtr->create( ) );
					pdxIgnoreUnreadFields = nativeptr->getPdxIgnoreUnreadFields();
          pdxReadSerialized = nativeptr->getPdxReadSerialized();

          appDomainEnable = DistributedSystem::SystemProperties->AppDomainEnabled;
          Log::SetLogLevel(static_cast<LogLevel>(gemfire::Log::logLevel( )));
					//TODO::split
          SafeConvertClassGeneric::SetAppDomainEnabled(appDomainEnable);

            Serializable::RegisterTypeGeneric(
              gemfire::GemfireTypeIds::PdxType,
              gcnew TypeFactoryMethodGeneric(GemStone::GemFire::Cache::Generic::Internal::PdxType::CreateDeserializable),
              nullptr);

           if(!m_connected)
           {
             //it registers types in unmanage layer, so should be once only 
             DistributedSystem::ManagedPostConnect();
             DistributedSystem::AppDomainInstancePostInitialization();
             DistributedSystem::connectInstance();
           }
          
           m_connected = true;
           
           return Cache::Create( nativeptr.ptr( ) );
        _GF_MG_EXCEPTION_CATCH_ALL2
          finally {
            DistributedSystem::registerCliCallback();
						Serializable::RegisterPDXManagedCacheableKey(appDomainEnable);
					GemStone::GemFire::Cache::Generic::Internal::PdxTypeRegistry::PdxIgnoreUnreadFields = pdxIgnoreUnreadFields; 
          GemStone::GemFire::Cache::Generic::Internal::PdxTypeRegistry::PdxReadSerialized = pdxReadSerialized; 
          DistributedSystem::releaseDisconnectLock();
        }
      }

      Cache^ CacheFactory::GetInstance( DistributedSystem^ system )
      {
        _GF_MG_EXCEPTION_TRY2

          gemfire::DistributedSystemPtr p_system(
            GetNativePtr<gemfire::DistributedSystem>( system ) );
          gemfire::CachePtr& nativeptr(
            gemfire::CacheFactory::getInstance( p_system ) );

          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      Cache^ CacheFactory::GetInstanceCloseOk( DistributedSystem^ system )
      {
        _GF_MG_EXCEPTION_TRY2

          gemfire::DistributedSystemPtr p_system(
            GetNativePtr<gemfire::DistributedSystem>( system ) );
          gemfire::CachePtr& nativeptr(
            gemfire::CacheFactory::getInstanceCloseOk( p_system ) );

          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      Cache^ CacheFactory::GetAnyInstance( )
      {
        _GF_MG_EXCEPTION_TRY2

          gemfire::CachePtr& nativeptr(
            gemfire::CacheFactory::getAnyInstance( ) );
          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
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
			  _GF_MG_EXCEPTION_TRY2

			  NativePtr->setFreeConnectionTimeout( connectionTimeout );

        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::SetLoadConditioningInterval( Int32 loadConditioningInterval )
		  {
			  _GF_MG_EXCEPTION_TRY2

			  NativePtr->setLoadConditioningInterval( loadConditioningInterval );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::SetSocketBufferSize( Int32 bufferSize )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setSocketBufferSize( bufferSize );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::SetReadTimeout( Int32 timeout )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setReadTimeout( timeout );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::SetMinConnections( Int32 minConnections )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setMinConnections( minConnections );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::SetMaxConnections( Int32 maxConnections )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setMaxConnections( maxConnections );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::SetIdleTimeout( Int32 idleTimeout )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setIdleTimeout( idleTimeout );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::SetRetryAttempts( Int32 retryAttempts )
      {
			  _GF_MG_EXCEPTION_TRY2

			  NativePtr->setRetryAttempts( retryAttempts );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::SetPingInterval( Int32 pingInterval )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setPingInterval( pingInterval );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

      CacheFactory^ CacheFactory::SetUpdateLocatorListInterval( Int32 updateLocatorListInterval )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setUpdateLocatorListInterval( updateLocatorListInterval );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

      CacheFactory^ CacheFactory::SetStatisticInterval( Int32 statisticInterval )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setStatisticInterval( statisticInterval );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

      CacheFactory^ CacheFactory::SetServerGroup( String^ group )
      {
			  _GF_MG_EXCEPTION_TRY2

        ManagedString mg_servergroup( group );
        NativePtr->setServerGroup( mg_servergroup.CharPtr );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::AddLocator( String^ host, Int32 port )
      {
			  _GF_MG_EXCEPTION_TRY2

        ManagedString mg_host( host );
        NativePtr->addLocator( mg_host.CharPtr, port );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

      CacheFactory^ CacheFactory::AddServer( String^ host, Int32 port )
      {
			  _GF_MG_EXCEPTION_TRY2

			  ManagedString mg_host( host );
        NativePtr->addServer( mg_host.CharPtr, port );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::SetSubscriptionEnabled( Boolean enabled )
      {
			  _GF_MG_EXCEPTION_TRY2

			  NativePtr->setSubscriptionEnabled( enabled );
        return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

      CacheFactory^ CacheFactory::SetPRSingleHopEnabled( Boolean enabled )
      {
        _GF_MG_EXCEPTION_TRY2

          NativePtr->setPRSingleHopEnabled(enabled);
          return this;

         _GF_MG_EXCEPTION_CATCH_ALL2
      }

		  CacheFactory^ CacheFactory::SetSubscriptionRedundancy( Int32 redundancy )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setSubscriptionRedundancy( redundancy );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::SetSubscriptionMessageTrackingTimeout( Int32 messageTrackingTimeout )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setSubscriptionMessageTrackingTimeout( messageTrackingTimeout );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

		  CacheFactory^ CacheFactory::SetSubscriptionAckInterval( Int32 ackInterval )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setSubscriptionAckInterval( ackInterval );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
		  }

      CacheFactory^ CacheFactory::SetThreadLocalConnections( bool enabled )
      {
        _GF_MG_EXCEPTION_TRY2

        NativePtr->setThreadLocalConnections( enabled );

        _GF_MG_EXCEPTION_CATCH_ALL2

        return this;
      }

      CacheFactory^ CacheFactory::SetMultiuserAuthentication( bool multiuserAuthentication )
      {
			  _GF_MG_EXCEPTION_TRY2

          NativePtr->setMultiuserAuthentication( multiuserAuthentication );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
	   }

			CacheFactory^ CacheFactory::SetPdxIgnoreUnreadFields(bool ignore)
			{
				_GF_MG_EXCEPTION_TRY2

          NativePtr->setPdxIgnoreUnreadFields( ignore );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
			}

      CacheFactory^ CacheFactory::SetPdxReadSerialized(bool pdxReadSerialized)
      {
        	_GF_MG_EXCEPTION_TRY2

          NativePtr->setPdxReadSerialized( pdxReadSerialized );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
      }

      CacheFactory^ CacheFactory::Set(String^ name, String^ value)
      {
        _GF_MG_EXCEPTION_TRY2
          ManagedString mg_name( name );
          ManagedString mg_value( value );
          NativePtr->set( mg_name.CharPtr, mg_value.CharPtr );
          return this;

			  _GF_MG_EXCEPTION_CATCH_ALL2
      }
      } // end namespace Generic
    }
  }
}
