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

namespace Apache
{
  namespace Geode
  {
    namespace Client
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

           apache::geode::client::PropertiesPtr nativepropsptr(
            GetNativePtr<apache::geode::client::Properties>(dsProps));

          apache::geode::client::CacheFactoryPtr& nativeptr( apache::geode::client::CacheFactory::createCacheFactory( nativepropsptr) );         
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
             apache::geode::client::PropertiesPtr nativepropsptr(
               GetNativePtr<apache::geode::client::Properties>(m_dsProps));
            DistributedSystem::AppDomainInstanceInitialization(nativepropsptr);                  
          }

          apache::geode::client::CachePtr& nativeptr( NativePtr->create( ) );
					pdxIgnoreUnreadFields = nativeptr->getPdxIgnoreUnreadFields();
          pdxReadSerialized = nativeptr->getPdxReadSerialized();

          appDomainEnable = DistributedSystem::SystemProperties->AppDomainEnabled;
          Log::SetLogLevel(static_cast<LogLevel>(apache::geode::client::Log::logLevel( )));
					//TODO::split
          SafeConvertClassGeneric::SetAppDomainEnabled(appDomainEnable);

            Serializable::RegisterTypeGeneric(
              apache::geode::client::GemfireTypeIds::PdxType,
              gcnew TypeFactoryMethodGeneric(Apache::Geode::Client::Generic::Internal::PdxType::CreateDeserializable),
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
					Apache::Geode::Client::Generic::Internal::PdxTypeRegistry::PdxIgnoreUnreadFields = pdxIgnoreUnreadFields; 
          Apache::Geode::Client::Generic::Internal::PdxTypeRegistry::PdxReadSerialized = pdxReadSerialized; 
          DistributedSystem::releaseDisconnectLock();
        }
      }

      Cache^ CacheFactory::GetInstance( DistributedSystem^ system )
      {
        _GF_MG_EXCEPTION_TRY2

          apache::geode::client::DistributedSystemPtr p_system(
            GetNativePtr<apache::geode::client::DistributedSystem>( system ) );
          apache::geode::client::CachePtr& nativeptr(
            apache::geode::client::CacheFactory::getInstance( p_system ) );

          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      Cache^ CacheFactory::GetInstanceCloseOk( DistributedSystem^ system )
      {
        _GF_MG_EXCEPTION_TRY2

          apache::geode::client::DistributedSystemPtr p_system(
            GetNativePtr<apache::geode::client::DistributedSystem>( system ) );
          apache::geode::client::CachePtr& nativeptr(
            apache::geode::client::CacheFactory::getInstanceCloseOk( p_system ) );

          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      Cache^ CacheFactory::GetAnyInstance( )
      {
        _GF_MG_EXCEPTION_TRY2

          apache::geode::client::CachePtr& nativeptr(
            apache::geode::client::CacheFactory::getAnyInstance( ) );
          return Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      String^ CacheFactory::Version::get( )
      {
        return ManagedString::Get( apache::geode::client::CacheFactory::getVersion( ) );
      }

      String^ CacheFactory::ProductDescription::get( )
      {
        return ManagedString::Get(
          apache::geode::client::CacheFactory::getProductDescription( ) );
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
