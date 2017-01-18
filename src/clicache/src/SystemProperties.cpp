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
#include "SystemProperties.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      SystemProperties::SystemProperties( Properties<String^, String^>^ properties )
      {
        _GF_MG_EXCEPTION_TRY2

          SetPtr(new apache::geode::client::SystemProperties(apache::geode::client::PropertiesPtr(
            GetNativePtr<apache::geode::client::Properties>(properties))), true);

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      SystemProperties::SystemProperties( Properties<String^, String^>^ properties,
        String^ configFile )
      {
        _GF_MG_EXCEPTION_TRY2

          ManagedString mg_configFile( configFile );
          apache::geode::client::PropertiesPtr propertiesptr(
            GetNativePtr<apache::geode::client::Properties>( properties ) );
          SetPtr( new apache::geode::client::SystemProperties( propertiesptr,
            mg_configFile.CharPtr ), true );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      void SystemProperties::LogSettings( )
      {
        NativePtr->logSettings( );
      }

      int32_t SystemProperties::StatisticsSampleInterval::get( )
      {
        return NativePtr->statisticsSampleInterval( );
      }

      bool SystemProperties::StatisticsEnabled::get( )
      {
        return NativePtr->statisticsEnabled( );
      }

      String^ SystemProperties::StatisticsArchiveFile::get( )
      {
        return ManagedString::Get( NativePtr->statisticsArchiveFile( ) );
      }

      String^ SystemProperties::LogFileName::get( )
      {
        return ManagedString::Get( NativePtr->logFilename( ) );
      }

      LogLevel SystemProperties::GFLogLevel::get( )
      {
        return static_cast<LogLevel>( NativePtr->logLevel( ) );
      }

      bool SystemProperties::HeapLRULimitEnabled::get( )
      {
        return NativePtr->heapLRULimitEnabled( );
      }
      
      size_t SystemProperties::HeapLRULimit::get( )
      {
        return NativePtr->heapLRULimit( );
      }
      
      int32_t SystemProperties::HeapLRUDelta::get( )
      {
        return NativePtr->heapLRUDelta( );
      }
      
      int32_t SystemProperties::MaxSocketBufferSize::get( )
      {
        return NativePtr->maxSocketBufferSize( );
      }
      
      int32_t SystemProperties::PingInterval::get( )
      {
        return NativePtr->pingInterval( );
      }
      
      int32_t SystemProperties::RedundancyMonitorInterval::get( )
      {
        return NativePtr->redundancyMonitorInterval( );
      }
      
      int32_t SystemProperties::NotifyAckInterval::get( )
      {
        return NativePtr->notifyAckInterval( );
      }
      
      int32_t SystemProperties::NotifyDupCheckLife::get( )
      {
        return NativePtr->notifyDupCheckLife( );
      }
      
      bool SystemProperties::DebugStackTraceEnabled::get( )
      {
        return NativePtr->debugStackTraceEnabled( );
      }

      bool SystemProperties::CrashDumpEnabled::get( )
      {
        return NativePtr->crashDumpEnabled();
      }

      bool SystemProperties::AppDomainEnabled::get( )
      {
        return NativePtr->isAppDomainEnabled();
      }

      String^ SystemProperties::Name::get( )
      {
        return ManagedString::Get( NativePtr->name( ) );
      }

      String^ SystemProperties::CacheXmlFile::get( )
      {
        return ManagedString::Get( NativePtr->cacheXMLFile( ) );
      }

      int32_t SystemProperties::LogFileSizeLimit::get( )
      {
        return NativePtr->logFileSizeLimit( );
      }

	  int32_t SystemProperties::LogDiskSpaceLimit::get( )
      {
		  return NativePtr->logDiskSpaceLimit( );
      }

      int32_t SystemProperties::StatsFileSizeLimit::get( )
      {
        return NativePtr->statsFileSizeLimit( );
      }

	  int32_t SystemProperties::StatsDiskSpaceLimit::get( )
      {
		  return NativePtr->statsDiskSpaceLimit( );
      }

      uint32_t SystemProperties::MaxQueueSize::get( )
      {
        return NativePtr->maxQueueSize( );
      }

      bool SystemProperties::SSLEnabled::get( )
      {
        return NativePtr->sslEnabled();
      }

      String^ SystemProperties::SSLKeyStore::get()
      {
        return ManagedString::Get(NativePtr->sslKeyStore());
      }

      String^ SystemProperties::SSLTrustStore::get()
      {
        return ManagedString::Get(NativePtr->sslTrustStore());
      }
      
      // adongre
      String^ SystemProperties::SSLKeystorePassword::get()
      {
        return ManagedString::Get(NativePtr->sslKeystorePassword());
      }


      bool SystemProperties::IsSecurityOn::get( )
      {
        return NativePtr->isSecurityOn( );
      }

      Properties<String^, String^>^ SystemProperties::GetSecurityProperties::get( )
      {
        return Properties<String^, String^>::Create<String^, String^>( NativePtr->getSecurityProperties( ).ptr( ) );
      }

      String^ SystemProperties::DurableClientId::get( )
      {
        return ManagedString::Get( NativePtr->durableClientId( ) );
      }

      uint32_t SystemProperties::DurableTimeout::get( )
      {
        return NativePtr->durableTimeout( );
      }

      uint32_t SystemProperties::ConnectTimeout::get( )
      {
        return NativePtr->connectTimeout( );
      }

      String^ SystemProperties::ConflateEvents::get( )
      {
        return ManagedString::Get( NativePtr->conflateEvents( ) );
      }

      uint32_t SystemProperties::SuspendedTxTimeout::get( )
      {
        return NativePtr->suspendedTxTimeout( );
      }

      bool SystemProperties::ReadTimeoutUnitInMillis::get( )
      {
        return NativePtr->readTimeoutUnitInMillis( );
      }

       bool SystemProperties::OnClientDisconnectClearPdxTypeIds::get( )
      {
        return NativePtr->onClientDisconnectClearPdxTypeIds( );
      }

      } // end namespace Generic
    }
  }
}
