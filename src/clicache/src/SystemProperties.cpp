/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

          SetPtr(new gemfire::SystemProperties(gemfire::PropertiesPtr(
            GetNativePtr<gemfire::Properties>(properties))), true);

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      SystemProperties::SystemProperties( Properties<String^, String^>^ properties,
        String^ configFile )
      {
        _GF_MG_EXCEPTION_TRY2

          ManagedString mg_configFile( configFile );
          gemfire::PropertiesPtr propertiesptr(
            GetNativePtr<gemfire::Properties>( properties ) );
          SetPtr( new gemfire::SystemProperties( propertiesptr,
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
