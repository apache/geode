/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/SystemProperties.hpp>
#include "impl/NativeWrapper.hpp"
#include "Log.hpp"
#include "Properties.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      /// <summary>
      /// A class for internal use, that encapsulates the properties that can be
      /// set through <see cref="DistributedSystem.Connect" />
      /// or a gfcpp.properties file.
      /// </summary>
      public ref class SystemProperties sealed
        : public Internal::UMWrap<gemfire::SystemProperties>
      {
      public:

        /// <summary>
        /// Constructor. Sets the default (hard-coded) values first, and then overwrites those with
        /// any values found in the given properties.
        /// </summary>
        /// <param name="properties">initialize with the given properties</param>
        //generic <class TPropKey, class TPropValue>
        SystemProperties( Properties<String^, String^>^ properties );

        /// <summary>
        /// Constructor.
        /// <ol>
        /// <li>Sets the default (hard-coded) values.</li>
        /// <li>Overwrites those with any values from <c>systemDefault/gfcpp.properties</c></li>
        /// <li>Overwrites those with any values from the given file (if it exists)
        /// or the local <c>./gfcpp.properties</c> (if the given file does not exist).</li>
        /// <li>Overwrites those with any values found in the given properties.</li>
        /// </ol>
        /// </summary>
        /// <param name="properties">these overwrite any other values already set</param>
        /// <param name="configFile">see summary</param>
        //generic <class TPropKey, class TPropValue>
        SystemProperties( Properties<String^, String^>^ properties, String^ configFile );

        /// <summary>
        /// Prints all settings to the process log.
        /// </summary>
        void LogSettings( );

        /// <summary>
        /// Returns the sampling interval, that is,
        /// how often the statistics thread writes to disk, in seconds.
        /// </summary>
        /// <returns>the statistics sampling interval</returns>
        property int32_t StatisticsSampleInterval
        {
          int32_t get( );
        }

        /// <summary>
        /// True if statistics are enabled (archived).
        /// </summary>
        /// <returns>true if enabled</returns>
        property bool StatisticsEnabled
        {
          bool get( );
        }

        /// <summary>
        /// Returns the name of the statistics archive file.
        /// </summary>
        /// <returns>the filename</returns>
        property String^ StatisticsArchiveFile
        {
          String^ get( );
        }

        /// <summary>
        /// Returns the name of the message log file.
        /// </summary>
        /// <returns>the filename</returns>
        property String^ LogFileName
        {
          String^ get( );
        }

        /// <summary>
        /// Returns the message logging level.
        /// </summary>
        /// <returns>the log level</returns>
        property LogLevel GFLogLevel
        {
          LogLevel get( );
        }

        /// <summary>
        /// Returns  a boolean that specifies if heapLRULimit has been enabled for the
        /// process. If enabled, the HeapLRULimit specifies the maximum amount of memory
        /// that values in a cache can use to store data before overflowing to disk or
        /// destroying entries to ensure that the server process never runs out of
        /// memory
        /// </summary>
        /// <returns>true if enabled</returns>
        property bool HeapLRULimitEnabled
        {
          bool get( );
        }

        /// <summary>
        /// Returns  the HeapLRULimit value (in bytes), the maximum memory that values
        /// in a cache can use to store data before overflowing to disk or destroying
        /// entries to ensure that the server process never runs out of memory due to
        /// cache memory usage
        /// </summary>
        /// <returns>the HeapLRULimit value</returns>
        property size_t HeapLRULimit
        {
          size_t get( );
        }

        /// <summary>
        /// Returns  the HeapLRUDelta value (a percent value). This specifies the
        /// percentage of entries the system will evict each time it detects that
        /// it has exceeded the HeapLRULimit. Defaults to 10%
        /// </summary>
        /// <returns>the HeapLRUDelta value</returns>
        property int32_t HeapLRUDelta
        {
          int32_t get( );
        }

        /// <summary>
        /// Returns  the maximum socket buffer size to use
        /// </summary>
        /// <returns>the MaxSocketBufferSize value</returns>
        property int32_t MaxSocketBufferSize
        {
          int32_t get( );
        }

        /// <summary>
        /// Returns  the time between two consecutive ping to servers
        /// </summary>
        /// <returns>the PingInterval value</returns>
        property int32_t PingInterval
        {
          int32_t get( );
        }

        /// <summary>
        /// Returns  the time between two consecutive checks for redundancy for HA
        /// </summary>
        /// <returns>the RedundancyMonitorInterval value</returns>
        property int32_t RedundancyMonitorInterval
        {
          int32_t get( );
        }

        /// <summary>
        /// Returns the periodic notify ack interval
        /// </summary>
        /// <returns>the NotifyAckInterval value</returns>
        property int32_t NotifyAckInterval
        {
          int32_t get( );
        }

        /// <summary>
        /// Returns the expiry time of an idle event id map entry for duplicate notification checking
        /// </summary>
        /// <returns>the NotifyDupCheckLife value</returns>
        property int32_t NotifyDupCheckLife
        {
          int32_t get( );
        }

        /// <summary>
        /// True if the stack trace is enabled.
        /// </summary>
        /// <returns>true if enabled</returns>
        property bool DebugStackTraceEnabled
        {
          bool get( );
        }

        /// <summary>
        /// True if the crash dump generation for unhandled fatal exceptions
        /// is enabled. If "log-file" property has been specified then they are
        /// created in the same directory as the log file, and having the same
        /// prefix as log file. By default crash dumps are created in the
        /// current working directory and have the "gemfire_cpp" prefix.
        ///
        /// The actual dump file will have timestamp and process ID
        /// in the full name.
        /// </summary>
        /// <returns>true if enabled</returns>
        property bool CrashDumpEnabled
        {
          bool get();
        }

        /// <summary>
        /// Whether client is running in multiple AppDomain or not.
        /// Default value is "false".
        /// </summary>
        /// <returns>true if enabled</returns>
        property bool AppDomainEnabled
        {
          bool get();
        }

        /// <summary>
        /// Returns the system name.
        /// </summary>
        /// <returns>the name</returns>
        property String^ Name
        {
          String^ get( );
        }

        /// <summary>
        /// Returns the name of the "cache.xml" file.
        /// </summary>
        /// <returns>the filename</returns>
        property String^ CacheXmlFile
        {
          String^ get( );
        }

        /// <summary>
        /// Returns the maximum log file size, in bytes, or 0 if unlimited.
        /// </summary>
        /// <returns>the maximum limit</returns>
        property int32_t LogFileSizeLimit
        {
          int32_t get( );
        }

        /// <summary>
        /// Returns the maximum log Disk size, in bytes, or 0 if unlimited.
        /// </summary>
        /// <returns>the maximum limit</returns>
        property int32_t LogDiskSpaceLimit
        {
          int32_t get( );
        }

		/// <summary>
        /// Returns the maximum statistics file size, in bytes, or 0 if unlimited.
        /// </summary>
        /// <returns>the maximum limit</returns>
        property int32_t StatsFileSizeLimit
        {
          int32_t get( );
        }

        /// <summary>
        /// Returns the maximum statistics Disk size, in bytes, or 0 if unlimited.
        /// </summary>
        /// <returns>the maximum limit</returns>
        property int32_t StatsDiskSpaceLimit
        {
          int32_t get( );
        }

		/// <summary>
        /// Returns the max queue size for notification messages
        /// </summary>
        /// <returns>the max queue size</returns>
        property uint32_t MaxQueueSize
        {
          uint32_t get( );
        }

        /// <summary>
        /// True if ssl connection support is enabled.
        /// </summary>
        /// <returns>true if enabled</returns>
        property bool SSLEnabled
        {
          bool get( );
        }

        /// <summary>
        /// Returns the SSL private keystore file path.
        /// </summary>
        /// <returns>the SSL private keystore file path</returns>
        property String^ SSLKeyStore
        {
          String^ get( );
        }

        /// <summary>
        /// Returns the SSL public certificate trust store file path.
        /// </summary>
        /// <returns>the SSL public certificate trust store file path</returns>
        property String^ SSLTrustStore
        {
          String^ get( );
        }

        // adongre
        /// <summary>
        /// Returns the client keystore password..
        /// </summary>
        /// <returns>Returns the client keystore password.</returns>
        property String^ SSLKeystorePassword
        {
          String^ get( );
        }

        /// <summary>
        /// True if client needs to be authenticated
        /// </summary>
        /// <returns>true if enabled</returns>
        property bool IsSecurityOn
        {
          bool get( );
        }

        /// <summary>
        /// Returns all the security properties
        /// </summary>
        /// <returns>the security properties</returns>
        //generic <class TPropKey, class TPropValue>
        property Properties<String^, String^>^ GetSecurityProperties {
          Properties<String^, String^>^ get( );
        }

        /// <summary>
        /// Returns the durable client's ID.
        /// </summary>
        /// <returns>the durable client ID</returns>
        property String^ DurableClientId
        {
          String^ get( );
        }

        /// <summary>
        /// Returns the durable client's timeout.
        /// </summary>
        /// <returns>the durable client timeout</returns>
        property uint32_t DurableTimeout
        {
          uint32_t get( );
        }

        /// <summary>
        /// Returns the connect timeout used for server and locator handshakes.
        /// </summary>
        /// <returns>the connect timeout used for server and locator handshakes</returns>
        property uint32_t ConnectTimeout
        {
          uint32_t get( );
        }

        /// <summary>
        /// Returns the conflate event's option
        /// </summary>
        /// <returns>the conflate event option</returns>
        property String^ ConflateEvents
        {
          String^ get( );
        }

        /// <summary>
        /// Returns the timeout after which suspended transactions are rolled back.
        /// </summary>
        /// <returns>the timeout for suspended transactions</returns>
        property uint32_t SuspendedTxTimeout
        {
          uint32_t get( );
        }

        /// <summary>
        /// This can be called to know whether read timeout unit is in milli second.
        /// </summary>
        /// <returns>true if enabled or false by default.</returns>
        property bool ReadTimeoutUnitInMillis
        {
          bool get( );
        }
        /// <summary>
        /// True if app want to clear pdx types ids on client disconnect
        /// </summary>
        /// <returns>true if enabled</returns>
        property bool OnClientDisconnectClearPdxTypeIds
        {
          bool get( );
        }

      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class, with null pointer check.
        /// </summary>
        /// <param name="nativeptr">native object pointer</param>
        /// <returns>
        /// the managed wrapper object, or null if the native pointer is null.
        /// </returns>
        inline static SystemProperties^ Create(
          gemfire::SystemProperties* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew SystemProperties( nativeptr ) : nullptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline SystemProperties( gemfire::SystemProperties* nativeptr )
          : UMWrap( nativeptr, false ) { }
      };
      } // end namespace Generic
    }
  }
}
