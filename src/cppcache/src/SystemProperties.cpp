/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include <string>
#include <stdlib.h>
#include <string.h>

#include <gfcpp/SystemProperties.hpp>
#include <CppCacheLibrary.hpp>
#include <gfcpp/Log.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <ace/OS.h>
#include <ace/DLL.h>

#if defined(_WIN32)
#include <windows.h>
#else
#include <dlfcn.h>
#endif

using namespace gemfire;
namespace gemfire_impl {
void* getFactoryFunc(const char* lib, const char* funcName);
}

using namespace gemfire_impl;

/******************************************************************************/

/**
 * The implementation of the SystemProperties class
 *
 *
 *
 */

namespace {

const char StatisticsSampleInterval[] = "statistic-sample-rate";
const char StatisticsEnabled[] = "statistic-sampling-enabled";
const char AppDomainEnabled[] = "appdomain-enabled";
const char StatisticsArchiveFile[] = "statistic-archive-file";
const char LogFilename[] = "log-file";
const char LogLevel[] = "log-level";

const char Name[] = "name";
const char JavaConnectionPoolSize[] = "connection-pool-size";
const char DebugStackTraceEnabled[] = "stacktrace-enabled";
// crash dump related properties
const char CrashDumpEnabled[] = "crash-dump-enabled";

const char LicenseFilename[] = "license-file";
const char LicenseType[] = "license-type";
const char CacheXMLFile[] = "cache-xml-file";
const char LogFileSizeLimit[] = "log-file-size-limit";
const char LogDiskSpaceLimit[] = "log-disk-space-limit";
const char StatsFileSizeLimit[] = "archive-file-size-limit";
const char StatsDiskSpaceLimit[] = "archive-disk-space-limit";
const char HeapLRULimit[] = "heap-lru-limit";
const char HeapLRUDelta[] = "heap-lru-delta";
const char MaxSocketBufferSize[] = "max-socket-buffer-size";
const char PingInterval[] = "ping-interval";
const char RedundancyMonitorInterval[] = "redundancy-monitor-interval";
const char DisableShufflingEndpoint[] = "disable-shuffling-of-endpoints";
const char NotifyAckInterval[] = "notify-ack-interval";
const char NotifyDupCheckLife[] = "notify-dupcheck-life";
const char DurableClientId[] = "durable-client-id";
const char DurableTimeout[] = "durable-timeout";
const char ConnectTimeout[] = "connect-timeout";
const char ConnectWaitTimeout[] = "connect-wait-timeout";
const char BucketWaitTimeout[] = "bucket-wait-timeout";
const char ConflateEvents[] = "conflate-events";
const char SecurityClientDhAlgo[] = "security-client-dhalgo";
const char SecurityClientKsPath[] = "security-client-kspath";
const char GridClient[] = "grid-client";
const char AutoReadyForEvents[] = "auto-ready-for-events";
const char SslEnabled[] = "ssl-enabled";
const char TimeStatisticsEnabled[] = "enable-time-statistics";
const char SslKeyStore[] = "ssl-keystore";
const char SslTrustStore[] = "ssl-truststore";
const char SslKeystorePassword[] =
    "ssl-keystore-password";  // adongre: Added for Ticket #758
const char ThreadPoolSize[] = "max-fe-threads";
const char SuspendedTxTimeout[] = "suspended-tx-timeout";
const char DisableChunkHandlerThread[] = "disable-chunk-handler-thread";
const char OnClientDisconnectClearPdxTypeIds[] =
    "on-client-disconnect-clear-pdxType-Ids";
const char TombstoneTimeoutInMSec[] = "tombstone-timeout";
const char DefaultConflateEvents[] = "server";
const char ReadTimeoutUnitInMillis[] = "read-timeout-unit-in-millis";

const char DefaultDurableClientId[] = "";
const uint32_t DefaultDurableTimeout = 300;

const uint32_t DefaultConnectTimeout = 59;
const uint32_t DefaultConnectWaitTimeout = 0;
const uint32_t DefaultBucketWaitTimeout = 0;

const int DefaultSamplingInterval = 1;
const bool DefaultSamplingEnabled = true;
const bool DefaultAppDomainEnabled = false;

const char DefaultStatArchive[] = "statArchive.gfs";
const char DefaultLogFilename[] = "";  // stdout...

const Log::LogLevel DefaultLogLevel = Log::Config;

const int DefaultJavaConnectionPoolSize = 5;
const bool DefaultDebugStackTraceEnabled = false;  // or true

// defaults for crash dump related properties
const bool DefaultCrashDumpEnabled = true;

const bool DefaultGridClient = false;
const bool DefaultAutoReadyForEvents = true;
const bool DefaultSslEnabled = false;
const bool DefaultTimeStatisticsEnabled = false;  // or true;

const char DefaultSslKeyStore[] = "";
const char DefaultSslTrustStore[] = "";
const char DefaultSslKeystorePassword[] = "";  // adongre: Added for Ticket #758
const char DefaultName[] = "";
const char DefaultCacheXMLFile[] = "";
const uint32_t DefaultLogFileSizeLimit = 0;     // = unlimited
const uint32_t DefaultLogDiskSpaceLimit = 0;    // = unlimited
const uint32_t DefaultStatsFileSizeLimit = 0;   // = unlimited
const uint32_t DefaultStatsDiskSpaceLimit = 0;  // = unlimited

const uint32_t DefaultMaxQueueSize = 80000;
const uint32_t DefaultHeapLRULimit = 0;  // = unlimited, disabled when it is 0
const int32_t DefaultHeapLRUDelta = 10;  // = unlimited, disabled when it is 0

const int32_t DefaultMaxSocketBufferSize = 65 * 1024;
const int32_t DefaultPingInterval = 10;
const int32_t DefaultRedundancyMonitorInterval = 10;
const int32_t DefaultNotifyAckInterval = 1;
const int32_t DefaultNotifyDupCheckLife = 300;
const char DefaultSecurityPrefix[] = "security-";
const char DefaultAuthIniLoaderFactory[] = "security-client-auth-factory";
const char DefaultAuthIniLoaderLibrary[] = "security-client-auth-library";
const char DefaultSecurityClientDhAlgo[] ATTR_UNUSED = "";
const char DefaultSecurityClientKsPath[] ATTR_UNUSED = "";
const uint32_t DefaultThreadPoolSize = ACE_OS::num_processors() * 2;
const uint32_t DefaultSuspendedTxTimeout = 30;
const uint32_t DefaultTombstoneTimeout = 480000;
// not disable; all region api will use chunk handler thread
const bool DefaultDisableChunkHandlerThread = false;
const bool DefaultReadTimeoutUnitInMillis = false;
const bool DefaultOnClientDisconnectClearPdxTypeIds = false;
}  // namespace

LibraryAuthInitializeFn SystemProperties::managedAuthInitializeFn = NULL;

SystemProperties::SystemProperties(const PropertiesPtr& propertiesPtr,
                                   const char* configFile)
    : m_statisticsSampleInterval(DefaultSamplingInterval),
      m_statisticsEnabled(DefaultSamplingEnabled),
      m_appDomainEnabled(DefaultAppDomainEnabled),
      m_statisticsArchiveFile(NULL),
      m_logFilename(NULL),
      m_logLevel(DefaultLogLevel),
      m_sessions(0 /* setup  later in processProperty */),
      m_name(NULL),
      m_debugStackTraceEnabled(DefaultDebugStackTraceEnabled),
      m_crashDumpEnabled(DefaultCrashDumpEnabled),
      m_disableShufflingEndpoint(false),
      m_cacheXMLFile(NULL),
      m_logFileSizeLimit(DefaultLogFileSizeLimit),
      m_logDiskSpaceLimit(DefaultLogDiskSpaceLimit),
      m_statsFileSizeLimit(DefaultStatsFileSizeLimit),
      m_statsDiskSpaceLimit(DefaultStatsDiskSpaceLimit),
      m_maxQueueSize(DefaultMaxQueueSize),
      m_javaConnectionPoolSize(DefaultJavaConnectionPoolSize),
      m_heapLRULimit(DefaultHeapLRULimit),
      m_heapLRUDelta(DefaultHeapLRUDelta),
      m_maxSocketBufferSize(DefaultMaxSocketBufferSize),
      m_pingInterval(DefaultPingInterval),
      m_redundancyMonitorInterval(DefaultRedundancyMonitorInterval),
      m_notifyAckInterval(DefaultNotifyAckInterval),
      m_notifyDupCheckLife(DefaultNotifyDupCheckLife),
      m_AuthIniLoaderLibrary(NULLPTR),
      m_AuthIniLoaderFactory(NULLPTR),
      m_securityClientDhAlgo(NULLPTR),
      m_securityClientKsPath(NULLPTR),
      m_authInitializer(NULLPTR),
      m_durableClientId(NULL),
      m_durableTimeout(DefaultDurableTimeout),
      m_connectTimeout(DefaultConnectTimeout),
      m_connectWaitTimeout(DefaultConnectWaitTimeout),
      m_bucketWaitTimeout(DefaultBucketWaitTimeout),
      m_gridClient(DefaultGridClient),
      m_autoReadyForEvents(DefaultAutoReadyForEvents),
      m_sslEnabled(DefaultSslEnabled),
      m_timestatisticsEnabled(DefaultTimeStatisticsEnabled),
      m_sslKeyStore(NULL),
      m_sslTrustStore(NULL),
      m_sslKeystorePassword(NULL),  // adongre: Added for Ticket #758
      m_conflateEvents(NULL),
      m_threadPoolSize(DefaultThreadPoolSize),
      m_suspendedTxTimeout(DefaultSuspendedTxTimeout),
      m_tombstoneTimeoutInMSec(DefaultTombstoneTimeout),
      m_disableChunkHandlerThread(DefaultDisableChunkHandlerThread),
      m_readTimeoutUnitInMillis(DefaultReadTimeoutUnitInMillis),
      m_onClientDisconnectClearPdxTypeIds(
          DefaultOnClientDisconnectClearPdxTypeIds) {
  processProperty(ConflateEvents, DefaultConflateEvents);

  processProperty(DurableClientId, DefaultDurableClientId);

  processProperty(SslKeyStore, DefaultSslKeyStore);
  processProperty(SslTrustStore, DefaultSslTrustStore);
  // adongre: Added for Ticket #758
  processProperty(SslKeystorePassword, DefaultSslKeystorePassword);

  processProperty(StatisticsArchiveFile, DefaultStatArchive);

  processProperty(LogFilename, DefaultLogFilename);
  processProperty(CacheXMLFile, DefaultCacheXMLFile);
  processProperty(Name, DefaultName);

  // now that defaults are set, consume files and override the defaults.
  class ProcessPropsVisitor : public Properties::Visitor {
    SystemProperties* m_sysProps;

   public:
    explicit ProcessPropsVisitor(SystemProperties* sysProps)
        : m_sysProps(sysProps) {}
    void visit(CacheableKeyPtr& key, CacheablePtr& value) {
      CacheableStringPtr prop = key->toString();
      CacheableStringPtr val;
      if (value != NULLPTR) {
        val = value->toString();
      }
      m_sysProps->processProperty(prop->asChar(), val->asChar());
    }
  } processPropsVisitor(this);

  m_securityPropertiesPtr = Properties::create();
  PropertiesPtr givenConfigPtr = Properties::create();
  // Load the file from product tree.
  try {
    std::string defsysprops =
        CppCacheLibrary::getProductDir() + "/defaultSystem/gfcpp.properties";
    givenConfigPtr->load(defsysprops.c_str());
  } catch (Exception&) {
    LOGERROR(
        "Unable to determine Product Directory. Please set the "
        "GFCPP environment variable.");
    throw;
  }

  // Load the file from current directory.
  if (configFile == NULL) {
    givenConfigPtr->load("./gfcpp.properties");
  } else {
    givenConfigPtr->load(configFile);
  }
  // process each loaded property.
  givenConfigPtr->foreach (processPropsVisitor);

  // Now consume any properties provided by the Properties object in code.
  if (propertiesPtr != NULLPTR) {
    propertiesPtr->foreach (processPropsVisitor);
  }

  m_AuthIniLoaderLibrary =
      m_securityPropertiesPtr->find(DefaultAuthIniLoaderLibrary);
  m_AuthIniLoaderFactory =
      m_securityPropertiesPtr->find(DefaultAuthIniLoaderFactory);
  m_securityClientDhAlgo = m_securityPropertiesPtr->find(SecurityClientDhAlgo);
  m_securityClientKsPath = m_securityPropertiesPtr->find(SecurityClientKsPath);

  // Deleting inorder to prevent it from sending to Server.
  m_securityPropertiesPtr->remove(DefaultAuthIniLoaderLibrary);
  m_securityPropertiesPtr->remove(DefaultAuthIniLoaderFactory);
}

SystemProperties::~SystemProperties() {
  GF_SAFE_DELETE_ARRAY(m_statisticsArchiveFile);
  GF_SAFE_DELETE_ARRAY(m_logFilename);
  GF_SAFE_DELETE_ARRAY(m_name);
  GF_SAFE_DELETE_ARRAY(m_cacheXMLFile);
  GF_SAFE_DELETE_ARRAY(m_durableClientId);
  GF_SAFE_DELETE_ARRAY(m_sslKeyStore);
  GF_SAFE_DELETE_ARRAY(m_sslTrustStore);
  // adongre: Added for Ticket #758
  GF_SAFE_DELETE_ARRAY(m_sslKeystorePassword);
  GF_SAFE_DELETE_ARRAY(m_conflateEvents);
}

void SystemProperties::throwError(const char* msg) {
  LOGERROR(msg);
  throw GemfireConfigException(msg);
}

void SystemProperties::processProperty(const char* property,
                                       const char* value) {
  std::string prop = property;
  if (prop == ThreadPoolSize) {
    char* end;
    uint32_t si = strtoul(value, &end, 10);
    if (!*end) {
      m_threadPoolSize = si;
    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }
  } else if (prop == MaxSocketBufferSize) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_maxSocketBufferSize = si;
    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == PingInterval) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_pingInterval = si;
    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == RedundancyMonitorInterval) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_redundancyMonitorInterval = si;
    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == NotifyAckInterval) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_notifyAckInterval = si;
    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }
  } else if (prop == NotifyDupCheckLife) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_notifyDupCheckLife = si;
    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == StatisticsSampleInterval) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_statisticsSampleInterval = si;

    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }
  } else if (prop == DurableTimeout) {
    char* end;
    uint32_t si = strtoul(value, &end, 10);
    if (!*end) {
      m_durableTimeout = si;

    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == ConnectTimeout) {
    char* end;
    uint32_t si = strtoul(value, &end, 10);
    if (!*end) {
      m_connectTimeout = si;

    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == ConnectWaitTimeout) {
    char* end;
    uint32_t si = strtoul(value, &end, 10);
    if (!*end) {
      m_connectWaitTimeout = si;

    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == BucketWaitTimeout) {
    char* end;
    uint32_t si = strtoul(value, &end, 10);
    if (!*end) {
      m_bucketWaitTimeout = si;

    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == DisableShufflingEndpoint) {
    std::string val = value;
    if (val == "false") {
      m_disableShufflingEndpoint = false;
    } else if (val == "true") {
      m_disableShufflingEndpoint = true;
    }
  } else if (prop == AppDomainEnabled) {
    std::string val = value;
    if (val == "false") {
      m_appDomainEnabled = false;
    } else if (val == "true") {
      m_appDomainEnabled = true;
    }
  } else if (prop == DebugStackTraceEnabled) {
    std::string val = value;
    if (val == "false") {
      m_debugStackTraceEnabled = false;
    } else if (val == "true") {
      m_debugStackTraceEnabled = true;
    } else {
      throwError(("SystemProperties: non-boolean " + prop + "=" + val).c_str());
    }

  } else if (prop == GridClient) {
    std::string val = value;
    if (val == "false") {
      m_gridClient = false;
    } else if (val == "true") {
      m_gridClient = true;
    } else {
      throwError(("SystemProperties: non-boolean " + prop + "=" + val).c_str());
    }
  } else if (prop == AutoReadyForEvents) {
    std::string val = value;
    if (val == "false") {
      m_autoReadyForEvents = false;
    } else if (val == "true") {
      m_autoReadyForEvents = true;
    } else {
      throwError(("SystemProperties: non-boolean " + prop + "=" + val).c_str());
    }
  } else if (prop == SslEnabled) {
    std::string val = value;
    if (val == "false") {
      m_sslEnabled = false;
    } else if (val == "true") {
      m_sslEnabled = true;
    } else {
      throwError(("SystemProperties: non-boolean " + prop + "=" + val).c_str());
    }

  } else if (prop == TimeStatisticsEnabled) {
    std::string val = value;
    if (val == "false") {
      m_timestatisticsEnabled = false;
    } else if (val == "true") {
      m_timestatisticsEnabled = true;
    } else {
      throwError(("SystemProperties: non-boolean " + prop + "=" + val).c_str());
    }

  } else if (prop == CrashDumpEnabled) {
    std::string val = value;
    if (val == "false") {
      m_crashDumpEnabled = false;
    } else if (val == "true") {
      m_crashDumpEnabled = true;
    } else {
      throwError(("SystemProperties: non-boolean " + prop + "=" + val).c_str());
    }
  } else if (prop == StatisticsEnabled) {
    std::string val = value;
    if (val == "false") {
      m_statisticsEnabled = false;
    } else if (val == "true") {
      m_statisticsEnabled = true;
    } else {
      throwError(("SystemProperties: non-boolean " + prop + "=" + val).c_str());
    }

  } else if (prop == StatisticsArchiveFile) {
    if (m_statisticsArchiveFile != NULL) {
      delete[] m_statisticsArchiveFile;
    }
    size_t len = strlen(value) + 1;
    m_statisticsArchiveFile = new char[len];
    ACE_OS::strncpy(m_statisticsArchiveFile, value, len);

  } else if (prop == LogFilename) {
    if (m_logFilename != NULL) {
      delete[] m_logFilename;
    }
    if (value != NULL) {
      size_t len = strlen(value) + 1;
      m_logFilename = new char[len];
      ACE_OS::strncpy(m_logFilename, value, len);
    }
  } else if (prop == LogLevel) {
    try {
      Log::LogLevel level = Log::charsToLevel(value);
      m_logLevel = level;

    } catch (IllegalArgumentException&) {
      throwError(("SystemProperties: unknown log level " + prop + "=" + value)
                     .c_str());
    }

  } else if (prop == JavaConnectionPoolSize) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_javaConnectionPoolSize = si;
    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == Name) {
    if (m_name != NULL) {
      delete[] m_name;
    }
    if (value != NULL) {
      size_t len = strlen(value) + 1;
      m_name = new char[len];
      ACE_OS::strncpy(m_name, value, len);
    }
  } else if (prop == DurableClientId) {
    if (m_durableClientId != NULL) {
      delete[] m_durableClientId;
      m_durableClientId = NULL;
    }
    if (value != NULL) {
      size_t len = strlen(value) + 1;
      m_durableClientId = new char[len];
      ACE_OS::strncpy(m_durableClientId, value, len);
    }
  } else if (prop == SslKeyStore) {
    if (m_sslKeyStore != NULL) {
      delete[] m_sslKeyStore;
      m_sslKeyStore = NULL;
    }
    if (value != NULL) {
      size_t len = strlen(value) + 1;
      m_sslKeyStore = new char[len];
      ACE_OS::strncpy(m_sslKeyStore, value, len);
    }
  } else if (prop == SslTrustStore) {
    if (m_sslTrustStore != NULL) {
      delete[] m_sslTrustStore;
      m_sslTrustStore = NULL;
    }
    if (value != NULL) {
      size_t len = strlen(value) + 1;
      m_sslTrustStore = new char[len];
      ACE_OS::strncpy(m_sslTrustStore, value, len);
    }
    // adongre: Added for Ticket #758
  } else if (prop == SslKeystorePassword) {
    if (m_sslKeystorePassword != NULL) {
      delete[] m_sslKeystorePassword;
      m_sslKeystorePassword = NULL;
    }
    if (value != NULL) {
      size_t len = strlen(value) + 1;
      m_sslKeystorePassword = new char[len];
      ACE_OS::strncpy(m_sslKeystorePassword, value, len);
    }
  } else if (prop == ConflateEvents) {
    if (m_conflateEvents != NULL) {
      delete[] m_conflateEvents;
      m_conflateEvents = NULL;
    }
    if (value != NULL) {
      size_t len = strlen(value) + 1;
      m_conflateEvents = new char[len];
      ACE_OS::strncpy(m_conflateEvents, value, len);
    }
  } else if (prop == LicenseFilename) {
    // ignore license-file
  } else if (prop == LicenseType) {
    // ignore license-type
  } else if (prop == CacheXMLFile) {
    if (m_cacheXMLFile != NULL) {
      delete[] m_cacheXMLFile;
    }
    if (value != NULL) {
      size_t len = strlen(value) + 1;
      m_cacheXMLFile = new char[len];
      ACE_OS::strncpy(m_cacheXMLFile, value, len);
    }

  } else if (prop == LogFileSizeLimit) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_logFileSizeLimit = si;

    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }
  } else if (prop == LogDiskSpaceLimit) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_logDiskSpaceLimit = si;

    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }
  } else if (prop == StatsFileSizeLimit) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_statsFileSizeLimit = si;

    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == StatsDiskSpaceLimit) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_statsDiskSpaceLimit = si;
    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == HeapLRULimit) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_heapLRULimit = si;

    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }
  } else if (prop == HeapLRUDelta) {
    char* end;
    long si = strtol(value, &end, 10);
    if (!*end) {
      m_heapLRUDelta = si;

    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }
  } else if (prop == SuspendedTxTimeout) {
    char* end;
    uint32_t si = strtoul(value, &end, 10);
    if (!*end) {
      m_suspendedTxTimeout = si;

    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }

  } else if (prop == TombstoneTimeoutInMSec) {  // Added system properties for
                                                // TombStone-Timeout.
    char* end;
    uint32_t si = strtoul(value, &end, 10);
    if (!*end) {
      m_tombstoneTimeoutInMSec = si;
    } else {
      throwError(
          ("SystemProperties: non-integer " + prop + "=" + value).c_str());
    }
  } else if (strncmp(property, DefaultSecurityPrefix,
                     sizeof(DefaultSecurityPrefix) - 1) == 0) {
    m_securityPropertiesPtr->insert(property, value);
  } else if (prop == DisableChunkHandlerThread) {
    std::string val = value;
    if (val == "false") {
      m_disableChunkHandlerThread = false;
    } else if (val == "true") {
      m_disableChunkHandlerThread = true;
    } else {
      throwError(("SystemProperties: non-boolean " + prop + "=" + val).c_str());
    }
  } else if (prop == OnClientDisconnectClearPdxTypeIds) {
    std::string val = value;
    if (val == "false") {
      m_onClientDisconnectClearPdxTypeIds = false;
    } else if (val == "true") {
      m_onClientDisconnectClearPdxTypeIds = true;
    } else {
      throwError(("SystemProperties: non-boolean " + prop + "=" + val).c_str());
    }
  } else if (prop == ReadTimeoutUnitInMillis) {
    std::string val = value;
    if (val == "false") {
      m_readTimeoutUnitInMillis = false;
    } else if (val == "true") {
      m_readTimeoutUnitInMillis = true;
    } else {
      throwError(("SystemProperties: non-boolean " + prop + "=" + val).c_str());
    }
  } else {
    char msg[1000];
    ACE_OS::snprintf(msg, 1000, "SystemProperties: unknown property: %s = %s",
                     property, value);
    throwError(msg);
  }
}

void SystemProperties::logSettings() {
  // *** PLEASE ADD IN ALPHABETICAL ORDER - USER VISIBLE ***

  std::string settings = "GemFire Native Client System Properties:";

  char buf[2048];

  settings += "\n  appdomain-enabled = ";
  settings += isAppDomainEnabled() ? "true" : "false";

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, statsDiskSpaceLimit());
  settings += "\n  archive-disk-space-limit = ";
  settings += buf;

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, statsFileSizeLimit());
  settings += "\n  archive-file-size-limit = ";
  settings += buf;

  settings += "\n  auto-ready-for-events = ";
  settings += autoReadyForEvents() ? "true" : "false";

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, bucketWaitTimeout());
  settings += "\n  bucket-wait-timeout = ";
  settings += buf;

  settings += "\n  cache-xml-file = ";
  settings += cacheXMLFile();

  settings += "\n  conflate-events = ";
  settings += conflateEvents();

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, connectTimeout());
  settings += "\n  connect-timeout = ";
  settings += buf;

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, javaConnectionPoolSize());
  settings += "\n  connection-pool-size = ";
  settings += buf;

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, connectWaitTimeout());
  settings += "\n  connect-wait-timeout = ";
  settings += buf;

  settings += "\n  crash-dump-enabled = ";
  settings += crashDumpEnabled() ? "true" : "false";

  settings += "\n  disable-chunk-handler-thread = ";
  settings += disableChunkHandlerThread() ? "true" : "false";

  settings += "\n  disable-shuffling-of-endpoints = ";
  settings += isEndpointShufflingDisabled() ? "true" : "false";

  settings += "\n  durable-client-id = ";
  settings += durableClientId();

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, durableTimeout());
  settings += "\n  durable-timeout = ";
  settings += buf;

  // *** PLEASE ADD IN ALPHABETICAL ORDER - USER VISIBLE ***

  settings += "\n  enable-time-statistics = ";
  settings += getEnableTimeStatistics() ? "true" : "false";

  settings += "\n  grid-client = ";
  settings += isGridClient() ? "true" : "false";

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, heapLRUDelta());
  settings += "\n  heap-lru-delta = ";
  settings += buf;
  /* adongre  - Coverity II
   * CID 29195: Printf arg type mismatch (PW.PRINTF_ARG_MISMATCH)
   */
  ACE_OS::snprintf(buf, 2048, "%" PRIu32, static_cast<int>(heapLRULimit()));
  settings += "\n  heap-lru-limit = ";
  settings += buf;

  // settings += "\n  license-file = ";
  // settings += licenseFilename();

  // settings += "\n  license-type = ";
  // settings += licenseType();

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, logDiskSpaceLimit());
  settings += "\n  log-disk-space-limit = ";
  settings += buf;

  settings += "\n  log-file = ";
  settings += logFilename();

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, logFileSizeLimit());
  settings += "\n  log-file-size-limit = ";
  settings += buf;

  settings += "\n  log-level = ";
  settings += Log::levelToChars(logLevel());

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, threadPoolSize());
  settings += "\n  max-fe-threads = ";
  settings += buf;

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, maxSocketBufferSize());
  settings += "\n  max-socket-buffer-size = ";
  settings += buf;

  ACE_OS::snprintf(buf, 2048, "%" PRIi32, notifyAckInterval());
  settings += "\n  notify-ack-interval = ";
  settings += buf;

  ACE_OS::snprintf(buf, 2048, "%" PRIi32, notifyDupCheckLife());
  settings += "\n  notify-dupcheck-life = ";
  settings += buf;

  settings += "\n  on-client-disconnect-clear-pdxType-Ids = ";
  settings += onClientDisconnectClearPdxTypeIds() ? "true" : "false";

  // *** PLEASE ADD IN ALPHABETICAL ORDER - USER VISIBLE ***

  ACE_OS::snprintf(buf, 2048, "%" PRIi32, pingInterval());
  settings += "\n  ping-interval = ";
  settings += buf;

  settings += "\n  read-timeout-unit-in-millis = ";
  settings += readTimeoutUnitInMillis() ? "true" : "false";

  ACE_OS::snprintf(buf, 2048, "%" PRIi32, redundancyMonitorInterval());
  settings += "\n  redundancy-monitor-interval = ";
  settings += buf;

  settings += "\n  security-client-auth-factory = ";
  settings += authInitFactory();

  settings += "\n  security-client-auth-library = ";
  settings += authInitLibrary();

  settings += "\n  security-client-dhalgo = ";
  settings += securityClientDhAlgo();

  settings += "\n  security-client-kspath = ";
  settings += securityClientKsPath();

  settings += "\n  ssl-enabled = ";
  settings += sslEnabled() ? "true" : "false";

  settings += "\n  ssl-keystore = ";
  settings += sslKeyStore();

  settings += "\n  ssl-truststore = ";
  settings += sslTrustStore();

  // settings += "\n ssl-keystore-password = ";
  // settings += sslKeystorePassword();

  settings += "\n  stacktrace-enabled = ";
  settings += debugStackTraceEnabled() ? "true" : "false";

  settings += "\n  statistic-archive-file = ";
  settings += statisticsArchiveFile();

  settings += "\n  statistic-sampling-enabled = ";
  settings += statisticsEnabled() ? "true" : "false";

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, statisticsSampleInterval());
  settings += "\n  statistic-sample-rate = ";
  settings += buf;

  ACE_OS::snprintf(buf, 2048, "%" PRIu32, suspendedTxTimeout());
  settings += "\n  suspended-tx-timeout = ";
  settings += buf;

  // tombstone-timeout
  ACE_OS::snprintf(buf, 2048, "%" PRIu32, tombstoneTimeoutInMSec());
  settings += "\n  tombstone-timeout = ";
  settings += buf;

  // *** PLEASE ADD IN ALPHABETICAL ORDER - USER VISIBLE ***

  LOGCONFIG(settings.c_str());
}

AuthInitializePtr SystemProperties::getAuthLoader() {
  if ((m_authInitializer == NULLPTR) && (m_AuthIniLoaderLibrary != NULLPTR &&
                                         m_AuthIniLoaderFactory != NULLPTR)) {
    if (managedAuthInitializeFn != NULL &&
        strchr(m_AuthIniLoaderFactory->asChar(), '.') != NULL) {
      // this is a managed library
      m_authInitializer = (*managedAuthInitializeFn)(
          m_AuthIniLoaderLibrary->asChar(), m_AuthIniLoaderFactory->asChar());
    } else {
      AuthInitialize* (*funcptr)();
      funcptr = reinterpret_cast<AuthInitialize* (*)()>(getFactoryFunc(
          m_AuthIniLoaderLibrary->asChar(), m_AuthIniLoaderFactory->asChar()));
      if (funcptr == NULL) {
        LOGERROR("Failed to acquire handle to AuthInitialize library");
        return NULLPTR;
      }
      AuthInitialize* p = funcptr();
      m_authInitializer = p;
    }
  } else if (m_authInitializer == NULLPTR) {
    LOGFINE("No AuthInitialize library or factory configured");
  }
  return m_authInitializer;
}
