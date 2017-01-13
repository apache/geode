#ifndef _GEMFIRE_SYSTEMPROPERTIES_HPP_
#define _GEMFIRE_SYSTEMPROPERTIES_HPP_

/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "Properties.hpp"
#include "Log.hpp"
#include "AuthInitialize.hpp"

/** @file
*/

namespace gemfire {

/**
 * The SystemProperties class
 *
 *
 */

// Factory function typedefs to register the managed authInitialize
typedef AuthInitialize* (*LibraryAuthInitializeFn)(const char* assemblyPath,
                                                   const char* factFuncName);

/**
  * A class for internal use, that encapsulates the properties that can be
  * set from DistributedSystem::connect.
  *
  */

class CPPCACHE_EXPORT SystemProperties {
 public:
  /**
   * Constructor. Will set the default values first, and then overwrite with
   * the values found in the given Properties object (if any), and
   * then from the values in the given file (if it exists).
   *
   * If useMemType is true, use the given member type; if false, always set
   * member type to SERVER.
   */
  SystemProperties(const PropertiesPtr& propertiesPtr,
                   const char* configFile = NULL);

  /**
   * Destructor.
   */
  ~SystemProperties();

  /** print all settings to the process log. */
  void logSettings();

  const uint32_t threadPoolSize() const { return m_threadPoolSize; }

  /**
   * Returns the sampling interval of the sampling thread.
   * This would be how often the statistics thread writes to disk in seconds.
   */
  const uint32_t statisticsSampleInterval() const {
    return m_statisticsSampleInterval;
  }

  /**
   * Tells whether statistics needs to be archived or not.
   */
  bool statisticsEnabled() const { return m_statisticsEnabled; }

  /**
   * Whether SSL is enabled for socket connections.
   */
  bool sslEnabled() const { return m_sslEnabled; }

  /**
   * Whether time stats are enabled for the statistics.
   */
  bool getEnableTimeStatistics() const /*timestatisticsEnabled()*/
  {
    return m_timestatisticsEnabled;
  } /*m_timestatisticsEnabled*/

  /**
  * Returns the path of the private key file for SSL use.
  */
  const char* sslKeyStore() const { return m_sslKeyStore; }

  /**
   * Returns the client keystore password.
   */
  const char* sslKeystorePassword() const { return m_sslKeystorePassword; }

  /**
   * Returns the path of the public key file for SSL use.
   */
  const char* sslTrustStore() const { return m_sslTrustStore; }

  /**
   * Returns the name of the filename into which statistics would
   * be archived.
   */
  const char* statisticsArchiveFile() const { return m_statisticsArchiveFile; }

  /**
   * Returns the name of the filename into which logging would
   * be done.
   */
  const char* logFilename() const { return m_logFilename; }

  /**
   * Returns the log level at which logging would be done.
   */
  Log::LogLevel logLevel() const { return m_logLevel; }

  /**
   * Returns  a boolean that specifies if heapLRULimit has been enabled for the
   * process. If enabled, the HeapLRULimit specifies the maximum amount of
   * memory
   * that values in a cache can use to store data before overflowing to disk or
   * destroying entries to ensure that the server process never runs out of
   * memory
   *
   */
  const bool heapLRULimitEnabled() const { return (m_heapLRULimit > 0); }

  /**
    * Returns  the HeapLRULimit value (in bytes), the maximum memory that values
    * in a cache can use to store data before overflowing to disk or destroying
    * entries to ensure that the server process never runs out of memory due to
    * cache memory usage
    *
    */
  const size_t heapLRULimit() const { return m_heapLRULimit; }

  /**
    * Returns  the HeapLRUDelta value (a percent value). This specifies the
    * percentage of entries the system will evict each time it detects that
    * it has exceeded the HeapLRULimit. Defaults to 10%
    */
  const int32_t heapLRUDelta() const { return m_heapLRUDelta; }
  /**
   * Returns  the maximum socket buffer size to use
   */
  const int32_t maxSocketBufferSize() const { return m_maxSocketBufferSize; }

  /**
   * Returns  the time between two consecutive ping to servers
   */
  const int32_t pingInterval() const { return m_pingInterval; }
  /**
   * Returns  the time between two consecutive checks for redundancy for HA
   */
  const int32_t redundancyMonitorInterval() const {
    return m_redundancyMonitorInterval;
  }

  /**
   * Returns the periodic notify ack interval
   */
  const int32_t notifyAckInterval() const { return m_notifyAckInterval; }

  /**
  * Returns the expiry time of an idle event id map entry for duplicate
  * notification checking
  */
  const int32_t notifyDupCheckLife() const { return m_notifyDupCheckLife; }

  /**
   * Returns the durable client ID
   */
  const char* durableClientId() const { return m_durableClientId; }

  /**
   * Returns the durable timeout
   */
  const uint32_t durableTimeout() const { return m_durableTimeout; }

  /**
   * Returns the connect timeout used for server and locator handshakes
   */
  const uint32_t connectTimeout() const { return m_connectTimeout; }

  /**
  * Returns the connect wait timeout(in millis) used for to connect to server
  * This is only applicable for linux
  */
  const uint32_t connectWaitTimeout() const { return m_connectWaitTimeout; }

  /**
  * Returns the connect wait timeout(in millis) used for to connect to server
  * This is only applicable for linux
  */
  const uint32_t bucketWaitTimeout() const { return m_bucketWaitTimeout; }

  /**
   * Returns client Queueconflation option
   */
  char* conflateEvents() { return m_conflateEvents; }

  /**
  * Returns  true if the stack trace is enabled ,false otherwise
  */
  const bool debugStackTraceEnabled() const { return m_debugStackTraceEnabled; }

  /**
   * Returns true if crash dump generation for unhandled fatal errors
   * is enabled, false otherwise.
   * By default crash dumps are created in the current working directory.
   * If log-file has been specified then they are created in the same
   * directory as the log file, and having the same prefix as log file.
   * The default prefix is "gemfire_cpp".
   * The actual dump file will have timestamp and process ID in the full name.
   */
  inline const bool crashDumpEnabled() const { return m_crashDumpEnabled; }

  const char* name() const { return m_name; }

  const char* cacheXMLFile() const { return m_cacheXMLFile; }

  /**
  * Returns the log-file-size-limit.
  */
  const uint32_t logFileSizeLimit() const { return m_logFileSizeLimit; }

  /**
  * Returns the log-disk-space-limit.
  */
  const uint32_t logDiskSpaceLimit() const { return m_logDiskSpaceLimit; }

  /**
  * Returns the stat-file-space-limit.
  */
  const uint32_t statsFileSizeLimit() const { return m_statsFileSizeLimit; }

  /**
  * Returns the stat-disk-size-limit.
  */
  const uint32_t statsDiskSpaceLimit() const { return m_statsDiskSpaceLimit; }

  const uint32_t maxQueueSize() { return m_maxQueueSize; }

  const uint32_t javaConnectionPoolSize() const {
    return m_javaConnectionPoolSize;
  }
  void setjavaConnectionPoolSize(uint32_t size) {
    m_javaConnectionPoolSize = size;
  }

  /**
   * This can be call to know whether chunkhandler thread is disable for that
   * opertaion
   */
  bool disableChunkHandlerThread() const { return m_disableChunkHandlerThread; }

  /**
   * This can be call to know whether read timeout unit is in milli second
   */
  bool readTimeoutUnitInMillis() const { return m_readTimeoutUnitInMillis; }

  /**
   * This can be call multiple time to disable chunkhandler thread for those
   * operations
   */
  void setDisableChunkHandlerThread(bool set) {
    m_disableChunkHandlerThread = set;
  }

  /**
  * returns true if app want to clear pdx type ids when client disconnect.
  * deafult is false.
  */
  bool onClientDisconnectClearPdxTypeIds() const {
    return m_onClientDisconnectClearPdxTypeIds;
  }

  /**
   * Set to true if app want to clear pdx type ids when client disconnect.
   * deafult is false.
   */
  void setOnClientDisconnectClearPdxTypeIds(bool set) {
    m_onClientDisconnectClearPdxTypeIds = set;
  }

  /** Return the security auth library */
  inline const char* authInitLibrary() const {
    return (m_AuthIniLoaderLibrary == NULLPTR
                ? ""
                : m_AuthIniLoaderLibrary->asChar());
  }

  /** Return the security auth factory */
  inline const char* authInitFactory() const {
    return (m_AuthIniLoaderFactory == NULLPTR
                ? ""
                : m_AuthIniLoaderFactory->asChar());
  }

  /** Return the security diffie hellman secret key algo */
  const char* securityClientDhAlgo() {
    return (m_securityClientDhAlgo == NULLPTR
                ? ""
                : m_securityClientDhAlgo->asChar());
  }

  /** Return the keystore (.pem file ) path */
  const char* securityClientKsPath() {
    return (m_securityClientKsPath == NULLPTR
                ? ""
                : m_securityClientKsPath->asChar());
  }

  /** Returns securityPropertiesPtr.
   * @return  PropertiesPtr value.
  */
  PropertiesPtr getSecurityProperties() const {
    return m_securityPropertiesPtr;
  }

  /** Checks whether Security is on or off.
   * @return  bool value.
  */
  inline bool isSecurityOn() const {
    return (m_AuthIniLoaderFactory != NULLPTR &&
            m_AuthIniLoaderLibrary != NULLPTR);
  }

  /** Checks whether list of endpoint is shuffeled or not.
   * @return  bool value.
  */
  inline bool isEndpointShufflingDisabled() const {
    return m_disableShufflingEndpoint;
  }

  /**
   * Check whether Diffie-Hellman based credentials encryption is on.
   * @return bool flag to indicate whether DH for credentials is on.
   */
  bool isDhOn() {
    return isSecurityOn() && m_securityClientDhAlgo != NULLPTR &&
           m_securityClientDhAlgo->length() > 0;
  }

  /**
   * Checks to see if this native client is being invoked as part of small
   * grid jobs; use this setting to disable some creation of threads and
   * reducing start/stop time. Note that this setting can cause improper
   * behaviour in some cases like:
   *  1) client that is setup in listening mode and a server failure may not
   *     lead to failover by client
   *  2) while shutting down the client will not send a proper CLOSE_CONNECTION
   *     message so server will report EOF exceptions and may detect client
   *     disconnect after quite some time
   * Also note that there may be some performance loss in queries and
   * Region::getAll due to unavailability of parallel processing threads.
   *
   * @return true if the "grid-client" property is set
   */
  inline bool isGridClient() const { return m_gridClient; }

  /**
   * This property checks whether C# client is running in multiple appdoamin or
   * not.
   * Default value is "false".
   */
  inline bool isAppDomainEnabled() const { return m_appDomainEnabled; }

  /**
   * Whether a non durable client starts to receive and process
   * subscription events automatically.
   * If set to false then a non durable client should call the
   * Cache::readyForEvents() method after all regions are created
   * and listeners attached for the client to start receiving events
   * whether the client is initialized programmatically or declaratively.
   * @return the value of the property.
   */
  inline bool autoReadyForEvents() const { return m_autoReadyForEvents; }

  /**
  * Returns the timeout after which suspended transactions are rolled back.
  */
  const uint32_t suspendedTxTimeout() const { return m_suspendedTxTimeout; }

  /**
   * Returns the tombstone timeout .
   */
  const uint32_t tombstoneTimeoutInMSec() const {
    return m_tombstoneTimeoutInMSec;
  }

 private:
  uint32_t m_statisticsSampleInterval;

  bool m_statisticsEnabled;

  bool m_appDomainEnabled;

  char* m_statisticsArchiveFile;

  char* m_logFilename;

  Log::LogLevel m_logLevel;

  int m_sessions;

  char* m_name;

  bool m_debugStackTraceEnabled;

  bool m_crashDumpEnabled;

  bool m_disableShufflingEndpoint;

  char* m_cacheXMLFile;

  uint32_t m_logFileSizeLimit;
  uint32_t m_logDiskSpaceLimit;

  uint32_t m_statsFileSizeLimit;
  uint32_t m_statsDiskSpaceLimit;

  uint32_t m_maxQueueSize;
  uint32_t m_javaConnectionPoolSize;

  int32_t m_heapLRULimit;
  int32_t m_heapLRUDelta;
  int32_t m_maxSocketBufferSize;
  int32_t m_pingInterval;
  int32_t m_redundancyMonitorInterval;

  int32_t m_notifyAckInterval;
  int32_t m_notifyDupCheckLife;

  PropertiesPtr m_securityPropertiesPtr;
  CacheableStringPtr m_AuthIniLoaderLibrary;
  CacheableStringPtr m_AuthIniLoaderFactory;
  CacheableStringPtr m_securityClientDhAlgo;
  CacheableStringPtr m_securityClientKsPath;
  AuthInitializePtr m_authInitializer;

  char* m_durableClientId;
  uint32_t m_durableTimeout;

  uint32_t m_connectTimeout;
  uint32_t m_connectWaitTimeout;
  uint32_t m_bucketWaitTimeout;

  bool m_gridClient;

  bool m_autoReadyForEvents;

  bool m_sslEnabled;
  bool m_timestatisticsEnabled;
  char* m_sslKeyStore;
  char* m_sslTrustStore;

  char* m_sslKeystorePassword;

  char* m_conflateEvents;

  uint32_t m_threadPoolSize;
  uint32_t m_suspendedTxTimeout;
  uint32_t m_tombstoneTimeoutInMSec;
  bool m_disableChunkHandlerThread;
  bool m_readTimeoutUnitInMillis;
  bool m_onClientDisconnectClearPdxTypeIds;

 private:
  /**
   * Processes the given property/value pair, saving
   * the results internally:
   */
  void processProperty(const char* property, const char* value);

  /** Gets the authInitialize loader for the system.
   * @return  a pointer that points to the system's ,
   * <code>AuthLoader</code> , NULLPTR if there is no AuthLoader for this
   * system.
   */
  AuthInitializePtr getAuthLoader();

 private:
  SystemProperties(const SystemProperties& rhs);  // never defined
  void operator=(const SystemProperties& rhs);    // never defined

  void throwError(const char* msg);

 public:
  static LibraryAuthInitializeFn managedAuthInitializeFn;

  friend class DistributedSystemImpl;
};

}  // namespace

#endif
