/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ThinClientPoolDM.hpp"
#include "TcrEndpoint.hpp"
#include "ThinClientRegion.hpp"
#include <gfcpp/ResultCollector.hpp>
#include "ExecutionImpl.hpp"
#include "ExpiryHandler_T.hpp"
#include <ace/INET_Addr.h>
#include "ExpiryTaskManager.hpp"
#include <gfcpp/SystemProperties.hpp>
#include <statistics/PoolStatsSampler.hpp>
#include "DistributedSystemImpl.hpp"
#include "UserAttributes.hpp"
#include <algorithm>
#include "ThinClientStickyManager.hpp"
#include <gfcpp/PoolManager.hpp>

#include "NonCopyable.hpp"

using namespace gemfire;
using namespace gemfire_statistics;

ExpiryTaskManager* getCacheImplExpiryTaskManager();
void removePool(const char*);

/* adongre
 * CID 28730: Other violation (MISSING_COPY)
 * Class "GetAllWork" owns resources that are managed in its constructor and
 * destructor but has no user-written copy constructor.
 * FIX : Make the class NonCopyable
 */
class GetAllWork : public PooledWork<GfErrType>,
                   private NonCopyable,
                   private NonAssignable {
  ThinClientPoolDM* m_poolDM;
  BucketServerLocationPtr m_serverLocation;
  TcrMessage* m_request;
  TcrMessageReply* m_reply;
  MapOfUpdateCounters m_mapOfUpdateCounters;
  bool m_attemptFailover;
  bool m_isBGThread;
  bool m_addToLocalCache;
  UserAttributesPtr m_userAttribute;
  ChunkedGetAllResponse* m_responseHandler;
  std::string m_regionName;
  const VectorOfCacheableKeyPtr m_keys;
  const RegionPtr m_region;
  TcrChunkedResult* m_resultCollector;
  const UserDataPtr& m_aCallbackArgument;

 public:
  GetAllWork(ThinClientPoolDM* poolDM, const RegionPtr& region,
             const BucketServerLocationPtr& serverLocation,
             const VectorOfCacheableKeyPtr& keys, bool attemptFailover,
             bool isBGThread, bool addToLocalCache,
             ChunkedGetAllResponse* responseHandler,
             const UserDataPtr& aCallbackArgument)
      : m_poolDM(poolDM),
        m_serverLocation(serverLocation),
        m_attemptFailover(attemptFailover),
        m_isBGThread(isBGThread),
        m_addToLocalCache(addToLocalCache),
        m_userAttribute(NULLPTR),
        m_responseHandler(responseHandler),
        m_regionName(region->getFullPath()),
        m_keys(keys),
        m_region(region),
        m_aCallbackArgument(aCallbackArgument) {
    m_request = new TcrMessageGetAll(region.ptr(), m_keys.ptr(), m_poolDM,
                                     m_aCallbackArgument);
    m_reply = new TcrMessageReply(true, m_poolDM);
    if (m_poolDM->isMultiUserMode()) {
      m_userAttribute = TSSUserAttributesWrapper::s_gemfireTSSUserAttributes
                            ->getUserAttributes();
    }

    // HashMapOfCacheablePtr values = NULLPTR;
    // if (!m_addToLocalCache) {
    // values = new HashMapOfCacheable();
    //}

    // HashMapOfExceptionPtr exceptions(new HashMapOfException());
    // VectorOfCacheableKeyPtr resultKeys(new VectorOfCacheableKey());

    m_resultCollector = (new ChunkedGetAllResponse(
        *m_reply, dynamic_cast<ThinClientRegion*>(m_region.ptr()), m_keys.ptr(),
        m_responseHandler->getValues(), m_responseHandler->getExceptions(),
        m_responseHandler->getResultKeys(),
        m_responseHandler->getUpdateCounters(), 0, m_addToLocalCache,
        m_responseHandler->getResponseLock()));

    m_reply->setChunkedResultHandler(m_resultCollector);
  }

  ~GetAllWork() {
    delete m_request;
    delete m_reply;
    delete m_resultCollector;
  }

  TcrMessage* getReply() { return m_reply; }

  void init() {}
  GfErrType execute(void) {
    // init();

    GuardUserAttribures gua;

    if (m_userAttribute != NULLPTR) {
      gua.setProxyCache(m_userAttribute->getProxyCache());
    }
    m_request->InitializeGetallMsg(
        m_request->getCallbackArgument());  // now init getall msg
    return m_poolDM->sendSyncRequest(*m_request, *m_reply, m_attemptFailover,
                                     m_isBGThread, m_serverLocation);
  }
};

const char* ThinClientPoolDM::NC_Ping_Thread = "NC Ping Thread";
const char* ThinClientPoolDM::NC_MC_Thread = "NC MC Thread";
#define PRIMARY_QUEUE_NOT_AVAILABLE -2

ThinClientPoolDM::ThinClientPoolDM(const char* name,
                                   PoolAttributesPtr poolAttrs,
                                   TcrConnectionManager& connManager)
    : ThinClientBaseDM(connManager, NULL),
      Pool(poolAttrs),
      m_poolName(name),
      m_stats(NULL),
      m_sticky(false),
      m_updateLocatorListSema(0),
      m_pingSema(0),
      m_cliCallbackSema(0),
      m_isDestroyed(false),
      m_destroyPending(false),
      m_destroyPendingHADM(false),
      m_isMultiUserMode(false),
      m_locHelper(NULL),
      m_poolSize(0),
      m_numRegions(0),
      m_server(0),
      m_connSema(0),
      m_connManageTask(NULL),
      m_pingTask(NULL),
      m_updateLocatorListTask(NULL),
      m_cliCallbackTask(NULL),
      m_pingTaskId(-1),
      m_updateLocatorListTaskId(-1),
      m_connManageTaskId(-1),
      m_PoolStatsSampler(NULL),
      m_clientMetadataService(NULL),
      m_primaryServerQueueSize(PRIMARY_QUEUE_NOT_AVAILABLE) {
  static bool firstGurd = false;
  if (firstGurd) ClientProxyMembershipID::increaseSynchCounter();
  firstGurd = true;

  SystemProperties* sysProp = DistributedSystem::getSystemProperties();
  // to set security flag at pool level
  this->m_isSecurityOn = sysProp->isSecurityOn();

  ACE_TCHAR hostName[256];
  ACE_OS::hostname(hostName, sizeof(hostName) - 1);
  ACE_INET_Addr driver("", hostName, "tcp");
  uint32_t hostAddr = driver.get_ip_address();
  uint16_t hostPort = 0;
  const char* durableId = (sysProp != NULL) ? sysProp->durableClientId() : NULL;

  std::string poolSeparator = "_gem_";
  std::string clientDurableId =
      (durableId == NULL || strlen(durableId) == 0)
          ? durableId
          : durableId + (m_poolName.c_str() != NULL
                             ? (poolSeparator + m_poolName)
                             : "");

  const uint32_t durableTimeOut =
      (sysProp != NULL) ? sysProp->durableTimeout() : 0;
  m_memId = new ClientProxyMembershipID(
      hostName, hostAddr, hostPort, clientDurableId.c_str(), durableTimeOut);

  if (m_attrs->m_initLocList.size() == 0 &&
      m_attrs->m_initServList.size() == 0) {
    std::string msg = "No locators or servers provided for pool named ";
    msg += name;
    throw IllegalStateException(msg.c_str());
  }
  reset();
  m_locHelper = new ThinClientLocatorHelper(m_attrs->m_initLocList, this);

  m_stats = new PoolStats(m_poolName.c_str());

  if (!sysProp->isEndpointShufflingDisabled()) {
    if (m_attrs->m_initServList.size() > 0) {
      RandGen randgen;
      m_server = randgen(static_cast<uint32_t>(m_attrs->m_initServList.size()));
    }
  }
  if (m_attrs->getPRSingleHopEnabled()) {
    m_clientMetadataService = new ClientMetadataService(PoolPtr(this));
  }
  m_manager = new ThinClientStickyManager(this);
}

void ThinClientPoolDM::init() {
  LOGDEBUG("ThinClientPoolDM::init: Starting pool initialization");

  SystemProperties* sysProp = DistributedSystem::getSystemProperties();
  m_isMultiUserMode = this->getMultiuserAuthentication();
  if (m_isMultiUserMode) {
    LOGINFO("Multiuser authentication is enabled for pool %s",
            m_poolName.c_str());
  }
  // to set security flag at pool level
  this->m_isSecurityOn = sysProp->isSecurityOn();

  LOGDEBUG("ThinClientPoolDM::init: security in on/off = %d ",
           this->m_isSecurityOn);

  m_connManager.init(true);
  //  int min = m_attrs->getMinConnections();
  //  int limit = 2*min, count = 0;
  //
  //  //Add Min Connections @ start
  //  GfErrType err = GF_NOERR;
  //  while (limit-- && count < min) {
  //    TcrConnection* conn = NULL;
  //    std::set< ServerLocation > excludeServers;
  //    err = createPoolConnection( conn, excludeServers );
  //    if (isFatalError(err)) {
  //      if ( err == GF_CACHE_LOCATOR_EXCEPTION ) {
  //        LOGWARN( "No locators were available during pool initialization." );
  //        continue;
  //      }
  //      GfErrTypeToException("ThinClientPoolDM::init", err);
  //    }else if(err != GF_NOERR){
  //      continue;
  //    }else {
  //      LOGDEBUG("ThinClientPoolDM::init: Adding a connection to the pool");
  //      ++count;
  //      //Stats
  //      getStats().incMinPoolSizeConnects();
  //      put( conn, false );
  //    }
  //  }

  /*
  if (m_isMultiUserMode == true)
    LOGCONFIG("Multi user security mode is enabled.");
    */

  SystemProperties* props = DistributedSystem::getSystemProperties();

  LOGDEBUG("ThinClientPoolDM::init: is grid client = %d ",
           props->isGridClient());

  if (!props->isGridClient()) {
    ThinClientPoolDM::startBackgroundThreads();
  }

  LOGDEBUG("ThinClientPoolDM::init: Completed initialization");
}

PropertiesPtr ThinClientPoolDM::getCredentials(TcrEndpoint* ep) {
  PropertiesPtr tmpSecurityProperties =
      DistributedSystem::getSystemProperties()->getSecurityProperties();

  AuthInitializePtr authInitialize = DistributedSystem::m_impl->getAuthLoader();

  if (authInitialize != NULLPTR) {
    LOGFINER(
        "ThinClientPoolDM::getCredentials: acquired handle to authLoader, "
        "invoking getCredentials %s",
        ep->name().c_str());
    /* adongre
     * CID 28901: Copy into fixed size buffer (STRING_OVERFLOW)
     * You might overrun the 100 byte fixed-size string "tmpEndpoint" by copying
     * the
     * return value of "stlp_std::basic_string<char,
     * stlp_std::char_traits<char>,
     * stlp_std::allocator<char> >::c_str() const" without checking the length.
     */
    // char tmpEndpoint[100] = { '\0' } ;
    // strcpy(tmpEndpoint, ep->name().c_str());
    PropertiesPtr tmpAuthIniSecurityProperties = authInitialize->getCredentials(
        tmpSecurityProperties, /*tmpEndpoint*/ ep->name().c_str());
    return tmpAuthIniSecurityProperties;
  }
  return NULLPTR;
}

void ThinClientPoolDM::startBackgroundThreads() {
  LOGDEBUG("ThinClientPoolDM::startBackgroundThreads: Starting ping thread");
  m_pingTask = new GF_TASK_T<ThinClientPoolDM>(
      this, &ThinClientPoolDM::pingServer, NC_Ping_Thread);
  m_pingTask->start();

  SystemProperties* props = DistributedSystem::getSystemProperties();

  if (props->onClientDisconnectClearPdxTypeIds() == true) {
    m_cliCallbackTask =
        new GF_TASK_T<ThinClientPoolDM>(this, &ThinClientPoolDM::cliCallback);
    m_cliCallbackTask->start();
  }

  LOGDEBUG("ThinClientPoolDM::startBackgroundThreads: Creating ping task");
  ACE_Event_Handler* pingHandler =
      new ExpiryHandler_T<ThinClientPoolDM>(this, &ThinClientPoolDM::doPing);

  long pingInterval = getPingInterval() / (1000 * 2);
  if (pingInterval > 0) {
    LOGDEBUG(
        "ThinClientPoolDM::startBackgroundThreads: Scheduling ping task at %ld",
        pingInterval);
    m_pingTaskId = getCacheImplExpiryTaskManager()->scheduleExpiryTask(
        pingHandler, 1, pingInterval, false);
  } else {
    LOGDEBUG(
        "ThinClientPoolDM::startBackgroundThreads: Not Scheduling ping task as "
        "ping interval %ld",
        getPingInterval());
  }

  long updateLocatorListInterval = getUpdateLocatorListInterval();

  if (updateLocatorListInterval > 0) {
    m_updateLocatorListTask = new GF_TASK_T<ThinClientPoolDM>(
        this, &ThinClientPoolDM::updateLocatorList);
    m_updateLocatorListTask->start();

    updateLocatorListInterval = updateLocatorListInterval / 1000;  // seconds
    LOGDEBUG(
        "ThinClientPoolDM::startBackgroundThreads: Creating updateLocatorList "
        "task");
    ACE_Event_Handler* updateLocatorListHandler =
        new ExpiryHandler_T<ThinClientPoolDM>(
            this, &ThinClientPoolDM::doUpdateLocatorList);

    LOGDEBUG(
        "ThinClientPoolDM::startBackgroundThreads: Scheduling updater Locator "
        "task at %ld",
        updateLocatorListInterval);
    m_updateLocatorListTaskId =
        getCacheImplExpiryTaskManager()->scheduleExpiryTask(
            updateLocatorListHandler, 1, updateLocatorListInterval, false);
  }

  LOGDEBUG(
      "ThinClientPoolDM::startBackgroundThreads: Starting manageConnections "
      "thread");
  // Manage Connection Thread
  m_connManageTask = new GF_TASK_T<ThinClientPoolDM>(
      this, &ThinClientPoolDM::manageConnections, NC_MC_Thread);
  m_connManageTask->start();

  int idle = getIdleTimeout();
  int load = getLoadConditioningInterval();

  if (load != -1) {
    if (load < idle || idle == -1) {
      idle = load;
    }
  }

  if (idle != -1) {
    LOGDEBUG(
        "ThinClientPoolDM::startBackgroundThreads: Starting manageConnections "
        "task");
    ACE_Event_Handler* connHandler = new ExpiryHandler_T<ThinClientPoolDM>(
        this, &ThinClientPoolDM::doManageConnections);

    LOGDEBUG(
        "ThinClientPoolDM::startBackgroundThreads: Scheduling "
        "manageConnections task");
    m_connManageTaskId = getCacheImplExpiryTaskManager()->scheduleExpiryTask(
        connHandler, 1, idle / 1000 + 1, false);
  }

  LOGDEBUG(
      "ThinClientPoolDM::startBackgroundThreads: Starting remote query "
      "service");
  // Init Query Service
  m_remoteQueryServicePtr =
      new RemoteQueryService(m_connManager.getCacheImpl(), this);
  m_remoteQueryServicePtr->init();

  LOGDEBUG(
      "ThinClientPoolDM::startBackgroundThreads: Starting pool stat sampler");
  if (m_PoolStatsSampler == NULL && getStatisticInterval() > -1 &&
      DistributedSystem::getSystemProperties()->statisticsEnabled()) {
    m_PoolStatsSampler = new PoolStatsSampler(
        getStatisticInterval() / 1000 + 1, m_connManager.getCacheImpl(), this);
    m_PoolStatsSampler->start();
  }

  // starting chunk processing helper thread
  ThinClientBaseDM::init();

  if (m_clientMetadataService != NULL) {
    // m_clientMetadataService->start(PoolManager::find(m_poolName.c_str()));
    m_clientMetadataService->start();
  }
}
int ThinClientPoolDM::manageConnections(volatile bool& isRunning) {
  LOGFINE("ThinClientPoolDM: starting manageConnections thread");

  while (isRunning) {
    m_connSema.acquire();
    if (isRunning) {
      manageConnectionsInternal(isRunning);
      while (m_connSema.tryacquire() != -1) {
        ;
      }
    }
  }
  LOGFINE("ThinClientPoolDM: ending manageConnections thread");
  return 0;
}

void ThinClientPoolDM::cleanStaleConnections(volatile bool& isRunning) {
  if (!isRunning) {
    return;
  }

  LOGDEBUG("Cleaning stale connections");

  int idle = getIdleTimeout();

  ACE_Time_Value _idle(idle / 1000, (idle % 1000) * 1000);
  ACE_Time_Value _nextIdle = _idle;
  {
    // ACE_Guard<ACE_Recursive_Thread_Mutex> poolguard(m_queueLock);

    TcrConnection* conn = NULL;

    std::vector<TcrConnection*> savelist;
    std::vector<TcrConnection*> replacelist;
    std::set<ServerLocation> excludeServers;

    while ((conn = getNoWait()) != NULL && isRunning) {
      if (canItBeDeleted(conn)) {
        replacelist.push_back(conn);
      } else if (conn) {
        ACE_Time_Value nextIdle =
            _idle - (ACE_OS::gettimeofday() - conn->getLastAccessed());
        if ((ACE_Time_Value(0, 0) < nextIdle) && (nextIdle < _nextIdle)) {
          _nextIdle = nextIdle;
        }
        savelist.push_back(conn);
      }
    }

    size_t replaceCount =
        m_attrs->getMinConnections() - static_cast<int>(savelist.size());

    for (std::vector<TcrConnection*>::const_iterator iter = replacelist.begin();
         iter != replacelist.end(); ++iter) {
      TcrConnection* conn = *iter;
      if (replaceCount <= 0) {
        GF_SAFE_DELETE_CON(conn);
        removeEPConnections(1, false);
        getStats().incLoadCondDisconnects();
        LOGDEBUG("Removed a connection");
      } else {
        GfErrType error = GF_NOERR;
        TcrConnection* newConn = NULL;
        bool maxConnLimit = false;
        error = createPoolConnection(newConn, excludeServers, maxConnLimit,
                                     /*hasExpired(conn) ? NULL :*/ conn);
        if (newConn) {
          ACE_Time_Value nextIdle =
              _idle - (ACE_OS::gettimeofday() - newConn->getLastAccessed());
          if ((ACE_Time_Value(0, 0) < nextIdle) && (nextIdle < _nextIdle)) {
            _nextIdle = nextIdle;
          }
          savelist.push_back(newConn);
          if (newConn != conn) {
            GF_SAFE_DELETE_CON(conn);
            removeEPConnections(1, false);
            getStats().incLoadCondDisconnects();
            LOGDEBUG("Removed a connection");
          }
        } else {
          if (hasExpired(conn)) {
            GF_SAFE_DELETE_CON(conn);
            removeEPConnections(1, false);
            getStats().incLoadCondDisconnects();
            LOGDEBUG("Removed a connection");
          } else {
            conn->updateCreationTime();
            ACE_Time_Value nextIdle =
                _idle - (ACE_OS::gettimeofday() - conn->getLastAccessed());
            if ((ACE_Time_Value(0, 0) < nextIdle) && (nextIdle < _nextIdle)) {
              _nextIdle = nextIdle;
            }
            savelist.push_back(conn);
          }
        }
      }
      replaceCount--;
    }

    LOGDEBUG("Preserving %d connections", savelist.size());

    for (std::vector<TcrConnection*>::const_iterator iter = savelist.begin();
         iter != savelist.end(); ++iter) {
      put(*iter, false);
    }
  }
  if (m_connManageTaskId >= 0 && isRunning &&
      getCacheImplExpiryTaskManager()->resetTask(
          m_connManageTaskId, static_cast<uint32_t>(_nextIdle.sec() + 1))) {
    LOGERROR("Failed to reschedule connection manager");
  } else {
    LOGFINEST("Rescheduled next connection manager run after %d seconds",
              _nextIdle.sec() + 1);
  }

  LOGDEBUG("Pool size is %d, pool counter is %d", size(), m_poolSize);
}
void ThinClientPoolDM::cleanStickyConnections(volatile bool& isRunning) {}

void ThinClientPoolDM::restoreMinConnections(volatile bool& isRunning) {
  if (!isRunning) {
    return;
  }

  LOGDEBUG("Restoring minimum connection level");

  int min = m_attrs->getMinConnections();
  int limit = 2 * min;

  std::set<ServerLocation> excludeServers;

  int restored = 0;

  if (m_poolSize < min) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> poolguard(m_queueLock);
    while (m_poolSize < min && limit-- && isRunning) {
      TcrConnection* conn = NULL;
      bool maxConnLimit = false;
      createPoolConnection(conn, excludeServers, maxConnLimit);
      if (conn) {
        put(conn, false);
        restored++;
        getStats().incMinPoolSizeConnects();
      }
    }
  }

  LOGDEBUG("Restored %d connections", restored);
  LOGDEBUG("Pool size is %d, pool counter is %d", size(), m_poolSize);
}

int ThinClientPoolDM::manageConnectionsInternal(volatile bool& isRunning) {
  try {
    LOGFINE(
        "ThinClientPoolDM::manageConnections(): checking connections in pool "
        "queue %d",
        size());

    cleanStaleConnections(isRunning);

    cleanStickyConnections(isRunning);

    restoreMinConnections(isRunning);

    // ((ThinClientLocatorHelper*)m_locHelper)->updateLocators(this->getServerGroup());

    getStats().setCurPoolConnections(m_poolSize);
  } catch (const Exception& e) {
    LOGERROR(e.getMessage());
  } catch (const std::exception& e) {
    LOGERROR(e.what());
  } catch (...) {
    LOGERROR("Unexpected exception during manage connections");
  }
  return 0;
}

std::string ThinClientPoolDM::selectEndpoint(
    std::set<ServerLocation>& excludeServers,
    const TcrConnection* currentServer) {
  if (m_attrs->m_initLocList.size()) {  // query locators
    ServerLocation outEndpoint;
    std::string additionalLoc;
    LOGFINE("Asking locator for server from group [%s]",
            m_attrs->m_serverGrp.c_str());

    // Update Locator Request Stats
    getStats().incLoctorRequests();

    if (GF_NOERR !=
        ((ThinClientLocatorHelper*)m_locHelper)
            ->getEndpointForNewFwdConn(outEndpoint, additionalLoc,
                                       excludeServers, m_attrs->m_serverGrp,
                                       currentServer)) {
      throw IllegalStateException("Locator query failed");
    }
    // Update Locator stats
    getStats().setLocators(
        ((ThinClientLocatorHelper*)m_locHelper)->getCurLocatorsNum());
    getStats().incLoctorResposes();

    char epNameStr[128] = {0};
    ACE_OS::snprintf(epNameStr, 128, "%s:%d",
                     outEndpoint.getServerName().c_str(),
                     outEndpoint.getPort());
    LOGFINE("ThinClientPoolDM: Locator returned endpoint [%s]", epNameStr);
    return epNameStr;
  } else if (m_attrs->m_initServList
                 .size()) {  // use specified server endpoints
    // highly complex round-robin algorithm
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_endpointSelectionLock);
    if (m_server >= m_attrs->m_initServList.size()) {
      m_server = 0;
    }

    unsigned int epCount = 0;
    do {
      if (!excludeServer(m_attrs->m_initServList[m_server], excludeServers)) {
        LOGFINE("ThinClientPoolDM: Selecting endpoint [%s] from position %d",
                m_attrs->m_initServList[m_server].c_str(), m_server);
        return m_attrs->m_initServList[m_server++];
      } else {
        if (++m_server >= m_attrs->m_initServList.size()) {
          m_server = 0;
        }
      }
    } while (++epCount < m_attrs->m_initServList.size());

    throw NotConnectedException("No server endpoints are available.");
  } else {
    LOGERROR("No locators or servers provided");
    throw IllegalStateException("No locators or servers provided");
  }
}

void ThinClientPoolDM::addConnection(TcrConnection* conn) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(getPoolLock());

  put(conn, false);
  ++m_poolSize;
}
GfErrType ThinClientPoolDM::sendRequestToAllServers(
    const char* func, uint8_t getResult, uint32_t timeout, CacheablePtr args,
    ResultCollectorPtr& rs, CacheableStringPtr& exceptionPtr) {
  GfErrType err = GF_NOERR;

  HostAsm::atomicAdd(m_clientOps, 1);
  getStats().setCurClientOps(m_clientOps);
  /*get the ClientOp start time*/
  // int64 sampleStartNanos =Utils::startStatOpTime();

  ACE_Recursive_Thread_Mutex resultCollectorLock;

  CacheableStringArrayPtr csArray = getServers();

  if (csArray != NULLPTR && csArray->length() == 0) {
    LOGWARN("No server found to execute the function");
    return GF_NOSERVER_FOUND;
  }

  int feIndex = 0;
  FunctionExecution* fePtrList = new FunctionExecution[csArray->length()];
  ThreadPool* threadPool = TPSingleton::instance();
  UserAttributesPtr userAttr =
      TSSUserAttributesWrapper::s_gemfireTSSUserAttributes->getUserAttributes();
  for (int i = 0; i < csArray->length(); i++) {
    CacheableStringPtr cs = csArray[i];
    std::string endpointStr(cs->asChar());
    TcrEndpoint* ep = NULL;
    /*
    std::string endpointStr = Utils::convertHostToCanonicalForm(cs->asChar() );
    */
    if (m_endpoints.find(endpointStr, ep)) {
      ep = addEP(cs->asChar());
    } else if (!ep->connected()) {
      LOGFINE(
          "ThinClientPoolDM::sendRequestToAllServers server not connected %s ",
          cs->asChar());
      // continue;
    }
    FunctionExecution* funcExe = &fePtrList[feIndex++];
    funcExe->setParameters(func, getResult, timeout, args, ep, this,
                           &resultCollectorLock, &rs, userAttr);
    threadPool->perform(funcExe);
  }
  GfErrType finalErrorReturn = GF_NOERR;

  for (int i = 0; i < feIndex; i++) {
    FunctionExecution* funcExe = &fePtrList[i];
    err = funcExe->getResult();
    if (err != GF_NOERR) {
      if (funcExe->getException() == NULLPTR) {
        if (err == GF_TIMOUT) {
          getStats().incTimeoutClientOps();
        } else {
          getStats().incFailedClientOps();
        }
        if (err == GF_IOERR) {
          err = GF_NOTCON;
        }
      } else {
        exceptionPtr = funcExe->getException();
      }

      // delete [] fePtrList;
      // return err;
    }

    if (err == GF_AUTHENTICATION_FAILED_EXCEPTION ||
        err == GF_NOT_AUTHORIZED_EXCEPTION ||
        err == GF_AUTHENTICATION_REQUIRED_EXCEPTION) {
      finalErrorReturn = err;
    } else if (!(finalErrorReturn == GF_AUTHENTICATION_FAILED_EXCEPTION ||
                 finalErrorReturn == GF_NOT_AUTHORIZED_EXCEPTION ||
                 finalErrorReturn ==
                     GF_AUTHENTICATION_REQUIRED_EXCEPTION))  // returning auth
                                                             // errors
    // to client..preference
    // over other errors..
    {
      finalErrorReturn = err;
    }
  }

  if (static_cast<uint8_t>(getResult & 2) == static_cast<uint8_t>(2)) {
    rs->endResults();
  }

  HostAsm::atomicAdd(m_clientOps, -1);
  getStats().setCurClientOps(m_clientOps);
  getStats().incSucceedClientOps();
  /*Update the time stat for clientOpsTime */
  // Utils::updateStatOpTime(getStats().getStats(),
  // m_poolStatType->getInstance()->getClientOpsSucceededTimeId(),
  // sampleStartNanos);

  delete[] fePtrList;
  return finalErrorReturn;
}

const CacheableStringArrayPtr ThinClientPoolDM::getLocators() const {
  int32_t size = static_cast<int32_t>(m_attrs->m_initLocList.size());
  CacheableStringPtr* ptrArr = new CacheableStringPtr[size];

  for (int i = 0; i < size; ++i) {
    ptrArr[i] = CacheableString::create(
        m_attrs->m_initLocList[i].c_str(),
        static_cast<int32_t>(m_attrs->m_initLocList[i].length()));
  }
  return CacheableStringArray::createNoCopy(ptrArr, size);
}

const CacheableStringArrayPtr ThinClientPoolDM::getServers() {
  int32_t size = static_cast<int32_t>(m_attrs->m_initServList.size());
  CacheableStringPtr* ptrArr = NULL;

  if (size > 0) {
    ptrArr = new CacheableStringPtr[size];
    for (int32_t i = 0; i < size; ++i) {
      ptrArr[i] = CacheableString::create(
          m_attrs->m_initServList[i].c_str(),
          static_cast<int32_t>(m_attrs->m_initServList[i].length()));
    }
  }

  if (size > 0) return CacheableStringArray::createNoCopy(ptrArr, size);

  size = static_cast<int32_t>(m_attrs->m_initLocList.size());

  // get dynamic added servers using locators
  if (size > 0) {
    std::vector<ServerLocation> vec;
    ((ThinClientLocatorHelper*)m_locHelper)
        ->getAllServers(vec, m_attrs->m_serverGrp);

    ptrArr = new CacheableStringPtr[vec.size()];
    std::vector<ServerLocation>::iterator it;

    size = static_cast<int32_t>(vec.size());

    char buffer[256] = {'\0'};
    int i = 0;
    for (it = vec.begin(); it < vec.end(); it++) {
      ServerLocation serLoc = *it;
      ACE_OS::snprintf(buffer, 256, "%s:%d", serLoc.getServerName().c_str(),
                       serLoc.getPort());
      ptrArr[i++] = CacheableString::create(buffer);
    }
  }

  return CacheableStringArray::createNoCopy(ptrArr, size);
}

void ThinClientPoolDM::stopPingThread() {
  if (m_pingTask) {
    LOGFINE("ThinClientPoolDM::destroy(): Closing ping thread.");
    m_pingTask->stopNoblock();
    m_pingSema.release();
    m_pingTask->wait();
    GF_SAFE_DELETE(m_pingTask);
    if (m_pingTaskId >= 0) {
      getCacheImplExpiryTaskManager()->cancelTask(m_pingTaskId);
    }
  }
}

void ThinClientPoolDM::stopUpdateLocatorListThread() {
  if (m_updateLocatorListTask) {
    LOGFINE("ThinClientPoolDM::destroy(): Closing updateLocatorList thread.");
    m_updateLocatorListTask->stopNoblock();
    m_updateLocatorListSema.release();
    m_updateLocatorListTask->wait();
    GF_SAFE_DELETE(m_updateLocatorListTask);
    if (m_updateLocatorListTaskId >= 0) {
      getCacheImplExpiryTaskManager()->cancelTask(m_updateLocatorListTaskId);
    }
  }
}

void ThinClientPoolDM::stopCliCallbackThread() {
  if (m_cliCallbackTask) {
    LOGFINE("ThinClientPoolDM::destroy(): Closing cliCallback thread.");
    m_cliCallbackTask->stopNoblock();
    m_cliCallbackSema.release();
    m_cliCallbackTask->wait();
    GF_SAFE_DELETE(m_cliCallbackTask);
  }
}

void ThinClientPoolDM::destroy(bool keepAlive) {
  LOGDEBUG("ThinClientPoolDM::destroy...");
  if (!m_isDestroyed && (!m_destroyPending || m_destroyPendingHADM)) {
    checkRegions();
    TcrMessage::setKeepAlive(keepAlive);
    if (m_remoteQueryServicePtr != NULLPTR) {
      m_remoteQueryServicePtr->close();
      m_remoteQueryServicePtr = NULLPTR;
    }

    LOGDEBUG("Closing PoolStatsSampler thread.");
    if (m_PoolStatsSampler != NULL) {
      m_PoolStatsSampler->stop();
      GF_SAFE_DELETE(m_PoolStatsSampler);
    }
    LOGDEBUG("PoolStatsSampler thread closed .");
    stopCliCallbackThread();
    LOGDEBUG("ThinClientPoolDM::destroy( ): Closing connection manager.");
    if (m_connManageTask) {
      m_connManageTask->stopNoblock();
      m_connSema.release();
      m_connManageTask->wait();
      GF_SAFE_DELETE(m_connManageTask);
      if (m_connManageTaskId >= 0) {
        getCacheImplExpiryTaskManager()->cancelTask(m_connManageTaskId);
      }
    }

    LOGDEBUG("Closing PoolStatsSampler thread.");
    stopPingThread();
    stopUpdateLocatorListThread();

    if (m_clientMetadataService != NULL) {
      m_clientMetadataService->stop();
    }
    // closing all the thread local connections ( sticky).
    LOGDEBUG("ThinClientPoolDM::destroy( ): closing FairQueue, pool size = %d",
             m_poolSize);
    close();
    LOGDEBUG("ThinClientPoolDM::destroy( ): after close ");

    /**********************************************************************
      ==31849==    at 0x4007D75: operator new(unsigned int)
     (vg_replace_malloc.c:313)
      ==31849==    by 0x423063E: gemfire::ThinClientPoolHADM::createEP(char
     const*) (ThinClientPoolHADM.hpp:113)
      ==31849==    by 0x4375CB7: gemfire::ThinClientPoolDM::addEP(char const*)
     (ThinClientPoolDM.cpp:1893)
      ==31849==    by 0x437DC29:
     gemfire::ThinClientPoolDM::addEP(gemfire::ServerLocation&)
     (ThinClientPoolDM.cpp:1880)
      ==31849==    by 0x43861D9:
     gemfire::ThinClientRedundancyManager::getAllEndpoints(stlp_std::vector<gemfire::TcrEndpoint*,
     stlp_std::allocator<gemfire::TcrEndpoint*> >&)
     (ThinClientRedundancyManager.cpp:912)
      ==31849==    by 0x4386D0E:
     gemfire::ThinClientRedundancyManager::initialize(int)
     (ThinClientRedundancyManager.cpp:613)
      ==31849==    by 0x4382301:
     gemfire::ThinClientPoolHADM::startBackgroundThreads()
     (ThinClientPoolHADM.cpp:37)
      ==31849==    by 0x422F11F: gemfire::PoolFactory::create(char const*)
     (PoolFactory.cpp:163)
      ==31849==    by 0x8066755: CacheHelper::createPooledRegion(char const*,
     bool, char const*, char const*, char const*, bool, bool, int, int, int,
     int, int, gemfire::SharedPtr<gemfire::CacheListener> const&,
     gemfire::ExpirationAction::Action) (in
     /export/pnq-gst-dev01a/users/adongre/valgrind_702/nc/ThinClient702X_maint/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
      ==31849==    by 0x805AD4D: createPooledRegion(char const*, bool, char
     const*, char const*, char const*, bool, bool) [clone .clone.2] (in
     /export/pnq-gst-dev01a/users/adongre/valgrind_702/nc/ThinClient702X_maint/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
      ==31849==    by 0x8066F6E: Task_StepOne_Pooled_Locator::doTask() (in
     /export/pnq-gst-dev01a/users/adongre/valgrind_702/nc/ThinClient702X_maint/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
      ==31849==    by 0x806DB7A: dunit::TestSlave::begin() (in
     /export/pnq-gst-dev01a/users/adongre/valgrind_702/nc/ThinClient702X_maint/build-artifacts/linux/tests/cppcache/testThinClientPutAll)
      ==31849==
     ******************************************************************************/
    for (ACE_Map_Manager<std::string, TcrEndpoint*,
                         ACE_Recursive_Thread_Mutex>::iterator iter =
             m_endpoints.begin();
         iter != m_endpoints.end(); ++iter) {
      TcrEndpoint* ep = (*iter).int_id_;
      LOGFINE("ThinClientPoolDM: forcing endpoint delete for %d in destructor",
              ep->name().c_str());
      GF_SAFE_DELETE(ep);
    }

    // Close Stats
    getStats().close();

    if (m_clientMetadataService != NULL) {
      GF_SAFE_DELETE(m_clientMetadataService);
    }

    removePool(m_poolName.c_str());

    stopChunkProcessor();
    m_manager->closeAllStickyConnections();
    // delete m_manager; m_manager = NULL;
    m_isDestroyed = true;
    LOGDEBUG("ThinClientPoolDM::destroy( ): after close m_isDestroyed = %d ",
             m_isDestroyed);
  }
  if (m_poolSize != 0) {
    LOGFINE("Pool connection size is not zero %d", m_poolSize);
  }
}

bool ThinClientPoolDM::isDestroyed() const {
  // TODO: dummy implementation
  return m_isDestroyed;
}

QueryServicePtr ThinClientPoolDM::getQueryService() {
  // TODO:
  if (m_isMultiUserMode) {
    LOGERROR(
        "Pool is in multiuser authentication mode. Get query service using "
        "RegionService.getQueryService()");
    throw UnsupportedOperationException(
        "Pool is in multiuser authentication mode. Get QueryService() using "
        "RegionService.getQueryService()");
  }

  return getQueryServiceWithoutCheck();
}

QueryServicePtr ThinClientPoolDM::getQueryServiceWithoutCheck() {
  if (!(m_remoteQueryServicePtr == NULLPTR)) {
    return m_remoteQueryServicePtr;
  }
  SystemProperties* props = DistributedSystem::getSystemProperties();

  if (props->isGridClient()) {
    LOGWARN("Initializing query service while grid-client setting is enabled.");
    // Init Query Service
    m_remoteQueryServicePtr =
        new RemoteQueryService(m_connManager.getCacheImpl(), this);
    m_remoteQueryServicePtr->init();
  } else {
    LOGWARN("Remote query service is not initialized.");
  }

  return m_remoteQueryServicePtr;
}
void ThinClientPoolDM::sendUserCacheCloseMessage(bool keepAlive) {
  LOGDEBUG("ThinClientPoolDM::sendUserCacheCloseMessage");
  UserAttributesPtr userAttribute =
      TSSUserAttributesWrapper::s_gemfireTSSUserAttributes->getUserAttributes();

  std::map<std::string, UserConnectionAttributes*>& uca =
      userAttribute->getUserConnectionServers();

  std::map<std::string, UserConnectionAttributes*>::iterator it;

  for (it = uca.begin(); it != uca.end(); it++) {
    UserConnectionAttributes* uca = (*it).second;
    if (uca->isAuthenticated() && uca->getEndpoint()->connected()) {
      TcrMessageRemoveUserAuth request(keepAlive, this);
      TcrMessageReply reply(true, this);

      sendRequestToEP(request, reply, uca->getEndpoint());

      uca->setUnAuthenticated();
    } else {
      uca->setUnAuthenticated();
    }
  }
}

TcrConnection* ThinClientPoolDM::getConnectionInMultiuserMode(
    UserAttributesPtr userAttribute) {
  LOGDEBUG("ThinClientPoolDM::getConnectionInMultiuserMode:");
  UserConnectionAttributes* uca = userAttribute->getConnectionAttribute();
  if (uca != NULL) {
    TcrEndpoint* ep = uca->getEndpoint();
    LOGDEBUG(
        "ThinClientPoolDM::getConnectionInMultiuserMode endpoint got = %s ",
        ep->name().c_str());
    return getFromEP(ep);
  } else {
    return NULL;
  }
}

int32_t ThinClientPoolDM::GetPDXIdForType(SerializablePtr pdxType) {
  LOGDEBUG("ThinClientPoolDM::GetPDXIdForType:");

  GfErrType err = GF_NOERR;

  TcrMessageGetPdxIdForType request(pdxType, this);

  TcrMessageReply reply(true, this);

  err = sendSyncRequest(request, reply);

  if (err != GF_NOERR) {
    GfErrTypeToException("Operation Failed", err);
  } else if (reply.getMessageType() == TcrMessage::EXCEPTION) {
    LOGDEBUG("ThinClientPoolDM::GetPDXTypeById: Exception = %s ",
             reply.getException());
    throw IllegalStateException("Failed to register PdxSerializable Type");
  }

  int32_t pdxTypeId =
      static_cast<CacheableInt32*>(reply.getValue().ptr())->value();

  // need to broadcast this id to all other pool
  {
    const HashMapOfPools& pools = PoolManager::getAll();

    for (HashMapOfPools::Iterator iter = pools.begin(); iter != pools.end();
         ++iter) {
      ThinClientPoolDM* currPool =
          static_cast<ThinClientPoolDM*>(iter.second().ptr());

      if (currPool != this) {
        currPool->AddPdxType(pdxType, pdxTypeId);
      }
    }
  }

  return pdxTypeId;
}

void ThinClientPoolDM::AddPdxType(SerializablePtr pdxType, int32_t pdxTypeId) {
  LOGDEBUG("ThinClientPoolDM::GetPDXIdForType:");

  GfErrType err = GF_NOERR;

  TcrMessageAddPdxType request(pdxType, this, pdxTypeId);

  TcrMessageReply reply(true, this);

  err = sendSyncRequest(request, reply);

  if (err != GF_NOERR) {
    GfErrTypeToException("Operation Failed", err);
  } else if (reply.getMessageType() == TcrMessage::EXCEPTION) {
    LOGDEBUG("ThinClientPoolDM::GetPDXTypeById: Exception = %s ",
             reply.getException());
    throw IllegalStateException("Failed to register PdxSerializable Type");
  }
}

SerializablePtr ThinClientPoolDM::GetPDXTypeById(int32_t typeId) {
  LOGDEBUG("ThinClientPoolDM::GetPDXTypeById:");

  GfErrType err = GF_NOERR;

  TcrMessageGetPdxTypeById request(typeId, this);

  TcrMessageReply reply(true, this);

  err = sendSyncRequest(request, reply);

  if (err != GF_NOERR) {
    GfErrTypeToException("Operation Failed", err);
  } else if (reply.getMessageType() == TcrMessage::EXCEPTION) {
    LOGDEBUG("ThinClientPoolDM::GetPDXTypeById: Exception = %s ",
             reply.getException());
    throw IllegalStateException("Failed to understand PdxSerializable Type");
  }

  return reply.getValue();
}

int32_t ThinClientPoolDM::GetEnumValue(SerializablePtr enumInfo) {
  LOGDEBUG("ThinClientPoolDM::GetEnumValue:");

  GfErrType err = GF_NOERR;

  TcrMessageGetPdxIdForEnum request(enumInfo, this);

  TcrMessageReply reply(true, this);

  err = sendSyncRequest(request, reply);

  if (err != GF_NOERR) {
    GfErrTypeToException("Operation Failed", err);
  } else if (reply.getMessageType() == TcrMessage::EXCEPTION) {
    LOGDEBUG("ThinClientPoolDM::GetEnumValue: Exception = %s ",
             reply.getException());
    throw IllegalStateException("Failed to register Pdx enum Type");
  }

  int32_t enumVal =
      static_cast<CacheableInt32*>(reply.getValue().ptr())->value();

  // need to broadcast this id to all other pool
  {
    const HashMapOfPools& pools = PoolManager::getAll();

    for (HashMapOfPools::Iterator iter = pools.begin(); iter != pools.end();
         ++iter) {
      ThinClientPoolDM* currPool =
          static_cast<ThinClientPoolDM*>(iter.second().ptr());

      if (currPool != this) {
        currPool->AddEnum(enumInfo, enumVal);
      }
    }
  }

  return enumVal;
}

SerializablePtr ThinClientPoolDM::GetEnum(int32_t val) {
  LOGDEBUG("ThinClientPoolDM::GetEnum:");

  GfErrType err = GF_NOERR;

  TcrMessageGetPdxEnumById request(val, this);

  TcrMessageReply reply(true, this);

  err = sendSyncRequest(request, reply);

  if (err != GF_NOERR) {
    GfErrTypeToException("Operation Failed", err);
  } else if (reply.getMessageType() == TcrMessage::EXCEPTION) {
    LOGDEBUG("ThinClientPoolDM::GetEnum: Exception = %s ",
             reply.getException());
    throw IllegalStateException("Failed to understand enum Type");
  }

  return reply.getValue();
}

void ThinClientPoolDM::AddEnum(SerializablePtr enumInfo, int enumVal) {
  LOGDEBUG("ThinClientPoolDM::AddEnum:");

  GfErrType err = GF_NOERR;

  TcrMessageAddPdxEnum request(enumInfo, this, enumVal);

  TcrMessageReply reply(true, this);

  err = sendSyncRequest(request, reply);

  if (err != GF_NOERR) {
    GfErrTypeToException("Operation Failed", err);
  } else if (reply.getMessageType() == TcrMessage::EXCEPTION) {
    LOGDEBUG("ThinClientPoolDM::AddEnum: Exception = %s ",
             reply.getException());
    throw IllegalStateException("Failed to register enum Type");
  }
}

GfErrType ThinClientPoolDM::sendUserCredentials(PropertiesPtr credentials,
                                                TcrConnection*& conn,
                                                bool isBGThread,
                                                bool& isServerException) {
  LOGDEBUG("ThinClientPoolDM::sendUserCredentials:");

  GfErrType err = GF_NOERR;

  TcrMessageUserCredential request(credentials, this);

  TcrMessageReply reply(true, this);

  err =
      conn->getEndpointObject()->sendRequestConnWithRetry(request, reply, conn);

  if (conn) err = handleEPError(conn->getEndpointObject(), reply, err);

  LOGDEBUG(
      "ThinClientPoolDM::sendUserCredentials: Error after sending cred request "
      "= %d ",
      err);

  if (err == GF_NOERR) {
    switch (reply.getMessageType()) {
      case TcrMessage::RESPONSE: {
        // nothing to be done;
        break;
      }
      case TcrMessage::EXCEPTION: {
        if (err == GF_NOERR) {
          putInQueue(
              conn, isBGThread);  // connFound is only relevant for Sticky conn.
        }
        // this will set error type if there is some server exception
        err = ThinClientRegion::handleServerException(
            "ThinClientPoolDM::sendUserCredentials AuthException",
            reply.getException());
        isServerException = true;
        break;
      }
      default: {
        if (err == GF_NOERR) {
          putInQueue(
              conn, isBGThread);  // connFound is only relevant for Sticky conn.
        }
        LOGERROR(
            "Unknown message type %d during secure response, possible "
            "serialization mismatch",
            reply.getMessageType());
        err = GF_MSG;

        break;
      }
    }
  }
  return err;
}

TcrEndpoint* ThinClientPoolDM::getSingleHopServer(
    TcrMessage& request, int8_t& version,
    BucketServerLocationPtr& serverlocation,
    std::set<ServerLocation>& excludeServers) {
  const CacheableKeyPtr& key = request.getKeyRef();
  if (m_clientMetadataService == NULL || key == NULLPTR) return NULL;
  RegionPtr region(request.getRegion());
  TcrEndpoint* ep = NULL;
  if (region == NULLPTR) {
    m_connManager.getCacheImpl()->getRegion(request.getRegionName().c_str(),
                                            region);
  }
  if (region != NULLPTR) {
    m_clientMetadataService->getBucketServerLocation(
        region, key, request.getValueRef(), request.getCallbackArgumentRef(),
        request.forPrimary(), serverlocation, version);

    if (serverlocation != NULLPTR && serverlocation->isValid()) {
      LOGFINE("Server host and port are %s:%d",
              serverlocation->getServerName().c_str(),
              serverlocation->getPort());
      ep = getEndPoint(serverlocation, version, excludeServers);
    }
  }
  return ep;
}

TcrEndpoint* ThinClientPoolDM::getEndPoint(
    const BucketServerLocationPtr& serverLocation, int8_t& version,
    std::set<ServerLocation>& excludeServers) {
  TcrEndpoint* ep = NULL;
  if (serverLocation->isValid()) {
    /*if (serverLocation->isPrimary()) {
      version = serverLocation->getVersion();
    } else {
      version = 0;
    }*/

    if (excludeServer(serverLocation->getEpString(), excludeServers)) {
      LOGFINE("ThinClientPoolDM::getEndPoint Exclude Server true for %s ",
              serverLocation->getEpString().c_str());
      return ep;
    }

    // ACE_TCHAR serverLocn[256];
    // ACE_OS::snprintf(serverLocn, 256, "%s:%d",
    // serverLocation->getServerName().c_str(),
    //  serverLocation->getPort());
    if (m_endpoints.find(serverLocation->getEpString(), ep) != -1) {
      LOGDEBUG("Endpoint for single hop is %p", ep);
      return ep;
    }

    // do for pool with endpoints. Add endpoint into m_endpoints only when we
    // did not find it above and it is in the pool's m_initServList.
    for (std::vector<std::string>::iterator itr =
             m_attrs->m_initServList.begin();
         itr != m_attrs->m_initServList.end(); ++itr) {
      if ((ACE_OS::strcmp(serverLocation->getEpString().c_str(),
                          (*itr).c_str()) == 0)) {
        ep = addEP(*(serverLocation.ptr()));  // see if this is new endpoint
        break;
      }
    }

    // do only for locator
    // if servergroup is there, then verify otherwise you may reach to another
    // group
    if (m_attrs->m_initLocList.size()) {
      std::string servGrp = this->getServerGroup();
      if (servGrp.length() > 0) {
        CacheableStringArrayPtr groups = serverLocation->getServerGroups();
        if ((groups != NULLPTR) && (groups->length() > 0)) {
          for (int i = 0; i < groups->length(); i++) {
            CacheableStringPtr cs = groups[i];
            if (cs->length() > 0) {
              std::string str = cs->toString();
              if ((ACE_OS::strcmp(str.c_str(), servGrp.c_str()) == 0)) {
                ep = addEP(
                    *(serverLocation.ptr()));  // see if this is new endpoint
                break;
              }
            }
          }
        }
      } else  // just add it
      {
        ep = addEP(*(serverLocation.ptr()));  // see if this is new endpoint
      }
    }
  }

  return ep;
}

// gets the endpoint from the list of endpoints using the endpoint Name
TcrEndpoint* ThinClientPoolDM::getEndPoint(std::string epNameStr) {
  TcrEndpoint* ep = NULL;
  if (m_endpoints.find(epNameStr, ep) != -1) {
    LOGDEBUG("Endpoint for single hop is %p", ep);
    return ep;
  }
  return ep;
}

GfErrType ThinClientPoolDM::sendSyncRequest(TcrMessage& request,
                                            TcrMessageReply& reply,
                                            bool attemptFailover,
                                            bool isBGThread) {
  int32_t type = request.getMessageType();

  if (!request.forTransaction() && m_attrs->getPRSingleHopEnabled() &&
      (type == TcrMessage::GET_ALL_70 ||
       type == TcrMessage::GET_ALL_WITH_CALLBACK) &&
      m_clientMetadataService != NULL) {
    GfErrType error = GF_NOERR;
    RegionPtr region;
    m_connManager.getCacheImpl()->getRegion(request.getRegionName().c_str(),
                                            region);
    HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>* locationMap =
        m_clientMetadataService->getServerToFilterMap(request.getKeys(), region,
                                                      request.forPrimary());

    if (locationMap == NULL) {
      request.InitializeGetallMsg(
          request.getCallbackArgument());  // now initialize getall msg
      return sendSyncRequest(request, reply, attemptFailover, isBGThread,
                             NULLPTR);
    }
    std::vector<GetAllWork*> getAllWorkers;
    ThreadPool* threadPool = TPSingleton::instance();
    ChunkedGetAllResponse* responseHandler =
        static_cast<ChunkedGetAllResponse*>(reply.getChunkedResultHandler());

    for (HashMapT<BucketServerLocationPtr, VectorOfCacheableKeyPtr>::Iterator
             locationIter = locationMap->begin();
         locationIter != locationMap->end(); locationIter++) {
      BucketServerLocationPtr serverLocation = locationIter.first();
      if (serverLocation == NULLPTR) {
      }
      VectorOfCacheableKeyPtr keys = locationIter.second();
      GetAllWork* worker =
          new GetAllWork(this, region, serverLocation, keys, attemptFailover,
                         isBGThread, responseHandler->getAddToLocalCache(),
                         responseHandler, request.getCallbackArgument());
      threadPool->perform(worker);
      getAllWorkers.push_back(worker);
    }
    reply.setMessageType(TcrMessage::RESPONSE);

    for (std::vector<GetAllWork*>::iterator iter = getAllWorkers.begin();
         iter != getAllWorkers.end(); iter++) {
      GetAllWork* worker = *iter;
      GfErrType err = worker->getResult();

      if (err != GF_NOERR) {
        error = err;
      }

      TcrMessage* currentReply = worker->getReply();
      if (currentReply->getMessageType() != TcrMessage::RESPONSE) {
        reply.setMessageType(currentReply->getMessageType());
      }
      // ChunkedGetAllResponsePtr
      // currentResponseHandler(currentReply->getChunkedResultHandler());
      // responseHandler->add(currentResponseHandler.ptr());

      delete worker;
    }
    delete locationMap;
    return error;
  } else {
    if (type == TcrMessage::GET_ALL_70 ||
        type == TcrMessage::GET_ALL_WITH_CALLBACK) {
      request.InitializeGetallMsg(
          request.getCallbackArgument());  // now initialize getall msg
    }
    return sendSyncRequest(request, reply, attemptFailover, isBGThread,
                           NULLPTR);
  }
}

GfErrType ThinClientPoolDM::sendSyncRequest(
    TcrMessage& request, TcrMessageReply& reply, bool attemptFailover,
    bool isBGThread, const BucketServerLocationPtr& serverLocation) {
  LOGDEBUG("ThinClientPoolDM::sendSyncRequest: ....%d %s",
           request.getMessageType(), m_poolName.c_str());
  // Increment clientOps
  HostAsm::atomicAdd(m_clientOps, 1);
  getStats().setCurClientOps(m_clientOps);

  /*get the ClientOp start time*/
  // int64 sampleStartNanos =Utils::startStatOpTime();
  GfErrType error = GF_NOTCON;

  UserAttributesPtr userAttr = NULLPTR;
  reply.setDM(this);

  int32_t type = request.getMessageType();

  if (!(type == TcrMessage::QUERY ||
        type == TcrMessage::QUERY_WITH_PARAMETERS ||
        type == TcrMessage::PUTALL ||
        type == TcrMessage::PUT_ALL_WITH_CALLBACK ||
        type == TcrMessage::EXECUTE_FUNCTION ||
        type == TcrMessage::EXECUTE_REGION_FUNCTION ||
        type == TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP ||
        type == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE)) {
    // set only when message is not query, putall and executeCQ
    reply.setTimeout(this->getReadTimeout() / 1000);
    request.setTimeout(this->getReadTimeout() / 1000);
  }

  bool retryAllEPsOnce = false;
  if (m_attrs->getRetryAttempts() == -1) {
    retryAllEPsOnce = true;
  }
  long retry = m_attrs->getRetryAttempts() + 1;
  TcrConnection* conn = NULL;
  std::set<ServerLocation> excludeServers;
  type = request.getMessageType();
  bool isAuthRequireExcep = false;
  int isAuthRequireExcepMaxTry = 2;
  bool firstTry = true;
  LOGFINE("sendSyncRequest:: retry = %d", retry);
  while (retryAllEPsOnce || retry-- ||
         (isAuthRequireExcep && isAuthRequireExcepMaxTry >= 0)) {
    isAuthRequireExcep = false;
    if (!firstTry) request.updateHeaderForRetry();
    // if it's a query or putall and we had a timeout, just return with the
    // newly
    // selected endpoint without failover-retry
    if ((type == TcrMessage::QUERY ||
         type == TcrMessage::QUERY_WITH_PARAMETERS ||
         type == TcrMessage::PUTALL ||
         type == TcrMessage::PUT_ALL_WITH_CALLBACK ||
         type == TcrMessage::EXECUTE_FUNCTION ||
         type == TcrMessage::EXECUTE_REGION_FUNCTION ||
         type == TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP ||
         type == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE) &&
        error == GF_TIMOUT) {
      return error;
    }

    GfErrType queueErr = GF_NOERR;
    uint32_t lastExcludeSize = static_cast<uint32_t>(excludeServers.size());
    int8_t version = 0;

    bool isUserNeedToReAuthenticate = false;
    bool singleHopConnFound = false;
    bool connFound = false;
    if (!this->m_isMultiUserMode ||
        (!TcrMessage::isUserInitiativeOps(request))) {
      conn = getConnectionFromQueueW(&queueErr, excludeServers, isBGThread,
                                     request, version, singleHopConnFound,
                                     connFound, serverLocation);
    } else {
      userAttr = TSSUserAttributesWrapper::s_gemfireTSSUserAttributes
                     ->getUserAttributes();
      if (userAttr == NULLPTR) {
        LOGWARN("Attempted operation type %d without credentials",
                request.getMessageType());
        return GF_NOT_AUTHORIZED_EXCEPTION;
      }
      // Can i assume here that we will always get connection here
      conn = getConnectionFromQueueW(&queueErr, excludeServers, isBGThread,
                                     request, version, singleHopConnFound,
                                     connFound, serverLocation);

      LOGDEBUG(
          "ThinClientPoolDM::sendSyncRequest: after "
          "getConnectionInMultiuserMode %d",
          isUserNeedToReAuthenticate);
      if (conn != NULL) {  // need to chk whether user is already authenticated
                           // to this endpoint or not.
        isUserNeedToReAuthenticate =
            !(userAttr->isEndpointAuthenticated(conn->getEndpointObject()));
      }
    }

    if (queueErr == GF_CLIENT_WAIT_TIMEOUT) {
      LOGFINE("Request timeout at client only");
      return GF_CLIENT_WAIT_TIMEOUT;
    } else if (queueErr == GF_CLIENT_WAIT_TIMEOUT_REFRESH_PRMETADATA) {
      // need to refresh meta data
      RegionPtr region;
      m_connManager.getCacheImpl()->getRegion(request.getRegionName().c_str(),
                                              region);
      if (region != NULLPTR) {
        LOGFINE(
            "Need to refresh pr-meta-data timeout in client only  with refresh "
            "metadata");
        ThinClientRegion* tcrRegion =
            dynamic_cast<ThinClientRegion*>(region.ptr());
        tcrRegion->setMetaDataRefreshed(false);
        m_clientMetadataService->enqueueForMetadataRefresh(
            region->getFullPath(), reply.getserverGroupVersion());
      }
      return GF_CLIENT_WAIT_TIMEOUT_REFRESH_PRMETADATA;
    }

    LOGDEBUG(
        "ThinClientPoolDM::sendSyncRequest: isUserNeedToReAuthenticate = %d ",
        isUserNeedToReAuthenticate);
    LOGDEBUG(
        "ThinClientPoolDM::sendSyncRequest: m_isMultiUserMode = %d  conn = %d  "
        "type = %d",
        m_isMultiUserMode, conn, type);

    if (!conn) {
      // lets assume all connection are in use will happen
      if (queueErr == GF_NOERR) {
        queueErr = GF_ALL_CONNECTIONS_IN_USE_EXCEPTION;
        HostAsm::atomicAdd(m_clientOps, -1);
        getStats().setCurClientOps(m_clientOps);
        getStats().incFailedClientOps();
        return queueErr;
      } else if (queueErr == GF_IOERR) {
        error = GF_NOTCON;
      } else {
        error = queueErr;
      }
    }
    if (conn) {
      TcrEndpoint* ep = conn->getEndpointObject();
      LOGDEBUG(
          "ThinClientPoolDM::sendSyncRequest: sendSyncReq "
          "ep->isAuthenticated() = %d ",
          ep->isAuthenticated());
      GfErrType userCredMsgErr = GF_NOERR;
      bool isServerException = false;
      if (TcrMessage::isUserInitiativeOps(request) &&
          (this->m_isSecurityOn || this->m_isMultiUserMode)) {
        if (!this->m_isMultiUserMode && !ep->isAuthenticated()) {
          // first authenticate him on this endpoint
          userCredMsgErr = this->sendUserCredentials(
              this->getCredentials(ep), conn, isBGThread, isServerException);
        } else if (isUserNeedToReAuthenticate) {
          userCredMsgErr = this->sendUserCredentials(
              userAttr->getCredentials(), conn, isBGThread, isServerException);
        }
      }

      if (userCredMsgErr == GF_NOERR) {
        error = ep->sendRequestConnWithRetry(request, reply, conn);
        error = handleEPError(ep, reply, error);
      } else {
        error = userCredMsgErr;
      }

      if (!isServerException) {
        if (error == GF_NOERR) {
          //  afterSendingRequest(request.getMessageType( ),reply, conn);
          LOGDEBUG("putting connection back in queue");
          putInQueue(conn,
                     isBGThread ||
                         request.getMessageType() == TcrMessage::GET_ALL_70 ||
                         request.getMessageType() ==
                             TcrMessage::GET_ALL_WITH_CALLBACK ||
                         request.getMessageType() ==
                             TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP,
                     request.forTransaction());  // connFound is only relevant
                                                 // for Sticky conn.
          LOGDEBUG("putting connection back in queue DONE");
          // LOGFINE("putting connection back in queue");
        } else {
          if (error != GF_TIMOUT) removeEPConnections(ep);
          // Update stats for the connection that failed.
          removeEPConnections(1, false);
          setStickyNull(isBGThread ||
                        request.getMessageType() == TcrMessage::GET_ALL_70 ||
                        request.getMessageType() ==
                            TcrMessage::GET_ALL_WITH_CALLBACK ||
                        request.getMessageType() ==
                            TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP);
          if (conn) {
            GF_SAFE_DELETE_CON(conn)
          }
          excludeServers.insert(ServerLocation(ep->name()));
        }
      } else {
        return error;  // server exception while sending credentail message to
      }
      // server...
    }

    if (error == GF_NOERR) {
      if ((this->m_isSecurityOn || this->m_isMultiUserMode)) {
        if (reply.getMessageType() == TcrMessage::EXCEPTION) {
          if (isAuthRequireException(reply.getException())) {
            TcrEndpoint* ep = conn->getEndpointObject();
            if (!this->m_isMultiUserMode) {
              ep->setAuthenticated(false);
            } else if (userAttr != NULLPTR) {
              userAttr->unAuthenticateEP(ep);
            }
            LOGFINEST(
                "After getting AuthenticationRequiredException trying again.");
            isAuthRequireExcepMaxTry--;
            isAuthRequireExcep = true;
            continue;
          } else if (isNotAuthorizedException(reply.getException())) {
            LOGDEBUG("received NotAuthorizedException");
            // TODO should we try again?
          }
        }
      }
      LOGFINER(
          "reply Metadata version is %d & bsl version is %d "
          "reply.isFEAnotherHop()=%d",
          reply.getMetaDataVersion(), version, reply.isFEAnotherHop());
      // LOGINFO("reply Metadata version is %d & bsl version is %d
      // reply.isFEAnotherHop()=%d connFound= %d", reply.getMetaDataVersion(),
      // version, reply.isFEAnotherHop(), connFound);
      /*if(reply.getMetaDataVersion() > 0 ) {
        LOGCONFIG("reply Metadata version is %d & bsl version is %d",
      reply.getMetaDataVersion(), version);
      }*/
      if (m_clientMetadataService != NULL && request.forSingleHop() &&
          (reply.getMetaDataVersion() != 0 ||
           (request.getMessageType() == TcrMessage::EXECUTE_REGION_FUNCTION &&
            request.getKeyRef() != NULLPTR && reply.isFEAnotherHop()))) {
        // Need to get direct access to Region's name to avoid referencing
        // temp data and causing crashes
        RegionPtr region;
        m_connManager.getCacheImpl()->getRegion(request.getRegionName().c_str(),
                                                region);
        if (region != NULLPTR) {
          if (!connFound)  // max limit case then don't refresh otherwise always
                           // refresh
          {
            LOGFINE("Need to refresh pr-meta-data");
            ThinClientRegion* tcrRegion =
                dynamic_cast<ThinClientRegion*>(region.ptr());
            tcrRegion->setMetaDataRefreshed(false);
          }
          m_clientMetadataService->enqueueForMetadataRefresh(
              region->getFullPath(), reply.getserverGroupVersion());
        }
      }
    }

    if (excludeServers.size() == lastExcludeSize) {
      excludeServers.clear();
      if (retryAllEPsOnce) {
        break;
      }
    }

    if (!attemptFailover || error == GF_NOERR) {
      HostAsm::atomicAdd(m_clientOps, -1);
      getStats().setCurClientOps(m_clientOps);
      if (error == GF_NOERR) {
        getStats().incSucceedClientOps(); /*inc Id for clientOs stat*/
      } else if (error == GF_TIMOUT) {
        getStats().incTimeoutClientOps();
      } else {
        getStats().incFailedClientOps();
      }
      // Top-level only sees NotConnectedException
      if (error == GF_IOERR) {
        error = GF_NOTCON;
      }
      /*Update the time stat for clientOpsTime */
      // Utils::updateStatOpTime(m_stats->getStats(),
      // m_poolStatType->getInstance()->getClientOpsSucceededTimeId(),
      // sampleStartNanos);
      return error;
    }

    conn = NULL;
    firstTry = false;
  }  // While

  HostAsm::atomicAdd(m_clientOps, -1);
  getStats().setCurClientOps(m_clientOps);

  /*Update the time stat for clientOpsTime */
  // Utils::updateStatOpTime(m_stats->getStats(),
  // m_poolStatType->getInstance()->getClientOpsSucceededTimeId(),
  // sampleStartNanos);

  if (error == GF_NOERR) {
    getStats().incSucceedClientOps();
  } else if (error == GF_TIMOUT) {
    getStats().incTimeoutClientOps();
  } else {
    getStats().incFailedClientOps();
  }

  // Top-level only sees NotConnectedException
  if (error == GF_IOERR) {
    error = GF_NOTCON;
  }
  return error;
}

// void ThinClientPoolDM::updateQueue(const char* regionPath) {
//  m_clientMetadataService->enqueueForMetadataRefresh(regionPath);
//}

void ThinClientPoolDM::removeEPConnections(int numConn,
                                           bool triggerManageConn) {
  // TODO: Delete EP

  // HostAsm::atomicAdd( m_poolSize, -1 * numConn  );
  reducePoolSize(numConn);
  // Stats
  // getStats().incPoolDisconnects( numConn );

  // getStats().setCurPoolConnections(m_poolSize);

  // Raise Semaphore for manage thread
  if (triggerManageConn) {
    m_connSema.release();
  }
}

// Tries to get connection to a endpoint. If no connection is available, it
// tries
// to create one. If it fails to create one,  it gets connection to any other
// server
// and fails over the transaction to that server.
// This function is used when the transaction is to be resumed to a specified
// server.
GfErrType ThinClientPoolDM::getConnectionToAnEndPoint(std::string epNameStr,
                                                      TcrConnection*& conn) {
  conn = NULL;

  GfErrType error = GF_NOERR;
  TcrEndpoint* theEP = getEndPoint(epNameStr);

  LOGFINE(
      "ThinClientPoolDM::getConnectionToAnEndPoint( ): Getting endpoint object "
      "for %s",
      epNameStr.c_str());
  if (theEP != NULL && theEP->connected()) {
    LOGFINE(
        "ThinClientPoolDM::getConnectionToAnEndPoint( ): Getting connection "
        "for endpoint %s",
        epNameStr.c_str());
    conn = getFromEP(theEP);
    // if connection is null, possibly because there are no idle connections
    // to this endpoint, create a new pool connection to this endpoint.
    bool maxConnLimit = false;
    if (conn == NULL) {
      LOGFINE(
          "ThinClientPoolDM::getConnectionToAnEndPoint( ): Create connection "
          "for endpoint %s",
          epNameStr.c_str());
      error = createPoolConnectionToAEndPoint(conn, theEP, maxConnLimit);
    }
  }

  // if connection is null, it has failed to get a connection to the specified
  // endpoint. Get a connection to any other server and failover the transaction
  // to that server.
  if (conn == NULL) {
    std::set<ServerLocation> excludeServers;
    bool maxConnLimit = false;
    LOGFINE(
        "ThinClientPoolDM::getConnectionToAnEndPoint( ): No connection "
        "available for endpoint %s. Create connection to any endpoint.",
        epNameStr.c_str());
    conn = getConnectionFromQueue(true, &error, excludeServers, maxConnLimit);
    /* adongre
     * CID 28680: Dereference after null check (FORWARD_NULL)Comparing "conn" to
     * null implies that "conn" might be null.
     * Passing null variable "conn" to function
     * "gemfire::TcrConnection::getEndpointObject() const", which dereferences
     * it.
     */
    // if(conn != NULL || error == GF_NOERR)
    if (conn != NULL && error == GF_NOERR) {
      if (conn->getEndpointObject()->name() != epNameStr) {
        LOGFINE(
            "ThinClientPoolDM::getConnectionToAnEndPoint( ): Endpoint %s "
            "different than the endpoint %s. New connection created and "
            "failing over.",
            epNameStr.c_str(), conn->getEndpointObject()->name().c_str());
        GfErrType failoverErr = doFailover(conn);
        if (failoverErr != GF_NOERR) {
          LOGFINE(
              "ThinClientPoolDM::getConnectionToAnEndPoint( ):Failed to "
              "failover transaction to another server. From endpoint %s to %s",
              epNameStr.c_str(), conn->getEndpointObject()->name().c_str());
          putInQueue(conn, false);
          conn = NULL;
        }
      }
    }
  }

  if (conn == NULL || error != GF_NOERR) {
    LOGFINE(
        "ThinClientPoolDM::getConnectionToAEndPoint( ):Failed to connect to %s",
        theEP->name().c_str());
    if (conn != NULL) GF_SAFE_DELETE(conn);
  }
  // else //no need of this, will  do this in StickyMgr while addding
  // conn->setAndGetBeingUsed( true );

  return error;
}

// Create a pool connection to specified endpoint. First checks if the number of
// connections has exceeded the maximum allowed. If not, create a connection to
// the specified endpoint. Else, throws an error.
GfErrType ThinClientPoolDM::createPoolConnectionToAEndPoint(
    TcrConnection*& conn, TcrEndpoint* theEP, bool& maxConnLimit,
    bool appThreadrequest) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_queueLock);
  GfErrType error = GF_NOERR;
  conn = NULL;
  int min = 0;
  {
    // ACE_Guard< ACE_Recursive_Thread_Mutex > _guard( m_queueLock );
    // Check if the pool size has exceeded maximum allowed.

    int max = m_attrs->getMaxConnections();
    if (max == -1) {
      max = 0x7fffffff;
    }
    min = m_attrs->getMinConnections();
    max = max > min ? max : min;

    if (m_poolSize >= max) {
      maxConnLimit = true;
      LOGFINER(
          "ThinClientPoolDM::createPoolConnectionToAEndPoint( ): current pool "
          "size has reached limit %d, %d",
          m_poolSize, max);
      return error;
    }
  }

  LOGFINE(
      "ThinClientPoolDM::createPoolConnectionToAEndPoint( ): creating a new "
      "connection to the endpoint %s",
      theEP->name().c_str());
  // if the pool size is within limits, create a new connection.
  error = theEP->createNewConnection(
      conn, false, false,
      DistributedSystem::getSystemProperties()->connectTimeout(), false, true,
      appThreadrequest);
  if (conn == NULL || error != GF_NOERR) {
    LOGFINE("2Failed to connect to %s", theEP->name().c_str());
    if (conn != NULL) GF_SAFE_DELETE(conn);
  } else {
    theEP->setConnected();
    ++m_poolSize;
    if (m_poolSize > min) {
      getStats().incLoadCondConnects();
    }
    // Update Stats
    getStats().incPoolConnects();
    getStats().setCurPoolConnections(m_poolSize);
  }
  m_connSema.release();

  return error;
}

void ThinClientPoolDM::reducePoolSize(int num) {
  LOGFINE("removing connection %d ,  pool-size =%d", num, m_poolSize);
  HostAsm::atomicAdd(m_poolSize, -1 * num);
  if (m_poolSize == 0) {
    if (m_cliCallbackTask != NULL) m_cliCallbackSema.release();
  }
}
GfErrType ThinClientPoolDM::createPoolConnection(
    TcrConnection*& conn, std::set<ServerLocation>& excludeServers,
    bool& maxConnLimit, const TcrConnection* currentserver)
// GfErrType ThinClientPoolDM::createPoolConnection( TcrConnection*& conn,
// std::set< ServerLocation >& excludeServers, const TcrConnection*
// currentserver)
{
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_queueLock);
  GfErrType error = GF_NOERR;
  int max = m_attrs->getMaxConnections();
  if (max == -1) {
    max = 0x7fffffff;
  }
  int min = m_attrs->getMinConnections();
  max = max > min ? max : min;
  LOGDEBUG(
      "ThinClientPoolDM::createPoolConnection( ): current pool size has "
      "reached limit %d, %d, %d",
      m_poolSize, max, min);
  // LOGINFO("max: %d, min: %d, poolSize: %d", max, min, m_poolSize);

  conn = NULL;
  {
    // ACE_Guard< ACE_Recursive_Thread_Mutex > _guard( m_queueLock );
    if (m_poolSize >= max) {
      LOGDEBUG(
          "ThinClientPoolDM::createPoolConnection( ): current pool size has "
          "reached limit %d, %d",
          m_poolSize, max);
      maxConnLimit = true;
      return error;
    }
    // m_poolSize++;
  }

  bool fatal = false;
  GfErrType fatalError = GF_NOERR;
  // LOGINFO("serverlist: %d, locList: %d", m_attrs->m_initServList.size(),
  // m_attrs->m_initLocList.size());

  while (true) {
    std::string epNameStr;
    try {
      epNameStr = selectEndpoint(excludeServers, currentserver);
    } catch (const NoAvailableLocatorsException&) {
      /*{//increase up so reduce it
       ACE_Guard< ACE_Recursive_Thread_Mutex > _guard( m_queueLock );
       reducePoolSize(1);
     }*/
      LOGFINE("Locator query failed");
      return GF_CACHE_LOCATOR_EXCEPTION;
    } catch (const Exception&) {
      /*{//increase up so reduce it
        ACE_Guard< ACE_Recursive_Thread_Mutex > _guard( m_queueLock );
        reducePoolSize(1);
      }*/
      LOGFINE("Endpoint selection failed");
      return GF_NOTCON;
    }
    LOGFINE("Connecting to %s", epNameStr.c_str());
    TcrEndpoint* ep = NULL;
    ep = addEP(epNameStr.c_str());

    if (currentserver != NULL &&
        epNameStr == currentserver->getEndpointObject()->name()) {
      LOGDEBUG("Updating existing connection: ", epNameStr.c_str());
      conn = const_cast<TcrConnection*>(currentserver);
      conn->updateCreationTime();
      /*{
  ACE_Guard< ACE_Recursive_Thread_Mutex > _guard( m_queueLock );
  reducePoolSize(1);;//we have not created new connection
}*/
      break;
    } else {
      error = ep->createNewConnection(
          conn, false, false,
          DistributedSystem::getSystemProperties()->connectTimeout(), false);
    }

    if (conn == NULL || error != GF_NOERR) {
      LOGFINE("1Failed to connect to %s", epNameStr.c_str());
      /*{
        ACE_Guard< ACE_Recursive_Thread_Mutex > _guard( m_queueLock );
        reducePoolSize(1);
      }*/
      excludeServers.insert(ServerLocation(ep->name()));
      if (conn != NULL) {
        GF_SAFE_DELETE(conn);
      }
      if (ThinClientBaseDM::isFatalError(error)) {
        // save this error for later to override the
        // error code to be returned
        fatalError = error;
        fatal = true;
      }
      if (ThinClientBaseDM::isFatalClientError(error)) {
        //  log the error string instead of error number.
        LOGFINE("Connection failed due to fatal client error %d", error);
        return error;
      }
    } else {
      ep->setConnected();
      ++m_poolSize;  // already increased
      if (m_poolSize > min) {
        getStats().incLoadCondConnects();
      }
      // Update Stats
      getStats().incPoolConnects();
      getStats().setCurPoolConnections(m_poolSize);
      break;
    }
  }
  m_connSema.release();
  // if a fatal error occurred earlier and we don't have
  // a connection then return this saved error
  if (fatal && !conn && error != GF_NOERR) {
    return fatalError;
  }

  return error;
}

TcrConnection* ThinClientPoolDM::getConnectionFromQueue(
    bool timeout, GfErrType* error, std::set<ServerLocation>& excludeServers,
    bool& maxConnLimit) {
  int64_t timeoutTime = m_attrs->getFreeConnectionTimeout() /
                        1000;  // in millisec so divide by 1000

  getStats().setCurWaitingConnections(waiters());
  getStats().incWaitingConnections();

  /*get the start time for connectionWaitTime stat*/
  int64 sampleStartNanos = Utils::startStatOpTime();
  TcrConnection* mp =
      getUntil(timeoutTime, error, excludeServers, maxConnLimit);
  /*Update the time stat for clientOpsTime */
  Utils::updateStatOpTime(
      getStats().getStats(),
      PoolStatType::getInstance()->getTotalWaitingConnTimeId(),
      sampleStartNanos);
  return mp;
}

bool ThinClientPoolDM::isEndpointAttached(TcrEndpoint* ep) { return true; }

GfErrType ThinClientPoolDM::sendRequestToEP(const TcrMessage& request,
                                            TcrMessageReply& reply,
                                            TcrEndpoint* currentEndpoint) {
  LOGDEBUG("ThinClientPoolDM::sendRequestToEP()");
  int isAuthRequireExcepMaxTry = 2;
  bool isAuthRequireExcep = true;
  GfErrType error = GF_NOERR;
  while (isAuthRequireExcep && isAuthRequireExcepMaxTry >= 0) {
    isAuthRequireExcep = false;
    TcrConnection* conn = getFromEP(currentEndpoint);

    bool isTmpConnectedStatus = false;
    bool putConnInPool = true;
    if (conn == NULL) {
      LOGDEBUG(
          "ThinClientPoolDM::sendRequestToEP(): got NULL connection from pool, "
          "creating new connection in the pool.");
      bool maxConnLimit = false;
      error =
          createPoolConnectionToAEndPoint(conn, currentEndpoint, maxConnLimit);
      if (conn == NULL || error != GF_NOERR) {
        LOGDEBUG(
            "ThinClientPoolDM::sendRequestToEP(): couldnt create a pool "
            "connection, creating a temporary connection.");
        error = currentEndpoint->createNewConnection(
            conn, false, false,
            DistributedSystem::getSystemProperties()->connectTimeout(), false);
        putConnInPool = false;
        currentEndpoint->setConnectionStatus(true);
      }
    }

    if (conn == NULL || error != GF_NOERR) {
      LOGFINE("3Failed to connect to %s", currentEndpoint->name().c_str());
      if (conn != NULL) {
        GF_SAFE_DELETE(conn);
      }
      if (putConnInPool) {
        ACE_Guard<ACE_Recursive_Thread_Mutex> guard(getPoolLock());
        reducePoolSize(1);
      }
      currentEndpoint->setConnectionStatus(false);
      return error;
    } else if (!putConnInPool && !currentEndpoint->connected()) {
      isTmpConnectedStatus = true;
      currentEndpoint->setConnectionStatus(true);
    }

    int32_t type = request.getMessageType();

    // reply.setTimeout( this->getReadTimeout()/1000 );
    if (!(type == TcrMessage::QUERY || type == TcrMessage::PUTALL ||
          type == TcrMessage::PUT_ALL_WITH_CALLBACK ||
          type == TcrMessage::EXECUTE_FUNCTION ||
          type == TcrMessage::EXECUTE_REGION_FUNCTION ||
          type == TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP ||
          type == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE)) {
      reply.setTimeout(this->getReadTimeout() / 1000);
    }

    reply.setDM(this);
    UserAttributesPtr ua = NULLPTR;
    // in multi user mode need to chk whether user is authenticated or not
    // and then follow usual process which we did in send syncrequest.
    // need to user initiative ops
    LOGDEBUG("ThinClientPoolDM::sendRequestToEP: this->m_isMultiUserMode = %d",
             this->m_isMultiUserMode);
    bool isServerException = false;
    // if( !(TcrMessage::USER_CREDENTIAL_MESSAGE == request.getMessageType())){
    if (TcrMessage::isUserInitiativeOps((request)) &&
        (this->m_isSecurityOn || this->m_isMultiUserMode)) {
      if (!this->m_isMultiUserMode && !currentEndpoint->isAuthenticated()) {
        // first authenticate him on this endpoint
        error = this->sendUserCredentials(this->getCredentials(currentEndpoint),
                                          conn, false, isServerException);
      } else if (this->m_isMultiUserMode) {
        ua = TSSUserAttributesWrapper::s_gemfireTSSUserAttributes
                 ->getUserAttributes();
        if (ua == NULLPTR) {
          LOGWARN("Attempted operation type %d without credentials",
                  request.getMessageType());
          if (conn != NULL) putInQueue(conn, false, request.forTransaction());
          // GfErrTypeToException("Found operation without credential",
          // GF_NOT_AUTHORIZED_EXCEPTION);
          return GF_NOT_AUTHORIZED_EXCEPTION;
        } else {
          UserConnectionAttributes* uca =
              ua->getConnectionAttribute(currentEndpoint);

          if (uca == NULL) {
            error = this->sendUserCredentials(ua->getCredentials(), conn, false,
                                              isServerException);
          }
        }
      }
    }
    //}

    LOGDEBUG("ThinClientPoolDM::sendRequestToEP after getting creds");
    if (error == GF_NOERR && conn != NULL) {
      error =
          currentEndpoint->sendRequestConnWithRetry(request, reply, conn, true);
    }

    if (isServerException) return error;

    if (error == GF_NOERR) {
      int32_t replyMsgType = reply.getMessageType();
      if (replyMsgType == TcrMessage::EXCEPTION ||
          replyMsgType == TcrMessage::CQ_EXCEPTION_TYPE ||
          replyMsgType == TcrMessage::CQDATAERROR_MSG_TYPE) {
        error = ThinClientRegion::handleServerException(
            "ThinClientPoolDM::sendRequestToEP", reply.getException());
      }

      if (putConnInPool) {
        put(conn, false);
      } else {
        if (isTmpConnectedStatus) currentEndpoint->setConnectionStatus(false);
        conn->close();
        GF_SAFE_DELETE(conn);
      }
    } else if (error != GF_NOERR) {
      currentEndpoint->setConnectionStatus(false);
      if (putConnInPool) {
        ACE_Guard<ACE_Recursive_Thread_Mutex> guard(getPoolLock());
        removeEPConnections(1);
      }
    }

    if (error == GF_NOERR || error == GF_CACHESERVER_EXCEPTION ||
        error == GF_AUTHENTICATION_REQUIRED_EXCEPTION) {
      if ((this->m_isSecurityOn || this->m_isMultiUserMode)) {
        if (reply.getMessageType() == TcrMessage::EXCEPTION) {
          if (isAuthRequireException(reply.getException())) {
            if (!this->m_isMultiUserMode) {
              currentEndpoint->setAuthenticated(false);
            } else if (ua != NULLPTR) {
              ua->unAuthenticateEP(currentEndpoint);
            }
            LOGFINEST(
                "After getting AuthenticationRequiredException trying again.");
            isAuthRequireExcepMaxTry--;
            isAuthRequireExcep = true;
            if (isAuthRequireExcepMaxTry >= 0) error = GF_NOERR;
            continue;
          }
        }
      }
    }
  }
  LOGDEBUG("ThinClientPoolDM::sendRequestToEP Done.");
  return error;
}

TcrEndpoint* ThinClientPoolDM::addEP(ServerLocation& serverLoc) {
  std::string serverName = serverLoc.getServerName();
  int port = serverLoc.getPort();
  char endpointName[100];
  ACE_OS::snprintf(endpointName, 100, "%s:%d", serverName.c_str(), port);

  return addEP(endpointName);
}

TcrEndpoint* ThinClientPoolDM::addEP(const char* endpointName) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpointsLock);
  TcrEndpoint* ep = NULL;

  // std::string fullName = Utils::convertHostToCanonicalForm(endpointName );
  std::string fullName = endpointName;
  if (m_endpoints.find(fullName, ep)) {
    LOGFINE("Created new endpoint %s for pool %s", fullName.c_str(),
            m_poolName.c_str());
    ep = createEP(fullName.c_str());
    if (m_endpoints.bind(fullName, ep)) {
      LOGERROR("Failed to add endpoint %s to pool %s", fullName.c_str(),
               m_poolName.c_str());
      GF_DEV_ASSERT(
          "ThinClientPoolDM::addEP( ): failed to add endpoint" ? false : false);
    }
  }
  // Update Server Stats
  getStats().setServers(static_cast<int32_t>(m_endpoints.current_size()));
  return ep;
}

void ThinClientPoolDM::netDown() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(getPoolLock());
  close();
  reset();
}

void ThinClientPoolDM::pingServerLocal() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(getPoolLock());
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpointsLock);
  for (ACE_Map_Manager<std::string, TcrEndpoint*,
                       ACE_Recursive_Thread_Mutex>::iterator it =
           m_endpoints.begin();
       it != m_endpoints.end(); it++) {
    if ((*it).int_id_->connected()) {
      (*it).int_id_->pingServer(this);
      if (!(*it).int_id_->connected()) {
        removeEPConnections((*it).int_id_);
        removeCallbackConnection((*it).int_id_);
      }
    }
  }
}

int ThinClientPoolDM::updateLocatorList(volatile bool& isRunning) {
  LOGFINE("Starting updateLocatorList thread for pool %s", m_poolName.c_str());
  while (isRunning) {
    m_updateLocatorListSema.acquire();
    if (isRunning && !TcrConnectionManager::isNetDown) {
      ((ThinClientLocatorHelper*)m_locHelper)
          ->updateLocators(this->getServerGroup());
    }
  }
  LOGFINE("Ending updateLocatorList thread for pool %s", m_poolName.c_str());
  return 0;
}

int ThinClientPoolDM::pingServer(volatile bool& isRunning) {
  LOGFINE("Starting ping thread for pool %s", m_poolName.c_str());
  while (isRunning) {
    m_pingSema.acquire();
    if (isRunning && !TcrConnectionManager::isNetDown) {
      pingServerLocal();
      while (m_pingSema.tryacquire() != -1) {
        ;
      }
    }
  }
  LOGFINE("Ending ping thread for pool %s", m_poolName.c_str());
  return 0;
}

int ThinClientPoolDM::cliCallback(volatile bool& isRunning) {
  LOGFINE("Starting cliCallback thread for pool %s", m_poolName.c_str());
  while (isRunning) {
    m_cliCallbackSema.acquire();
    if (isRunning) {
      LOGFINE("Clearing Pdx Type Registry");
      // this call for csharp client
      DistributedSystemImpl::CallCliCallBack();
      // this call for cpp client
      PdxTypeRegistry::clear();
      while (m_cliCallbackSema.tryacquire() != -1) {
        ;
      }
    }
  }
  LOGFINE("Ending cliCallback thread for pool %s", m_poolName.c_str());
  return 0;
}

int ThinClientPoolDM::doPing(const ACE_Time_Value&, const void*) {
  m_pingSema.release();
  return 0;
}

int ThinClientPoolDM::doUpdateLocatorList(const ACE_Time_Value&, const void*) {
  m_updateLocatorListSema.release();
  return 0;
}

int ThinClientPoolDM::doManageConnections(const ACE_Time_Value&, const void*) {
  m_connSema.release();
  return 0;
}

void ThinClientPoolDM::releaseThreadLocalConnection() {
  m_manager->releaseThreadLocalConnection();
}
void ThinClientPoolDM::setThreadLocalConnection(TcrConnection* conn) {
  m_manager->addStickyConnection(conn);
}

bool ThinClientPoolDM::hasExpired(TcrConnection* conn) {
  int load = getLoadConditioningInterval();
  int idle = getIdleTimeout();

  if (load != -1) {
    if (load < idle || idle == -1) {
      idle = load;
    }
  }

  return conn->hasExpired(load);
}

bool ThinClientPoolDM::canItBeDeleted(TcrConnection* conn) {
  int load = getLoadConditioningInterval();
  int idle = getIdleTimeout();
  int min = getMinConnections();

  if (load != -1) {
    if (load < idle || idle == -1) {
      idle = load;
    }
  }

  bool hasExpired = conn->hasExpired(load);
  bool isIdle = conn->isIdle(idle);

  bool candidateForDeletion = hasExpired || (isIdle && m_poolSize > min);
  bool canBeDeleted = false;

  if (conn && candidateForDeletion) {
    TcrEndpoint* endPt = conn->getEndpointObject();
    bool queue = false;
    {
      ACE_Guard<ACE_Recursive_Thread_Mutex> poolguard(m_queueLock);  // PXR
      ACE_Guard<ACE_Recursive_Thread_Mutex> guardQueue(
          endPt->getQueueHostedMutex());
      queue = endPt->isQueueHosted();
      if (queue) {
        TcrConnection* connTemp = getFromEP(endPt);
        if (connTemp) {
          put(connTemp, false);
          canBeDeleted = true;
        }
      } else {
        canBeDeleted = true;
      }
    }
  }

  return canBeDeleted;
}

bool ThinClientPoolDM::excludeServer(std::string endpoint,
                                     std::set<ServerLocation>& excludeServers) {
  if (excludeServers.size() == 0 ||
      excludeServers.find(ServerLocation(endpoint)) == excludeServers.end()) {
    return false;
  } else {
    return true;
  }
}

bool ThinClientPoolDM::excludeConnection(
    TcrConnection* conn, std::set<ServerLocation>& excludeServers) {
  return excludeServer(conn->getEndpointObject()->name(), excludeServers);
}

TcrConnection* ThinClientPoolDM::getFromEP(TcrEndpoint* theEP) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_queueLock);
  for (std::deque<TcrConnection*>::iterator itr = m_queue.begin();
       itr != m_queue.end(); itr++) {
    if ((*itr)->getEndpointObject() == theEP) {
      LOGDEBUG("ThinClientPoolDM::getFromEP got connection");
      TcrConnection* retVal = *itr;
      m_queue.erase(itr);
      return retVal;
    }
  }

  /*TcrConnection* conn = NULL;
  GfErrType error =  createPoolConnectionToAEndPoint(conn, theEP);

  if(error == GF_NOERR)
    return conn;*/

  return NULL;
}

void ThinClientPoolDM::removeEPConnections(TcrEndpoint* theEP) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_queueLock);
  int32_t size = static_cast<int32_t>(m_queue.size());
  int numConn = 0;

  while (size--) {
    TcrConnection* curConn = m_queue.back();
    m_queue.pop_back();
    if (curConn->getEndpointObject() != theEP) {
      m_queue.push_front(curConn);
    } else {
      curConn->close();
      GF_SAFE_DELETE(curConn);
      numConn++;
    }
  }

  removeEPConnections(numConn);
}

TcrConnection* ThinClientPoolDM::getNoGetLock(
    bool& isClosed, GfErrType* error, std::set<ServerLocation>& excludeServers,
    bool& maxConnLimit) {
  TcrConnection* returnT = NULL;
  {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_queueLock);

    do {
      returnT = popFromQueue(isClosed);
      if (returnT) {
        if (excludeConnection(returnT, excludeServers)) {
          returnT->close();
          GF_SAFE_DELETE(returnT);
          removeEPConnections(1, false);
        } else {
          break;
        }
      } else {
        break;
      }
    } while (!returnT);
  }

  if (!returnT) {
    *error = createPoolConnection(returnT, excludeServers, maxConnLimit);
  }

  return returnT;
}

bool ThinClientPoolDM::exclude(TcrConnection* conn,
                               std::set<ServerLocation>& excludeServers) {
  return excludeConnection(conn, excludeServers);
}

void ThinClientPoolDM::incRegionCount() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_queueLock);

  if (!m_isDestroyed && !m_destroyPending) {
    m_numRegions++;
  } else {
    throw IllegalStateException("Pool has been destroyed.");
  }
}

void ThinClientPoolDM::decRegionCount() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_queueLock);

  m_numRegions--;
}

void ThinClientPoolDM::checkRegions() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_queueLock);

  if (m_numRegions > 0) {
    throw IllegalStateException(
        "Failed to destroy pool because regions are connected with it.");
  }

  m_destroyPending = true;
}
void ThinClientPoolDM::updateNotificationStats(bool isDeltaSuccess,
                                               long timeInNanoSecond) {
  if (isDeltaSuccess) {
    getStats().incProcessedDeltaMessages();
    getStats().incProcessedDeltaMessagesTime(timeInNanoSecond);
  } else {
    getStats().incDeltaMessageFailures();
  }
}

GfErrType ThinClientPoolDM::doFailover(TcrConnection* conn) {
  m_manager->setStickyConnection(conn, true);
  TcrMessageTxFailover request;
  TcrMessageReply reply(true, NULL);

  GfErrType err = this->sendSyncRequest(request, reply);

  if (err == GF_NOERR) {
    switch (reply.getMessageType()) {
      case TcrMessage::REPLY: {
        break;
      }
      case TcrMessage::EXCEPTION: {
        const char* exceptionMsg = reply.getException();
        err = ThinClientRegion::handleServerException(
            "CacheTransactionManager::failover", exceptionMsg);
        break;
      }
      case TcrMessage::REQUEST_DATA_ERROR: {
        err = GF_CACHESERVER_EXCEPTION;
        break;
      }
      default: {
        LOGERROR("Unknown message type in failover reply %d",
                 reply.getMessageType());
        err = GF_MSG;
        break;
      }
    }
  }

  return err;
}
