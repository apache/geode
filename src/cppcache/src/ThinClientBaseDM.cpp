/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/gfcpp_globals.hpp>
#include "ThinClientBaseDM.hpp"
#include "ThinClientRegion.hpp"
#include "TcrMessage.hpp"
#include "TcrEndpoint.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include "Utils.hpp"
#include "CacheImpl.hpp"
#include <gfcpp/SystemProperties.hpp>
//#include "UserAttributes.hpp"
#include "ProxyCache.hpp"

using namespace gemfire;

volatile bool ThinClientBaseDM::s_isDeltaEnabledOnServer = true;
const char* ThinClientBaseDM::NC_ProcessChunk = "NC ProcessChunk";

ThinClientBaseDM::ThinClientBaseDM(TcrConnectionManager& connManager,
                                   ThinClientRegion* theRegion)
    : m_region(theRegion),
      m_connManager(connManager),
      m_initDone(false),
      m_clientNotification(false),
      m_chunks(true),
      m_chunkProcessor(NULL) {}

ThinClientBaseDM::~ThinClientBaseDM() {}

void ThinClientBaseDM::init() {
  if (!DistributedSystem::getSystemProperties()->isGridClient()) {
    // start the chunk processing thread
    if (!DistributedSystem::getSystemProperties()
             ->disableChunkHandlerThread()) {
      startChunkProcessor();
    }
  }
  m_initDone = true;
}

bool ThinClientBaseDM::isSecurityOn() {
  SystemProperties* sysProp = DistributedSystem::getSystemProperties();
  return sysProp->isSecurityOn();
}

void ThinClientBaseDM::destroy(bool keepalive) {
  if (!m_initDone) {
    // nothing to be done
    return;
  }
  // stop the chunk processing thread
  stopChunkProcessor();
  m_initDone = false;
}

GfErrType ThinClientBaseDM::sendSyncRequestRegisterInterest(
    TcrMessage& request, TcrMessageReply& reply, bool attemptFailover,
    ThinClientRegion* region, TcrEndpoint* endpoint) {
  GfErrType err = GF_NOERR;

  if (endpoint == NULL) {
    err = sendSyncRequest(request, reply, attemptFailover);
  } else {
    reply.setDM(this);
    if (endpoint->connected()) {
      err = sendRequestToEP(request, reply, endpoint);
    } else {
      err = GF_NOTCON;
    }
  }

  if (err == GF_NOERR) {
    switch (reply.getMessageType()) {
      case TcrMessage::REPLY:
      case TcrMessage::RESPONSE:
      case TcrMessage::RESPONSE_FROM_PRIMARY:
      case TcrMessage::RESPONSE_FROM_SECONDARY:
        break;

      case TcrMessage::EXCEPTION:
        err = ThinClientRegion::handleServerException("registerInterest",
                                                      reply.getException());
        break;

      case TcrMessage::REGISTER_INTEREST_DATA_ERROR:
        // LOGERROR( "An error occurred while registering interest "
        //    "on the endpoint %s", getActiveEndpoint( )->name( ).c_str( ) );
        err = GF_CACHESERVER_EXCEPTION;
        break;

      case TcrMessage::UNREGISTER_INTEREST_DATA_ERROR:
        // LOGERROR( "An error occurred while un-registering interest "
        //    "on the endpoint %s", getActiveEndpoint( )->name( ).c_str( ) );
        err = GF_CACHESERVER_EXCEPTION;
        break;

      default:
        LOGERROR(
            "Unknown message type %d during register subscription interest",
            reply.getMessageType());
        err = GF_MSG;
        break;
    }
  }

  // top level should only see NotConnectedException
  if (err == GF_IOERR) {
    err = GF_NOTCON;
  }
  return err;
}

GfErrType ThinClientBaseDM::handleEPError(TcrEndpoint* ep,
                                          TcrMessageReply& reply,
                                          GfErrType error) {
  if (error == GF_NOERR) {
    if (reply.getMessageType() == TcrMessage::EXCEPTION) {
      const char* exceptStr = reply.getException();
      if (exceptStr != NULL) {
        bool markServerDead = unrecoverableServerError(exceptStr);
        bool doFailover = (markServerDead || nonFatalServerError(exceptStr));
        if (doFailover) {
          LOGFINE(
              "ThinClientDistributionManager::sendRequestToEP: retrying for "
              "server [%s] exception: %s",
              ep->name().c_str(), exceptStr);
          error = GF_NOTCON;
          if (markServerDead) {
            ep->setConnectionStatus(false);
          }
        }
      }
    } /*else if ( !ep->connected( ) ) {
      error = GF_NOTCON;
    }*/
  }
  return error;
}

GfErrType ThinClientBaseDM::sendRequestToEndPoint(const TcrMessage& request,
                                                  TcrMessageReply& reply,
                                                  TcrEndpoint* ep) {
  GfErrType error = GF_NOERR;
  LOGDEBUG("ThinClientBaseDM::sendRequestToEP: invoking endpoint send for: %s",
           ep->name().c_str());
  error = ep->send(request, reply);
  LOGDEBUG(
      "ThinClientBaseDM::sendRequestToEP: completed endpoint send for: %s "
      "[error:%d]",
      ep->name().c_str(), error);
  return handleEPError(ep, reply, error);
}

/**
 * If we receive an exception back from the server, we should retry on
 * other servers for some exceptions. Some exceptions indicate the server
 * is no longer usable while others indicate a temporary condition on
 * the server so that need not be marked as dead.
 * This method is for exceptions when server should be marked as dead.
 */
bool ThinClientBaseDM::unrecoverableServerError(const char* exceptStr) {
  return ((strstr(exceptStr, "org.apache.geode.cache.CacheClosedException") !=
           NULL) ||
          /*(strstr(exceptStr,
              "org.apache.geode.cache.execute.FunctionException") != NULL) ||*/
          (strstr(exceptStr,
                  "org.apache.geode.distributed.ShutdownException") != NULL) ||
          (strstr(exceptStr, "java.lang.OutOfMemoryError") != NULL));
}

/**
 * If we receive an exception back from the server, we should retry on
 * other servers for some exceptions. Some exceptions indicate the server
 * is no longer usable while others indicate a temporary condition on
 * the server so that need not be marked as dead.
 * This method is for exceptions when server should *not* be marked as dead.
 */
bool ThinClientBaseDM::nonFatalServerError(const char* exceptStr) {
  return ((strstr(exceptStr, "org.apache.geode.distributed.TimeoutException") !=
           NULL) ||
          (strstr(exceptStr, "org.apache.geode.ThreadInterruptedException") !=
           NULL) ||
          (strstr(exceptStr, "java.lang.IllegalStateException") != NULL));
}

void ThinClientBaseDM::failover() {
  // Empty Implementation
}

void ThinClientBaseDM::queueChunk(TcrChunkedContext* chunk) {
  LOGDEBUG("ThinClientBaseDM::queueChunk");
  const uint32_t timeout = 1;
  if (m_chunkProcessor == NULL) {
    LOGDEBUG("ThinClientBaseDM::queueChunk2");
    // process in same thread if no chunk processor thread
    chunk->handleChunk(true);
    GF_SAFE_DELETE(chunk);
  } else if (!m_chunks.putUntil(chunk, timeout, 0)) {
    LOGDEBUG("ThinClientBaseDM::queueChunk3");
    // if put in queue fails due to whatever reason then process in same thread
    LOGFINE(
        "addChunkToQueue: timed out while adding to queue of "
        "unbounded size after waiting for %d secs",
        timeout);
    chunk->handleChunk(true);
    GF_SAFE_DELETE(chunk);
  } else {
    LOGDEBUG("Adding message to ThinClientBaseDM::queueChunk");
  }
}

// the chunk processing thread
int ThinClientBaseDM::processChunks(volatile bool& isRunning) {
  TcrChunkedContext* chunk;
  LOGFINE("Starting chunk process thread for region %s",
          (m_region != NULL ? m_region->getFullPath() : "(null)"));
  while (isRunning) {
    chunk = m_chunks.getUntil(0, 100000);
    if (chunk) {
      chunk->handleChunk(false);
      GF_SAFE_DELETE(chunk);
    }
  }
  LOGFINE("Ending chunk process thread for region %s",
          (m_region != NULL ? m_region->getFullPath() : "(null)"));
  GF_DEV_ASSERT(m_chunks.size() == 0);
  return 0;
}

// start the chunk processing thread
void ThinClientBaseDM::startChunkProcessor() {
  if (m_chunkProcessor == NULL) {
    m_chunks.open();
    m_chunkProcessor = new GF_TASK_T<ThinClientBaseDM>(
        this, &ThinClientBaseDM::processChunks, NC_ProcessChunk);
    m_chunkProcessor->start();
  }
}

// stop the chunk processing thread
void ThinClientBaseDM::stopChunkProcessor() {
  if (m_chunkProcessor != NULL) {
    m_chunkProcessor->stop();
    m_chunks.close();
    GF_SAFE_DELETE(m_chunkProcessor);
  }
}

void ThinClientBaseDM::beforeSendingRequest(const TcrMessage& request,
                                            TcrConnection* conn) {
  LOGDEBUG(
      "ThinClientBaseDM::beforeSendingRequest %d  "
      "TcrMessage::isUserInitiativeOps(request) = %d ",
      request.isMetaRegion(), TcrMessage::isUserInitiativeOps(request));
  LOGDEBUG(
      "ThinClientBaseDM::beforeSendingRequest %d this->isMultiUserMode() = %d "
      "messageType = %d ",
      this->isSecurityOn(), this->isMultiUserMode(), request.getMessageType());
  if (!(request.isMetaRegion()) && TcrMessage::isUserInitiativeOps(request) &&
      (this->isSecurityOn() || this->isMultiUserMode())) {
    int64_t connId = 0;
    int64_t uniqueId = 0;

    if (!this->isMultiUserMode()) {
      connId = conn->getConnectionId();
      uniqueId = conn->getEndpointObject()->getUniqueId();
    } else {
      UserAttributesPtr userAttribute =
          TSSUserAttributesWrapper::s_gemfireTSSUserAttributes
              ->getUserAttributes();
      connId = conn->getConnectionId();
      if (!(request.getMessageType() == TcrMessage::USER_CREDENTIAL_MESSAGE)) {
        uniqueId =
            userAttribute->getConnectionAttribute(conn->getEndpointObject())
                ->getUniqueId();
      }
    }

    if (request.getMessageType() == TcrMessage::USER_CREDENTIAL_MESSAGE) {
      TcrMessage* req = const_cast<TcrMessage*>(&request);
      req->createUserCredentialMessage(conn);
      req->addSecurityPart(connId, conn);
    } else if (TcrMessage::isUserInitiativeOps(request)) {
      TcrMessage* req = const_cast<TcrMessage*>(&request);
      req->addSecurityPart(connId, uniqueId, conn);
    }
  }
}
void ThinClientBaseDM::afterSendingRequest(const TcrMessage& request,
                                           TcrMessageReply& reply,
                                           TcrConnection* conn) {
  LOGDEBUG("ThinClientBaseDM::afterSendingRequest reply msgtype = %d ",
           reply.getMessageType());
  if (!reply.isMetaRegion() && TcrMessage::isUserInitiativeOps(request) &&
      (this->isSecurityOn() || this->isMultiUserMode())) {
    // need to handle encryption/decryption
    if (request.getMessageType() == TcrMessage::USER_CREDENTIAL_MESSAGE) {
      if (TcrMessage::RESPONSE == reply.getMessageType()) {
        if (this->isMultiUserMode()) {
          UserAttributesPtr userAttribute =
              TSSUserAttributesWrapper::s_gemfireTSSUserAttributes
                  ->getUserAttributes();
          userAttribute->setConnectionAttributes(conn->getEndpointObject(),
                                                 reply.getUniqueId(conn));
        } else {
          conn->getEndpointObject()->setUniqueId(reply.getUniqueId(conn));
        }
      }
      conn->setConnectionId(reply.getConnectionId(conn));
    } else if (TcrMessage::isUserInitiativeOps(request)) {
      // bugfix: if noack op then reuse previous security token.
      conn->setConnectionId(reply.getMessageType() == TcrMessage::INVALID
                                ? conn->getConnectionId()
                                : reply.getConnectionId(conn));
    }
  }
}
