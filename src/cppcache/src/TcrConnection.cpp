/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "TcrConnection.hpp"
#include <gfcpp/DistributedSystem.hpp>
#include <gfcpp/SystemProperties.hpp>
#include "Connector.hpp"
#include "TcpSslConn.hpp"
#include "ClientProxyMembershipID.hpp"
#include "ThinClientPoolHADM.hpp"
#include <memory.h>
#include <ace/INET_Addr.h>
#include <ace/OS.h>
#include "TcrEndpoint.hpp"

#include "GemfireTypeIdsImpl.hpp"
#include "TcrConnectionManager.hpp"
#include "DistributedSystemImpl.hpp"
#include "Version.hpp"

#include "DiffieHellman.hpp"
#include "Utils.hpp"  // for RandGen for server challenge
#include "ThinClientRegion.hpp"

using namespace gemfire;
const int HEADER_LENGTH = 17;
const int MAXBUFSIZE ATTR_UNUSED = 65536;
const int BODYLENPOS ATTR_UNUSED = 4;
const int64_t INITIAL_CONNECTION_ID = 26739;

#define throwException(ex)                              \
  {                                                     \
    LOGFINEST("%s: %s", ex.getName(), ex.getMessage()); \
    throw ex;                                           \
  }
bool TcrConnection::InitTcrConnection(
    TcrEndpoint* endpointObj, const char* endpoint, Set<uint16_t>& ports,
    bool isClientNotification, bool isSecondary, uint32_t connectTimeout) {
  m_conn = NULL;
  m_endpointObj = endpointObj;
  m_poolDM = dynamic_cast<ThinClientPoolDM*>(m_endpointObj->getPoolHADM());
  // add to the connection reference counter of the endpoint
  m_endpointObj->addConnRefCounter(1);
  // m_connected = isConnected;
  m_hasServerQueue = NON_REDUNDANT_SERVER;
  m_queueSize = 0;
  m_dh = NULL;
  // m_chunksProcessSema = 0;
  m_creationTime = ACE_OS::gettimeofday();
  connectionId = INITIAL_CONNECTION_ID;
  m_lastAccessed = ACE_OS::gettimeofday();

  LOGDEBUG(
      "Tcrconnection const isSecondary = %d and isClientNotification = %d, "
      "this = %p,  conn ref to endopint %d",
      isSecondary, isClientNotification, this,
      m_endpointObj->getConnRefCounter());
  bool isPool = false;
  m_isBeingUsed = false;
  GF_DEV_ASSERT(endpoint != NULL);
  m_endpoint = endpoint;
  // Precondition:
  // 1. isSecondary ==> isClientNotification

  GF_DEV_ASSERT(!isSecondary || isClientNotification);

  DistributedSystemPtr dsys = DistributedSystem::getInstance();

  // Create TcpConn object which manages a socket connection with the endpoint.
  if (endpointObj && endpointObj->getPoolHADM()) {
    m_conn = createConnection(
        m_endpoint, connectTimeout,
        static_cast<int32_t>(
            endpointObj->getPoolHADM()->getSocketBufferSize()));
    isPool = true;
  } else {
    m_conn = createConnection(m_endpoint, connectTimeout, 0);
  }

  GF_DEV_ASSERT(m_conn != NULL);

  DataOutput handShakeMsg;
  bool isNotificationChannel = false;
  // Send byte Acceptor.CLIENT_TO_SERVER = (byte) 100;
  // Send byte Acceptor.SERVER_TO_CLIENT = (byte) 101;
  if (isClientNotification) {
    isNotificationChannel = true;
    if (isSecondary) {
      handShakeMsg.write(static_cast<int8_t>(SECONDARY_SERVER_TO_CLIENT));
    } else {
      handShakeMsg.write(static_cast<int8_t>(PRIMARY_SERVER_TO_CLIENT));
    }
  } else {
    handShakeMsg.write(static_cast<int8_t>(CLIENT_TO_SERVER));
  }

  // added for versioned client
  int8_t versionOrdinal = Version::getOrdinal();
  handShakeMsg.write(versionOrdinal);

  LOGFINE("Client version ordinal is %d", versionOrdinal);

  handShakeMsg.write(static_cast<int8_t>(REPLY_OK));

  // Send byte REPLY_OK = (byte)58;
  if (!isClientNotification) {
    m_port = m_conn->getPort();
    ports.insert(m_port);
  } else {
    // add the local ports to message
    Set<uint16_t>::Iterator iter = ports.iterator();
    handShakeMsg.writeInt(static_cast<int32_t>(ports.size()));
    while (iter.hasNext()) {
      handShakeMsg.writeInt(static_cast<int32_t>(iter.next()));
    }
  }

  //  Writing handshake readtimeout value for CSVER_51+.
  if (!isClientNotification) {
    // SW: The timeout has been artificially raised to the highest
    // permissible value for bug #232 for now.
    //  minus 10 sec because the GFE 5.7 gridDev branch adds a
    // 5 sec buffer which was causing an int overflow.
    handShakeMsg.writeInt((int32_t)0x7fffffff - 10000);
  }

  // Write header for byte FixedID since GFE 5.7
  handShakeMsg.write(static_cast<int8_t>(GemfireTypeIdsImpl::FixedIDByte));
  // Writing byte for ClientProxyMembershipID class id=38 as registered on the
  // java server.
  handShakeMsg.write(
      static_cast<int8_t>(GemfireTypeIdsImpl::ClientProxyMembershipId));
  if (endpointObj->getPoolHADM()) {
    ClientProxyMembershipID* memId =
        endpointObj->getPoolHADM()->getMembershipId();
    uint32_t memIdBufferLength;
    const char* memIdBuffer = memId->getDSMemberId(memIdBufferLength);
    handShakeMsg.writeBytes((int8_t*)memIdBuffer, memIdBufferLength);
  } else {
    ACE_TCHAR hostName[256];
    ACE_OS::hostname(hostName, sizeof(hostName) - 1);

    ACE_INET_Addr driver("", hostName, "tcp");
    uint32_t hostAddr = driver.get_ip_address();
    uint16_t hostPort = 0;

    // Add 3 durable Subcription properties to ClientProxyMembershipID
    SystemProperties* sysProp = DistributedSystem::getSystemProperties();

    const char* durableId =
        (sysProp != NULL) ? sysProp->durableClientId() : NULL;
    const uint32_t durableTimeOut =
        (sysProp != NULL) ? sysProp->durableTimeout() : 0;

    // Write ClientProxyMembershipID serialized object.
    uint32_t memIdBufferLength;
    ClientProxyMembershipID memId(hostName, hostAddr, hostPort, durableId,
                                  durableTimeOut);
    const char* memIdBuffer = memId.getDSMemberId(memIdBufferLength);
    handShakeMsg.writeBytes((int8_t*)memIdBuffer, memIdBufferLength);
  }
  handShakeMsg.writeInt((int32_t)1);

  bool isDhOn = false;
  bool requireServerAuth = false;
  PropertiesPtr credentials;
  CacheableBytesPtr serverChallenge;

  SystemProperties* tmpSystemProperties =
      DistributedSystem::getSystemProperties();

  // Write overrides (just conflation for now)
  handShakeMsg.write(getOverrides(tmpSystemProperties));

  bool tmpIsSecurityOn = tmpSystemProperties->isSecurityOn();
  isDhOn = tmpSystemProperties->isDhOn();

  if (m_endpointObj) {
    tmpIsSecurityOn = tmpSystemProperties->isSecurityOn() ||
                      this->m_endpointObj->isMultiUserMode();
    CacheableStringPtr dhalgo =
        tmpSystemProperties->getSecurityProperties()->find(
            "security-client-dhalgo");

    LOGDEBUG("TcrConnection this->m_endpointObj->isMultiUserMode() = %d ",
             this->m_endpointObj->isMultiUserMode());
    if (this->m_endpointObj->isMultiUserMode()) {
      if (dhalgo != NULLPTR && dhalgo->length() > 0) isDhOn = true;
    }
  }

  LOGDEBUG(
      "TcrConnection algo name %s tmpIsSecurityOn = %d isDhOn = %d "
      "isNotificationChannel = %d ",
      tmpSystemProperties->securityClientDhAlgo(), tmpIsSecurityOn, isDhOn,
      isNotificationChannel);
  bool doIneedToSendCreds = true;
  if (isNotificationChannel && m_endpointObj &&
      this->m_endpointObj->isMultiUserMode()) {
    isDhOn = false;
    tmpIsSecurityOn = false;
    doIneedToSendCreds = false;
  }

  if (isNotificationChannel && !doIneedToSendCreds) {
    handShakeMsg.write(
        static_cast<uint8_t>(SECURITY_MULTIUSER_NOTIFICATIONCHANNEL));
  } else if (isDhOn) {
    m_dh = new DiffieHellman();
    m_dh->initDhKeys(tmpSystemProperties->getSecurityProperties());
    handShakeMsg.write(static_cast<uint8_t>(SECURITY_CREDENTIALS_DHENCRYPT));
  } else if (tmpIsSecurityOn) {
    handShakeMsg.write(static_cast<uint8_t>(SECURITY_CREDENTIALS_NORMAL));
  } else {
    handShakeMsg.write(static_cast<uint8_t>(SECURITY_CREDENTIALS_NONE));
  }

  if (tmpIsSecurityOn) {
    try {
      LOGFINER("TcrConnection: about to invoke authloader");
      PropertiesPtr tmpSecurityProperties =
          tmpSystemProperties->getSecurityProperties();
      if (tmpSecurityProperties == NULLPTR) {
        LOGWARN("TcrConnection: security properties not found.");
      }
      // AuthInitializePtr authInitialize =
      // tmpSystemProperties->getAuthLoader();
      //:only for backward connection
      if (isClientNotification) {
        AuthInitializePtr authInitialize =
            DistributedSystem::m_impl->getAuthLoader();
        if (authInitialize != NULLPTR) {
          LOGFINER(
              "TcrConnection: acquired handle to authLoader, "
              "invoking getCredentials");
          /* adongre
           * CID 28898: Copy into fixed size buffer (STRING_OVERFLOW)
           * You might overrun the 100 byte fixed-size string "tmpEndpoint" by
           * copying "this->m_endpoint" without checking the length.
           * Note: This defect has an elevated risk because the source argument
           * is a parameter of the current function.
           */
          // char tmpEndpoint[100] = { '\0' } ;
          // strcpy(tmpEndpoint, m_endpoint);
          PropertiesPtr tmpAuthIniSecurityProperties =
              authInitialize->getCredentials(tmpSecurityProperties,
                                             /*tmpEndpoint*/ m_endpoint);
          LOGFINER("TcrConnection: after getCredentials ");
          credentials = tmpAuthIniSecurityProperties;
        }
      }

      if (isDhOn) {
        CacheableStringPtr ksPath =
            tmpSecurityProperties->find("security-client-kspath");
        requireServerAuth = (ksPath != NULLPTR && ksPath->length() > 0);
        handShakeMsg.writeBoolean(requireServerAuth);
        LOGFINE(
            "HandShake: Server authentication using RSA signature %s required",
            requireServerAuth ? "is" : "not");

        // Send the symmetric key algorithm name string
        handShakeMsg.write(
            static_cast<int8_t>(GemfireTypeIds::CacheableString));
        handShakeMsg.writeASCII(tmpSystemProperties->securityClientDhAlgo());

        // Send the client's DH public key to the server
        // CacheableBytesPtr dhPubKey = DiffieHellman::getPublicKey();
        CacheableBytesPtr dhPubKey = m_dh->getPublicKey();
        LOGDEBUG("DH pubkey send len is %d", dhPubKey->length());
        dhPubKey->toData(handShakeMsg);

        if (requireServerAuth) {
          char serverChallengeBytes[64] = {0};
          RandGen getrand;
          for (int pos = 0; pos < 64; pos++) {
            serverChallengeBytes[pos] = getrand(255);
          }
          serverChallenge = CacheableBytes::create(
              reinterpret_cast<const uint8_t*>(serverChallengeBytes), 64);
          serverChallenge->toData(handShakeMsg);
        }
      } else {                       // if isDhOn
        if (isClientNotification) {  //:only for backward connection
          credentials->toData(handShakeMsg);
        }
      }  // else isDhOn
    } catch (const AuthenticationRequiredException&) {
      LOGDEBUG("AuthenticationRequiredException got");
      throw;
    } catch (const AuthenticationFailedException&) {
      LOGDEBUG("AuthenticationFailedException got");
      throw;
    } catch (const Exception& ex) {
      LOGWARN("TcrConnection: failed to acquire handle to authLoader: [%s] %s",
              ex.getName(), ex.getMessage());
      throwException(
          AuthenticationFailedException("TcrConnection: failed "
                                        "to load authInit library: ",
                                        ex.getMessage()));
    }
  }

  uint32_t msgLengh;
  char* data = (char*)handShakeMsg.getBuffer(&msgLengh);
  LOGFINE("Attempting handshake with endpoint %s for %s%s connection", endpoint,
          isClientNotification ? (isSecondary ? "secondary " : "primary ") : "",
          isClientNotification ? "subscription" : "client");
  ConnErrType error = sendData(data, msgLengh, connectTimeout, false);

  if (error == CONN_NOERR) {
    CacheableBytesPtr acceptanceCode = readHandshakeData(1, connectTimeout);

    LOGDEBUG(" Handshake: Got Accept Code %d", acceptanceCode[0]);
    /* adongre */
    if (acceptanceCode[0] == REPLY_SSL_ENABLED &&
        !tmpSystemProperties->sslEnabled()) {
      LOGERROR("SSL is enabled on server, enable SSL in client as well");
      AuthenticationRequiredException ex(
          "SSL is enabled on server, enable SSL in client as well");
      GF_SAFE_DELETE_CON(m_conn);
      throwException(ex);
    }

    // if diffie-hellman based credential encryption is enabled
    if (isDhOn && acceptanceCode[0] == REPLY_OK) {
      // read the server's DH public key
      CacheableBytesPtr pubKeyBytes = readHandshakeByteArray(connectTimeout);
      LOGDEBUG(" Handshake: Got pubKeySize %d", pubKeyBytes->length());

      // set the server's public key on client's DH side
      // DiffieHellman::setPublicKeyOther(pubKeyBytes);
      m_dh->setPublicKeyOther(pubKeyBytes);

      // Note: SK Algo is set in DistributedSystem::connect()
      // DiffieHellman::computeSharedSecret();
      m_dh->computeSharedSecret();

      if (requireServerAuth) {
        // Read Subject Name
        CacheableStringPtr subjectName = readHandshakeString(connectTimeout);
        LOGDEBUG("Got subject %s", subjectName->asChar());
        // read the server's signature bytes
        CacheableBytesPtr responseBytes =
            readHandshakeByteArray(connectTimeout);
        LOGDEBUG("Handshake: Got response size %d", responseBytes->length());
        LOGDEBUG("Handshake: Got serverChallenge size %d",
                 serverChallenge->length());
        if (!m_dh->verify(subjectName, serverChallenge, responseBytes)) {
          throwException(AuthenticationFailedException(
              "Handshake: failed to verify server challenge response"));
        }
        LOGFINE("HandShake: Verified server challenge response");
      }

      // read the challenge bytes from the server
      CacheableBytesPtr challengeBytes = readHandshakeByteArray(connectTimeout);
      LOGDEBUG("Handshake: Got challengeSize %d", challengeBytes->length());

      // encrypt the credentials and challenge bytes
      DataOutput cleartext;
      if (isClientNotification) {  //:only for backward connection
        credentials->toData(cleartext);
      }
      challengeBytes->toData(cleartext);
      CacheableBytesPtr ciphertext =
          m_dh->encrypt(cleartext.getBuffer(), cleartext.getBufferLength());

      DataOutput sendCreds;
      ciphertext->toData(sendCreds);
      uint32_t credLen;
      char* credData = (char*)sendCreds.getBuffer(&credLen);
      // send the encrypted bytes and check the response
      error = sendData(credData, credLen, connectTimeout, false);

      if (error == CONN_NOERR) {
        acceptanceCode = readHandshakeData(1, connectTimeout);
        LOGDEBUG("Handshake: Got acceptanceCode Finally %d", acceptanceCode[0]);
      } else {
        int32_t lastError = ACE_OS::last_error();
        LOGERROR("Handshake failed, errno: %d, server may not be running",
                 lastError);
        GF_SAFE_DELETE_CON(m_conn);
        if (error & CONN_TIMEOUT) {
          throwException(TimeoutException(
              "TcrConnection::TcrConnection: "
              "connection timed out during diffie-hellman handshake"));
        } else {
          throwException(
              GemfireIOException("TcrConnection::TcrConnection: "
                                 "Handshake failure during diffie-hellman"));
        }
      }
    }

    CacheableBytesPtr serverQueueStatus = readHandshakeData(1, connectTimeout);

    //  TESTING: Durable clients - set server queue status.
    // 0 - Non-Redundant , 1- Redundant , 2- Primary
    if (serverQueueStatus[0] == 1) {
      m_hasServerQueue = REDUNDANT_SERVER;
    } else if (serverQueueStatus[0] == 2) {
      m_hasServerQueue = PRIMARY_SERVER;
    } else {
      m_hasServerQueue = NON_REDUNDANT_SERVER;
    }
    CacheableBytesPtr queueSizeMsg = readHandshakeData(4, connectTimeout);
    DataInput dI(queueSizeMsg->value(), queueSizeMsg->length());
    int32_t queueSize = 0;
    dI.readInt(&queueSize);
    m_queueSize = queueSize > 0 ? queueSize : 0;

    m_endpointObj->setServerQueueStatus(m_hasServerQueue, m_queueSize);

    ////////////////////////// Set Pool Specific Q Size only when
    ///////////////////////////////////
    ////////////////////////// 1. SereverQStatus = Primary or
    ///////////////////////////////////
    ////////////////////////// 2. SereverQStatus = Non-Redundant and
    ///////////////////////////////////
    ////////////////////////// 3. Only when handshake is for subscription
    ///////////////////////////////////
    if (m_poolDM != NULL) {
      if ((m_hasServerQueue == PRIMARY_SERVER ||
           m_hasServerQueue == NON_REDUNDANT_SERVER) &&
          isClientNotification) {
        m_poolDM->setPrimaryServerQueueSize(queueSize);
      }
    }

    if (!isClientNotification) {
      // Read and ignore the DistributedMember object
      CacheableBytesPtr arrayLenHeader = readHandshakeData(1, connectTimeout);
      int32_t recvMsgLen = static_cast<int32_t>(arrayLenHeader[0]);
      // now check for array length headers - since GFE 5.7
      if (static_cast<int8_t>(arrayLenHeader[0]) == -2) {
        CacheableBytesPtr recvMsgLenBytes =
            readHandshakeData(2, connectTimeout);
        DataInput dI2(recvMsgLenBytes->value(), recvMsgLenBytes->length());
        int16_t recvMsgLenShort = 0;
        dI2.readInt(&recvMsgLenShort);
        recvMsgLen = recvMsgLenShort;
      } else if (static_cast<int8_t>(arrayLenHeader[0]) == -3) {
        CacheableBytesPtr recvMsgLenBytes =
            readHandshakeData(4, connectTimeout);
        DataInput dI2(recvMsgLenBytes->value(), recvMsgLenBytes->length());
        dI2.readInt(&recvMsgLen);
      }
      CacheableBytesPtr recvMessage =
          readHandshakeData(recvMsgLen, connectTimeout);
      // If the distributed member has not been set yet, set it.
      if (getEndpointObject()->getDistributedMemberID() == 0) {
        LOGDEBUG("Deserializing distributed member Id");
        DataInput diForClient(recvMessage->value(), recvMessage->length());
        ClientProxyMembershipIDPtr member;
        diForClient.readObject(member);
        uint16_t memId = CacheImpl::getMemberListForVersionStamp()->add(
            (DSMemberForVersionStampPtr)member);
        getEndpointObject()->setDistributedMemberID(memId);
        LOGDEBUG("Deserialized distributed member Id %d", memId);
      }
    }

    CacheableBytesPtr recvMsgLenBytes = readHandshakeData(2, connectTimeout);
    DataInput dI3(recvMsgLenBytes->value(), recvMsgLenBytes->length());
    uint16_t recvMsgLen2 = 0;
    dI3.readInt(&recvMsgLen2);
    CacheableBytesPtr recvMessage =
        readHandshakeData(recvMsgLen2, connectTimeout);

    if (!isClientNotification) {
      CacheableBytesPtr deltaEnabledMsg = readHandshakeData(1, connectTimeout);
      DataInput di(deltaEnabledMsg->value(), 1);
      bool isDeltaEnabledOnServer;
      di.readBoolean(&isDeltaEnabledOnServer);
      ThinClientBaseDM::setDeltaEnabledOnServer(isDeltaEnabledOnServer);
    }

    switch (acceptanceCode[0]) {
      case REPLY_OK:
      case SUCCESSFUL_SERVER_TO_CLIENT:
        LOGFINER("Handshake reply: %u,%u,%u", acceptanceCode[0],
                 serverQueueStatus[0], recvMsgLen2);
        if (isClientNotification) readHandshakeInstantiatorMsg(connectTimeout);
        break;
      case REPLY_AUTHENTICATION_FAILED: {
        AuthenticationFailedException ex((char*)recvMessage->value());
        GF_SAFE_DELETE_CON(m_conn);
        throwException(ex);
        // not expected to be reached
        break;
      }
      case REPLY_AUTHENTICATION_REQUIRED: {
        AuthenticationRequiredException ex((char*)recvMessage->value());
        GF_SAFE_DELETE_CON(m_conn);
        throwException(ex);
        // not expected to be reached
        break;
      }
      case REPLY_DUPLICATE_DURABLE_CLIENT: {
        DuplicateDurableClientException ex((char*)recvMessage->value());
        GF_SAFE_DELETE_CON(m_conn);
        throwException(ex);
        // not expected to be reached
        break;
      }
      case REPLY_REFUSED:
      case REPLY_INVALID:
      case UNSUCCESSFUL_SERVER_TO_CLIENT: {
        LOGERROR("Handshake rejected by server[%s]: %s",
                 m_endpointObj->name().c_str(), (char*)recvMessage->value());
        CacheServerException ex(
            "TcrConnection::TcrConnection: "
            "Handshake rejected by server: ",
            (char*)recvMessage->value());
        GF_SAFE_DELETE_CON(m_conn);
        throw ex;
      }
      default: {
        LOGERROR(
            "Unknown error[%d] received from server [%s] in handshake: "
            "%s",
            acceptanceCode[0], m_endpointObj->name().c_str(),
            recvMessage->value());
        MessageException ex(
            "TcrConnection::TcrConnection: Unknown error"
            " received from server in handshake: ",
            (char*)recvMessage->value());
        GF_SAFE_DELETE_CON(m_conn);
        throw ex;
      }
    }

  } else {
    int32_t lastError = ACE_OS::last_error();
    LOGFINE("Handshake failed, errno: %d: %s", lastError,
            ACE_OS::strerror(lastError));
    GF_SAFE_DELETE_CON(m_conn);
    if (error & CONN_TIMEOUT) {
      throw TimeoutException(
          "TcrConnection::TcrConnection: "
          "connection timed out during handshake");
    } else {
      throw GemfireIOException(
          "TcrConnection::TcrConnection: "
          "Handshake failure");
    }
  }

  // TODO: we can authenticate endpoint here if pool is not in multiuser mode.
  // for backward connection we send credentials to server in handshake itself.
  // for forward connection we need to send credentail to server
  //---if pool in not in multiuser node
  //---or old endpoint case.

  if (this->m_endpointObj && !isNotificationChannel && tmpIsSecurityOn &&
      (!isPool || !this->m_endpointObj->isMultiUserMode())) {
    // this->m_endpointObj->authenticateEndpoint(this);
    return true;
  }

  return false;
}

Connector* TcrConnection::createConnection(const char* endpoint,
                                           uint32_t connectTimeout,
                                           int32_t maxBuffSizePool) {
  Connector* socket = NULL;
  if (DistributedSystem::getSystemProperties()->sslEnabled()) {
    socket = new TcpSslConn(endpoint, connectTimeout, maxBuffSizePool);
  } else {
    socket = new TcpConn(endpoint, connectTimeout, maxBuffSizePool);
  }
  // as socket.init() calls throws exception...
  m_conn = socket;
  socket->init();
  return socket;
}

/* The timeout behaviour for different methods is as follows:
* receive():
*   Header: default timeout
*   Body: default timeout
* sendRequest()/sendRequestForChunkedResponse():
*  default timeout during send; for receive:
*   Header: default timeout * default timeout retries to handle large payload
*           if a timeout other than default timeout is specified then
*           that is used instead
*   Body: default timeout
*/
inline ConnErrType TcrConnection::receiveData(char* buffer, int32_t length,
                                              uint32_t receiveTimeoutSec,
                                              bool checkConnected,
                                              bool isNotificationMessage,
                                              int32_t notPublicApiWithTimeout) {
  GF_DEV_ASSERT(buffer != NULL);
  GF_DEV_ASSERT(m_conn != NULL);

  // if gfcpp property unit set then sendTimeoutSec will be in millisecond
  // otherwise it will be in second
  if (DistributedSystem::getSystemProperties()->readTimeoutUnitInMillis()) {
    LOGFINER("recieveData %d %d ", receiveTimeoutSec, notPublicApiWithTimeout);
    if (notPublicApiWithTimeout == TcrMessage::QUERY ||
        notPublicApiWithTimeout == TcrMessage::QUERY_WITH_PARAMETERS ||
        notPublicApiWithTimeout == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE ||
        /*notPublicApiWithTimeout == TcrMessage::GETDURABLECQS_MSG_TYPE || this
           is not public yet*/
        notPublicApiWithTimeout == TcrMessage::EXECUTE_FUNCTION ||
        notPublicApiWithTimeout == TcrMessage::EXECUTE_REGION_FUNCTION ||
        notPublicApiWithTimeout ==
            TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP) {
      // then app has set timeout in millis, change it to microSeconds
      receiveTimeoutSec = receiveTimeoutSec * 1000;
      LOGDEBUG("recieveData2 %d ", receiveTimeoutSec);
    } else {
      receiveTimeoutSec = receiveTimeoutSec * 1000 * 1000;
    }
  } else {  // it is set as seconds and change it to microsecond
    receiveTimeoutSec = receiveTimeoutSec * 1000 * 1000;
  }

  uint32_t defaultWaitSecs =
      isNotificationMessage ? 1 * 1000 * 1000 : 2 * 1000 * 1000;
  // uint32_t defaultMicroSecs = (sendTimeoutSec % (1000*1000))
  if (defaultWaitSecs > receiveTimeoutSec) defaultWaitSecs = receiveTimeoutSec;

  int32_t startLen = length;

  while (length > 0 && receiveTimeoutSec > 0) {
    if (checkConnected && !m_connected) {
      return CONN_IOERR;
    }
    if (receiveTimeoutSec < defaultWaitSecs) {
      defaultWaitSecs = receiveTimeoutSec;
    }
    int32_t readBytes = m_conn->receive(buffer, length, defaultWaitSecs, 0);
    int32_t lastError = ACE_OS::last_error();
    length -= readBytes;
    if (length > 0 && lastError != ETIME && lastError != ETIMEDOUT) {
      return CONN_IOERR;
    }
    buffer += readBytes;
    /*
      Update pools byteRecieved stat here.
      readMessageChunked, readMessage, readHandshakeData,
      readHandshakeRawData, readHandShakeBytes, readHandShakeInt,
      readHandshakeString, all call TcrConnection::receiveData.
    */
    LOGDEBUG("TcrConnection::receiveData length = %d defaultWaitSecs = %d",
             length, defaultWaitSecs);
    if (m_poolDM != NULL) {
      LOGDEBUG("TcrConnection::receiveData readBytes = %d", readBytes);
      m_poolDM->getStats().incReceivedBytes(static_cast<int64>(readBytes));
    }
    receiveTimeoutSec -= defaultWaitSecs;
    if ((length == startLen) && isNotificationMessage) {  // no data read
      break;
    }
  }
  //  Postconditions for checking bounds.
  GF_DEV_ASSERT(startLen >= length);
  GF_DEV_ASSERT(length >= 0);
  return (length == 0 ? CONN_NOERR
                      : (length == startLen ? CONN_NODATA : CONN_TIMEOUT));
}

inline ConnErrType TcrConnection::sendData(const char* buffer, int32_t length,
                                           uint32_t sendTimeoutSec,
                                           bool checkConnected) {
  uint32_t dummy = 0;
  return sendData(dummy, buffer, length, sendTimeoutSec, checkConnected);
}

inline ConnErrType TcrConnection::sendData(uint32_t& timeSpent,
                                           const char* buffer, int32_t length,
                                           uint32_t sendTimeoutSec,
                                           bool checkConnected,
                                           int32_t notPublicApiWithTimeout) {
  GF_DEV_ASSERT(buffer != NULL);
  GF_DEV_ASSERT(m_conn != NULL);
  bool isPublicApiTimeout = false;
  // if gfcpp property unit set then sendTimeoutSec will be in millisecond
  // otherwise it will be in second
  if (DistributedSystem::getSystemProperties()->readTimeoutUnitInMillis()) {
    LOGFINER("sendData %d  %d", sendTimeoutSec, notPublicApiWithTimeout);
    if (notPublicApiWithTimeout == TcrMessage::QUERY ||
        notPublicApiWithTimeout == TcrMessage::QUERY_WITH_PARAMETERS ||
        notPublicApiWithTimeout == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE ||
        /*notPublicApiWithTimeout == TcrMessage::GETDURABLECQS_MSG_TYPE || this
           is not public yet*/
        notPublicApiWithTimeout == TcrMessage::EXECUTE_FUNCTION ||
        notPublicApiWithTimeout == TcrMessage::EXECUTE_REGION_FUNCTION ||
        notPublicApiWithTimeout ==
            TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP) {
      // then app has set timeout in millis, change it to microSeconds
      sendTimeoutSec = sendTimeoutSec * 1000;
      isPublicApiTimeout = true;
      LOGDEBUG("sendData2 %d ", sendTimeoutSec);
    } else {
      sendTimeoutSec = sendTimeoutSec * 1000;
    }
  } else {  // it is set as seconds and change it to microsecond
    sendTimeoutSec = sendTimeoutSec * 1000 * 1000;
  }

  uint32_t defaultWaitSecs = 2 * 1000 * 1000;  // 2 second
  // uint32_t defaultMicroSecs = (sendTimeoutSec % (1000*1000))
  if (defaultWaitSecs > sendTimeoutSec) defaultWaitSecs = sendTimeoutSec;
  LOGDEBUG(
      "before send len %d sendTimeoutSec = %d checkConnected = %d m_connected "
      "%d",
      length, sendTimeoutSec, checkConnected, m_connected);
  while (length > 0 && sendTimeoutSec > 0) {
    if (checkConnected && !m_connected) {
      return CONN_IOERR;
    }
    LOGDEBUG("before send ");
    if (sendTimeoutSec < defaultWaitSecs) {
      defaultWaitSecs = sendTimeoutSec;
    }
    int32_t sentBytes = m_conn->send(buffer, length, defaultWaitSecs, 0);

    length -= sentBytes;
    buffer += sentBytes;
    // we don't want to decrement the remaining time for the last iteration
    if (length == 0) {
      break;
    }
    int32_t lastError = ACE_OS::last_error();
    if (length > 0 && lastError != ETIME && lastError != ETIMEDOUT) {
      return CONN_IOERR;
    }

    timeSpent += defaultWaitSecs;
    sendTimeoutSec -= defaultWaitSecs;
  }
  if (isPublicApiTimeout) {  // it should go in millis
    timeSpent = timeSpent / 1000;
  } else {  // it should go in seconds
    timeSpent = timeSpent / (1000 * 1000);
  }
  return (length == 0 ? CONN_NOERR : CONN_TIMEOUT);
}

char* TcrConnection::sendRequest(const char* buffer, int32_t len,
                                 size_t* recvLen, uint32_t sendTimeoutSec,
                                 uint32_t receiveTimeoutSec, int32 request) {
  LOGDEBUG("TcrConnection::sendRequest");
  uint32_t timeSpent = 0;

  send(timeSpent, buffer, len, sendTimeoutSec);

  if (timeSpent >= receiveTimeoutSec)
    throwException(
        TimeoutException("TcrConnection::send: connection timed out"));

  receiveTimeoutSec -= timeSpent;
  ConnErrType opErr = CONN_NOERR;
  return readMessage(recvLen, receiveTimeoutSec, true, &opErr, false, request);
}

void TcrConnection::sendRequestForChunkedResponse(const TcrMessage& request,
                                                  int32_t len,
                                                  TcrMessageReply& reply,
                                                  uint32_t sendTimeoutSec,
                                                  uint32_t receiveTimeoutSec) {
  int32_t msgType = request.getMessageType();
  // ACE_OS::memcpy(&msgType, buffer, 4);
  // msgType = ntohl(msgType);

  /*receiveTimeoutSec = (msgType == TcrMessage::QUERY ||
    msgType == TcrMessage::QUERY_WITH_PARAMETERS ||
    msgType == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE ||
    msgType == TcrMessage::GETDURABLECQS_MSG_TYPE ||
    msgType == TcrMessage::EXECUTE_FUNCTION ||
    msgType == TcrMessage::EXECUTE_REGION_FUNCTION ||
    msgType == TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP)
    ? reply.getTimeout() : receiveTimeoutSec;

  //send + recieve should be part of API timeout
  sendTimeoutSec = (msgType == TcrMessage::QUERY ||
    msgType == TcrMessage::QUERY_WITH_PARAMETERS ||
    msgType == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE ||
    msgType == TcrMessage::GETDURABLECQS_MSG_TYPE ||
    msgType == TcrMessage::EXECUTE_FUNCTION ||
    msgType == TcrMessage::EXECUTE_REGION_FUNCTION ||
    msgType == TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP)
    ? reply.getTimeout() : sendTimeoutSec;
    */
  switch (msgType) {
    case TcrMessage::QUERY:
    case TcrMessage::QUERY_WITH_PARAMETERS:
    case TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE:
    case TcrMessage::GETDURABLECQS_MSG_TYPE:
    case TcrMessage::EXECUTE_FUNCTION:
    case TcrMessage::EXECUTE_REGION_FUNCTION:
    case TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP: {
      receiveTimeoutSec = reply.getTimeout();
      sendTimeoutSec = reply.getTimeout();
      break;
    }
    default:
      break;
  }
  /*if((msgType == TcrMessage::QUERY ||
    msgType == TcrMessage::QUERY_WITH_PARAMETERS ||
    msgType == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE ||
    msgType == TcrMessage::GETDURABLECQS_MSG_TYPE ||
    msgType == TcrMessage::EXECUTE_FUNCTION ||
    msgType == TcrMessage::EXECUTE_REGION_FUNCTION))
  {
    receiveTimeoutSec = reply.getTimeout();
    sendTimeoutSec = reply.getTimeout();
  }*/

  // send(buffer, len, sendTimeoutSec);
  uint32_t timeSpent = 0;
  send(timeSpent, request.getMsgData(), len, sendTimeoutSec, true, msgType);

  if (timeSpent >= receiveTimeoutSec)
    throwException(
        TimeoutException("TcrConnection::send: connection timed out"));

  receiveTimeoutSec -= timeSpent;

  // to help in decoding the reply based on what was the request type
  reply.setMessageTypeRequest(msgType);
  // no need of it now, this will not come here
  if (msgType == TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP) {
    ChunkedFunctionExecutionResponse* resultCollector =
        static_cast<ChunkedFunctionExecutionResponse*>(
            reply.getChunkedResultHandler());
    if (resultCollector->getResult() == false) {
      LOGDEBUG(
          "TcrConnection::sendRequestForChunkedResponse: function execution, "
          "no response desired");
      return;
    }
  }
  readMessageChunked(reply, receiveTimeoutSec, true);
}

void TcrConnection::send(const char* buffer, int len, uint32_t sendTimeoutSec,
                         bool checkConnected) {
  uint32_t dummy = 0;
  send(dummy, buffer, len, sendTimeoutSec, checkConnected);
}

void TcrConnection::send(uint32_t& timeSpent, const char* buffer, int len,
                         uint32_t sendTimeoutSec, bool checkConnected,
                         int32_t notPublicApiWithTimeout) {
  GF_DEV_ASSERT(m_conn != NULL);

  // LOGINFO("TcrConnection::send: [%p] sending request to endpoint %s;",
  //:  this, m_endpoint);

  LOGDEBUG(
      "TcrConnection::send: [%p] sending request to endpoint %s; bytes: %s",
      this, m_endpoint, Utils::convertBytesToString(buffer, len)->asChar());

  ConnErrType error = sendData(timeSpent, buffer, len, sendTimeoutSec,
                               checkConnected, notPublicApiWithTimeout);

  LOGFINER(
      "TcrConnection::send: completed send request to endpoint %s "
      "with error: %d",
      m_endpoint, error);

  if (error != CONN_NOERR) {
    if (error == CONN_TIMEOUT) {
      throwException(
          TimeoutException("TcrConnection::send: connection timed out"));
    } else {
      throwException(
          GemfireIOException("TcrConnection::send: connection failure"));
    }
  }
}

char* TcrConnection::receive(size_t* recvLen, ConnErrType* opErr,
                             uint32_t receiveTimeoutSec) {
  GF_DEV_ASSERT(m_conn != NULL);

  return readMessage(recvLen, receiveTimeoutSec, false, opErr, true);
}

char* TcrConnection::readMessage(size_t* recvLen, uint32_t receiveTimeoutSec,
                                 bool doHeaderTimeoutRetries,
                                 ConnErrType* opErr, bool isNotificationMessage,
                                 int32 request) {
  char msg_header[HEADER_LENGTH];
  int32_t msgType, msgLen;
  ConnErrType error;

  uint32_t headerTimeout = receiveTimeoutSec;
  if (doHeaderTimeoutRetries &&
      receiveTimeoutSec == DEFAULT_READ_TIMEOUT_SECS) {
    headerTimeout = DEFAULT_READ_TIMEOUT_SECS * DEFAULT_TIMEOUT_RETRIES;
  }

  LOGDEBUG("TcrConnection::readMessage: receiving reply from endpoint %s",
           m_endpoint);

  error = receiveData(msg_header, HEADER_LENGTH, headerTimeout, true,
                      isNotificationMessage);
  LOGDEBUG("TcrConnection::readMessage after recieve data");
  if (error != CONN_NOERR) {
    //  the !isNotificationMessage ensures that notification channel
    // gets the TimeoutException when no data was received and is ignored by
    // notification channel; when data has been received then it throws
    // GemfireIOException that causes the channel to close as required
    if (error == CONN_NODATA ||
        (error == CONN_TIMEOUT && !isNotificationMessage)) {
      if (isNotificationMessage) {
        // fix #752 - do not throw periodic TimeoutException for subscription
        // channels to avoid frequent stack trace processing.
        return NULL;
      } else {
        throwException(TimeoutException(
            "TcrConnection::readMessage: "
            "connection timed out while receiving message header"));
      }
    } else {
      if (isNotificationMessage) {
        *opErr = CONN_IOERR;
        return NULL;
      }
      throwException(GemfireIOException(
          "TcrConnection::readMessage: "
          "connection failure while receiving message header"));
    }
  }

  LOGDEBUG(
      "TcrConnection::readMessage: received header from endpoint %s; "
      "bytes: %s",
      m_endpoint,
      Utils::convertBytesToString(msg_header, HEADER_LENGTH)->asChar());

  DataInput input(reinterpret_cast<uint8_t*>(msg_header), HEADER_LENGTH);
  input.readInt(&msgType);
  input.readInt(&msgLen);
  //  check that message length is valid.
  if (!(msgLen > 0) && request == TcrMessage::GET_CLIENT_PR_METADATA) {
    char* fullMessage;
    *recvLen = HEADER_LENGTH + msgLen;
    GF_NEW(fullMessage, char[HEADER_LENGTH + msgLen]);
    ACE_OS::memcpy(fullMessage, msg_header, HEADER_LENGTH);
    return fullMessage;
    // exit(0);
  }
  // GF_DEV_ASSERT(msgLen > 0);

  // user has to delete this pointer
  char* fullMessage;
  *recvLen = HEADER_LENGTH + msgLen;
  GF_NEW(fullMessage, char[HEADER_LENGTH + msgLen]);
  ACE_OS::memcpy(fullMessage, msg_header, HEADER_LENGTH);

  uint32_t mesgBodyTimeout = receiveTimeoutSec;
  if (isNotificationMessage) {
    mesgBodyTimeout = receiveTimeoutSec * DEFAULT_TIMEOUT_RETRIES;
  }
  error = receiveData(fullMessage + HEADER_LENGTH, msgLen, mesgBodyTimeout,
                      true, isNotificationMessage);
  if (error != CONN_NOERR) {
    delete[] fullMessage;
    //  the !isNotificationMessage ensures that notification channel
    // gets the GemfireIOException and not TimeoutException;
    // this is required since header has already been read meaning there could
    // be stale data on socket and so it should close the notification channel
    // while TimeoutException is normally ignored by notification channel
    if ((error & CONN_TIMEOUT) && !isNotificationMessage) {
      throwException(TimeoutException(
          "TcrConnection::readMessage: "
          "connection timed out while receiving message body"));
    } else {
      if (isNotificationMessage) {
        *opErr = CONN_IOERR;
        return NULL;
      }
      throwException(GemfireIOException(
          "TcrConnection::readMessage: "
          "connection failure while receiving message body"));
    }
  }

  LOGDEBUG(
      "TcrConnection::readMessage: received message body from "
      "endpoint %s; bytes: %s",
      m_endpoint,
      Utils::convertBytesToString(fullMessage + HEADER_LENGTH, msgLen)
          ->asChar());

  // This is the test case when msg type is GET_CLIENT_PR_METADATA and msgLen is
  // 0.
  /*if (request == TcrMessage::GET_CLIENT_PR_METADATA) {
  LOGCONFIG("Amey request == TcrMessage::GET_CLIENT_PR_METADATA");
  char* fullMessage2;
  *recvLen = HEADER_LENGTH;
  GF_NEW( fullMessage2, char[HEADER_LENGTH ] );
  ACE_OS::memcpy(fullMessage2, msg_header, HEADER_LENGTH);
  return fullMessage2;
  }*/

  return fullMessage;
}

void TcrConnection::readMessageChunked(TcrMessageReply& reply,
                                       uint32_t receiveTimeoutSec,
                                       bool doHeaderTimeoutRetries) {
  const int HDR_LEN = 5;
  const int HDR_LEN_12 = 12;
  uint8_t msg_header[HDR_LEN_12 + HDR_LEN];
  ConnErrType error;

  uint32_t headerTimeout = receiveTimeoutSec;
  if (doHeaderTimeoutRetries &&
      receiveTimeoutSec == DEFAULT_READ_TIMEOUT_SECS) {
    headerTimeout = DEFAULT_READ_TIMEOUT_SECS * DEFAULT_TIMEOUT_RETRIES;
  }

  LOGFINER(
      "TcrConnection::readMessageChunked: receiving reply from "
      "endpoint %s",
      m_endpoint);

  error =
      receiveData(reinterpret_cast<char*>(msg_header), HDR_LEN_12 + HDR_LEN,
                  headerTimeout, true, false, reply.getMessageTypeRequest());
  if (error != CONN_NOERR) {
    if (error & CONN_TIMEOUT) {
      throwException(TimeoutException(
          "TcrConnection::readMessageChunked: "
          "connection timed out while receiving message header"));
    } else {
      throwException(GemfireIOException(
          "TcrConnection::readMessageChunked: "
          "connection failure while receiving message header"));
    }
  }

  LOGDEBUG(
      "TcrConnection::readMessageChunked: received header from "
      "endpoint %s; bytes: %s",
      m_endpoint,
      Utils::convertBytesToString(msg_header, HDR_LEN_12)->asChar());

  DataInput input(msg_header, HDR_LEN_12);
  int32_t msgType;
  input.readInt(&msgType);
  reply.setMessageType(msgType);
  int32_t txId;
  int32_t numOfParts;
  input.readInt(&numOfParts);
  LOGDEBUG("TcrConnection::readMessageChunked numberof parts = %d ",
           numOfParts);
  // input.advanceCursor(4);
  input.readInt(&txId);
  reply.setTransId(txId);

  // bool isLastChunk = false;
  uint8_t isLastChunk = 0x0;

  int chunkNum = 0;

  // Initialize the chunk processing
  reply.startProcessChunk(m_chunksProcessSema);

  //  indicate an end to chunk processing and wait for processing
  // to end even if reading the chunks fails in middle
  struct FinalizeProcessChunk {
   private:
    TcrMessage& m_reply;
    uint16_t m_endpointmemId;

   public:
    FinalizeProcessChunk(TcrMessageReply& reply, uint16_t endpointmemId)
        : m_reply(reply), m_endpointmemId(endpointmemId) {}
    ~FinalizeProcessChunk() {
      // Enqueue a NULL chunk indicating a wait for processing to complete.
      m_reply.processChunk(NULL, 0, m_endpointmemId);
    }
  } endProcessChunk(reply, m_endpointObj->getDistributedMemberID());
  bool first = true;
  do {
    // uint8_t chunk_header[HDR_LEN];
    if (!first) {
      error = receiveData(reinterpret_cast<char*>(msg_header + HDR_LEN_12),
                          HDR_LEN, headerTimeout, true, false,
                          reply.getMessageTypeRequest());
      if (error != CONN_NOERR) {
        if (error & CONN_TIMEOUT) {
          throwException(TimeoutException(
              "TcrConnection::readMessageChunked: "
              "connection timed out while receiving chunk header"));
        } else {
          throwException(GemfireIOException(
              "TcrConnection::readMessageChunked: "
              "connection failure while receiving chunk header"));
        }
      }
    } else {
      first = false;
    }
    ++chunkNum;

    LOGDEBUG(
        "TcrConnection::readMessageChunked: received chunk header %d "
        "from endpoint %s; bytes: %s",
        chunkNum, m_endpoint,
        Utils::convertBytesToString((msg_header + HDR_LEN_12), HDR_LEN)
            ->asChar());

    DataInput inp((msg_header + HDR_LEN_12), HDR_LEN);
    int32_t chunkLen;
    inp.readInt(&chunkLen);
    //  check that chunk length is valid.
    GF_DEV_ASSERT(chunkLen > 0);
    // inp.readBoolean(&isLastChunk);
    inp.read(&isLastChunk);

    uint8_t* chunk_body;
    GF_NEW(chunk_body, uint8_t[chunkLen]);
    error = receiveData(reinterpret_cast<char*>(chunk_body), chunkLen,
                        receiveTimeoutSec, true, false,
                        reply.getMessageTypeRequest());
    if (error != CONN_NOERR) {
      delete[] chunk_body;
      if (error & CONN_TIMEOUT) {
        throwException(TimeoutException(
            "TcrConnection::readMessageChunked: "
            "connection timed out while receiving chunk body"));
      } else {
        throwException(GemfireIOException(
            "TcrConnection::readMessageChunked: "
            "connection failure while receiving chunk body"));
      }
    }

    LOGDEBUG(
        "TcrConnection::readMessageChunked: received chunk body %d "
        "from endpoint %s; bytes: %s",
        chunkNum, m_endpoint,
        Utils::convertBytesToString(chunk_body, chunkLen)->asChar());
    // Process the chunk; the actual processing is done by a separate thread
    // ThinClientBaseDM::m_chunkProcessor.

    reply.processChunk(chunk_body, chunkLen,
                       m_endpointObj->getDistributedMemberID(), isLastChunk);
  } while (!(isLastChunk & 0x1));

  LOGFINER(
      "TcrConnection::readMessageChunked: read full reply "
      "from endpoint %s",
      m_endpoint);
}

void TcrConnection::close() {
  // If this is a short lived grid client, don't bother with this close ack
  // message
  if (DistributedSystem::getSystemProperties()->isGridClient()) {
    return;
  }

  TcrMessage* closeMsg = TcrMessage::getCloseConnMessage();
  try {
    // LOGINFO("TcrConnection::close DC  = %d; netdown = %d endpoint %s",
    // TcrConnectionManager::TEST_DURABLE_CLIENT_CRASH,
    // TcrConnectionManager::isNetDown, m_endpoint);
    if (!TcrConnectionManager::TEST_DURABLE_CLIENT_CRASH &&
        !TcrConnectionManager::isNetDown) {
      send(closeMsg->getMsgData(), closeMsg->getMsgLength(), 2, false);
    }
  } catch (Exception& e) {
    LOGINFO("Close connection message failed with msg: %s", e.getMessage());
  } catch (...) {
    LOGINFO("Close connection message failed");
  }
}

/* adongre
 * CID 28738: Unsigned compared against 0 (NO_EFFECT)
 * This less-than-zero comparison of an unsigned value is never true. "msgLength
 * < 0U".
 * Changing the unint32_t to int32_t
 */
// CacheableBytesPtr TcrConnection::readHandshakeData(uint32_t msgLength,
// uint32_t connectTimeout )
CacheableBytesPtr TcrConnection::readHandshakeData(int32_t msgLength,
                                                   uint32_t connectTimeout) {
  ConnErrType error = CONN_NOERR;
  if (msgLength < 0) {
    msgLength = 0;
  }
  char* recvMessage;
  GF_NEW(recvMessage, char[msgLength + 1]);
  recvMessage[msgLength] = '\0';
  if (msgLength == 0) {
    return CacheableBytes::createNoCopy(reinterpret_cast<uint8_t*>(recvMessage),
                                        1);
  }
  if ((error = receiveData(recvMessage, msgLength, connectTimeout, false)) !=
      CONN_NOERR) {
    if (error & CONN_TIMEOUT) {
      GF_SAFE_DELETE_ARRAY(recvMessage);
      GF_SAFE_DELETE_CON(m_conn);
      throwException(
          TimeoutException("TcrConnection::TcrConnection: "
                           "Timeout in handshake"));
    } else {
      GF_SAFE_DELETE_ARRAY(recvMessage);
      GF_SAFE_DELETE_CON(m_conn);
      throwException(
          GemfireIOException("TcrConnection::TcrConnection: "
                             "Handshake failure"));
    }
  } else {
    return CacheableBytes::createNoCopy(reinterpret_cast<uint8_t*>(recvMessage),
                                        msgLength + 1);
  }
  return NULLPTR;
}

// read just the bytes without the trailing null terminator
/* adongre
 * CID 28739: Unsigned compared against 0 (NO_EFFECT)
 * change the input parameter from unint32_t to int32_t
 * as the comparasion case is valid
 */
CacheableBytesPtr TcrConnection::readHandshakeRawData(int32_t msgLength,
                                                      uint32_t connectTimeout) {
  ConnErrType error = CONN_NOERR;
  if (msgLength < 0) {
    msgLength = 0;
  }
  if (msgLength == 0) {
    return NULLPTR;
  }
  char* recvMessage;
  GF_NEW(recvMessage, char[msgLength]);
  if ((error = receiveData(recvMessage, msgLength, connectTimeout, false)) !=
      CONN_NOERR) {
    if (error & CONN_TIMEOUT) {
      GF_SAFE_DELETE_ARRAY(recvMessage);
      GF_SAFE_DELETE_CON(m_conn);
      throwException(
          TimeoutException("TcrConnection::TcrConnection: "
                           "Timeout in handshake"));
    } else {
      GF_SAFE_DELETE_ARRAY(recvMessage);
      GF_SAFE_DELETE_CON(m_conn);
      throwException(
          GemfireIOException("TcrConnection::TcrConnection: "
                             "Handshake failure"));
    }
    // not expected to be reached
    return NULLPTR;
  } else {
    return CacheableBytes::createNoCopy(reinterpret_cast<uint8_t*>(recvMessage),
                                        msgLength);
  }
}

// read a byte array
CacheableBytesPtr TcrConnection::readHandshakeByteArray(
    uint32_t connectTimeout) {
  /*CacheableBytesPtr codeBytes = readHandshakeData(1, connectTimeout);
  DataInput codeDI(codeBytes->value(), codeBytes->length());
  uint8_t code = 0;
  codeDI.read(&code);
  uint32_t arraySize = 0;
  if (code == 0xFF) {
  return NULLPTR;
  } else {
  int32_t tempLen = code;
  if (tempLen > 252) {  // 252 is java's ((byte)-4 && 0xFF)
  if (code == 0xFE) {
  CacheableBytesPtr lenBytes = readHandshakeData(2, connectTimeout);
  DataInput lenDI(lenBytes->value(), lenBytes->length());
  uint16_t val;
  lenDI.readInt(&val);
  tempLen = val;
  } else if (code == 0xFD) {
  CacheableBytesPtr lenBytes = readHandshakeData(4, connectTimeout);
  DataInput lenDI(lenBytes->value(), lenBytes->length());
  uint32_t val;
  lenDI.readInt(&val);
  tempLen = val;
  } else {
  GF_SAFE_DELETE(m_conn);
  throwException(IllegalStateException("unexpected array length code"));
  }
  }
  arraySize = tempLen;
  }*/

  uint32_t arraySize = readHandshakeArraySize(connectTimeout);
  return readHandshakeRawData(arraySize, connectTimeout);
}

// read a byte array
uint32_t TcrConnection::readHandshakeArraySize(uint32_t connectTimeout) {
  CacheableBytesPtr codeBytes = readHandshakeData(1, connectTimeout);
  DataInput codeDI(codeBytes->value(), codeBytes->length());
  uint8_t code = 0;
  codeDI.read(&code);
  uint32_t arraySize = 0;
  if (code == 0xFF) {
    return 0;
  } else {
    int32_t tempLen = code;
    if (tempLen > 252) {  // 252 is java's ((byte)-4 && 0xFF)
      if (code == 0xFE) {
        CacheableBytesPtr lenBytes = readHandshakeData(2, connectTimeout);
        DataInput lenDI(lenBytes->value(), lenBytes->length());
        uint16_t val;
        lenDI.readInt(&val);
        tempLen = val;
      } else if (code == 0xFD) {
        CacheableBytesPtr lenBytes = readHandshakeData(4, connectTimeout);
        DataInput lenDI(lenBytes->value(), lenBytes->length());
        uint32_t val;
        lenDI.readInt(&val);
        tempLen = val;
      } else {
        GF_SAFE_DELETE_CON(m_conn);
        throwException(IllegalStateException("unexpected array length code"));
      }
    }
    arraySize = tempLen;
  }

  return arraySize;
}

void TcrConnection::readHandshakeInstantiatorMsg(uint32_t connectTimeout) {
  int hashMapSize = readHandshakeArraySize(connectTimeout);
  for (int i = 0; i < hashMapSize; i++) {
    readHandShakeBytes(6, connectTimeout);  // reading integer and arraylist
                                            // type
    int aLen = readHandshakeArraySize(connectTimeout);
    for (int j = 0; j < aLen; j++) {
      readHandshakeString(connectTimeout);
    }
  }

  hashMapSize = readHandshakeArraySize(connectTimeout);
  for (int i = 0; i < hashMapSize; i++) {
    readHandShakeBytes(5, connectTimeout);  // reading integer
    readHandshakeString(connectTimeout);
  }

  // added in 3.6 and 6.6
  int hashMapSize2 = readHandshakeArraySize(connectTimeout);
  for (int i = 0; i < hashMapSize2; i++) {
    readHandShakeBytes(6, connectTimeout);  // reading integer and arraylist
                                            // type
    int aLen = readHandshakeArraySize(connectTimeout);
    for (int j = 0; j < aLen; j++) {
      readHandshakeString(connectTimeout);
    }
  }
}
void TcrConnection::readHandShakeBytes(int numberOfBytes,
                                       uint32_t connectTimeout) {
  ConnErrType error = CONN_NOERR;
  uint8_t* recvMessage;
  GF_NEW(recvMessage, uint8_t[numberOfBytes]);

  if ((error = receiveData(reinterpret_cast<char*>(recvMessage), numberOfBytes,
                           connectTimeout, false)) != CONN_NOERR) {
    if (error & CONN_TIMEOUT) {
      GF_SAFE_DELETE_ARRAY(recvMessage);
      GF_SAFE_DELETE_CON(m_conn);
      throwException(
          TimeoutException("TcrConnection::TcrConnection: "
                           "Timeout in handshake"));
    } else {
      GF_SAFE_DELETE_ARRAY(recvMessage);
      GF_SAFE_DELETE_CON(m_conn);
      throwException(
          GemfireIOException("TcrConnection::TcrConnection: "
                             "Handshake failure"));
    }
  }

  GF_SAFE_DELETE_ARRAY(recvMessage);
}

int32_t TcrConnection::readHandShakeInt(uint32_t connectTimeout) {
  ConnErrType error = CONN_NOERR;
  uint8_t* recvMessage;
  GF_NEW(recvMessage, uint8_t[4]);

  if ((error = receiveData(reinterpret_cast<char*>(recvMessage), 4,
                           connectTimeout, false)) != CONN_NOERR) {
    if (error & CONN_TIMEOUT) {
      GF_SAFE_DELETE_ARRAY(recvMessage);
      GF_SAFE_DELETE_CON(m_conn);
      throwException(
          TimeoutException("TcrConnection::TcrConnection: "
                           "Timeout in handshake"));
    } else {
      GF_SAFE_DELETE_ARRAY(recvMessage);
      GF_SAFE_DELETE_CON(m_conn);
      throwException(
          GemfireIOException("TcrConnection::TcrConnection: "
                             "Handshake failure"));
    }
  }

  DataInput di(recvMessage, 4);
  int32_t val;
  di.readInt(&val);

  GF_SAFE_DELETE_ARRAY(recvMessage);

  return val;
}
CacheableStringPtr TcrConnection::readHandshakeString(uint32_t connectTimeout) {
  ConnErrType error = CONN_NOERR;

  char cstypeid;
  if (receiveData(&cstypeid, 1, connectTimeout, false) != CONN_NOERR) {
    GF_SAFE_DELETE_CON(m_conn);
    if (error & CONN_TIMEOUT) {
      LOGFINE("Timeout receiving string typeid");
      throwException(
          TimeoutException("TcrConnection::TcrConnection: "
                           "Timeout in handshake reading string type ID"));
    } else {
      LOGFINE("IO error receiving string typeid");
      throwException(
          GemfireIOException("TcrConnection::TcrConnection: "
                             "Handshake failure reading string type ID"));
    }
  }

  LOGDEBUG("Received string typeid as %d", cstypeid);

  uint32_t length = 0;
  switch (static_cast<int8_t>(cstypeid)) {
    case GemfireTypeIds::CacheableNullString: {
      return NULLPTR;
      break;
    }
    case GF_STRING: {
      uint16_t shortLen = 0;
      CacheableBytesPtr lenBytes = readHandshakeData(2, connectTimeout);
      DataInput lenDI(lenBytes->value(), lenBytes->length());
      lenDI.readInt(&shortLen);
      length = shortLen;
      break;
    }
    default: {
      GF_SAFE_DELETE_CON(m_conn);
      throwException(
          GemfireIOException("TcrConnection::TcrConnection: "
                             "Handshake failure: Unexpected string type ID"));
    }
  }

  LOGDEBUG(" Received string len %d", length);

  if (length == 0) {
    return NULLPTR;
  }

  char* recvMessage;
  GF_NEW(recvMessage, char[length + 1]);
  recvMessage[length] = '\0';

  if ((error = receiveData(recvMessage, length, connectTimeout, false)) !=
      CONN_NOERR) {
    if (error & CONN_TIMEOUT) {
      GF_SAFE_DELETE_ARRAY(recvMessage);
      GF_SAFE_DELETE_CON(m_conn);
      LOGFINE("Timeout receiving string data");
      throwException(
          TimeoutException("TcrConnection::TcrConnection: "
                           "Timeout in handshake reading string bytes"));
    } else {
      GF_SAFE_DELETE_ARRAY(recvMessage);
      GF_SAFE_DELETE_CON(m_conn);
      LOGFINE("IO error receiving string data");
      throwException(
          GemfireIOException("TcrConnection::TcrConnection: "
                             "Handshake failure reading string bytes"));
    }
    // not expected to be reached
    return NULLPTR;
  } else {
    LOGDEBUG(" Received string data [%s]", recvMessage);
    CacheableStringPtr retval =
        CacheableString::createNoCopy(recvMessage, length);
    return retval;
  }
}
bool TcrConnection::hasExpired(int expiryTime) {
  if (expiryTime == -1) {
    return false;
  }

  ACE_Time_Value _expiryTime(expiryTime / 1000, (expiryTime % 1000) * 1000);

  if (ACE_OS::gettimeofday() - m_creationTime > _expiryTime) {
    return true;
  } else {
    return false;
  }
}

bool TcrConnection::isIdle(int idleTime) {
  if (idleTime == -1) {
    return false;
  }

  ACE_Time_Value _idleTime(idleTime / 1000, (idleTime % 1000) * 1000);

  if (ACE_OS::gettimeofday() - m_lastAccessed > _idleTime) {
    return true;
  } else {
    return false;
  }
}

void TcrConnection::touch() { m_lastAccessed = ACE_OS::gettimeofday(); }

ACE_Time_Value TcrConnection::getLastAccessed() { return m_lastAccessed; }

uint8_t TcrConnection::getOverrides(SystemProperties* props) {
  const char* conflate = props->conflateEvents();
  uint8_t conflateByte = 0;
  if (conflate != NULL) {
    if (ACE_OS::strcasecmp(conflate, "true") == 0) {
      conflateByte = 1;
    } else if (ACE_OS::strcasecmp(conflate, "false") == 0) {
      conflateByte = 2;
    }
  }
  /*
  const char * removeUnresponsive = props->removeUnresponsiveClientOverride();
  uint8_t removeByte = 0;
  if (removeUnresponsive != NULL ) {
  if ( ACE_OS::strcasecmp(removeUnresponsive, "true") == 0 ) {
  removeByte = 1;
  } else if ( ACE_OS::strcasecmp(removeUnresponsive, "false") == 0 ) {
  removeByte = 2;
  }
  }
  const char * notify = props->notifyBySubscriptionOverride();
  uint8_t notifyByte = 0;
  if (notify != NULL ) {
  if ( ACE_OS::strcasecmp(notify, "true") == 0 ) {
  notifyByte = 1;
  } else if ( ACE_OS::strcasecmp(notify, "false") == 0 ) {
  notifyByte = 2;
  }
  }
  return (((notifyByte << 2) | removeByte) << 2) | conflateByte;
  */
  return conflateByte;
}

void TcrConnection::updateCreationTime() {
  m_creationTime = ACE_OS::gettimeofday();
  touch();
}

TcrConnection::~TcrConnection() {
  LOGDEBUG("Tcrconnection destructor %p . conn ref to endopint %d", this,
           m_endpointObj->getConnRefCounter());
  m_endpointObj->addConnRefCounter(-1);
  if (m_conn != NULL) {
    LOGDEBUG("closing the connection");
    m_conn->close();
    GF_SAFE_DELETE_CON(m_conn);
  }

  if (m_dh != NULL) {
    m_dh->clearDhKeys();
    GF_SAFE_DELETE(m_dh);
  }
}

bool TcrConnection::setAndGetBeingUsed(volatile bool isBeingUsed,
                                       bool forTransaction) {
  uint32_t currentValue = 0;
  uint32_t retVal = 0U;

  if (!forTransaction) {
    if (isBeingUsed) {
      if (m_isUsed == 1 || m_isUsed == 2) return false;
      retVal = HostAsm::atomicCompareAndExchange(m_isUsed, 1, currentValue);
      if (retVal == currentValue) return true;
      return false;
    } else {
      m_isUsed = 0;
      return true;
    }
  } else {
    if (isBeingUsed) {
      if (m_isUsed == 1) {  // already used
        return false;
      }
      if (m_isUsed == 2) {  // transaction thread has set, reused it
        return true;
      }
      retVal = HostAsm::atomicCompareAndExchange(
          m_isUsed, 2 /*for transaction*/, currentValue);
      if (retVal == currentValue) return true;
      return false;
    } else {
      // m_isUsed = 0;//this will done by releasing the connection by
      // transaction at the end of transaction
      return true;
    }
  }
}
