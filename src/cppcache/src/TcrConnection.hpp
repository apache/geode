/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __TCR_CONNECTION_HPP__
#define __TCR_CONNECTION_HPP__

#include <ace/Semaphore.h>
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include "Connector.hpp"
#include "Set.hpp"
#include "TcrMessage.hpp"
#include <gfcpp/CacheableBuiltins.hpp>
#include "DiffieHellman.hpp"

#define DEFAULT_TIMEOUT_RETRIES 12
#define PRIMARY_SERVER_TO_CLIENT 101
#define SECONDARY_SERVER_TO_CLIENT 102
#define SUCCESSFUL_SERVER_TO_CLIENT 105
#define UNSUCCESSFUL_SERVER_TO_CLIENT 106
#define CLIENT_TO_SERVER 100
#define REPLY_OK 59
#define REPLY_OK_CS43 58
#define REPLY_REFUSED 60
#define REPLY_INVALID 61
#define REPLY_SSL_ENABLED 21
#define REPLY_AUTHENTICATION_REQUIRED 62
#define REPLY_AUTHENTICATION_FAILED 63
#define REPLY_DUPLICATE_DURABLE_CLIENT 64

#define SECURITY_CREDENTIALS_NONE 0
#define SECURITY_CREDENTIALS_NORMAL 1
#define SECURITY_CREDENTIALS_DHENCRYPT 2
#define SECURITY_MULTIUSER_NOTIFICATIONCHANNEL 3

/** Closes and Deletes connection only if it exists */
#define GF_SAFE_DELETE_CON(x) \
  {                           \
    x->close();               \
    delete x;                 \
    x = NULL;                 \
  }

namespace gemfire {

enum ConnErrType {
  CONN_NOERR = 0x0,
  CONN_NODATA = 0x1,
  CONN_TIMEOUT = 0x3,
  CONN_IOERR = 0x4,
  CONN_OTHERERR = 0x8
};

enum ServerQueueStatus {
  NON_REDUNDANT_SERVER = 0,
  REDUNDANT_SERVER = 1,
  PRIMARY_SERVER = 2
};

class TcrEndpoint;
class SystemProperties;
class ThinClientPoolDM;
class CPPCACHE_EXPORT TcrConnection {
 public:
  /** Create one connection, endpoint is in format of hostname:portno
  * It will do handshake with j-server. There're 2 types of handshakes:
  * 1) handshake for request
  *    send following bytes:
  *    CLIENT_TO_SERVER
  *    REPLY_OK
  *    2 bytes for the length of idenfifier
  *    a string with "hostname:processId" as identifier
  *
  *    if send succeeds, handshake succeeds. Otherwise, construction
  *    fails.
  *
  * 2) handshake for client notification
  *    send following bytes:
  *    SERVER_TO_CLIENT
  *    1 (4 bytes, we can hard-code)
  *    12345 (4 bytes, we can hard-code)
  *
  *    So the total bytes to send are 9
  *    read one byte from server, it should be CLIENT_TO_SERVER
  *    Otherwise, construction fails.
  * @param     ports     List of local ports for connections to endpoint
  * @param     numPorts  Size of ports list
  */
  bool InitTcrConnection(TcrEndpoint* endpointObj, const char* endpoint,
                         Set<uint16_t>& ports,
                         bool isClientNotification = false,
                         bool isSecondary = false,
                         uint32_t connectTimeout = DEFAULT_CONNECT_TIMEOUT);

  TcrConnection(volatile const bool& isConnected)
      : connectionId(0),
        m_dh(NULL),
        m_endpoint(NULL),
        m_endpointObj(NULL),
        m_connected(isConnected),
        m_conn(NULL),
        m_hasServerQueue(NON_REDUNDANT_SERVER),
        m_queueSize(0),
        m_port(0),
        m_chunksProcessSema(0),
        m_isBeingUsed(false),
        m_isUsed(0),
        m_poolDM(NULL) {}

  /* destroy the connection */
  ~TcrConnection();

  /**
  * send a synchronized request to server.
  *
  * It will send the buffer, then wait to receive 17 bytes and save in
  * msg_header.
  * msg_header[0] is message type.
  * msg_header[1],msg_header[2],msg_header[3],msg_header[4] will be a 4 bytes
  * integer,
  * let's say, msgLen, which specifies the length of next read. byteReads some
  * number of
  * call read again for msgLen bytes, and save the bytes into msg_body.
  * concatenate the msg_header and msg_body into buffer, msg. The msg should be
  * a '0' ended
  * string. i.e. If the msg_header plus msg_body has 100 chars, msg should be a
  * 101 char array
  * to contain the '0' in the end. We need it to get length of the msg.
  * Return the msg.
  *
  * @param      buffer the buffer to send
  * @param      len length of the data to send
  * @param      sendTimeoutSec write timeout in sec
  * @param      recvLen output parameter for length of the received message
  * @param      receiveTimeoutSec read timeout in sec
  * @return     byte arrary of response. '0' ended.
  * @exception  GemfireIOException  if an I/O error occurs (socket failure).
  * @exception  TimeoutException  if timeout happens at any of the 3 socket
  * operation: 1 write, 2 read
  */
  char* sendRequest(const char* buffer, int32_t len, size_t* recvLen,
                    uint32_t sendTimeoutSec = DEFAULT_WRITE_TIMEOUT,
                    uint32_t receiveTimeoutSec = DEFAULT_READ_TIMEOUT_SECS,
                    int32 request = -1);

  /**
  * send a synchronized request to server for REGISTER_INTEREST_LIST.
  *
  * @param      buffer the buffer to send
  *             len length of the data to send
  *             message vector, which will return chunked TcrMessage.
  *             sendTimeoutSec write timeout in sec
  *             receiveTimeoutSec read timeout in sec
  * @exception  GemfireIOException  if an I/O error occurs (socket failure).
  * @exception  TimeoutException  if timeout happens at any of the 3 socket
  * operation: 1 write, 2 read
  */
  void sendRequestForChunkedResponse(
      const TcrMessage& request, int32_t len, TcrMessageReply& message,
      uint32_t sendTimeoutSec = DEFAULT_WRITE_TIMEOUT,
      uint32_t receiveTimeoutSec = DEFAULT_READ_TIMEOUT_SECS);

  /**
  * send an asynchronized request to server. No response is expected.
  * we need to use it to send CLOSE_CONNECTION msg
  *
  * @param      buffer the buffer to send
  *             len length of the data to send
  *             sendTimeoutSec write timeout in sec
  * @return     no return. Because it either succeeds, or throw exception.
  * @exception  GemfireIOException  if an I/O error occurs (socket failure).
  * @exception  TimeoutException  if timeout happens at any of the 3 socket
  * operation: 1 write, 2 read
  */
  void send(const char* buffer, int len,
            uint32_t sendTimeoutSec = DEFAULT_WRITE_TIMEOUT,
            bool checkConnected = true);

  void send(uint32_t& timeSpent, const char* buffer, int len,
            uint32_t sendTimeoutSec = DEFAULT_WRITE_TIMEOUT,
            bool checkConnected = true, int32_t notPublicApiWithTimeout =
                                            -2 /*NOT_PUBLIC_API_WITH_TIMEOUT*/);

  /**
  * This method is for receiving client notification. It will read 2 times as
  * reading reply in sendRequest()
  *
  * @param      recvLen output parameter for length of the received message
  * @param      receiveTimeoutSec read timeout in sec
  * @return     byte arrary of response. '0' ended.
  * @exception  GemfireIOException  if an I/O error occurs (socket failure).
  * @exception  TimeoutException  if timeout happens at any of the 3 socket
  * operation: 1 write, 2 read
  */
  char* receive(size_t* recvLen, ConnErrType* opErr,
                uint32_t receiveTimeoutSec = DEFAULT_READ_TIMEOUT_SECS);

  //  readMessage is now public
  /**
  * This method reads a message from the socket connection and returns the byte
  * array of response.
  * @param      recvLen output parameter for length of the received message
  * @param      receiveTimeoutSec read timeout in seconds
  * @param      doHeaderTimeoutRetries retry when header receive times out
  * @return     byte array of response. '0' ended.
  * @exception  GemfireIOException  if an I/O error occurs (socket failure).
  * @exception  TimeoutException  if timeout happens during read
  */
  char* readMessage(size_t* recvLen, uint32_t receiveTimeoutSec,
                    bool doHeaderTimeoutRetries, ConnErrType* opErr,
                    bool isNotificationMessage = false, int32 request = -1);

  /**
  * This method reads an interest list response  message from the socket
  * connection and sets the reply message
  * parameter.
  * @param      reply response message
  * @param      receiveTimeoutSec read timeout in sec
  * @param      doHeaderTimeoutRetries retry when header receive times out
  * @exception  GemfireIOException  if an I/O error occurs (socket failure).
  * @exception  TimeoutException  if timeout happens during read
  */
  void readMessageChunked(TcrMessageReply& reply, uint32_t receiveTimeoutSec,
                          bool doHeaderTimeoutRetries);

  /**
  * Send close connection message to the server.
  */
  void close();

  //  Durable clients: return true if server has HA queue.
  ServerQueueStatus inline getServerQueueStatus(int32_t& queueSize) {
    queueSize = m_queueSize;
    return m_hasServerQueue;
  }

  uint16 inline getPort() { return m_port; }

  TcrEndpoint* getEndpointObject() const { return m_endpointObj; }
  bool isBeingUsed() { return m_isBeingUsed; }
  bool setAndGetBeingUsed(
      volatile bool isBeingUsed,
      bool forTransaction);  // { m_isBeingUsed = isBeingUsed ;}

  // helpers for pool connection manager
  void touch();
  bool hasExpired(int expiryTime);
  bool isIdle(int idleTime);
  ACE_Time_Value getLastAccessed();
  void updateCreationTime();

  int64_t getConnectionId() {
    LOGDEBUG("TcrConnection::getConnectionId() = %d ", connectionId);
    return connectionId;
  }

  void setConnectionId(int64_t id) {
    LOGDEBUG("Tcrconnection:setConnectionId() = %d ", id);
    connectionId = id;
  }

  CacheableBytesPtr encryptBytes(CacheableBytesPtr data) {
    if (m_dh != NULL)
      return m_dh->encrypt(data);
    else
      return data;
  }

  CacheableBytesPtr decryptBytes(CacheableBytesPtr data) {
    if (m_dh != NULL)
      return m_dh->decrypt(data);
    else
      return data;
  }

 private:
  int64_t connectionId;
  DiffieHellman* m_dh;
  /**
  * To read Intantiator message(which meant for java client), here we are
  * ignoring it
  */
  void readHandshakeInstantiatorMsg(uint32_t connectTimeout);

  /**
   * Packs the override settings bits into bytes - currently a single byte for
   * conflation, remove-unresponsive-client and notify-by-subscription.
   */
  uint8_t getOverrides(SystemProperties* props);

  /**
  * To read the from stream
  */
  int32_t readHandShakeInt(uint32_t connectTimeout);

  /*
  * To read the arraysize
  */
  uint32_t readHandshakeArraySize(uint32_t connectTimeout);

  /*
  * This function reads "numberOfBytes" and ignores it.
  */
  void readHandShakeBytes(int numberOfBytes, uint32_t connectTimeout);

  /** Create a normal or SSL connection */
  Connector* createConnection(const char* ipaddr,
                              uint32_t waitSeconds = DEFAULT_CONNECT_TIMEOUT,
                              int32_t maxBuffSizePool = 0);

  /**
  * Reads bytes from socket and handles error conditions in case of Handshake.
  */
  /* adongre
   * CID 28738: Unsigned compared against 0 (NO_EFFECT)
   * This less-than-zero comparison of an unsigned value is never true.
   * "msgLength < 0U".
   */
  CacheableBytesPtr readHandshakeData(int32_t msgLength,
                                      uint32_t connectTimeout);

  /**
  * Reads raw bytes (without appending NULL terminator) from socket and handles
  * error conditions in case of Handshake.
  */
  /* adongre
   * CID 28739: Unsigned compared against 0 (NO_EFFECT)
   * change the input parameter from unint32_t to int32_t
   * as the comparasion case is valid
   */
  // CacheableBytesPtr readHandshakeRawData( uint32_t msgLength, uint32_t
  // connectTimeout );
  CacheableBytesPtr readHandshakeRawData(int32_t msgLength,
                                         uint32_t connectTimeout);
  /**
  * Reads a string from socket and handles error conditions in case of
  * Handshake.
  */
  CacheableStringPtr readHandshakeString(uint32_t connectTimeout);

  /**
  * Reads a byte array (using initial length) from socket and handles error
  * conditions in case of Handshake.
  */
  CacheableBytesPtr readHandshakeByteArray(uint32_t connectTimeout);

  /**
   * Send data to the connection till sendTimeoutSec
   */
  ConnErrType sendData(const char* buffer, int32_t length,
                       uint32_t sendTimeoutSec, bool checkConnected = true);

  ConnErrType sendData(
      uint32_t& timeSpent, const char* buffer, int32_t length,
      uint32_t sendTimeoutSec, bool checkConnected = true,
      int32_t notPublicApiWithTimeout = -2 /*NOT_PUBLIC_API_WITH_TIMEOUT*/);

  /**
   * Read data from the connection till receiveTimeoutSec
   */
  ConnErrType receiveData(
      char* buffer, int32_t length, uint32_t receiveTimeoutSec,
      bool checkConnected = true, bool isNotificationMessage = false,
      int32_t notPublicApiWithTimeout = -2 /*NOT_PUBLIC_API_WITH_TIMEOUT*/);

  const char* m_endpoint;
  TcrEndpoint* m_endpointObj;
  volatile const bool& m_connected;
  Connector* m_conn;
  ServerQueueStatus m_hasServerQueue;
  int32_t m_queueSize;
  uint16 m_port;

  // semaphore to synchronize with the chunked response processing thread
  ACE_Semaphore m_chunksProcessSema;
  ACE_Time_Value m_creationTime;
  ACE_Time_Value m_lastAccessed;
  // Disallow copy constructor and assignment operator.
  TcrConnection(const TcrConnection&);
  TcrConnection& operator=(const TcrConnection&);
  volatile bool m_isBeingUsed;
  volatile uint32_t m_isUsed;
  ThinClientPoolDM* m_poolDM;
};
}

#endif  // __TCR_CONNECTION_HPP__
