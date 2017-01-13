/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __UDPIpc_hpp__
#define __UDPIpc_hpp__

#include "Service.hpp"
#include "PerfFwk.hpp"
#include "FwkLog.hpp"
#include "GsRandom.hpp"

#include <ace/Task.h>
#include <ace/Thread_Mutex.h>
#include <ace/INET_Addr.h>

#include <ace/SOCK_Dgram.h>
#include <ace/TSS_T.h>

#include <string>
#include <list>

#ifdef _WIN32

#define popen _popen
#define pclose _pclose
#define MODE "wt"

#else  // linux, et. al.

#include <unistd.h>
#define MODE "w"

#endif  // WIN32

namespace gemfire {
namespace testframework {

#define UDP_HEADER_SIZE 8
#define UDP_MSG_TAG (uint8_t)189

enum UdpCmds { Null, ACK, ACK_REQUEST, ADDR_REQUEST, ADDR_RESPONSE };

typedef struct {
  uint8_t tag;
  uint8_t cmd;
  uint16_t id;
  uint32_t length;
} UdpHeader;

class UDPMessage : public IPCMessage {
 protected:
  ACE_INET_Addr m_sender;
  UdpHeader m_hdr;

 public:
  UDPMessage() {
    clearHdr();
    m_msg.clear();
  }

  UDPMessage(UdpCmds cmd) {
    clearHdr();
    m_msg.clear();
    setCmd(cmd);
  }

  UDPMessage(std::string content) : IPCMessage(content) { clearHdr(); }

  UDPMessage(UDPMessage& msg) : IPCMessage(msg.getMessage()) {
    clearHdr();
    setCmd(msg.getCmd());
  }

  virtual ~UDPMessage() {}

  void setCmd(UdpCmds cmd) { m_hdr.cmd = cmd; }

  UdpCmds getCmd() { return (UdpCmds)m_hdr.cmd; }

  ACE_INET_Addr& getSender() { return m_sender; }

  void setSender(ACE_INET_Addr& addr) { m_sender = addr; }

  bool receiveFrom(ACE_SOCK_Dgram& io, const ACE_Time_Value* timeout = NULL);

  bool sendTo(ACE_SOCK_Dgram& io, ACE_INET_Addr& who);

  bool ping(ACE_SOCK_Dgram& io, ACE_INET_Addr& who);

  bool send(ACE_SOCK_Dgram& io);

  std::string dump(int32_t max = 0);

  bool needToAck() { return (m_hdr.cmd == ACK_REQUEST); }

  const char* cmdString(uint32_t cmd) {
    const char* UdpStrings[] = {"Null", "ACK", "ACK_REQUEST", "ADDR_REQUEST",
                                "ADDR_RESPONSE"};
    if (cmd > 4) return "UNKNOWN";
    return UdpStrings[cmd];
  }

  void clearHdr() {
    m_hdr.tag = UDP_MSG_TAG;
    m_hdr.cmd = 0;
    m_hdr.id = 0;
    m_hdr.length = 0;
  }

  virtual void clear() {
    clearHdr();
    m_msg.clear();
  }
};

class UDPMessageClient {
 private:
  ACE_INET_Addr m_server;
  ACE_SOCK_Dgram m_io;

 public:
  UDPMessageClient(std::string server);

  ~UDPMessageClient() { m_io.close(); }

  ACE_SOCK_Dgram& getConn() { return m_io; }

  ACE_INET_Addr& getServer() { return m_server; }
};

class UDPMessageQueues : public SharedTaskObject {
 private:
  AtomicInc m_cntInbound;
  AtomicInc m_cntOutbound;
  AtomicInc m_cntProcessed;

  SafeQueue<UDPMessage> m_inbound;
  SafeQueue<UDPMessage> m_outbound;

  std::string m_label;

 public:
  UDPMessageQueues(std::string label) : m_label(label) {}
  ~UDPMessageQueues() {
    FWKINFO(m_label << "MessageQueues::Inbound   count: "
                    << m_cntInbound.value());
    FWKINFO(m_label << "MessageQueues::Processed count: "
                    << m_cntProcessed.value());
    FWKINFO(m_label << "MessageQueues::Outbound  count: "
                    << m_cntOutbound.value());
    FWKINFO(m_label << "MessageQueues::Inbound  still queued: "
                    << m_inbound.size());
    FWKINFO(m_label << "MessageQueues::Outbound still queued: "
                    << m_outbound.size());
  }

  void putInbound(UDPMessage* msg) {
    m_inbound.enqueue(msg);
    m_cntInbound++;
  }

  void putOutbound(UDPMessage* msg) {
    m_outbound.enqueue(msg);
    m_cntProcessed++;
  }

  UDPMessage* getInbound() { return m_inbound.dequeue(); }

  UDPMessage* getOutbound() {
    UDPMessage* msg = m_outbound.dequeue();
    if (msg != NULL) m_cntOutbound++;
    return msg;
  }

  virtual void initialize() {}
  virtual void finalize() {}
};

class Receiver : public ServiceTask {
 private:
  ACE_TSS<ACE_SOCK_Dgram> m_io;
  int32_t m_basePort;
  ACE_thread_t m_listener;
  AtomicInc m_offset;
  std::list<std::string> m_addrs;
  UDPMessageQueues* m_queues;
  ACE_Thread_Mutex m_mutex;

 public:
  Receiver(UDPMessageQueues* shared, int32_t port)
      : ServiceTask(shared), m_basePort(port), m_listener(0), m_mutex() {
    m_queues = dynamic_cast<UDPMessageQueues*>(m_shared);
  }

  virtual ~Receiver() {}

  bool isListener() { return (m_listener == ACE_Thread::self()); }

  int32_t doTask();

  void initialize();

  void finalize() { m_io->close(); }
};

class STReceiver : public ServiceTask {
 private:
  ACE_SOCK_Dgram m_io;
  int32_t m_basePort;
  UDPMessageQueues* m_queues;
  std::string m_addr;

 public:
  STReceiver(UDPMessageQueues* shared, int32_t port)
      : ServiceTask(shared), m_basePort(port) {
    m_queues = dynamic_cast<UDPMessageQueues*>(m_shared);
  }

  virtual ~STReceiver() {}

  int32_t doTask();

  void initialize();

  void finalize() { m_io.close(); }
};

class Processor : public ServiceTask {
 private:
  UDPMessageQueues* m_queues;
  // UNUSED bool m_sendReply;

 public:
  Processor(UDPMessageQueues* shared, bool sendReply = false)
      : ServiceTask(shared) /* UNUSED , m_sendReply( sendReply )*/ {
    m_queues = dynamic_cast<UDPMessageQueues*>(m_shared);
  }

  virtual ~Processor() {}

  int32_t doTask() {
    while (*m_run) {
      UDPMessage* msg = m_queues->getInbound();
      if (msg != NULL) {
        m_queues->putOutbound(msg);
      }
    }
    return 0;
  }
  void initialize() {}
  void finalize() {}
};

class Responder : public ServiceTask {
 private:
  ACE_TSS<ACE_SOCK_Dgram> m_io;
  int32_t m_basePort;
  AtomicInc m_offset;
  UDPMessageQueues* m_queues;

 public:
  Responder(UDPMessageQueues* shared, int32_t port)
      : ServiceTask(shared), m_basePort(port) {
    m_queues = dynamic_cast<UDPMessageQueues*>(m_shared);
  }

  virtual ~Responder() {}

  int32_t doTask();

  void initialize();

  void finalize() { m_io->close(); }
};
}  // testframework
}  // gemfire

#endif  // __UDPIpc_hpp__
