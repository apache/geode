/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/DataInput.hpp>
#include "EventId.hpp"
#include "GemfireTypeIdsImpl.hpp"
#include "ClientProxyMembershipID.hpp"
#include <ace/TSS_T.h>

#include <ace/OS.h>
#include <ace/INET_Addr.h>

namespace gemfire {

// to be used only with ACE_TSS<> or WinTSS<>
class EventIdTSS {
 private:
  static SpinLock s_eidThrIdLock;
  static int64_t s_eidThrId;

  int64_t m_eidThrTSS;
  int64_t m_eidSeqTSS;

 public:
  // this should get called just once per thread due to first access to TSS
  EventIdTSS() {
    {
      SpinLockGuard _guard(s_eidThrIdLock);
      m_eidThrTSS = ++s_eidThrId;
    }
    m_eidSeqTSS = 0;
  }

  inline int64_t getEidThr() { return m_eidThrTSS; }

  inline int64_t getAndIncEidSeq() { return m_eidSeqTSS++; }

  inline int64_t getSeqNum() { return m_eidSeqTSS - 1; }

  static ACE_TSS<EventIdTSS> s_eventId;

};  // class EventIdTSS

SpinLock EventIdTSS::s_eidThrIdLock;
int64_t EventIdTSS::s_eidThrId = 0;
ACE_TSS<EventIdTSS> EventIdTSS::s_eventId;

void EventId::toData(DataOutput& output) const {
  //  This method is always expected to write out nonstatic distributed
  //  memberid.
  output.writeBytes(reinterpret_cast<const int8_t*>(m_eidMem), m_eidMemLen);
  output.writeArrayLen(18);
  char longCode = 3;
  output.write(static_cast<uint8_t>(longCode));
  output.writeInt(m_eidThr);
  output.write(static_cast<uint8_t>(longCode));
  output.writeInt(m_eidSeq);
  output.writeInt(m_bucketId);
  output.write(m_breadcrumbCounter);
}

Serializable* EventId::fromData(DataInput& input) {
  // TODO: statics being assigned; not thread-safe??
  input.readArrayLen(&m_eidMemLen);
  input.readBytesOnly(reinterpret_cast<int8_t*>(m_eidMem), m_eidMemLen);
  int32_t arrayLen;
  input.readArrayLen(&arrayLen);
  char numberCode = 0;
  input.read(reinterpret_cast<uint8_t*>(&numberCode));
  m_eidThr = getEventIdData(input, numberCode);
  input.read(reinterpret_cast<uint8_t*>(&numberCode));
  m_eidSeq = getEventIdData(input, numberCode);
  input.readInt(&m_bucketId);
  input.read(&m_breadcrumbCounter);
  return this;
}

const char* EventId::getMemId() const { return m_eidMem; }

int32_t EventId::getMemIdLen() const { return m_eidMemLen; }

int64_t EventId::getThrId() const { return m_eidThr; }

int64_t EventId::getSeqNum() const { return m_eidSeq; }

int64_t EventId::getEventIdData(DataInput& input, char numberCode) {
  int64_t retVal = 0;

  //  Read number based on numeric code written by java server.
  if (numberCode == 0) {
    int8_t byteVal;
    input.read(&byteVal);
    retVal = byteVal;
  } else if (numberCode == 1) {
    int16_t shortVal;
    input.readInt(&shortVal);
    retVal = shortVal;
  } else if (numberCode == 2) {
    int32_t intVal;
    input.readInt(&intVal);
    retVal = intVal;
  } else if (numberCode == 3) {
    int64_t longVal;
    input.readInt(&longVal);
    retVal = longVal;
  }

  return retVal;
}

Serializable* EventId::createDeserializable() {
  return new EventId(false);  // use false since we dont want to inc sequence
                              // (for de-serialization)
}

int32_t EventId::classId() const { return 0; }

int8_t EventId::typeId() const { return GemfireTypeIdsImpl::EventId; }

EventId::EventId(char* memId, uint32_t memIdLen, int64_t thr, int64_t seq) {
  // TODO: statics being assigned; not thread-safe??
  ACE_OS::memcpy(m_eidMem, memId, memIdLen);
  m_eidMemLen = memIdLen;
  m_eidThr = thr;
  m_eidSeq = seq;
  m_bucketId = -1;
  m_breadcrumbCounter = 0;
}

EventId::EventId(bool doInit, uint32_t reserveSize,
                 bool fullValueAfterDeltaFail)
    : /* adongre
       * CID 28934: Uninitialized scalar field (UNINIT_CTOR)
       */
      m_eidMemLen(0),
      m_eidThr(0),
      m_eidSeq(0),
      m_bucketId(-1),
      m_breadcrumbCounter(0) {
  if (!doInit) return;

  if (fullValueAfterDeltaFail) {
    /// need to send old sequence id
    initFromTSS_SameThreadIdAndSameSequenceId();
  } else {
    initFromTSS();
  }

  for (uint32_t i = 0; i < reserveSize; i++) {
    EventIdTSS::s_eventId->getAndIncEidSeq();
  }
}

void EventId::initFromTSS() {
  m_eidThr = EventIdTSS::s_eventId->getEidThr();
  m_eidSeq = EventIdTSS::s_eventId->getAndIncEidSeq();
}

void EventId::initFromTSS_SameThreadIdAndSameSequenceId() {
  m_eidThr = EventIdTSS::s_eventId->getEidThr();
  m_eidSeq = EventIdTSS::s_eventId->getSeqNum();
}

EventId::~EventId() {}

/** used to render as a string for logging. */
size_t EventId::logString(char* buffer, size_t maxLength) const {
  return ACE_OS::snprintf(buffer, maxLength,
                          "EventId( memID=[binary], thr=%" PRIi64
                          ", seq=%" PRIi64 " )",
                          m_eidThr, m_eidSeq);
}
}  // namespace gemfire
