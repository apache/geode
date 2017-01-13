/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "EventIdMap.hpp"

using namespace gemfire;

EventIdMap::~EventIdMap() { clear(); }

void EventIdMap::init(int32_t expirySecs) { m_expiry = expirySecs; }

void EventIdMap::clear() {
  GUARD_MAP;
  m_map.clear();
}

EventIdMapEntry EventIdMap::make(EventIdPtr eventid) {
  EventSourcePtr sid(new EventSource(
      eventid->getMemId(), eventid->getMemIdLen(), eventid->getThrId()));
  EventSequencePtr seq(new EventSequence(eventid->getSeqNum()));
  return std::make_pair(sid, seq);
}

bool EventIdMap::isDuplicate(EventSourcePtr key, EventSequencePtr value) {
  GUARD_MAP;
  EventIdMapType::Iterator entry = m_map.find(key);

  if (entry != m_map.end() && ((*value.ptr()) <= (*entry.second().ptr()))) {
    return true;
  }
  return false;
}

bool EventIdMap::put(EventSourcePtr key, EventSequencePtr value, bool onlynew) {
  GUARD_MAP;

  value->touch(m_expiry);

  EventIdMapType::Iterator entry = m_map.find(key);

  if (entry != m_map.end()) {
    if (onlynew && ((*value.ptr()) <= (*entry.second().ptr()))) {
      return false;
    } else {
      m_map.update(key, value);
      return true;
    }
  } else {
    m_map.insert(key, value);
    return true;
  }
}

bool EventIdMap::touch(EventSourcePtr key) {
  GUARD_MAP;

  EventIdMapType::Iterator entry = m_map.find(key);

  if (entry != m_map.end()) {
    entry.second()->touch(m_expiry);
    return true;
  } else {
    return false;
  }
}

bool EventIdMap::remove(EventSourcePtr key) {
  GUARD_MAP;

  EventIdMapType::Iterator entry = m_map.find(key);

  if (entry != m_map.end()) {
    m_map.erase(key);
    return true;
  } else {
    return false;
  }
}

// side-effect: sets acked flags to true
EventIdMapEntryList EventIdMap::getUnAcked() {
  GUARD_MAP;

  EventIdMapEntryList entries;

  for (EventIdMapType::Iterator entry = m_map.begin(); entry != m_map.end();
       entry++) {
    if (entry.second()->getAcked()) {
      continue;
    }

    entry.second()->setAcked(true);
    entries.push_back(std::make_pair(entry.first(), entry.second()));
  }

  return entries;
}

uint32_t EventIdMap::clearAckedFlags(EventIdMapEntryList& entries) {
  GUARD_MAP;

  uint32_t cleared = 0;

  for (EventIdMapEntryList::iterator item = entries.begin();
       item != entries.end(); item++) {
    EventIdMapType::Iterator entry = m_map.find((*item).first);

    if (entry != m_map.end()) {
      entry.second()->setAcked(false);
      cleared++;
    }
  }

  return cleared;
}

uint32_t EventIdMap::expire(bool onlyacked) {
  GUARD_MAP;

  uint32_t expired = 0;

  EventIdMapEntryList entries;

  ACE_Time_Value current = ACE_OS::gettimeofday();

  for (EventIdMapType::Iterator entry = m_map.begin(); entry != m_map.end();
       entry++) {
    if (onlyacked && !entry.second()->getAcked()) {
      continue;
    }

    if (entry.second()->getDeadline() < current) {
      entries.push_back(std::make_pair(entry.first(), entry.second()));
    }
  }

  for (EventIdMapEntryList::iterator expiry = entries.begin();
       expiry != entries.end(); expiry++) {
    m_map.erase((*expiry).first);
    expired++;
  }

  return expired;
}

EventSource::EventSource(const char* memId, int32_t memIdLen, int64_t thrId) {
  init();

  if (memId == NULL || memIdLen <= 0) {
    return;
  }

  m_thrId = thrId;

  m_srcIdLen = memIdLen + sizeof(thrId);  // 8; // sizeof(thrId or int64_t);
  m_srcId = new char[m_srcIdLen];
  memcpy(m_srcId, memId, memIdLen);

  // convert the int64 thrId to a byte-array and place at the end of m_srcId
  memcpy(m_srcId + memIdLen, &thrId, sizeof(thrId));
}

EventSource::~EventSource() { clear(); }

void EventSource::init() {
  m_srcId = NULL;
  m_srcIdLen = 0;
  m_hash = 0;
  m_thrId = -1;
}

void EventSource::clear() {
  delete[] m_srcId;
  init();
}

char* EventSource::getSrcId() { return m_srcId; }

int32_t EventSource::getSrcIdLen() { return m_srcIdLen; }

char* EventSource::getMemId() { return m_srcId; }

int32_t EventSource::getMemIdLen() { return m_srcIdLen - sizeof(m_thrId); }

int64_t EventSource::getThrId() { return m_thrId; }

uint32_t EventSource::hashcode() {
  if (m_srcId == NULL || m_srcIdLen <= 0) {
    return 0;
  }

  if (m_hash == 0) {
    m_hash = ACE::hash_pjw(m_srcId, m_srcIdLen);
  }

  return m_hash;
}

bool EventSource::operator==(const EventSource& rhs) const {
  if (this->m_srcId == NULL || (&rhs)->m_srcId == NULL ||
      this->m_srcIdLen != (&rhs)->m_srcIdLen) {
    return false;
  }

  return memcmp(this->m_srcId, (&rhs)->m_srcId, this->m_srcIdLen) == 0;
}

void EventSequence::init() {
  m_seqNum = -1;
  m_acked = false;
  m_deadline = ACE_OS::gettimeofday();
}

void EventSequence::clear() { init(); }

EventSequence::EventSequence() { init(); }

EventSequence::EventSequence(int64_t seqNum) {
  init();
  m_seqNum = seqNum;
}

EventSequence::~EventSequence() { clear(); }

void EventSequence::touch(int32_t ageSecs) {
  m_deadline = ACE_OS::gettimeofday();
  m_deadline += ageSecs;
}

void EventSequence::touch(int64_t seqNum, int32_t ageSecs) {
  touch(ageSecs);
  m_seqNum = seqNum;
  m_acked = false;
}

int64_t EventSequence::getSeqNum() { return m_seqNum; }

void EventSequence::setSeqNum(int64_t seqNum) { m_seqNum = seqNum; }

bool EventSequence::getAcked() { return m_acked; }

void EventSequence::setAcked(bool acked) { m_acked = acked; }

ACE_Time_Value EventSequence::getDeadline() { return m_deadline; }

void EventSequence::setDeadline(ACE_Time_Value deadline) {
  m_deadline = deadline;
}

bool EventSequence::operator<=(const EventSequence& rhs) const {
  return this->m_seqNum <= (&rhs)->m_seqNum;
}
