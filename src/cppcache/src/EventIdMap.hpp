/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __EVENT_ID_MAP_HPP__
#define __EVENT_ID_MAP_HPP__

#include <ace/ACE.h>
#include <ace/Time_Value.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Guard_T.h>

#include "EventId.hpp"
#include <gfcpp/HashMapT.hpp>
#include <gfcpp/SharedPtr.hpp>

#include <vector>
#include <utility>

namespace gemfire {

class EventSource;
class EventSequence;
class EventIdMap;

typedef SharedPtr<EventSource> EventSourcePtr;
typedef SharedPtr<EventSequence> EventSequencePtr;
typedef SharedPtr<EventIdMap> EventIdMapPtr;

typedef HashMapT<EventSourcePtr, EventSequencePtr> EventIdMapType;

typedef std::pair<EventSourcePtr, EventSequencePtr> EventIdMapEntry;
typedef std::vector<EventIdMapEntry> EventIdMapEntryList;

typedef ACE_Guard<ACE_Recursive_Thread_Mutex> MapGuard;

#define GUARD_MAP MapGuard mapguard(m_lock)

/** @class EventIdMap EventIdMap.hpp
 *
 * This is the class that encapsulates a HashMap and
 * provides the operations for duplicate checking and
 * expiry of idle event IDs from notifications.
 */
class CPPCACHE_EXPORT EventIdMap : public SharedBase {
  int32_t m_expiry;
  EventIdMapType m_map;
  ACE_Recursive_Thread_Mutex m_lock;

  // hidden
  EventIdMap(const EventIdMap &);
  EventIdMap &operator=(const EventIdMap &);

 public:
  EventIdMap()
      : /* adongre
         * CID 28935: Uninitialized scalar field (UNINIT_CTOR)
         */
        m_expiry(0){};

  void clear();

  /** Initialize with preset expiration time in seconds */
  void init(int32_t expirySecs);

  ~EventIdMap();

  /** Find out if entry is duplicate
   * @return true if the entry exists else false
   */
  bool isDuplicate(EventSourcePtr key, EventSequencePtr value);

  /** Construct an EventIdMapEntry from an EventIdPtr */
  static EventIdMapEntry make(EventIdPtr eventid);

  /** Put an item and return true if it is new or false if it existed and was
   * updated
   * @param onlynew Only put if the sequence id does not exist or is higher
   * @return true if the entry was updated or inserted otherwise false
   */
  bool put(EventSourcePtr key, EventSequencePtr value, bool onlynew = false);

  /** Update the deadline for the entry
   * @return true if the entry exists else false
   */
  bool touch(EventSourcePtr key);

  /** Remove an item from the map
   *  @return true if the entry was found and removed else return false
   */
  bool remove(EventSourcePtr key);

  /** Collect all map entries who acked flag is false and set their acked flags
   * to true */
  EventIdMapEntryList getUnAcked();

  /** Clear all acked flags in the list and return the number of entries cleared
   * @param entries List of entries whos flags are to be cleared
   * @return The number of entries whos flags were cleared
   */
  uint32_t clearAckedFlags(EventIdMapEntryList &entries);

  /** Remove entries whos deadlines have passed and return the number of entries
   * removed
   * @param onlyacked Either check only entries whos acked flag is true
   * otherwise check all entries
   * @return The number of entries removed
   */
  uint32_t expire(bool onlyacked);
};

/** @class EventSource
 *
 * EventSource is the combination of MembershipId and ThreadId from the EventId
 */
class CPPCACHE_EXPORT EventSource : public SharedBase {
  char *m_srcId;
  int32_t m_srcIdLen;
  int64_t m_thrId;

  uint32_t m_hash;

  void init();

  // hide copy ctor and assignment operator
  EventSource();
  EventSource(const EventSource &);
  EventSource &operator=(const EventSource &);

 public:
  void clear();

  EventSource(const char *memId, int32_t memIdLen, int64_t thrId);
  ~EventSource();

  uint32_t hashcode();
  bool operator==(const EventSource &rhs) const;

  // Accessors

  char *getSrcId();
  int32_t getSrcIdLen();
  char *getMemId();
  int32_t getMemIdLen();
  int64_t getThrId();
};

/** @class EventSequence
 *
 * EventSequence is the combination of SequenceNum from EventId, a timestamp and
 * a flag indicating whether or not it is ACKed
 */
class CPPCACHE_EXPORT EventSequence : public SharedBase {
  int64_t m_seqNum;
  bool m_acked;
  ACE_Time_Value m_deadline;  // current time plus the expiration delay (age)

  void init();

 public:
  void clear();

  EventSequence();
  EventSequence(int64_t seqNum);
  ~EventSequence();

  void touch(int32_t ageSecs);  // update deadline
  void touch(
      int64_t seqNum,
      int32_t ageSecs);  // update deadline, clear acked flag and set seqNum

  // Accessors:

  int64_t getSeqNum();
  void setSeqNum(int64_t seqNum);

  bool getAcked();
  void setAcked(bool acked);

  ACE_Time_Value getDeadline();
  void setDeadline(ACE_Time_Value deadline);

  bool operator<=(const EventSequence &rhs) const;
};

};  // namespace gemfire;

#endif  // __EVENT_ID_MAP_HPP__
