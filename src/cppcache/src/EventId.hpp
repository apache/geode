#ifndef _GEMFIRE_EVENTID_HPP_
#define _GEMFIRE_EVENTID_HPP_
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/Cacheable.hpp>
#include "GemfireTypeIdsImpl.hpp"
#include "SpinLock.hpp"
#include <gfcpp/DataOutput.hpp>

#include <string>

/** @file
*/

namespace gemfire {

/**
 * EventID "token" with a Distributed Member ID, Thread ID and per-thread
 * Sequence ID
 */
class CPPCACHE_EXPORT EventId : public Cacheable {
 private:
  char m_eidMem[512];
  int32_t m_eidMemLen;
  int64_t m_eidThr;
  int64_t m_eidSeq;
  int32_t m_bucketId;
  int8_t m_breadcrumbCounter;

 public:
  /**
   *@brief Accessor methods
   **/
  const char* getMemId() const;
  int32_t getMemIdLen() const;
  int64_t getThrId() const;
  int64_t getSeqNum() const;

  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const;

  /**
   *@brief deserialize this object
   **/
  virtual Serializable* fromData(DataInput& input);

  virtual uint32_t objectSize() const {
    uint32_t objectSize = 0;
    objectSize += sizeof(uint8_t);
    objectSize += sizeof(int64_t);
    objectSize += sizeof(uint8_t);
    objectSize += sizeof(int64_t);
    objectSize += sizeof(int32_t);  // bucketID
    objectSize += sizeof(int8_t);   // breadCrumbCounter
    return objectSize;
  }

  /**
   * @brief creation function for strings.
   */
  static Serializable* createDeserializable();

  /**
   *@brief return the classId of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and derserialize into.
   */
  virtual int32_t classId() const;

  /**
   *@brief return the typeId of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and derserialize into.
   */
  virtual int8_t typeId() const;

  /**
   * Internal Data Serializable Fixed ID size type - since GFE 5.7
   */
  virtual int8_t DSFID() const { return GemfireTypeIdsImpl::FixedIDByte; };

  /** Returns a pointer to a new eventid value. */
  static EventIdPtr create(char* memId, uint32_t memIdLen, int64_t thr,
                           int64_t seq) {
    return EventIdPtr(new EventId(memId, memIdLen, thr, seq));
  }

  /** Destructor. */
  virtual ~EventId();

  /** used to render as a string for logging. */
  virtual size_t logString(char* buffer, size_t maxLength) const;

  int64_t getEventIdData(DataInput& input, char numberCode);

  inline void writeIdsData(DataOutput& output) {
    //  Write EventId threadid and seqno.
    int idsBufferLength = 18;
    output.writeInt(idsBufferLength);
    output.write((uint8_t)0);
    char longCode = 3;
    output.write((uint8_t)longCode);
    output.writeInt(m_eidThr);
    output.write((uint8_t)longCode);
    output.writeInt(m_eidSeq);
  }

  /** Constructor, given the values. */
  EventId(char* memId, uint32_t memIdLen, int64_t thr, int64_t seq);
  /** Constructor, used for deserialization. */
  EventId(bool doInit = true, uint32_t reserveSize = 0,
          bool fullValueAfterDeltaFail = false);  // set init=false if we dont
                                                  // want to inc sequence (for
                                                  // de-serialization)

 protected:
  void initFromTSS();  // init from TSS and increment per-thread sequence number
  void initFromTSS_SameThreadIdAndSameSequenceId();  // for after delta message
                                                     // fail, and server is
                                                     // looking for full value.
                                                     // So eventiD should be
                                                     // same

 private:
  // never implemented.
  void operator=(const EventId& other);
  EventId(const EventId& other);
};
}

#endif  //_GEMFIRE_EVENTID_HPP_
