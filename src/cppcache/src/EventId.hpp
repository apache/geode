#pragma once

#ifndef GEODE_EVENTID_H_
#define GEODE_EVENTID_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/Cacheable.hpp>
#include "GeodeTypeIdsImpl.hpp"
#include "SpinLock.hpp"
#include <gfcpp/DataOutput.hpp>

#include <string>

/** @file
*/

namespace apache {
namespace geode {
namespace client {

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
  virtual int8_t DSFID() const { return GeodeTypeIdsImpl::FixedIDByte; };

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
    output.write(static_cast<uint8_t>(0));
    char longCode = 3;
    output.write(static_cast<uint8_t>(longCode));
    output.writeInt(m_eidThr);
    output.write(static_cast<uint8_t>(longCode));
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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_EVENTID_H_
