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
/*
 * FarSideEntryOp.cpp
 *
 *  Created on: 22-Feb-2011
 *      Author: ankurs
 */

#include "FarSideEntryOp.hpp"
#include "RegionCommit.hpp"
#include "ClientProxyMembershipID.hpp"
#include "DiskVersionTag.hpp"

namespace apache {
namespace geode {
namespace client {

FarSideEntryOp::FarSideEntryOp(RegionCommit* region)
    :  // UNUSED m_region(region),
       /* adongre
        *
        */
      m_op(0),
      m_modSerialNum(0),
      m_eventOffset(0),
      m_didDestroy(false)

{}

FarSideEntryOp::~FarSideEntryOp() {}

bool FarSideEntryOp::isDestroy(int8_t op) {
  return op == DESTROY || op == LOCAL_DESTROY || op == EVICT_DESTROY ||
         op == EXPIRE_DESTROY || op == EXPIRE_LOCAL_DESTROY || op == REMOVE;
}

bool FarSideEntryOp::isInvalidate(int8_t op) {
  return op == INVALIDATE || op == LOCAL_INVALIDATE ||
         op == EXPIRE_INVALIDATE || op == EXPIRE_LOCAL_INVALIDATE;
}

void FarSideEntryOp::fromData(DataInput& input, bool largeModCount,
                              uint16_t memId) {
  input.readObject(m_key);
  input.read(&m_op);
  if (largeModCount) {
    input.readInt(&m_modSerialNum);
  } else {
    int8_t modSerialNum;
    input.read(&modSerialNum);
    m_modSerialNum = modSerialNum;
  }
  uint8_t firstByte;
  input.read(&firstByte);
  if (firstByte != GemfireTypeIds::NullObj) {
    input.rewindCursor(1);
    input.readObject(m_callbackArg);
  }

  skipFilterRoutingInfo(input);
  m_versionTag = TcrMessage::readVersionTagPart(input, memId);
  // SerializablePtr sPtr;
  // input.readObject(sPtr);
  input.readInt(&m_eventOffset);
  if (!isDestroy(m_op)) {
    input.readBoolean(&m_didDestroy);
    if (!isInvalidate(m_op)) {
      bool isToken;
      input.readBoolean(&isToken);
      if (isToken) {
        int8_t objType;
        int32_t rewind = 1;
        int16_t fixedId = 0;
        input.read(&objType);
        if (objType == GemfireTypeIdsImpl::FixedIDShort) {
          input.readInt(&fixedId);
          rewind += 2;
        }

        //			  public static final short TOKEN_INVALID = 141;
        //			  public static final short TOKEN_LOCAL_INVALID
        //=
        // 142;
        //			  public static final short TOKEN_DESTROYED =
        // 143;
        //			  public static final short TOKEN_REMOVED = 144;
        //			  public static final short TOKEN_REMOVED2 =
        // 145;
        if (fixedId >= 141 && fixedId < 146) {
          m_value = NULLPTR;
        } else {
          input.rewindCursor(rewind);
          input.readObject(m_value);
        }
      } else {
        // uint8_t* buf = NULL;
        int32_t len;
        input.readArrayLen(&len);
        input.readObject(m_value);

        // input.readBytes(&buf, &len);
        // m_value = CacheableBytes::create(buf, len);
      }
    }
  }
}

void FarSideEntryOp::apply(RegionPtr& region) {
  // LocalRegion* localRegion = static_cast<LocalRegion*>(region.ptr());
  // localRegion->acquireReadLock();

  RegionInternalPtr ri = region;
  if (isDestroy(m_op)) {
    ri->txDestroy(m_key, m_callbackArg, m_versionTag);
  } else if (isInvalidate(m_op)) {
    ri->txInvalidate(m_key, m_callbackArg, m_versionTag);
  } else {
    ri->txPut(m_key, m_value, m_callbackArg, m_versionTag);
  }
}

void FarSideEntryOp::skipFilterRoutingInfo(DataInput& input) {
  int8_t structType;
  uint8_t classByte;
  CacheablePtr tmp;
  input.read(&structType);  // this is DataSerializable (45)

  if (structType == GemfireTypeIds::NullObj) {
    return;
  } else if (structType == GemfireTypeIdsImpl::DataSerializable) {
    input.read(&classByte);
    input.readObject(tmp);
    int32_t size;
    input.readInt(&size);
    for (int i = 0; i < size; i++) {
      ClientProxyMembershipID memId;
      // memId.fromData(input);
      memId.readEssentialData(input);

      int32_t len;
      input.readArrayLen(&len);

      /*
                        for(int j = 0; j < len; j++)
                        {
                                input.readObject(tmp);
                                input.readObject(tmp);
                        }
      */

      bool hasCQs;
      input.readBoolean(&hasCQs);

      if (hasCQs) {
        input.readArrayLen(&len);
        for (int j = 0; j < len; j++) {
          int64_t ignore;
          input.readUnsignedVL(&ignore);
          input.readUnsignedVL(&ignore);
        }
      }

      input.readInt(&len);
      if (len != -1) {
        bool isLong;
        input.readBoolean(&isLong);

        for (int j = 0; j < len; j++) {
          if (isLong) {
            int64_t val;
            input.readInt(&val);
          } else {
            int32_t val;
            input.readInt(&val);
          }
        }
      }

      input.readInt(&len);
      if (len != -1) {
        bool isLong;
        input.readBoolean(&isLong);

        for (int j = 0; j < len; j++) {
          if (isLong) {
            int64_t val;
            input.readInt(&val);
          } else {
            int32_t val;
            input.readInt(&val);
          }
        }
      }
    }
  } else {
    LOGERROR(
        "FarSideEntryOp::skipFilterRoutingInfo Unexpected type id: %d while "
        "desirializing commit response",
        structType);
    GfErrTypeThrowException(
        "FarSideEntryOp::skipFilterRoutingInfo Unable to handle commit "
        "response",
        GF_CACHE_ILLEGAL_STATE_EXCEPTION);
  }
}
/*
EntryEventPtr FarSideEntryOp::getEntryEvent(Cache* cache)
{
        return EntryEventPtr(new EntryEvent(
                        m_region->getRegion(cache),
                        m_key,
                        NULLPTR,
                        m_value,
                        m_callbackArg,
                        false));
}
*/
}  // namespace client
}  // namespace geode
}  // namespace apache
