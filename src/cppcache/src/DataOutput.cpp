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
#include <gfcpp/Log.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/SystemProperties.hpp>
#include <SerializationRegistry.hpp>
#include <ace/TSS_T.h>

#include <ace/Recursive_Thread_Mutex.h>
#include <vector>

namespace apache {
namespace geode {
namespace client {

ACE_Recursive_Thread_Mutex g_bigBufferLock;
uint32_t DataOutput::m_highWaterMark = 50 * 1024 * 1024;
uint32_t DataOutput::m_lowWaterMark = 8192;

/** This represents a allocation in this thread local pool. */
class BufferDesc {
 public:
  uint8_t* m_buf;
  uint32_t m_size;

  BufferDesc(uint8_t* buf, uint32_t size) : m_buf(buf), m_size(size) {}

  BufferDesc() : m_buf(NULL), m_size(0) {}

  ~BufferDesc() {}

  BufferDesc& operator=(const BufferDesc& other) {
    /* adongre
     * CID 28889: Other violation (SELF_ASSIGN)No protection against the object
     * assigning to itself.
     */
    if (this != &other) {
      m_buf = other.m_buf;
      m_size = other.m_size;
    }
    return *this;
  }

  BufferDesc(const BufferDesc& other)
      : m_buf(other.m_buf), m_size(other.m_size) {}
};

/** Thread local pool of buffers for DataOutput objects. */
class TSSDataOutput {
 private:
  std::vector<BufferDesc> m_buffers;

 public:
  TSSDataOutput();
  ~TSSDataOutput();

  uint8_t* getBuffer(uint32_t* size) {
    if (!m_buffers.empty()) {
      BufferDesc desc = m_buffers.back();
      m_buffers.pop_back();
      *size = desc.m_size;
      return desc.m_buf;
    } else {
      uint8_t* buf;
      *size = 8192;
      GF_ALLOC(buf, uint8_t, 8192);
      return buf;
    }
  }

  void poolBuffer(uint8_t* buf, uint32_t size) {
    BufferDesc desc(buf, size);
    m_buffers.push_back(desc);
  }

  static ACE_TSS<TSSDataOutput> s_tssDataOutput;
};

TSSDataOutput::TSSDataOutput() : m_buffers() {
  m_buffers.reserve(10);
  LOGDEBUG("DATAOUTPUT poolsize is %d", m_buffers.size());
}

TSSDataOutput::~TSSDataOutput() {
  while (!m_buffers.empty()) {
    BufferDesc desc = m_buffers.back();
    m_buffers.pop_back();
    GF_FREE(desc.m_buf);
  }
}

ACE_TSS<TSSDataOutput> TSSDataOutput::s_tssDataOutput;

DataOutput::DataOutput() : m_poolName(NULL), m_size(0), m_haveBigBuffer(false) {
  m_buf = m_bytes = DataOutput::checkoutBuffer(&m_size);
}

uint8_t* DataOutput::checkoutBuffer(uint32_t* size) {
  return TSSDataOutput::s_tssDataOutput->getBuffer(size);
}

void DataOutput::checkinBuffer(uint8_t* buffer, uint32_t size) {
  TSSDataOutput::s_tssDataOutput->poolBuffer(buffer, size);
}

void DataOutput::writeObjectInternal(const Serializable* ptr, bool isDelta) {
  SerializationRegistry::serialize(ptr, *this, isDelta);
}

void DataOutput::acquireLock() { g_bigBufferLock.acquire(); }

void DataOutput::releaseLock() { g_bigBufferLock.release(); }
}  // namespace client
}  // namespace geode
}  // namespace apache
