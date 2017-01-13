/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/Log.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/SystemProperties.hpp>
#include <SerializationRegistry.hpp>
#include <ace/TSS_T.h>

#include <ace/Recursive_Thread_Mutex.h>
#include <vector>

namespace gemfire {

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
}  // namespace gemfire
