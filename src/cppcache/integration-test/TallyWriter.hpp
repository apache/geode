/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _TEST_TALLYWRITER_HPP_
#define _TEST_TALLYWRITER_HPP_ 1

#include <gfcpp/GemfireCppCache.hpp>

using namespace gemfire;
using namespace test;

class TallyWriter;

typedef gemfire::SharedPtr<TallyWriter> TallyWriterPtr;

class TallyWriter : virtual public CacheWriter {
 private:
  int m_creates;
  int m_updates;
  int m_invalidates;
  int m_destroys;
  bool isWriterInvoke;
  bool isCallbackCalled;
  bool isWriterfailed;
  CacheableKeyPtr m_lastKey;
  CacheablePtr m_lastValue;
  CacheableKeyPtr m_callbackArg;

 public:
  TallyWriter()
      : CacheWriter(),
        m_creates(0),
        m_updates(0),
        m_invalidates(0),
        m_destroys(0),
        isWriterInvoke(false),
        isCallbackCalled(false),
        isWriterfailed(false),
        m_lastKey(),
        m_lastValue(),
        m_callbackArg(NULLPTR) {
    LOG("TallyWriter Constructor called");
  }

  virtual ~TallyWriter() {}

  virtual bool beforeCreate(const EntryEvent& event) {
    m_creates++;
    checkcallbackArg(event);
    LOG("TallyWriter::beforeCreate");
    return !isWriterfailed;
  }

  virtual bool beforeUpdate(const EntryEvent& event) {
    m_updates++;
    checkcallbackArg(event);
    LOG("TallyWriter::beforeUpdate");
    return !isWriterfailed;
  }

  virtual bool beforeInvalidate(const EntryEvent& event) {
    m_invalidates++;
    checkcallbackArg(event);
    LOG("TallyWriter::beforeInvalidate");
    return !isWriterfailed;
  }

  virtual bool beforeDestroy(const EntryEvent& event) {
    m_destroys++;
    checkcallbackArg(event);
    LOG("TallyWriter::beforeDestroy");
    return !isWriterfailed;
  }

  virtual bool beforeRegionDestroy(const RegionEvent& event) {
    LOG("TallyWriter::beforeRegionDestroy");
    return !isWriterfailed;
  }

  virtual void close(const RegionPtr& region) { LOG("TallyWriter::close"); }

  int expectCreates(int expected) {
    int tries = 0;
    while ((m_creates < expected) && (tries < 200)) {
      SLEEP(100);
      tries++;
    }
    return m_creates;
  }

  int getCreates() { return m_creates; }

  int expectUpdates(int expected) {
    int tries = 0;
    while ((m_updates < expected) && (tries < 200)) {
      SLEEP(100);
      tries++;
    }
    return m_updates;
  }

  int getUpdates() { return m_updates; }
  int getInvalidates() { return m_invalidates; }
  int getDestroys() { return m_destroys; }
  void setWriterFailed() { isWriterfailed = true; }
  void setCallBackArg(const CacheableKeyPtr& callbackArg) {
    m_callbackArg = callbackArg;
  }
  void resetWriterInvokation() {
    isWriterInvoke = false;
    isCallbackCalled = false;
  }
  bool isWriterInvoked() { return isWriterInvoke; }
  bool isCallBackArgCalled() { return isCallbackCalled; }
  CacheableKeyPtr getLastKey() { return m_lastKey; }

  CacheablePtr getLastValue() { return m_lastValue; }

  void showTallies() {
    char buf[1024];
    sprintf(buf,
            "TallyWriter state: (updates = %d, creates = %d, invalidates = %d, "
            "destroy = %d)",
            getUpdates(), getCreates(), getInvalidates(), getDestroys());
    LOG(buf);
  }
  void checkcallbackArg(const EntryEvent& event) {
    if (!isWriterInvoke) isWriterInvoke = true;
    if (m_callbackArg != NULLPTR) {
      CacheableKeyPtr callbkArg =
          dynCast<CacheableKeyPtr>(event.getCallbackArgument());
      if (strcmp(m_callbackArg->toString()->asChar(),
                 callbkArg->toString()->asChar()) == 0)
        isCallbackCalled = true;
    }
  }
};

#endif  //_TEST_TALLYWRITER_HPP_
