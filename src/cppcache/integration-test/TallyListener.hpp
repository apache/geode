/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _TEST_TALLYLISTENER_HPP_
#define _TEST_TALLYLISTENER_HPP_

#include <gfcpp/GemfireCppCache.hpp>
#include <string>

using namespace gemfire;
class TallyListener;

typedef gemfire::SharedPtr<TallyListener> TallyListenerPtr;

class TallyListener : public CacheListener {
 private:
  int m_creates;
  int m_updates;
  int m_invalidates;
  int m_destroys;
  int m_clears;
  bool isListnerInvoked;
  bool isCallbackCalled;
  CacheableKeyPtr m_lastKey;
  CacheablePtr m_lastValue;
  CacheableKeyPtr m_callbackArg;
  bool m_ignoreTimeout;
  bool m_quiet;

 public:
  TallyListener()
      : CacheListener(),
        m_creates(0),
        m_updates(0),
        m_invalidates(0),
        m_destroys(0),
        m_clears(0),
        isListnerInvoked(false),
        isCallbackCalled(false),
        m_lastKey(),
        m_lastValue(),
        m_callbackArg(NULLPTR),
        m_ignoreTimeout(false),
        m_quiet(false) {
    LOG("TallyListener contructor called");
  }

  virtual ~TallyListener() {}

  void beQuiet(bool v) { m_quiet = v; }

  void ignoreTimeouts(bool ignore) { m_ignoreTimeout = ignore; }

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
  void resetListnerInvokation() {
    isListnerInvoked = false;
    isCallbackCalled = false;
  }
  int getUpdates() { return m_updates; }
  int expectInvalidates(int expected) {
    LOG("calling expectInvalidates ");
    int tries = 0;
    while ((m_invalidates < expected) && (tries < 200)) {
      SLEEP(100);
      tries++;
    }
    return m_invalidates;
  }
  int expectDestroys(int expected) {
    LOG("calling expectDestroys ");
    int tries = 0;
    while ((m_destroys < expected) && (tries < 200)) {
      SLEEP(100);
      tries++;
    }
    return m_destroys;
  }

  int getInvalidates() { return m_invalidates; }
  int getDestroys() { return m_destroys; }
  bool isListenerInvoked() { return isListnerInvoked; }
  void setCallBackArg(const CacheableKeyPtr& callbackArg) {
    m_callbackArg = callbackArg;
  }
  CacheableKeyPtr getLastKey() { return m_lastKey; }

  CacheablePtr getLastValue() { return m_lastValue; }
  bool isCallBackArgCalled() { return isCallbackCalled; }
  void checkcallbackArg(const EntryEvent& event) {
    if (!isListnerInvoked) isListnerInvoked = true;
    if (m_callbackArg != NULLPTR) {
      CacheableKeyPtr callbkArg =
          dynCast<CacheableKeyPtr>(event.getCallbackArgument());
      if (strcmp(m_callbackArg->toString()->asChar(),
                 callbkArg->toString()->asChar()) == 0)
        isCallbackCalled = true;
    }
  }

  int getClears() { return m_clears; }

  virtual void afterCreate(const EntryEvent& event);

  virtual void afterUpdate(const EntryEvent& event);

  virtual void afterInvalidate(const EntryEvent& event);

  virtual void afterDestroy(const EntryEvent& event);

  virtual void afterRegionClear(const RegionEvent& event) {
    CacheListener::afterRegionClear(event);
  }

  virtual void afterRegionClear(const EntryEvent& event);

  virtual void afterRegionInvalidate(const RegionEvent& event) {}

  virtual void afterRegionDestroy(const RegionEvent& event) {}

  void showTallies() {
    char buf[1024];
    sprintf(buf,
            "TallyListener state: (updates = %d, creates = %d , invalidates = "
            "%d destroys = %d Regionclears = %d)",
            getUpdates(), getCreates(), getInvalidates(), getDestroys(),
            getClears());
    LOG(buf);
  }
};

void TallyListener::afterCreate(const EntryEvent& event) {
  m_creates++;
  LOGDEBUG("TallyListener::afterCreate called m_creates = %d ", m_creates);
  m_lastKey = event.getKey();
  m_lastValue = event.getNewValue();
  checkcallbackArg(event);

  CacheableStringPtr strPtr = dynCast<CacheableStringPtr>(event.getNewValue());
  char keytext[100];
  m_lastKey->logString(keytext, 100);
  if (!m_quiet) {
    char buf[1024];
    sprintf(buf, "TallyListener create - key = \"%s\", value = \"%s\"", keytext,
            strPtr->asChar());
    LOGDEBUG(buf);
  }
  std::string keyString(keytext);
  if ((!m_ignoreTimeout) && (keyString.find("timeout") != std::string::npos)) {
    LOG("TallyListener: Sleeping 10 seconds to force a timeout.");
    SLEEP(10000);  // this should give the client cause to timeout...
    LOG("TallyListener: done sleeping..");
  }
}

void TallyListener::afterUpdate(const EntryEvent& event) {
  m_updates++;
  m_lastKey = event.getKey();
  m_lastValue = event.getNewValue();
  checkcallbackArg(event);
  CacheableStringPtr strPtr = dynCast<CacheableStringPtr>(event.getNewValue());
  char keytext[100];
  m_lastKey->logString(keytext, 100);
  if (!m_quiet) {
    char buf[1024];
    sprintf(buf, "TallyListener update - key = \"%s\", value = \"%s\"", keytext,
            strPtr->asChar());
    LOG(buf);
  }
  std::string keyString(keytext);
  if ((!m_ignoreTimeout) && (keyString.find("timeout") != std::string::npos)) {
    LOG("TallyListener: Sleeping 10 seconds to force a timeout.");
    SLEEP(10000);  // this should give the client cause to timeout...
    LOG("TallyListener: done sleeping..");
  }
}
void TallyListener::afterInvalidate(const EntryEvent& event) {
  m_invalidates++;
  checkcallbackArg(event);
}
void TallyListener::afterDestroy(const EntryEvent& event) {
  m_destroys++;
  checkcallbackArg(event);
}
void TallyListener::afterRegionClear(const EntryEvent& event) {
  m_clears++;
  LOGINFO("TallyListener::afterRegionClear m_clears = %d", m_clears);
  checkcallbackArg(event);
}

#endif
