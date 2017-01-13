#ifndef __GEMFIRE_CQ_STATE_H__
#define __GEMFIRE_CQ_STATE_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @class CqState CqState.hpp
 *
 * This interface gives information on the state of a CqQuery.
 * It is provided by the getState method of the CqQuery instance.
 */
class CPPCACHE_EXPORT CqState {
 public:
  // corresponding to gemfire.cache.query.internal.CqStateImpl
  typedef enum {
    STOPPED = 0,
    RUNNING = 1,
    CLOSED = 2,
    CLOSING = 3,
    INVALID
  } StateType;
  /**
   * Returns the state in string form.
   */
  const char* toString() const;

  /**
   * Returns true if the CQ is in Running state.
   */
  bool isRunning() const;

  /**
   * Returns true if the CQ is in Stopped state.
   */
  bool isStopped() const;

  /**
   * Returns true if the CQ is in Closed state.
   */
  bool isClosed() const;

  /**
   * Returns true if the CQ is in Closing state.
   */
  bool isClosing() const;
  void setState(CqState::StateType state);
  CqState::StateType getState();

 private:
  StateType m_state;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CQ_STATE_H__
