#ifndef __GEMFIRE_CQ_STATUS_LISTENER_HPP__
#define __GEMFIRE_CQ_STATUS_LISTENER_HPP__
/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "CqListener.hpp"

namespace gemfire {

/**
* Extension of CqListener. Adds two new methods to CqListener, one that
* is called when the cq is connected and one that is called when
* the cq is disconnected
*
* @since 7.0
*/

class CPPCACHE_EXPORT CqStatusListener : public CqListener {
 public:
  /**
  * Called when the cq loses connection with all servers
  */
  virtual void onCqDisconnected();

  /**
  * Called when the cq establishes a connection with a server
  */
  virtual void onCqConnected();
};

}  // namespace gemfire

#endif  //#ifndef __GEMFIRE_CQ_STATUS_LISTENER_HPP__
