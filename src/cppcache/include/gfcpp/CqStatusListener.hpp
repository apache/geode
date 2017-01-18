#ifndef __GEMFIRE_CQ_STATUS_LISTENER_HPP__
#define __GEMFIRE_CQ_STATUS_LISTENER_HPP__
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
