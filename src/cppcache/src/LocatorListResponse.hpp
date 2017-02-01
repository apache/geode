#pragma once

#ifndef GEODE_LOCATORLISTRESPONSE_H_
#define GEODE_LOCATORLISTRESPONSE_H_

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
#include <vector>
#include "GeodeTypeIdsImpl.hpp"
#include "ServerLocationResponse.hpp"
#include "ServerLocation.hpp"
namespace apache {
namespace geode {
namespace client {
class DataInput;
class LocatorListResponse : public ServerLocationResponse {
 private:
  std::vector<ServerLocation> m_locators;
  bool m_isBalanced;
  void readList(DataInput& input);

 public:
  LocatorListResponse()
      :                         /* adonre
                                 * CID 28938: Uninitialized scalar field (UNINIT_CTOR) *
                                */
        m_isBalanced(false) {}  // Default constru needed for de-serialization
  virtual LocatorListResponse* fromData(DataInput& input);
  virtual int8_t typeId() const;
  virtual uint32_t objectSize() const;
  const std::vector<ServerLocation>& getLocators() const;
  bool isBalanced() const;
  virtual ~LocatorListResponse() {}  // Virtual destructor
  static Serializable* create();
};
typedef SharedPtr<LocatorListResponse> LocatorListResponsePtr;
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_LOCATORLISTRESPONSE_H_
