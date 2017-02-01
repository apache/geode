#pragma once

#ifndef GEODE_LOCATORLISTREQUEST_H_
#define GEODE_LOCATORLISTREQUEST_H_

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
#include <string>
#include "ServerLocationRequest.hpp"
#include "GeodeTypeIdsImpl.hpp"
namespace apache {
namespace geode {
namespace client {
class DataOutput;
class DataInput;
class Serializable;
class LocatorListRequest : public ServerLocationRequest {
 private:
  std::string m_servergroup;

 public:
  LocatorListRequest(const std::string& servergroup = "");
  virtual void toData(DataOutput& output) const;
  virtual Serializable* fromData(DataInput& input);
  virtual int8_t typeId() const;
  virtual uint32_t objectSize() const;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_LOCATORLISTREQUEST_H_
