#pragma once

#ifndef GEODE_PDXENUMINSTANTIATOR_H_
#define GEODE_PDXENUMINSTANTIATOR_H_

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

#include <gfcpp/Serializable.hpp>
#include <gfcpp/CacheableEnum.hpp>

namespace apache {
namespace geode {
namespace client {

class PdxEnumInstantiator : public Serializable {
 private:
  CacheableEnumPtr m_enumObject;

 public:
  PdxEnumInstantiator();

  virtual ~PdxEnumInstantiator();

  static Serializable* createDeserializable() {
    return new PdxEnumInstantiator();
  }

  virtual int8_t typeId() const;

  virtual void toData(DataOutput& output) const;

  virtual Serializable* fromData(DataInput& input);

  virtual int32_t classId() const { return 0; }

  CacheableStringPtr toString() const;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_PDXENUMINSTANTIATOR_H_
