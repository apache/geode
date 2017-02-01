#pragma once

#ifndef GEODE_GFCPP_PROPERTIES_H_
#define GEODE_GFCPP_PROPERTIES_H_

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

/**
 * @file
 */
#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "Serializable.hpp"
#include "DataInput.hpp"
#include "DataOutput.hpp"
#include "Cacheable.hpp"
#include "CacheableKey.hpp"
#include "CacheableString.hpp"

namespace apache {
namespace geode {
namespace client {

/**
 * @class Properties Properties.hpp
 * Contains a set of (key, value) pair properties with key being the name of
 * the property; value, the value of the property.
 *
 */

class CPPCACHE_EXPORT Properties : public Serializable {
 public:
  class Visitor {
   public:
    virtual void visit(CacheableKeyPtr& key, CacheablePtr& value) = 0;
    virtual ~Visitor() {}
  };

  /**
   * Return the value for the given key, or NULLPTR if not found.
   *
   * @throws NullPointerException if the key is null
   */
  CacheableStringPtr find(const char* key);
  /**
   * Return the value for the given <code>CacheableKey</code>,
   * or NULLPTR if not found.
   *
   * @throws NullPointerException if the key is NULLPTR
   */
  CacheablePtr find(const CacheableKeyPtr& key);

  /**
   * Add or update the string value for key.
   *
   * @throws NullPointerException if the key is null
   */
  void insert(const char* key, const char* value);

  /**
   * Add or update the int value for key.
   *
   * @throws NullPointerException if the key is null
   */
  void insert(const char* key, const int value);

  /**
   * Add or update Cacheable value for CacheableKey
   *
   * @throws NullPointerException if the key is NULLPTR
   */
  void insert(const CacheableKeyPtr& key, const CacheablePtr& value);

  /**
   * Remove the key from the collection.
   *
   * @throws NullPointerException if the key is null
   */
  void remove(const char* key);

  /**
   * Remove the <code>CacheableKey</code> from the collection.
   *
   * @throws NullPointerException if the key is NULLPTR
   */
  void remove(const CacheableKeyPtr& key);

  /** Execute the Visitor's <code>visit( const char* key, const char* value
   * )</code>
   * method for each entry in the collection.
   */
  void foreach (Visitor& visitor) const;

  /** Return the number of entries in the collection. */
  uint32_t getSize() const;

  /** Add the contents of other to this instance, replacing any existing
   * values with those from other.
   */
  void addAll(const PropertiesPtr& other);

  /** Factory method, returns an empty collection. */
  static PropertiesPtr create();

  /** Read property values from a file, overriding what is currently
   * in the properties object.
   */
  void load(const char* fileName);

  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const;

  /**
   *@brief deserialize this object
   **/
  virtual Serializable* fromData(DataInput& input);

  /** Return an empty instance for deserialization. */
  static Serializable* createDeserializable();

  /** Return class id for serialization. */
  virtual int32_t classId() const;

  /** Return type id for serialization. */
  virtual int8_t typeId() const;

  virtual uint32_t objectSize() const {
    return 0;  // don't care to set the right value
  }

  /** destructor. */
  virtual ~Properties();

 private:
  Properties();

  void* m_map;

 private:
  Properties(const Properties&);
  const Properties& operator=(const Properties&);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_PROPERTIES_H_
