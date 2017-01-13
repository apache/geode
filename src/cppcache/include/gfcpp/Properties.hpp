#ifndef __GEMFIRE_PROPERTIES_H__
#define __GEMFIRE_PROPERTIES_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 *
 * The specification of function behaviors is found in the corresponding .cpp
 *file.
 *
 *========================================================================
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

namespace gemfire {

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

};      // namespace gemfire
#endif  // ifndef __GEMFIRE_PROPERTIES_H__
