#ifndef __GEMFIRE_STRUCT_H__
#define __GEMFIRE_STRUCT_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "CacheableBuiltins.hpp"
#include "StructSet.hpp"
#include "SelectResults.hpp"
#include "Serializable.hpp"
#include "VectorT.hpp"
#include "HashMapT.hpp"

/**
 * @file
 */

namespace gemfire {

class StructSet;

/**
 * @class Struct Struct.hpp
 * A Struct has a StructSet as its parent. It contains the field values
 * returned after executing a Query obtained from a QueryService which in turn
 * is obtained from a Cache.
 */
class CPPCACHE_EXPORT Struct : public Serializable {
 public:
  /**
   * Constructor - meant only for internal use.
   */
  Struct(StructSet* ssPtr, VectorT<SerializablePtr>& fieldValues);

  /**
   * Factory function for registration of <code>Struct</code>.
   */
  static Serializable* createDeserializable();

  /**
   * Get the field value for the given index number.
   *
   * @param index the index number of the field value to get.
   * @returns A smart pointer to the field value or NULLPTR if index out of
   * bounds.
   */
  const SerializablePtr operator[](int32_t index) const;

  /**
   * Get the field value for the given field name.
   *
   * @param fieldName the name of the field whos value is required.
   * @returns A smart pointer to the field value.
   * @throws IllegalArgumentException if the field name is not found.
   */
  const SerializablePtr operator[](const char* fieldName) const;

  /**
   * Get the parent StructSet of this Struct.
   *
   * @returns A smart pointer to the parent StructSet of this Struct.
   */
  const StructSetPtr getStructSet() const;

  /**
   * Check whether another field value is available to iterate over in this
   * Struct.
   *
   * @returns true if available otherwise false.
   */
  bool hasNext() const;

  /**
   * Get the number of field values available.
   *
   * @returns the number of field values available.
   */
  int32_t length() const;

  /**
   * Get the next field value item available in this Struct.
   *
   * @returns A smart pointer to the next item in the Struct or NULLPTR if no
   * more available.
   */
  const SerializablePtr next();

  /**
   * Deserializes the Struct object from the DataInput. @TODO KN: better comment
   */
  virtual Serializable* fromData(DataInput& input);

  /**
   * Serializes this Struct object. @TODO KN: better comment
   */
  virtual void toData(DataOutput& output) const;

  /**
   * Returns the classId for internal use.
   */
  virtual int32_t classId() const;

  /**
   * Returns the typeId of Struct class.
   */
  virtual int8_t typeId() const;

  /**
   * Return the data serializable fixed ID size type for internal use.
   * @since GFE 5.7
   */
  virtual int8_t DSFID() const;

  /**
   * Returns the name of the field corresponding to the index number in the
   * Struct
   */
  virtual const char* getFieldName(int32_t index);

  /**
   * always returns 0
   */
  virtual uint32_t objectSize() const {
    return 0;  // does not get cached, so no need to account for it
  }

 private:
  void skipClassName(DataInput& input);

  Struct();

  HashMapT<CacheableStringPtr, CacheableInt32Ptr> m_fieldNames;
  VectorT<SerializablePtr> m_fieldValues;

  StructSet* m_parent;

  int32_t m_lastAccessIndex;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_STRUCT_H__
