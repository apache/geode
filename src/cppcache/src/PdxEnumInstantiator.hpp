/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* PdxEnumInstantiator.hpp
*/

#ifndef _GEMFIRE_PDXENUM_INSTANTIATOR_HPP_
#define _GEMFIRE_PDXENUM_INSTANTIATOR_HPP_

#include <gfcpp/Serializable.hpp>
#include <gfcpp/CacheableEnum.hpp>

namespace gemfire {

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
}
#endif /* _GEMFIRE_PDXENUM_INSTANTIATOR_HPP_ */
