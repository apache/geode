/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxInstantiator.hpp
 *
 *  Created on: Dec 28, 2011
 *      Author: npatel
 */

#ifndef _GEMFIRE_PDXINSTANTIATOR_HPP_
#define _GEMFIRE_PDXINSTANTIATOR_HPP_

#include <gfcpp/Serializable.hpp>

namespace gemfire {

class PdxInstantiator : public Serializable {
 private:
  PdxSerializablePtr m_userObject;

 public:
  PdxInstantiator();

  virtual ~PdxInstantiator();

  static Serializable* createDeserializable() { return new PdxInstantiator(); }

  virtual int8_t typeId() const;

  virtual void toData(DataOutput& output) const;

  virtual Serializable* fromData(DataInput& input);

  virtual int32_t classId() const { return 0x10; }

  CacheableStringPtr toString() const;
};
}
#endif /* _GEMFIRE_PDXINSTANTIATOR_HPP_ */
