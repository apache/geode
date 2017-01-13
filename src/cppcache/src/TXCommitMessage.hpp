/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TXCommitMessage.hpp
 *
 *  Created on: 21-Feb-2011
 *      Author: ankurs
 */

#ifndef TXCOMMITMESSAGE_HPP_
#define TXCOMMITMESSAGE_HPP_

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/DataInput.hpp>
#include "RegionCommit.hpp"
#include <gfcpp/VectorOfSharedBase.hpp>

namespace gemfire {
_GF_PTR_DEF_(TXCommitMessage, TXCommitMessagePtr);

class TXCommitMessage : public gemfire::Cacheable {
 public:
  TXCommitMessage();
  virtual ~TXCommitMessage();

  virtual Serializable* fromData(DataInput& input);
  virtual void toData(DataOutput& output) const;
  virtual int32_t classId() const;
  int8_t typeId() const;
  static Serializable* create();
  //	VectorOfEntryEvent getEvents(Cache* cache);

  void apply(Cache* cache);

 private:
  // UNUSED int32_t m_processorId;
  bool isAckRequired();

  VectorOfSharedBase m_regions;
};
}
#endif /* TXCOMMITMESSAGE_HPP_ */
