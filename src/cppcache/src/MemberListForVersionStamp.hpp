/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __MemberListForVersionStamp_HPP__
#define __MemberListForVersionStamp_HPP__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedPtr.hpp>
#include "DSMemberForVersionStamp.hpp"
#include "ace/RW_Thread_Mutex.h"
#include "ReadWriteLock.hpp"
#include <unordered_map>

namespace gemfire {
struct DistributedMemberWithIntIdentifier {
 public:
  DistributedMemberWithIntIdentifier(
      DSMemberForVersionStampPtr dsmember = NULLPTR, uint16_t id = 0) {
    this->m_member = dsmember;
    this->m_identifier = id;
  }
  DSMemberForVersionStampPtr m_member;
  uint16_t m_identifier;
};

class MemberListForVersionStamp : public SharedBase {
 public:
  MemberListForVersionStamp();
  virtual ~MemberListForVersionStamp();
  uint16_t add(DSMemberForVersionStampPtr member);
  DSMemberForVersionStampPtr getDSMember(uint16_t memberId);

 private:
  std::unordered_map<uint32_t, DSMemberForVersionStampPtr> m_members1;
  std::unordered_map<std::string, DistributedMemberWithIntIdentifier>
      m_members2;

  ACE_RW_Thread_Mutex m_mapLock;
  uint32_t m_memberCounter;
};

typedef SharedPtr<MemberListForVersionStamp> MemberListForVersionStampPtr;
}

#endif  // __MemberListForVersionStamp_HPP__
