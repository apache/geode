/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "MemberListForVersionStamp.hpp"
#include <gfcpp/Log.hpp>

using namespace gemfire;

MemberListForVersionStamp::MemberListForVersionStamp() { m_memberCounter = 0; }

MemberListForVersionStamp::~MemberListForVersionStamp() {}

// Two hash maps are needed in this class as we have two primary keys on which
// we want a search - integer counter and the hash key of the member.
// Add function searches whether the member is already added to the hash maps.
// If yes, return the integer counter. If not, add it to both the hash maps.
// This function is protected  using readers/writer lock
uint16_t MemberListForVersionStamp::add(DSMemberForVersionStampPtr member) {
  WriteGuard guard(m_mapLock);
  std::unordered_map<std::string, DistributedMemberWithIntIdentifier>::iterator
      it = m_members2.find(member->getHashKey());
  if (it != m_members2.end()) return (*it).second.m_identifier;
  DistributedMemberWithIntIdentifier dmwithIntId(member, ++m_memberCounter);
  m_members1[m_memberCounter] = member;
  m_members2[member->getHashKey()] = dmwithIntId;
  LOGDEBUG(
      "Adding a new member to the member list maintained for version stamps "
      "member Ids. HashKey: %s MemberCounter: %d",
      member->getHashKey().c_str(), m_memberCounter);
  return m_memberCounter;
}

// This function is protected  using readers/writer lock
DSMemberForVersionStampPtr MemberListForVersionStamp::getDSMember(
    uint16_t memberId) {
  ReadGuard guard(m_mapLock);
  std::unordered_map<uint32_t, DSMemberForVersionStampPtr>::iterator it =
      m_members1.find(memberId);
  if (it != m_members1.end()) return (*it).second;
  return NULLPTR;
}
