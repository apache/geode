/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_LRULIST_H__
#define __GEMFIRE_IMPL_LRULIST_H__

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/SharedPtr.hpp>
#include "SpinLock.hpp"

namespace gemfire {
// Bit mask for recently used
#define RECENTLY_USED_BITS 1ul
// Bit mask for evicted
#define EVICTED_BITS 2ul

/**
 * @brief This class encapsulates LRU specific properties for a LRUList node.
 */
class CPPCACHE_EXPORT LRUEntryProperties {
 public:
  inline LRUEntryProperties() : m_bits(0), m_persistenceInfo(NULL) {}

  inline void setRecentlyUsed() {
    HostAsm::atomicSetBits(m_bits, RECENTLY_USED_BITS);
  }

  inline void clearRecentlyUsed() {
    HostAsm::atomicClearBits(m_bits, RECENTLY_USED_BITS);
  }

  inline bool testRecentlyUsed() const {
    return (m_bits & RECENTLY_USED_BITS) == RECENTLY_USED_BITS;
  }

  inline bool testEvicted() const {
    return (m_bits & EVICTED_BITS) == EVICTED_BITS;
  }

  inline void setEvicted() { HostAsm::atomicSetBits(m_bits, EVICTED_BITS); }

  inline void clearEvicted() { HostAsm::atomicClearBits(m_bits, EVICTED_BITS); }

  inline void* getPersistenceInfo() const { return m_persistenceInfo; }

  inline void setPersistenceInfo(void* persistenceInfo) {
    m_persistenceInfo = persistenceInfo;
  }

 protected:
  // this constructor deliberately skips initializing any fields
  inline LRUEntryProperties(bool noInit) {}

 private:
  volatile uint32_t m_bits;
  void* m_persistenceInfo;
};

/**
 * @brief Maintains a list of entries returning them through head in
 * approximate LRU order. The <code>TEntry</code> template argument
 * must provide a <code>getLRUProperties</code> method that returns an
 * object of class <code>LRUEntryProperties</code>.
 */
template <typename TEntry, typename TCreateEntry>
class LRUList {
 protected:
  typedef SharedPtr<TEntry> LRUListEntryPtr;

  /**
   * @brief The entries in the LRU List are instances of LRUListNode.
   * This maintains the evicted and recently used state for each entry.
   */
  class LRUListNode {
   public:
    inline LRUListNode(const LRUListEntryPtr& entry)
        : m_entry(entry), m_nextLRUListNode(NULL) {}

    inline ~LRUListNode() {}

    inline void getEntry(LRUListEntryPtr& result) const { result = m_entry; }

    inline LRUListNode* getNextLRUListNode() const { return m_nextLRUListNode; }

    inline void setNextLRUListNode(LRUListNode* next) {
      m_nextLRUListNode = next;
    }

    inline void clearNextLRUListNode() { m_nextLRUListNode = NULL; }

   private:
    LRUListEntryPtr m_entry;
    LRUListNode* m_nextLRUListNode;

    // disabled
    LRUListNode(const LRUListNode&);
    LRUListNode& operator=(const LRUListNode&);
  };

 public:
  LRUList();
  ~LRUList();

  /**
   * @brief add an entry to the tail of the list.
   */
  void appendEntry(const LRUListEntryPtr& entry);

  /**
   * @brief return the least recently used node from the list,
   * and removing it from the list.
   */
  void getLRUEntry(LRUListEntryPtr& result);

 private:
  /**
   * @brief add a node to the tail of the list.
   */
  void appendNode(LRUListNode* aNode);

  /**
   * @brief return the head entry in the list,
   * and removing it from the list.
   */
  LRUListNode* getHeadNode(bool& isLast);

  SpinLock m_headLock;
  SpinLock m_tailLock;

  LRUListNode* m_headNode;
  LRUListNode* m_tailNode;

};  // LRUList

}  // gemfire

#endif  // __GEMFIRE_IMPL_LRULIST_H__
