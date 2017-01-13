/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testLRUList"

#include "fw_helper.hpp"

#ifdef WIN32

BEGIN_TEST(NotOnWindows)
  { LOG("Not everything is exported on windows."); }
END_TEST(NotOnWindows)

#else

//#define BUILD_CPPCACHE

#include <LRUList.cpp>
#include <gfcpp/GemfireCppCache.hpp>

using namespace gemfire;
using namespace test;

class MyNode : public SharedBase, public LRUEntryProperties {
 public:
  static MyNode* create(const CacheableKeyPtr& key = NULLPTR) {
    return new MyNode();
  }
  virtual ~MyNode() {}
  LRUEntryProperties& getLRUProperties() { return *this; }
  void setValue(int value) { m_value = value; }
  int getValue() { return m_value; }

 private:
  MyNode() : m_value(0) {}

  int m_value;
};

typedef SharedPtr<MyNode> MyNodePtr;

/**
 * @brief Test the LRU-ness of the LRUList.
 */
BEGIN_TEST(LRUListTest)
  {
    LRUList<MyNode, MyNode> lruList;
    // Create 10 Nodes to keep track of.
    MyNodePtr* tenNodes = new MyNodePtr[10];

    for (int i = 0; i < 10; i++) {
      tenNodes[i] = MyNode::create();
      tenNodes[i]->setValue(i);
      // add each node to list
      lruList.appendEntry(tenNodes[i]);
      MyNodePtr myPtr = dynCast<MyNodePtr>(tenNodes[i]);
      cout << "  appendEntry( " << myPtr->getValue() << " )" << endl;
    }

    // now mark odd indexed nodes recently used.
    for (int j = 1; j < 10; j += 2) {
      tenNodes[j]->setRecentlyUsed();
    }

    MyNodePtr aNode;
    char msgbuf[100];
    // check that we get the unmarked entries first. 0,2,4,6,8
    for (int k = 0; k < 10; k += 2) {
      lruList.getLRUEntry(aNode);
      sprintf(msgbuf, "expected node %d", k);
      cout << " gotEntry( " << aNode->getValue() << " )" << endl;
      ASSERT(aNode == tenNodes[k], msgbuf);
    }

    // now check that we get the remaining entries. 1,3,5,7,9
    for (int m = 1; m < 10; m += 2) {
      lruList.getLRUEntry(aNode);
      sprintf(msgbuf, "expected node %d", m);
      ASSERT(aNode == tenNodes[m], msgbuf);
    }
  }
END_TEST(LRUListTest)

BEGIN_TEST(TestEndOfList)
  {
    LRUList<MyNode, MyNode> lruList;
    // add ten entries.
    for (int i = 0; i < 10; i++) {
      MyNodePtr tmp(MyNode::create());
      tmp->setValue(i);
      lruList.appendEntry(tmp);
    }
    // drain the list...
    int k = 0;
    char msgbuf[100];
    while (k < 10) {
      MyNodePtr nodePtr;
      lruList.getLRUEntry(nodePtr);
      sprintf(msgbuf, "expected node %d", k);
      ASSERT(dynCast<MyNodePtr>(nodePtr)->getValue() == k, msgbuf);
      k++;
    }
    // now list should be empty...
    MyNodePtr emptyPtr;
    lruList.getLRUEntry(emptyPtr);
    ASSERT(emptyPtr == NULLPTR, "expected NULL");
    // do it again...
    emptyPtr = NULLPTR;
    ASSERT(emptyPtr == NULLPTR, "expected NULL");
    lruList.getLRUEntry(emptyPtr);
    ASSERT(emptyPtr == NULLPTR, "expected NULL");
    // now add something to the list... and retest...
    {
      MyNodePtr tmp(MyNode::create());
      tmp->setValue(100);
      lruList.appendEntry(tmp);
    }
    MyNodePtr hundredPtr;
    lruList.getLRUEntry(hundredPtr);
    ASSERT(hundredPtr != NULLPTR, "expected to not be NULL");
    ASSERT(dynCast<MyNodePtr>(hundredPtr)->getValue() == 100,
           "expected value of 100");
    lruList.getLRUEntry(emptyPtr);
    ASSERT(emptyPtr == NULLPTR, "expected NULL");
  }
END_TEST(TestEndOfList)

/**
 * @brief Test all the states of the LRUListEntry
 */
BEGIN_TEST(LRUListEntryTest)
  {
    MyNodePtr node(MyNode::create());
    node->setValue(20);
    // test initial state.
    ASSERT(node->testRecentlyUsed() == false, "should not be marked used.");
    ASSERT(node->testEvicted() == false, "should not yet be evicted.");

    // set used, and retest state.
    node->setRecentlyUsed();
    ASSERT(node->testRecentlyUsed() == true, "should be marked used.");
    ASSERT(node->testEvicted() == false, "should not yet be evicted.");

    // clear used, and retest state.
    node->clearRecentlyUsed();
    ASSERT(node->testRecentlyUsed() == false, "should not be marked used.");
    ASSERT(node->testEvicted() == false, "should not yet be evicted.");

    // set evicted, and retest state.
    node->setEvicted();
    ASSERT(node->testRecentlyUsed() == false, "should not be marked used.");
    ASSERT(node->testEvicted() == true, "should be evicted.");

    // clear evicted, and retest state.
    node->clearEvicted();
    ASSERT(node->testRecentlyUsed() == false, "should not be marked used.");
    ASSERT(node->testEvicted() == false, "should not yet be evicted.");

    // set both bits.
    node->setRecentlyUsed();
    node->setEvicted();
    ASSERT(node->testRecentlyUsed() == true, "should be marked used.");
    ASSERT(node->testEvicted() == true, "should be evicted.");

    // clear evicted, and retest state.
    node->clearEvicted();
    ASSERT(node->testRecentlyUsed() == true, "should be marked used.");
    ASSERT(node->testEvicted() == false, "should not yet be evicted.");

    // set both bits.
    node->setEvicted();
    node->clearRecentlyUsed();
    ASSERT(node->testRecentlyUsed() == false, "should not be marked used.");
    ASSERT(node->testEvicted() == true, "should be evicted.");
  }
END_TEST(LRUListEntryTest)

#endif
