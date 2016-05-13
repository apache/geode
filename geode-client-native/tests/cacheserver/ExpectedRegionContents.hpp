/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __ExpectedRegionContent_hpp__
#define __ExpectedRegionContent_hpp__

#include "GemfireCppCache.hpp"
#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/FwkLog.hpp"
#include <string>
namespace gemfire {
namespace testframework {

class ExpectedRegionContents {

// instance fields
// KeyIntevals.NONE
    bool m_containsKey_none;            // expected value of m_containsKey for KeyIntervals.NONE
    bool m_containsValue_none;          // expected value of m_containsValue for KeyIntervals.NONE
    bool m_getAllowed_none;             // if true, then check the value with a get

// KeyIntevals.INVALIDATE
    bool m_containsKey_invalidate;
    bool m_containsValue_invalidate;
    bool m_getAllowed_invalidate;

// KeyIntevals.LOCAL_INVALIDATE
    bool m_containsKey_localInvalidate;
    bool m_containsValue_localInvalidate;
    bool m_getAllowed_localInvalidate;

// KeyIntevals.DESTROY
    bool m_containsKey_destroy;
    bool m_containsValue_destroy;
    bool m_getAllowed_destroy;

// KeyIntevals.LOCAL_DESTROY
    bool m_containsKey_localDestroy;
    bool m_containsValue_localDestroy;
    bool m_getAllowed_localDestroy;

// KeyIntevals.UPDATE
    bool m_containsKey_update;
    bool m_containsValue_update;
    bool m_getAllowed_update;
    bool m_valueIsUpdated;

// KeyIntervals.GET
    bool m_containsKey_get;
    bool m_containsValue_get;
    bool m_getAllowed_get;

// new keys
    bool m_containsKey_newKey;
    bool m_containsValue_newKey;
    bool m_getAllowed_newKey;

// region size specifications
 int m_exactSize;
 int m_minSize;
 int m_maxSize;

public:
// constructors
 ExpectedRegionContents(bool m_containsKey, bool m_containsValue, bool m_getAllowed) {
   m_containsKey_none = m_containsKey;
   m_containsKey_invalidate = m_containsKey;
   m_containsKey_localInvalidate = m_containsKey;
   m_containsKey_destroy = m_containsKey;
   m_containsKey_localDestroy = m_containsKey;
   m_containsKey_update = m_containsKey;
   m_containsKey_get = m_containsKey;
   m_containsKey_newKey = m_containsKey;

   m_containsValue_none = m_containsValue;
   m_containsValue_invalidate = m_containsValue;
   m_containsValue_localInvalidate = m_containsValue;
   m_containsValue_destroy = m_containsValue;
   m_containsValue_localDestroy = m_containsValue;
   m_containsValue_update = m_containsValue;
   m_containsValue_get = m_containsValue;
   m_containsValue_newKey = m_containsValue;

   m_getAllowed_none = m_getAllowed;
   m_getAllowed_invalidate = m_getAllowed;
   m_getAllowed_localInvalidate = m_getAllowed;
   m_getAllowed_destroy = m_getAllowed;
   m_getAllowed_localDestroy = m_getAllowed;
   m_getAllowed_update = m_getAllowed;
   m_getAllowed_get = m_getAllowed;
   m_getAllowed_newKey = m_getAllowed;
   m_valueIsUpdated = false;

}

 ExpectedRegionContents(bool m_containsKeyNone,            bool m_containsValueNone,
                              bool m_containsKeyInvalidate,      bool m_containsValueInvalidate,
                              bool m_containsKeyLocalInvalidate, bool m_containsValueLocalInvalidate,
                              bool m_containsKeyDestroy,         bool m_containsValueDestroy,
                              bool m_containsKeyLocalDestroy,    bool m_containsValueLocalDestroy,
                              bool m_containsKeyUpdate,          bool m_containsValueUpdate,
                              bool m_containsKeyGet,             bool m_containsValueGet,
                              bool m_containsKeyNewKey,          bool m_containsValueNewKey,
                              bool m_getAllowed,
                              bool updated) {
   m_containsKey_none = m_containsKeyNone;
   m_containsValue_none = m_containsValueNone;

   m_containsKey_invalidate = m_containsKeyInvalidate;
   m_containsValue_invalidate = m_containsValueInvalidate;

   m_containsKey_localInvalidate = m_containsKeyLocalInvalidate;
   m_containsValue_localInvalidate = m_containsValueLocalInvalidate;

   m_containsKey_destroy = m_containsKeyDestroy;
   m_containsValue_destroy = m_containsValueDestroy;

   m_containsKey_localDestroy = m_containsKeyLocalDestroy;
   m_containsValue_localDestroy = m_containsValueLocalDestroy;

   m_containsKey_update = m_containsKeyUpdate;
   m_containsValue_update = m_containsValueUpdate;

   m_containsKey_get = m_containsKeyGet;
   m_containsValue_get = m_containsValueGet;

   m_containsKey_newKey = m_containsKeyNewKey;
   m_containsValue_newKey = m_containsValueNewKey;

   m_getAllowed_none = m_getAllowed;
   m_getAllowed_invalidate = m_getAllowed;
   m_getAllowed_localInvalidate = m_getAllowed;
   m_getAllowed_destroy = m_getAllowed;
   m_getAllowed_localDestroy = m_getAllowed;
   m_getAllowed_update = m_getAllowed;
   m_getAllowed_get = m_getAllowed;
   m_getAllowed_newKey = m_getAllowed;
   m_valueIsUpdated = updated;
}

//================================================================================
// getter methods
 bool containsKey_none() {
   return m_containsKey_none;
}
 bool containsValue_none() {
   return m_containsValue_none;
}
 bool getAllowed_none() {
   return m_getAllowed_none;
}
 bool containsKey_invalidate() {
   return m_containsKey_invalidate;
}
 bool containsValue_invalidate() {
   return m_containsValue_invalidate;
}
 bool getAllowed_invalidate() {
   return m_getAllowed_invalidate;
}
 bool containsKey_localInvalidate() {
   return m_containsKey_localInvalidate;
}
 bool containsValue_localInvalidate() {
   return m_containsValue_localInvalidate;
}
 bool getAllowed_localInvalidate() {
   return m_getAllowed_localInvalidate;
}
 bool containsKey_destroy() {
   return m_containsKey_destroy;
}
 bool containsValue_destroy() {
   return m_containsValue_destroy;
}
 bool getAllowed_destroy() {
   return m_getAllowed_destroy;
}
 bool containsKey_localDestroy() {
   return m_containsKey_localDestroy;
}
 bool containsValue_localDestroy() {
   return m_containsValue_localDestroy;
}
 bool getAllowed_localDestroy() {
   return m_getAllowed_localDestroy;
}
 bool containsKey_update() {
   return m_containsKey_update;
}
 bool containsValue_update() {
   return m_containsValue_update;
}
 bool getAllowed_update() {
   return m_getAllowed_update;
}
 bool valueIsUpdated() {
   return m_valueIsUpdated;
}
 bool containsKey_get() {
   return m_containsKey_get;
}
 bool containsValue_get() {
   return m_containsValue_get;
}
 bool getAllowed_get() {
   return m_getAllowed_get;
}
 bool containsKey_newKey() {
   return m_containsKey_newKey;
}
 bool containsValue_newKey() {
   return m_containsValue_newKey;
}
 bool getAllowed_newKey() {
   return m_getAllowed_newKey;
}
 int exactSize() {
   return m_exactSize;
}
 int minSize() {
   return m_minSize;
}
 int maxSize() {
   return m_maxSize;
}

//================================================================================
// setter methods

 void containsKey_none(bool abool) {
   m_containsKey_none = abool;
}

 void containsValue_none(bool abool) {
   m_containsValue_none = abool;
}

 void containsKey_invalidate(bool abool) {
   m_containsKey_invalidate = abool;
}

 void containsValue_invalidate(bool abool) {
   m_containsValue_invalidate = abool;
}

 void containsKey_localInvalidate(bool abool) {
   m_containsKey_localInvalidate = abool;
}

 void containsValue_localInvalidate(bool abool) {
   m_containsValue_localInvalidate = abool;
}

 void containsKey_destroy(bool abool) {
   m_containsKey_destroy = abool;
}

 void containsValue_destroy(bool abool) {
   m_containsValue_destroy = abool;
}

 void containsKey_localDestroy(bool abool) {
   m_containsKey_localDestroy = abool;
}

 void containsValue_localDestroy(bool abool) {
   m_containsValue_localDestroy = abool;
}

 void containsKey_update(bool abool) {
   m_containsKey_update = abool;
}

 void containsValue_update(bool abool) {
   m_containsValue_update = abool;
}

 void containsKey_get(bool abool) {
   m_containsKey_get = abool;
}

 void containsValue_get(bool abool) {
   m_containsValue_get = abool;
}

 void containsKey_newKey(bool abool) {
   m_containsKey_newKey = abool;
}

 void containsValue_newKey(bool abool) {
   m_containsValue_newKey = abool;
}

 void exactSize(int anInt) {
   m_exactSize = anInt;
}

 void minSize(int anInt) {
   m_minSize = anInt;
}

 void maxSize(int anInt) {
   m_maxSize = anInt;
}
 void valueIsUpdated(bool abool) {
   m_valueIsUpdated = abool;
}

//================================================================================
std::string toString() {
   std::string aStr;
   aStr+="\n   m_containsKey_none: ";
   aStr+=  m_containsKey_none?"true":"false";
   aStr+="\n   m_containsValue_none: " ;
   aStr+= m_containsValue_none?"true":"false";
   aStr+="\n   m_containsKey_invalidate: " ;
   aStr+= m_containsKey_invalidate?"true":"false";
   aStr+="\n   m_containsValue_invalidate: " ;
   aStr+= m_containsValue_invalidate?"true":"false";
   aStr+="\n   m_containsKey_localInvalidate: ";
   aStr+= m_containsKey_localInvalidate?"true":"false";
   aStr+="\n   m_containsValue_localInvalidate: " ;
   aStr+= m_containsValue_localInvalidate?"true":"false";
   aStr+="\n   m_containsKey_destroy: ";
   aStr+= m_containsKey_destroy?"true":"false";
   aStr+="\n   m_containsValue_destroy: " ;
   aStr+= m_containsValue_destroy?"true":"false";
   aStr+="\n   m_containsKey_localDestroy: " ;
   aStr+= m_containsKey_localDestroy?"true":"false";
   aStr+="\n   m_containsValue_localDestroy: " ;
   aStr+= m_containsValue_localDestroy?"true":"false";
   aStr+="\n   m_containsKey_update: " ;
   aStr+= m_containsKey_update?"true":"false";
   aStr+="\n   m_containsValue_update: " ;
   aStr+= m_containsValue_update?"true":"false";
   aStr+="\n   m_containsKey_get: " ;
   aStr+= m_containsKey_get?"true":"false";
   aStr+="\n   m_containsValue_get: ";
   aStr+= m_containsValue_get?"true":"false";
   aStr+="\n   m_containsKey_newKey: " ;
   aStr+= m_containsKey_newKey?"true":"false";
   aStr+="\n   m_containsValue_newKey: " ;
   aStr+= m_containsValue_newKey?"true":"false";
   aStr+="\n";
   //aStr+="   m_exactSize: " aStr+= m_exactSize;
   //aStr+="   m_minSize: " aStr+= m_minSize;
   //aStr+="   m_maxSize: " aStr+= m_maxSize;
   return aStr;
}

};
} // namespace testframework
} // namespace gemfire
#endif // __ExpectedRegionContent_hpp__
