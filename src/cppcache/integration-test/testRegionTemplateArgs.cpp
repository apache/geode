/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testRegionTemplateArgs"

#include <gfcpp/GemfireCppCache.hpp>
#include <CacheRegionHelper.hpp>
#include "fw_helper.hpp"

using namespace gemfire;

bool CheckBytesEqual(CacheableBytesPtr result, CacheablePtr expected) {
  CacheableBytesPtr expectedPtr = dynCast<CacheableBytesPtr>(expected);
  // Assume that the bytes are really a char*
  return (strcmp((char*)result->value(), (char*)expectedPtr->value()) == 0);
}

// This test checks the template methods of Region API with all possible
// combinations of key and value types.

BEGIN_TEST(CheckTemplates)
  {
    CacheFactoryPtr cacheFactoryPtr = CacheFactory::createCacheFactory();
    CachePtr cache = cacheFactoryPtr->create();
    AttributesFactory afact;
    RegionAttributesPtr attrs = afact.createRegionAttributes();
    RegionPtr regPtr;
    CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cache.ptr());
    cacheImpl->createRegion("TestRegion", attrs, regPtr);

    const char charKey[] = "test key";
    const char charVal[] = "test value";
    int intKey = 10;
    int intVal = 100;
    const char stringKey[] = "test string key";
    const char stringVal[] = "test string value";
    const char baseKey[] = "test base key";
    const char baseVal[] = "test base value";
    int int32Key = 100;
    CacheableStringPtr stringPtr = CacheableString::create(stringKey);
    CacheableInt32Ptr int32Ptr = CacheableInt32::create(int32Key);
    CacheableBytesPtr bytesPtr = CacheableBytes::create(
        reinterpret_cast<const uint8_t*>(stringVal), sizeof(stringVal));
    CacheableKeyPtr keyPtr = CacheableString::create(baseKey);
    CacheablePtr valPtr = CacheableBytes::create(
        reinterpret_cast<const uint8_t*>(baseVal), sizeof(baseVal));

    CacheableBytesPtr resValPtr;
    CacheableStringPtr resStringPtr;
    CacheableInt32Ptr resInt32Ptr;
    RegionEntryPtr resEntryPtr;

    /* ------ Test puts/gets of various shapes and sizes --------- */

    // With valPtr

    regPtr->put(keyPtr, valPtr);
    regPtr->put(stringPtr, valPtr);
    regPtr->put(int32Ptr, valPtr);
    regPtr->put(charKey, valPtr);
    regPtr->put(intKey, valPtr);

    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(keyPtr));
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "put/get:: incorrect valPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(stringPtr));
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "put/get:: incorrect valPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(int32Ptr));
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "put/get:: incorrect valPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(intKey));
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "put/get:: incorrect valPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(charKey));
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "put/get:: incorrect valPtr value");

    // End with valPtr

    // With bytesPtr

    regPtr->put(keyPtr, bytesPtr);
    regPtr->put(stringPtr, bytesPtr);
    regPtr->put(int32Ptr, bytesPtr);
    regPtr->put(charKey, bytesPtr);
    regPtr->put(intKey, bytesPtr);

    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(keyPtr));
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "put/get:: incorrect bytesPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(stringPtr));
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "put/get:: incorrect bytesPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(int32Ptr));
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "put/get:: incorrect bytesPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(intKey));
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "put/get:: incorrect bytesPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(charKey));
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "put/get:: incorrect bytesPtr value");

    // End with bytesPtr

    // With stringPtr

    regPtr->put(keyPtr, stringPtr);
    regPtr->put(stringPtr, stringPtr);
    regPtr->put(int32Ptr, stringPtr);
    regPtr->put(charKey, stringPtr);
    regPtr->put(intKey, stringPtr);

    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(keyPtr));
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "put/get:: incorrect stringPtr value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(stringPtr));
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "put/get:: incorrect stringPtr value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(int32Ptr));
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "put/get:: incorrect stringPtr value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(intKey));
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "put/get:: incorrect stringPtr value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(charKey));
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "put/get:: incorrect stringPtr value");

    // End with stringPtr

    // With int32Ptr

    regPtr->put(keyPtr, int32Ptr);
    regPtr->put(stringPtr, int32Ptr);
    regPtr->put(int32Ptr, int32Ptr);
    regPtr->put(charKey, int32Ptr);
    regPtr->put(intKey, int32Ptr);

    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(keyPtr));
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "put/get:: incorrect int32Ptr value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(stringPtr));
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "put/get:: incorrect int32Ptr value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(int32Ptr));
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "put/get:: incorrect int32Ptr value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(intKey));
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "put/get:: incorrect int32Ptr value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(charKey));
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "put/get:: incorrect int32Ptr value");

    // End with int32Ptr

    // With charVal

    regPtr->put(keyPtr, charVal);
    regPtr->put(stringPtr, charVal);
    regPtr->put(int32Ptr, charVal);
    regPtr->put(charKey, charVal);
    regPtr->put(intKey, charVal);

    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(keyPtr));
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "put/get:: incorrect charVal value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(stringPtr));
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "put/get:: incorrect charVal value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(int32Ptr));
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "put/get:: incorrect charVal value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(intKey));
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "put/get:: incorrect charVal value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(charKey));
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "put/get:: incorrect charVal value");

    // End with charVal

    // With intVal

    regPtr->put(keyPtr, intVal);
    regPtr->put(stringPtr, intVal);
    regPtr->put(int32Ptr, intVal);
    regPtr->put(charKey, intVal);
    regPtr->put(intKey, intVal);

    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(keyPtr));
    ASSERT(resInt32Ptr->value() == intVal, "put/get:: incorrect intVal value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(stringPtr));
    ASSERT(resInt32Ptr->value() == intVal, "put/get:: incorrect intVal value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(int32Ptr));
    ASSERT(resInt32Ptr->value() == intVal, "put/get:: incorrect intVal value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(intKey));
    ASSERT(resInt32Ptr->value() == intVal, "put/get:: incorrect intVal value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(charKey));
    ASSERT(resInt32Ptr->value() == intVal, "put/get:: incorrect intVal value");

    // End with intVal

    /* ---------- End put/get tests ------------ */

    /* ------ Test creates/gets/localDestroys of various shapes and sizes
     * --------- */

    // With valPtr

    regPtr->destroy(keyPtr);
    regPtr->destroy(stringPtr);
    regPtr->destroy(int32Ptr);
    regPtr->destroy(charKey);
    regPtr->destroy(intKey);

    regPtr->create(keyPtr, valPtr);
    regPtr->create(stringPtr, valPtr);
    regPtr->create(int32Ptr, valPtr);
    regPtr->create(charKey, valPtr);
    regPtr->create(intKey, valPtr);

    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(keyPtr));
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "create/get/localDestroy:: incorrect valPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(stringPtr));
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "create/get/localDestroy:: incorrect valPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(int32Ptr));
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "create/get/localDestroy:: incorrect valPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(intKey));
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "create/get/localDestroy:: incorrect valPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(charKey));
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "create/get/localDestroy:: incorrect valPtr value");

    regPtr->localInvalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/get/localDestroy:: key not destroyed");

    // End with valPtr

    // With bytesPtr

    regPtr->create(keyPtr, bytesPtr);
    regPtr->create(stringPtr, bytesPtr);
    regPtr->create(int32Ptr, bytesPtr);
    regPtr->create(charKey, bytesPtr);
    regPtr->create(intKey, bytesPtr);

    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(keyPtr));
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "create/get/localDestroy:: incorrect bytesPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(stringPtr));
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "create/get/localDestroy:: incorrect bytesPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(int32Ptr));
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "create/get/localDestroy:: incorrect bytesPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(intKey));
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "create/get/localDestroy:: incorrect bytesPtr value");
    resValPtr = dynCast<CacheableBytesPtr>(regPtr->get(charKey));
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "create/get/localDestroy:: incorrect bytesPtr value");

    regPtr->localInvalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/get/localDestroy:: key not destroyed");

    // End with bytesPtr

    // With stringPtr

    regPtr->create(keyPtr, stringPtr);
    regPtr->create(stringPtr, stringPtr);
    regPtr->create(int32Ptr, stringPtr);
    regPtr->create(charKey, stringPtr);
    regPtr->create(intKey, stringPtr);

    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(keyPtr));
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "create/get/localDestroy:: incorrect stringPtr value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(stringPtr));
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "create/get/localDestroy:: incorrect stringPtr value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(int32Ptr));
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "create/get/localDestroy:: incorrect stringPtr value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(intKey));
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "create/get/localDestroy:: incorrect stringPtr value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(charKey));
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "create/get/localDestroy:: incorrect stringPtr value");

    regPtr->localInvalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/get/localDestroy:: key not destroyed");

    // End with stringPtr

    // With int32Ptr

    regPtr->create(keyPtr, int32Ptr);
    regPtr->create(stringPtr, int32Ptr);
    regPtr->create(int32Ptr, int32Ptr);
    regPtr->create(charKey, int32Ptr);
    regPtr->create(intKey, int32Ptr);

    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(keyPtr));
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "create/get/localDestroy:: incorrect int32Ptr value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(stringPtr));
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "create/get/localDestroy:: incorrect int32Ptr value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(int32Ptr));
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "create/get/localDestroy:: incorrect int32Ptr value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(intKey));
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "create/get/localDestroy:: incorrect int32Ptr value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(charKey));
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "create/get/localDestroy:: incorrect int32Ptr value");

    regPtr->localInvalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/get/localDestroy:: key not destroyed");

    // End with int32Ptr

    // With charVal

    regPtr->create(keyPtr, charVal);
    regPtr->create(stringPtr, charVal);
    regPtr->create(int32Ptr, charVal);
    regPtr->create(charKey, charVal);
    regPtr->create(intKey, charVal);

    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(keyPtr));
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "create/get/localDestroy:: incorrect charVal value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(stringPtr));
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "create/get/localDestroy:: incorrect charVal value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(int32Ptr));
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "create/get/localDestroy:: incorrect charVal value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(intKey));
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "create/get/localDestroy:: incorrect charVal value");
    resStringPtr = dynCast<CacheableStringPtr>(regPtr->get(charKey));
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "create/get/localDestroy:: incorrect charVal value");

    regPtr->localInvalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/get/localDestroy:: key not destroyed");

    // End with charVal

    // With intVal

    regPtr->create(keyPtr, intVal);
    regPtr->create(stringPtr, intVal);
    regPtr->create(int32Ptr, intVal);
    regPtr->create(charKey, intVal);
    regPtr->create(intKey, intVal);

    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(keyPtr));
    ASSERT(resInt32Ptr->value() == intVal,
           "create/get/localDestroy:: incorrect intVal value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(stringPtr));
    ASSERT(resInt32Ptr->value() == intVal,
           "create/get/localDestroy:: incorrect intVal value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(int32Ptr));
    ASSERT(resInt32Ptr->value() == intVal,
           "create/get/localDestroy:: incorrect intVal value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(intKey));
    ASSERT(resInt32Ptr->value() == intVal,
           "create/get/localDestroy:: incorrect intVal value");
    resInt32Ptr = dynCast<CacheableInt32Ptr>(regPtr->get(charKey));
    ASSERT(resInt32Ptr->value() == intVal,
           "create/get/localDestroy:: incorrect intVal value");

    regPtr->localInvalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/get/localDestroy:: key not destroyed");
    regPtr->localInvalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/get/localDestroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/get/localDestroy:: key not invalidated");
    regPtr->localDestroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/get/localDestroy:: key not destroyed");

    // End with intVal

    /* ---------- End create/get/localDestroy tests ------------ */

    /* ------ Test creates/getEntry/destroys of various shapes and sizes
     * --------- */

    // With valPtr

    regPtr->create(keyPtr, valPtr);
    regPtr->create(stringPtr, valPtr);
    regPtr->create(int32Ptr, valPtr);
    regPtr->create(charKey, valPtr);
    regPtr->create(intKey, valPtr);

    resEntryPtr = regPtr->getEntry(keyPtr);
    resValPtr = dynCast<CacheableBytesPtr>(resEntryPtr->getValue());
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "create/getEntry/destroy:: incorrect valPtr value");
    resEntryPtr = regPtr->getEntry(stringPtr);
    resValPtr = dynCast<CacheableBytesPtr>(resEntryPtr->getValue());
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "create/getEntry/destroy:: incorrect valPtr value");
    resEntryPtr = regPtr->getEntry(int32Ptr);
    resValPtr = dynCast<CacheableBytesPtr>(resEntryPtr->getValue());
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "create/getEntry/destroy:: incorrect valPtr value");
    resEntryPtr = regPtr->getEntry(charKey);
    resValPtr = dynCast<CacheableBytesPtr>(resEntryPtr->getValue());
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "create/getEntry/destroy:: incorrect valPtr value");
    resEntryPtr = regPtr->getEntry(intKey);
    resValPtr = dynCast<CacheableBytesPtr>(resEntryPtr->getValue());
    ASSERT(CheckBytesEqual(resValPtr, valPtr),
           "create/getEntry/destroy:: incorrect valPtr value");

    regPtr->invalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/getEntry/destroy:: key not destroyed");

    // End with valPtr

    // With bytesPtr

    regPtr->create(keyPtr, bytesPtr);
    regPtr->create(stringPtr, bytesPtr);
    regPtr->create(int32Ptr, bytesPtr);
    regPtr->create(charKey, bytesPtr);
    regPtr->create(intKey, bytesPtr);

    resEntryPtr = regPtr->getEntry(keyPtr);
    resValPtr = dynCast<CacheableBytesPtr>(resEntryPtr->getValue());
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "create/getEntry/destroy:: incorrect bytesPtr value");
    resEntryPtr = regPtr->getEntry(stringPtr);
    resValPtr = dynCast<CacheableBytesPtr>(resEntryPtr->getValue());
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "create/getEntry/destroy:: incorrect bytesPtr value");
    resEntryPtr = regPtr->getEntry(int32Ptr);
    resValPtr = dynCast<CacheableBytesPtr>(resEntryPtr->getValue());
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "create/getEntry/destroy:: incorrect bytesPtr value");
    resEntryPtr = regPtr->getEntry(charKey);
    resValPtr = dynCast<CacheableBytesPtr>(resEntryPtr->getValue());
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "create/getEntry/destroy:: incorrect bytesPtr value");
    resEntryPtr = regPtr->getEntry(intKey);
    resValPtr = dynCast<CacheableBytesPtr>(resEntryPtr->getValue());
    ASSERT(CheckBytesEqual(resValPtr, bytesPtr),
           "create/getEntry/destroy:: incorrect bytesPtr value");

    regPtr->invalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/getEntry/destroy:: key not destroyed");

    // End with bytesPtr

    // With stringPtr

    regPtr->create(keyPtr, stringPtr);
    regPtr->create(stringPtr, stringPtr);
    regPtr->create(int32Ptr, stringPtr);
    regPtr->create(charKey, stringPtr);
    regPtr->create(intKey, stringPtr);

    resEntryPtr = regPtr->getEntry(keyPtr);
    resStringPtr = dynCast<CacheableStringPtr>(resEntryPtr->getValue());
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "create/getEntry/destroy:: incorrect stringPtr value");
    resEntryPtr = regPtr->getEntry(stringPtr);
    resStringPtr = dynCast<CacheableStringPtr>(resEntryPtr->getValue());
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "create/getEntry/destroy:: incorrect stringPtr value");
    resEntryPtr = regPtr->getEntry(int32Ptr);
    resStringPtr = dynCast<CacheableStringPtr>(resEntryPtr->getValue());
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "create/getEntry/destroy:: incorrect stringPtr value");
    resEntryPtr = regPtr->getEntry(intKey);
    resStringPtr = dynCast<CacheableStringPtr>(resEntryPtr->getValue());
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "create/getEntry/destroy:: incorrect stringPtr value");
    resEntryPtr = regPtr->getEntry(charKey);
    resStringPtr = dynCast<CacheableStringPtr>(resEntryPtr->getValue());
    ASSERT(resStringPtr.ptr() == stringPtr.ptr(),
           "create/getEntry/destroy:: incorrect stringPtr value");

    regPtr->invalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/getEntry/destroy:: key not destroyed");

    // End with stringPtr

    // With int32Ptr

    regPtr->create(keyPtr, int32Ptr);
    regPtr->create(stringPtr, int32Ptr);
    regPtr->create(int32Ptr, int32Ptr);
    regPtr->create(charKey, int32Ptr);
    regPtr->create(intKey, int32Ptr);

    resEntryPtr = regPtr->getEntry(keyPtr);
    resInt32Ptr = dynCast<CacheableInt32Ptr>(resEntryPtr->getValue());
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "create/getEntry/destroy:: incorrect int32Ptr value");
    resEntryPtr = regPtr->getEntry(stringPtr);
    resInt32Ptr = dynCast<CacheableInt32Ptr>(resEntryPtr->getValue());
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "create/getEntry/destroy:: incorrect int32Ptr value");
    resEntryPtr = regPtr->getEntry(int32Ptr);
    resInt32Ptr = dynCast<CacheableInt32Ptr>(resEntryPtr->getValue());
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "create/getEntry/destroy:: incorrect int32Ptr value");
    resEntryPtr = regPtr->getEntry(intKey);
    resInt32Ptr = dynCast<CacheableInt32Ptr>(resEntryPtr->getValue());
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "create/getEntry/destroy:: incorrect int32Ptr value");
    resEntryPtr = regPtr->getEntry(charKey);
    resInt32Ptr = dynCast<CacheableInt32Ptr>(resEntryPtr->getValue());
    ASSERT(resInt32Ptr.ptr() == int32Ptr.ptr(),
           "create/getEntry/destroy:: incorrect int32Ptr value");

    regPtr->invalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/getEntry/destroy:: key not destroyed");

    // End with int32Ptr

    // With charVal

    regPtr->create(keyPtr, charVal);
    regPtr->create(stringPtr, charVal);
    regPtr->create(int32Ptr, charVal);
    regPtr->create(charKey, charVal);
    regPtr->create(intKey, charVal);

    resEntryPtr = regPtr->getEntry(keyPtr);
    resStringPtr = dynCast<CacheableStringPtr>(resEntryPtr->getValue());
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "create/getEntry/destroy:: incorrect charVal value");
    resEntryPtr = regPtr->getEntry(stringPtr);
    resStringPtr = dynCast<CacheableStringPtr>(resEntryPtr->getValue());
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "create/getEntry/destroy:: incorrect charVal value");
    resEntryPtr = regPtr->getEntry(int32Ptr);
    resStringPtr = dynCast<CacheableStringPtr>(resEntryPtr->getValue());
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "create/getEntry/destroy:: incorrect charVal value");
    resEntryPtr = regPtr->getEntry(intKey);
    resStringPtr = dynCast<CacheableStringPtr>(resEntryPtr->getValue());
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "create/getEntry/destroy:: incorrect charVal value");
    resEntryPtr = regPtr->getEntry(charKey);
    resStringPtr = dynCast<CacheableStringPtr>(resEntryPtr->getValue());
    ASSERT(strcmp(resStringPtr->asChar(), charVal) == 0,
           "create/getEntry/destroy:: incorrect charVal value");

    regPtr->invalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/getEntry/destroy:: key not destroyed");

    // End with charVal

    // With intVal

    regPtr->create(keyPtr, intVal);
    regPtr->create(stringPtr, intVal);
    regPtr->create(int32Ptr, intVal);
    regPtr->create(charKey, intVal);
    regPtr->create(intKey, intVal);

    resEntryPtr = regPtr->getEntry(keyPtr);
    resInt32Ptr = dynCast<CacheableInt32Ptr>(resEntryPtr->getValue());
    ASSERT(resInt32Ptr->value() == intVal,
           "create/getEntry/destroy:: incorrect intVal value");
    resEntryPtr = regPtr->getEntry(stringPtr);
    resInt32Ptr = dynCast<CacheableInt32Ptr>(resEntryPtr->getValue());
    ASSERT(resInt32Ptr->value() == intVal,
           "create/getEntry/destroy:: incorrect intVal value");
    resEntryPtr = regPtr->getEntry(int32Ptr);
    resInt32Ptr = dynCast<CacheableInt32Ptr>(resEntryPtr->getValue());
    ASSERT(resInt32Ptr->value() == intVal,
           "create/getEntry/destroy:: incorrect intVal value");
    resEntryPtr = regPtr->getEntry(intKey);
    resInt32Ptr = dynCast<CacheableInt32Ptr>(resEntryPtr->getValue());
    ASSERT(resInt32Ptr->value() == intVal,
           "create/getEntry/destroy:: incorrect intVal value");
    resEntryPtr = regPtr->getEntry(charKey);
    resInt32Ptr = dynCast<CacheableInt32Ptr>(resEntryPtr->getValue());
    ASSERT(resInt32Ptr->value() == intVal,
           "create/getEntry/destroy:: incorrect intVal value");

    regPtr->invalidate(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(keyPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(keyPtr);
    ASSERT(regPtr->containsKey(keyPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(stringPtr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(stringPtr);
    ASSERT(regPtr->containsKey(stringPtr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(int32Ptr);
    ASSERT(regPtr->containsKey(int32Ptr) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(charKey);
    ASSERT(regPtr->containsKey(charKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(charKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(charKey);
    ASSERT(regPtr->containsKey(charKey) == false,
           "create/getEntry/destroy:: key not destroyed");
    regPtr->invalidate(intKey);
    ASSERT(regPtr->containsKey(intKey) == true,
           "create/getEntry/destroy:: key not present");
    ASSERT(regPtr->containsValueForKey(intKey) == false,
           "create/getEntry/destroy:: key not invalidated");
    regPtr->destroy(intKey);
    ASSERT(regPtr->containsKey(intKey) == false,
           "create/getEntry/destroy:: key not destroyed");

    // End with intVal

    /* ---------- End create/getEntry/destroy tests ------------ */

    cache->close();
  }
END_TEST(CheckTemplates)
