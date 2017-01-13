/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifdef TESTTASK
#undef TESTTASK
#endif
#ifdef TEST_EXPORT
#undef TEST_EXPORT
#endif

#if defined(_WIN32)
#define TESTTASK extern "C" __declspec(dllexport) int32_t
#define TEST_EXPORT extern "C" __declspec(dllexport)
#else
#define TESTTASK extern "C" int32_t
#define TEST_EXPORT extern "C"
#endif
