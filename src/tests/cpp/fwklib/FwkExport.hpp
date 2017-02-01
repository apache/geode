#pragma once

#ifndef APACHE_GEODE_GUARD_dd058cd17302e55845b453e0c42c27c9
#define APACHE_GEODE_GUARD_dd058cd17302e55845b453e0c42c27c9

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

#endif // APACHE_GEODE_GUARD_dd058cd17302e55845b453e0c42c27c9
