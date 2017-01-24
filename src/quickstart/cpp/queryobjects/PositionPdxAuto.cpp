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

#include "PositionPdxAuto.hpp"
#include <cwchar>
#include <wchar.h>

using namespace gemfire;
using namespace testobject;

int32_t PositionPdxAuto::cnt = 0;

PositionPdxAuto::PositionPdxAuto() { init(); }

PositionPdxAuto::PositionPdxAuto(const char* id, int32_t out) {
  init();

  size_t strSize = strlen(id) + 1;
  secId = new char[strSize];
  memcpy(secId, id, strSize);

  qty = out * (cnt % 2 == 0 ? 10.0 : 100.0);
  mktValue = qty * 1.2345998;
  sharesOutstanding = out;
  // secType = ( wchar_t * )malloc( ( wcslen( L"a" ) + 1 ) * sizeof( wchar_t )
  // );
  secType = new char[(strlen("a") + 1)];
  strcpy(secType, "a");

  pid = cnt++;
}

// This constructor is just for some internal data validation test
PositionPdxAuto::PositionPdxAuto(int32_t iForExactVal) {
  init();

  char* id = new char[iForExactVal + 2];
  for (int i = 0; i <= iForExactVal; i++) {
    id[i] = 'a';
  }
  id[iForExactVal + 1] = '\0';
  size_t strSize = strlen(id) + 1;
  secId = new char[strSize];
  memcpy(secId, id, strSize);

  delete[] id;
  qty = (iForExactVal % 2 == 0 ? 1000 : 100);
  mktValue = qty * 2;
  sharesOutstanding = iForExactVal;
  // secType = ( wchar_t * )malloc( ( wcslen( L"a" ) + 1 ) * sizeof( wchar_t )
  // );
  secType = new char[(strlen("a") + 1)];
  strcpy(secType, "a");
  pid = iForExactVal;
}

PositionPdxAuto::~PositionPdxAuto() {
  if (secType != NULL) {
    // free(secType);
    delete[] secType;
    secType = NULL;
  }

  if (secId != NULL) {
    // free(secId);
    delete[] secId;
    secId = NULL;
  }
}

void PositionPdxAuto::init() {
  avg20DaysVol = 0;
  bondRating = NULL;
  convRatio = 0.0;
  country = NULL;
  delta = 0.0;
  industry = 0;
  issuer = 0;
  mktValue = 0.0;
  qty = 0.0;
  secId = NULL;
  secLinks = NULL;
  secType = NULL;
  sharesOutstanding = 0;
  underlyer = NULL;
  volatility = 0;
  pid = 0;
}

CacheableStringPtr PositionPdxAuto::toString() const {
  char buf[1024];
  sprintf(buf, "PositionPdx Object:[ id=%d ]", this->pid);
  return CacheableString::create(buf);
}
