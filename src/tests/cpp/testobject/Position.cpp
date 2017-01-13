/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "Position.hpp"
#include <cwchar>
#include <wchar.h>

using namespace gemfire;
using namespace testobject;

int32_t Position::cnt = 0;

Position::Position() { init(); }

Position::Position(const char* id, int32_t out) {
  init();
  secId = CacheableString::create(id);
  qty = out - (cnt % 2 == 0 ? 1000 : 100);
  mktValue = qty * 1.2345998;
  sharesOutstanding = out;
  secType = new wchar_t[(wcslen(L"a") + 1)];
  wcscpy(secType, L"a");
  pid = cnt++;
}

// This constructor is just for some internal data validation test
Position::Position(int32_t iForExactVal) {
  init();
  char* id = new char[iForExactVal + 2];
  for (int i = 0; i <= iForExactVal; i++) {
    id[i] = 'a';
  }
  id[iForExactVal + 1] = '\0';
  secId = CacheableString::create(id);
  delete[] id;
  qty = (iForExactVal % 2 == 0 ? 1000 : 100);
  mktValue = qty * 2;
  sharesOutstanding = iForExactVal;
  secType = new wchar_t[(wcslen(L"a") + 1)];
  wcscpy(secType, L"a");
  pid = iForExactVal;
}

Position::~Position() {
  if (secType != NULL) {
    // free(secType);
    delete[] secType;
  }
}

void Position::init() {
  avg20DaysVol = 0;
  bondRating = NULLPTR;
  convRatio = 0.0;
  country = NULLPTR;
  delta = 0.0;
  industry = 0;
  issuer = 0;
  mktValue = 0.0;
  qty = 0.0;
  secId = NULLPTR;
  secLinks = NULLPTR;
  secType = NULL;
  sharesOutstanding = 0;
  underlyer = NULLPTR;
  volatility = 0;
  pid = 0;
}

void Position::toData(gemfire::DataOutput& output) const {
  output.writeInt(avg20DaysVol);
  output.writeObject(bondRating);
  output.writeDouble(convRatio);
  output.writeObject(country);
  output.writeDouble(delta);
  output.writeInt(industry);
  output.writeInt(issuer);
  output.writeDouble(mktValue);
  output.writeDouble(qty);
  output.writeObject(secId);
  output.writeObject(secLinks);
  output.writeUTF(secType);
  output.writeInt(sharesOutstanding);
  output.writeObject(underlyer);
  output.writeInt(volatility);
  output.writeInt(pid);
}

gemfire::Serializable* Position::fromData(gemfire::DataInput& input) {
  input.readInt(&avg20DaysVol);
  input.readObject(bondRating);
  input.readDouble(&convRatio);
  input.readObject(country);
  input.readDouble(&delta);
  input.readInt(&industry);
  input.readInt(&issuer);
  input.readDouble(&mktValue);
  input.readDouble(&qty);
  input.readObject(secId);
  input.readObject(secLinks);
  input.readUTF(&secType);
  input.readInt(&sharesOutstanding);
  input.readObject(underlyer);
  input.readInt(&volatility);
  input.readInt(&pid);
  return this;
}

CacheableStringPtr Position::toString() const {
  char buf[2048];
  sprintf(buf,
          "Position Object:[ secId=%s type=%ls sharesOutstanding=%d id=%d ]",
          secId->toString(), this->secType, this->sharesOutstanding, this->pid);
  return CacheableString::create(buf);
}
