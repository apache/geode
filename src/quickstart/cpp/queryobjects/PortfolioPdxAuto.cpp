#include "PortfolioPdxAuto.hpp"

using namespace gemfire;
using namespace testobject;

const char* /*PortfolioPdx::*/ secIds[] = {
    "SUN", "IBM", "YHOO", "GOOG", "MSFT", "AOL", "APPL", "ORCL", "SAP", "DELL"};

PortfolioPdxAuto::PortfolioPdxAuto(int32_t i, int32_t size,
                                   char** nm) /* : names(nm)*/ {
  id = i;

  char pkidbuf[256];
  sprintf(pkidbuf, "%d", i);
  size_t strSize = strlen(pkidbuf) + 1;
  pkid = new char[strSize];
  memcpy(pkid, pkidbuf, strSize);

  const char* statusStr = (i % 2 == 0) ? "active" : "inactive";
  int32_t statusSize = static_cast<int32_t>(strlen(statusStr)) + 1;
  status = new char[statusSize];
  memcpy(status, statusStr, statusSize);

  char buf[100];
  sprintf(buf, "type%d", (i % 3));
  size_t strSize2 = strlen(buf) + 1;
  type = new char[strSize2];
  memcpy(type, buf, strSize2);

  int numSecIds = sizeof(secIds) / sizeof(char*);
  position1 = new PositionPdxAuto(secIds[PositionPdxAuto::cnt % numSecIds],
                                  PositionPdxAuto::cnt * 1000);
  if (i % 2 != 0) {
    position2 = new PositionPdxAuto(secIds[PositionPdxAuto::cnt % numSecIds],
                                    PositionPdxAuto::cnt * 1000);
  } else {
    position2 = NULLPTR;
  }
  positions = CacheableHashMap::create();
  positions->insert(
      CacheableString::create(secIds[PositionPdxAuto::cnt % numSecIds]),
      position1);

  if (size > 0) {
    newVal = new int8_t[size];
    for (int index = 0; index < size; index++) {
      newVal[index] = (int8_t)'B';
    }
  }
  newValSize = size;

  time_t timeVal = 1310447869;
  creationDate = CacheableDate::create(timeVal);
  arrayNull = NULL;
  arrayZeroSize = new int8_t[0];
  arrayZeroSizeSize = 1;
}

PortfolioPdxAuto::~PortfolioPdxAuto() {
  if (newVal != NULL) {
    delete[] newVal;
    newVal = NULL;
  }
  if (status != NULL) {
    delete[] status;
    status = NULL;
  }
  if (pkid != NULL) {
    delete[] pkid;
    pkid = NULL;
  }

  if (type != NULL) {
    delete[] type;
    type = NULL;
  }

  if (newVal != NULL) {
    delete[] newVal;
    newVal = NULL;
  }
}

CacheableStringPtr PortfolioPdxAuto::toString() const {
  LOGINFO("PortfolioPdxAuto::toString() Start");
  char idbuf[1024];
  sprintf(idbuf, "PortfolioPdxObject: [ id=%d ]", id);

  char pkidbuf[1024];
  if (pkid != NULL) {
    sprintf(pkidbuf, " status=%s type=%s pkid=%s\n", this->status, this->type,
            this->pkid);
  } else {
    sprintf(pkidbuf, " status=%s type=%s pkid=%s\n", this->status, this->type,
            this->pkid);
  }
  char position1buf[2048];
  if (position1 != NULLPTR) {
    sprintf(position1buf, "\t\t\t  P1: %s", position1->toString()->asChar());
  } else {
    sprintf(position1buf, "\t\t\t  P1: %s", "NULL");
  }
  char position2buf[2048];
  if (position2 != NULLPTR) {
    sprintf(position2buf, " P2: %s", position2->toString()->asChar());
  } else {
    sprintf(position2buf, " P2: %s ]", "NULL");
  }
  char creationdatebuf[2048];
  if (creationDate != NULLPTR) {
    sprintf(creationdatebuf, "creation Date %s",
            creationDate->toString()->asChar());
  } else {
    sprintf(creationdatebuf, "creation Date %s", "NULL");
  }

  char stringBuf[7000];
  sprintf(stringBuf, "%s%s%s%s%s", idbuf, pkidbuf, creationdatebuf,
          position1buf, position2buf);
  return CacheableString::create(stringBuf);

  return CacheableString::create(idbuf);
}
