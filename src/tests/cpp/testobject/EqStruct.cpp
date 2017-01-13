/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "EqStruct.hpp"

using namespace gemfire;
using namespace testframework;
using namespace testobject;

EqStruct::EqStruct(int index) {
  myIndex = index;  // index
  state = (char *)"1";
  ACE_Time_Value startTime;
  startTime = ACE_OS::gettimeofday();
  ACE_UINT64 tusec = 0;
  startTime.to_usec(tusec);
  timestamp = tusec * 1000;
  executedPriceSum = 5.5;
  cxlQty = 10;
  isSyntheticOrder = 0;
  availQty = 100;
  positionQty = 10.0;
  isRestricted = 1;
  demandInd = (char *)"ASDSAD";
  side = (char *)"16";
  orderQty = 3000;
  price = 78.9;
  ordType = (char *)"D";
  stopPx = 22.3;
  senderCompID = (char *)"dsafdsf";
  tarCompID = (char *)"dsafsadfsaf";
  tarSubID = (char *)"rwetwj";
  handlInst = (char *)"M N";
  orderID = (char *)"sample";
  timeInForce = (char *)"4";
  clOrdID = (char *)"sample";
  orderCapacity = (char *)"6";
  cumQty = 0;
  symbol = (char *)"MSFT";
  symbolSfx = (char *)"0";
  execInst = (char *)"A";
  oldClOrdID = (char *)"";
  pegDifference = 0.1;
  discretionInst = (char *)"G";
  discretionOffset = 300.0;
  financeInd = (char *)"dsagfdsa";
  securityID = (char *)"MSFT.O";
  targetCompID = (char *)"LBLB";
  targetSubID = (char *)"EQUITY";
  isDoneForDay = 1;
  revisionSeqNum = 140;
  replaceQty = 0;
  usedClientAvailability = 45;
  clientAvailabilityKey = (char *)"UUUU";
  isIrregularSettlmnt = 1;

  var1 = (char *)"abcdefghijklmnopqrstuvwxyz";
  var2 = (char *)"abcdefghijklmnopqrstuvwxyz";
  var3 = (char *)"abcdefghijklmnopqrstuvwxyz";
  var4 = (char *)"abcdefghijklmnopqrstuvwxyz";
  var5 = (char *)"abcdefghijklmnopqrstuvwxyz";
  var6 = (char *)"abcdefghijklmnopqrstuvwxyz";
  var7 = (char *)"abcdefghijklmnopqrstuvwxyz";
  var8 = (char *)"abcdefghijklmnopqrstuvwxyz";
  var9 = (char *)"abcdefghijklmnopqrstuvwxyz";
}

EqStruct::~EqStruct() {}
void EqStruct::toData(gemfire::DataOutput &out) const {
  // Strings
  out.writeUTF(state);
  out.writeUTF(demandInd);
  out.writeUTF(side);
  out.writeUTF(ordType);
  out.writeUTF(senderCompID);
  out.writeUTF(tarCompID);
  out.writeUTF(tarSubID);
  out.writeUTF(handlInst);
  out.writeUTF(orderID);
  out.writeUTF(timeInForce);
  out.writeUTF(clOrdID);
  out.writeUTF(orderCapacity);
  out.writeUTF(symbol);
  out.writeUTF(symbolSfx);
  out.writeUTF(execInst);
  out.writeUTF(oldClOrdID);
  out.writeUTF(discretionInst);
  out.writeUTF(financeInd);
  out.writeUTF(securityID);
  out.writeUTF(targetCompID);
  out.writeUTF(targetSubID);
  out.writeUTF(clientAvailabilityKey);
  out.writeUTF(var1);
  out.writeUTF(var2);
  out.writeUTF(var3);
  out.writeUTF(var4);
  out.writeUTF(var5);
  out.writeUTF(var6);
  out.writeUTF(var7);
  out.writeUTF(var8);
  out.writeUTF(var9);

  // ints
  out.writeInt(myIndex);
  out.writeInt(cxlQty);
  out.writeInt(isSyntheticOrder);
  out.writeInt(isRestricted);
  out.writeInt(orderQty);
  out.writeInt(cumQty);
  out.writeInt(isDoneForDay);
  out.writeInt(revisionSeqNum);
  out.writeInt(replaceQty);
  out.writeInt(isIrregularSettlmnt);

  // longs
  out.writeInt(static_cast<int64_t>(timestamp));
  out.writeInt(static_cast<int64_t>(availQty));
  out.writeInt(static_cast<int64_t>(usedClientAvailability));

  // doubles
  out.writeDouble(executedPriceSum);
  out.writeDouble(positionQty);
  out.writeDouble(price);
  out.writeDouble(stopPx);
  out.writeDouble(pegDifference);
  out.writeDouble(discretionOffset);
}

gemfire::Serializable *EqStruct::fromData(gemfire::DataInput &in) {
  // Strings
  in.readUTF(&state);
  in.readUTF(&demandInd);
  in.readUTF(&side);
  in.readUTF(&ordType);
  in.readUTF(&senderCompID);
  in.readUTF(&tarCompID);
  in.readUTF(&tarSubID);
  in.readUTF(&handlInst);
  in.readUTF(&orderID);
  in.readUTF(&timeInForce);
  in.readUTF(&clOrdID);
  in.readUTF(&orderCapacity);
  in.readUTF(&symbol);
  in.readUTF(&symbolSfx);
  in.readUTF(&execInst);
  in.readUTF(&oldClOrdID);
  in.readUTF(&discretionInst);
  in.readUTF(&financeInd);
  in.readUTF(&securityID);
  in.readUTF(&targetCompID);
  in.readUTF(&targetSubID);
  in.readUTF(&clientAvailabilityKey);
  in.readUTF(&var1);
  in.readUTF(&var2);
  in.readUTF(&var3);
  in.readUTF(&var4);
  in.readUTF(&var5);
  in.readUTF(&var6);
  in.readUTF(&var7);
  in.readUTF(&var8);
  in.readUTF(&var9);

  // ints
  in.readInt(&myIndex);
  in.readInt(&cxlQty);
  in.readInt(&isSyntheticOrder);
  in.readInt(&isRestricted);
  in.readInt(&orderQty);
  in.readInt(&cumQty);
  in.readInt(&isDoneForDay);
  in.readInt(&revisionSeqNum);
  in.readInt(&replaceQty);
  in.readInt(&isIrregularSettlmnt);

  // longs
  in.readInt(reinterpret_cast<int64_t *>(&timestamp));
  in.readInt(&availQty);
  in.readInt(&usedClientAvailability);

  // doubles
  in.readDouble(&executedPriceSum);
  in.readDouble(&positionQty);
  in.readDouble(&price);
  in.readDouble(&stopPx);
  in.readDouble(&pegDifference);
  in.readDouble(&discretionOffset);
  return this;
}
CacheableStringPtr EqStruct::toString() const {
  char buf[102500];
  sprintf(buf, "EqStruct:[timestamp = %lld myIndex = %d cxlQty = %d ]",
          timestamp, myIndex, cxlQty);
  return CacheableString::create(buf);
}
