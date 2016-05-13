/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "FastAsset.hpp"
#include "fwklib/GsRandom.hpp"

using namespace gemfire;
using namespace testframework;
using namespace testobject;

FastAsset::FastAsset(int idx, int maxVal): assetId(idx)
{
  value = GsRandom::getInstance(12)-> nextDouble( 1, (double)maxVal );
}

FastAsset::~FastAsset() {
}
void FastAsset::toData( gemfire::DataOutput& output ) const {
  output.writeInt((int32_t)assetId);
  output.writeDouble(value);
}

gemfire::Serializable* FastAsset::fromData( gemfire::DataInput& input ){
  input.readInt((int32_t*)&assetId);
  input.readDouble(&value);
  return this;
}
