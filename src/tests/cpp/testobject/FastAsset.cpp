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
#include "FastAsset.hpp"
#include "fwklib/GsRandom.hpp"

using namespace apache::geode::client;
using namespace testframework;
using namespace testobject;

FastAsset::FastAsset(int idx, int maxVal) : assetId(idx) {
  value = GsRandom::getInstance(12)->nextDouble(1, static_cast<double>(maxVal));
}

FastAsset::~FastAsset() {}
void FastAsset::toData(apache::geode::client::DataOutput& output) const {
  output.writeInt(static_cast<int32_t>(assetId));
  output.writeDouble(value);
}

apache::geode::client::Serializable* FastAsset::fromData(
    apache::geode::client::DataInput& input) {
  input.readInt(&assetId);
  input.readDouble(&value);
  return this;
}
