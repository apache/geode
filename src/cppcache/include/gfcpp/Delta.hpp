#pragma once

#ifndef GEODE_GFCPP_DELTA_H_
#define GEODE_GFCPP_DELTA_H_


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

/**
 * @file
 */

#include "Cacheable.hpp"
#include "DataInput.hpp"
#include "DataOutput.hpp"
namespace apache {
namespace geode {
namespace client {

/**
 * This interface is used for delta propagation.
 * To use delta propagation, an application class must derive from <code>class
 * Delta</code> as well as <code>class Cacheable</code> publicly.
 * The methods <code>hasDelta( ), toDelta( )</code> and <code>fromDelta(
 * )</code> must be implemented by the class, as these methods are used by
 * GemFire
 * to detect the presence of delta in an object, to serialize the delta, and to
 * apply a serialized delta to an existing object
 * of the class.
 */

class Delta {
 public:
  /**
   * <code>hasDelta( )</code> is invoked by GemFire during <code>Region::put(
   * CacheableKeyPtr, CacheablePtr )</code> to determine if the object contains
   * a delta.
   * If <code>hasDelta( )</code> returns true, the delta in the object is
   * serialized by invoking <code>Delta::toDelta( DataOutput& )</code>.
   * If <code>hasDelta( )</code> returns false, the object is serialized by
   * invoking <code>Cacheable::toData( DataOutput& )</code>.
   */
  virtual bool hasDelta() = 0;

  /**
   * Writes out delta information to out in a user-defined format. This is
   * invoked on an application object after GemFire determines the presence
   * of delta in it by calling <code>hasDelta()</code> on the object.
   *
   * @throws IOException
   */
  virtual void toDelta(DataOutput& out) const = 0;

  /**
   * Reads in delta information to this object in a user-defined format. This is
   * invoked on an existing application object after GemFire determines the
   * presence of delta in <code>DataInput</code> instance.
   *
   * @throws IOException
   * @throws InvalidDeltaException if the delta in the <code>DataInput</code>
   * instance cannot be applied
   * to this instance (possible causes may include mismatch of Delta version or
   * logic error).
   */
  virtual void fromDelta(DataInput& in) = 0;

  /**
   * Creates a copy of the object on which delta is to be applied via
   * notification.
   * The region attribute for cloning must be set to 'true' in order to enable
   * cloning.
   * The default implementation of this method creates an object clone by first
   * serializing the object into
   * a buffer, then deserializing from the buffer thus creating a clone of the
   * original.
   */
  virtual DeltaPtr clone();

  virtual ~Delta() {}
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_GFCPP_DELTA_H_
