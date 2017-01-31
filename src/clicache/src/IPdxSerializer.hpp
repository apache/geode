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

#pragma once

#include "gf_defs.hpp"
#include "IPdxWriter.hpp"
#include "IPdxReader.hpp"

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      /// <summary>
      /// The IPdxSerializer interface allows domain classes to be 
      /// serialized and deserialized as PDXs without modification 
      /// of the domain class.
      /// A domain class should register delgate <see cref="Serializable.RegisterPdxType" /> to create new 
      /// instance of type for de-serilization.
      /// </summary>
      public interface class IPdxSerializer
      {
      public:

        /// <summary>
        /// Serializes this object in geode PDX format.
        /// </summary>
        /// <param name="o">
        /// the object which need to serialize
        /// </param>
        /// <param name="writer">
        /// the IPdxWriter object to use for serializing the object
        /// </param>
        bool ToData(Object^ o, IPdxWriter^ writer);

        /// <summary>
        /// Deserialize this object.
        /// </summary>
        /// <param name="classname">
        /// the classname whose object need to de-serialize
        /// </param>
        /// <param name="reader">
        /// the IPdxReader stream to use for reading the object data
        /// </param>
        Object^ FromData(String^ classname, IPdxReader^ reader);

      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

