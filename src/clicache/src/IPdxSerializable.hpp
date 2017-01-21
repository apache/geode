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
				/// When a domain class implements PdxSerializable it marks 
				/// itself as a PDX. 
				/// The implementation of toData provides the serialization 
				/// code and fromData provides the deserialization code. 
				/// These methods also define each field name and field type 
				/// of the PDX. Domain classes should serialize and de-serialize 
				/// all its member fields in same order in toData and fromData 
				/// method. 
				/// A domain class which implements this interface should register delgate <see cref="Serializable.RegisterPdxType" /> to create new 
				/// instance of type for de-serilization.
				/// </summary>
				public interface class IPdxSerializable
				{
				public:

					/// <summary>
					/// Serializes this object in gemfire PDX format.
					/// </summary>
					/// <param name="writer">
					/// the IPdxWriter object to use for serializing the object
					/// </param>
					void ToData( IPdxWriter^ writer );

					/// <summary>
					/// Deserialize this object.
					/// </summary>
					/// <param name="reader">
					/// the IPdxReader stream to use for reading the object data
					/// </param>
					void FromData( IPdxReader^ reader );

				};
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

