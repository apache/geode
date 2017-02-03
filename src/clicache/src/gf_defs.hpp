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

// These definitions are to help parsing by Doxygen.

/// @file gf_defs.hpp
/// API documentation helper file for the Doxygen source-comment-extraction tool.

#define STATICCLASS abstract sealed
#define GFINDEXER(x) default[ x ]

// Disable XML warnings
#pragma warning(disable: 4635)
#pragma warning(disable: 4638)
#pragma warning(disable: 4641)

// Disable native code generation warning
#pragma warning(disable: 4793)

// These provide Doxygen with namespace and file descriptions.

/// @namespace Apache::Geode
/// This namespace contains all the Geode .NET classes and utility classes.

/// @namespace Apache::Geode::Client
/// This namespace contains all the Geode .NET API classes and enumerations.

/// @namespace Apache::Geode::Client
/// This namespace contains all the Geode .NET Generics API classes and enumerations.

/// @namespace Apache::Geode::Client::Internal
/// This namespace contains internal Geode non-public .NET classes.

/// @namespace Apache::Geode::Client::Template
/// This namespace contains internal Geode .NET template classes.

/// @file gf_includes.hpp
/// Provides a commonly-used set of include directives.

/// @file AttributesFactory.hpp
/// Declares the AttributesFactory class.

/// @file AttributesMutator.hpp
/// Declares the AttributesMutator class.

/// @file CacheAttributes.hpp
/// Declares the CacheAttributes class.

/// @file CacheAttributesFactory.hpp
/// Declares the CacheAttributesFactory class.

/// @file CacheableBuiltins.hpp
/// Declares the CacheableBuiltinKey and CacheableBuiltinArray
/// template classes and their instantiations for CacheableBoolean,
/// CacheableByte, CacheableDouble, CacheableFloat, CacheableInt16,
/// CacheableInt32, CacheableInt64, CacheableBytes, CacheableDoubleArray,
/// CacheableFloatArray, CacheableInt16Array, CacheableInt32Array, 
/// CacheableInt64Array, BooleanArray and CharArray

/// @file CacheableBuiltins.hpp
/// Declared the built-in Geode serializable types.

/// @file CacheableDate.hpp
/// Declares the CacheableDate class.

/// @file CacheableFileName.hpp
/// Declares the CacheableFileName class.

/// @file CacheableHashMap.hpp
/// Declares the CacheableHashMap class.

/// @file CacheableHashSet.hpp
/// Declares the CacheableHashSet class.

/// @file CacheableKey.hpp
/// Declares the CacheableKey class.

/// @file CacheableObject.hpp
/// Declares the CacheableObject class.

/// @file CacheableObjectXml.hpp
/// Declares the CacheableObjectXml class.

/// @file CacheableString.hpp
/// Declares the CacheableString class.

/// @file CacheableStringArray.hpp
/// Declares the CacheableStringArray class.

/// @file CacheableUndefined.hpp
/// Declares the CacheableUndefined class.

/// @file CacheableVector.hpp
/// Declares the CacheableVector class.

/// @file CacheFactory.hpp
/// Declares the CacheFactory class.

/// @file Cache.hpp
/// Declares the Cache class.

/// @file CacheStatistics.hpp
/// Declares the CacheStatistics class.

/// @file CacheStatistics.hpp
/// Declares the CacheStatistics class.

/// @file DataInput.hpp
/// Declares the DataInput class.

/// @file DataOutput.hpp
/// Declares the DataOutput class.

/// @file DiskPolicyType.hpp
/// Declares the DiskPolicyType enumeration and DiskPolicy class.

/// @file DistributedSystem.hpp
/// Declares the DistributedSystem class.

/// @file EntryEvent.hpp
/// Declares the EntryEvent class.

/// @file ExceptionTypes.hpp
/// Declares the Geode exception type classes.

/// @file ExpirationAction.hpp
/// Declares the ExpirationAction enumeration and Expiration class.

/// @file GeodeClassIds.hpp
/// Declares the GeodeClassIds class.

/// @file IRegionService.hpp
/// Declares the IRegionService interface.

/// @file IRegionService.hpp
/// Declares the IRegionService interface.

/// @file IGeodeCache.hpp
/// Declares the IGeodeCache interface.

/// @file IGeodeCache.hpp
/// Declares the IGeodeCache interface.

/// @file ICacheableKey.hpp
/// Declares the ICacheableKey interface.

/// @file ICacheListener.hpp
/// Declares the ICacheListener interface.

/// @file ICacheListener.hpp
/// Declares the ICacheListener interface.

/// @file IPartitionResolver.hpp
/// Declares the IPartitionResolver interface.

/// @file IFixedPartitionResolver.hpp
/// Declares the IFixedPartitionResolver interface.

/// @file IPartitionResolver.hpp
/// Declares the IPartitionResolver interface.

/// @file IFixedPartitionResolver.hpp
/// Declares the IFixedPartitionResolver interface.

/// @file ICacheLoader.hpp
/// Declares the ICacheLoader interface.

/// @file ICacheWriter.hpp
/// Declares the ICacheWriter interface.

/// @file ICacheLoader.hpp
/// Declares the ICacheLoader interface.

/// @file ICacheWriter.hpp
/// Declares the ICacheWriter interface.

/// @file IGFSerializable.hpp
/// Declares the IGFSerializable interface.

/// @file ISelectResults.hpp
/// Declares the ISelectResults interface.

/// @file Log.hpp
/// Declares the Log class.

/// @file Log.hpp
/// Declares the Log class.

/// @file Properties.hpp
/// Declares the Properties class.

/// @file RegionShortcut.hpp
/// Declares the RegionShortcut enum class.

/// @file Query.hpp
/// Declares the Query class.

/// @file QueryService.hpp
/// Declares the QueryService class.

/// @file Region.hpp
/// Declares the Region class.

/// @file Region.hpp
/// Declares the Region class.

/// @file Region.hpp
/// Declares the Region class.

/// @file RegionEntry.hpp
/// Declares the RegionEntry class.

/// @file RegionEntry.hpp
/// Declares the RegionEntry class.

/// @file RegionEvent.hpp
/// Declares the RegionEvent class.

/// @file ResultSet.hpp
/// Declares the ResultSet class.

/// @file ScopeType.hpp
/// Declares the ScopeType enumeration and Scope class.

/// @file SelectResultsIterator.hpp
/// Declares the SelectResultsIterator class.

/// @file Serializable.hpp
/// Declares the Serializable class.

/// @file StructSet.hpp
/// Declares the StructSet class.

/// @file Struct.hpp
/// Declares the Struct class.

/// @file SystemProperties.hpp
/// Declares the SystemProperties class.

/// @file SystemProperties.hpp
/// Declares the SystemProperties class.

/// @file Utils.hpp
/// Declares the Utils class.

/// @file UserFunctionExecutionException.hpp
/// Declares the UserFunctionExecutionException class.

/// @file UserFunctionExecutionException.hpp
/// Declares the UserFunctionExecutionException class.

/// @file ICqStatusListener.hpp
/// Declares the ICqStatusListener interface.

/// @file ICqStatusListener.hpp
/// Declares the ICqStatusListener interface.

/// @file IPersistenceManager.hpp
/// Declares the generic IPersistenceManager interface.
