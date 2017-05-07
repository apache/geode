/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

// These definitions are to help parsing by Doxygen.

/// @mainpage Pivotal GemFire Native Client .NET Reference
/// @image html gemFireDotNETLogo.gif

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

/// @namespace GemStone::GemFire
/// This namespace contains all the GemFire .NET classes and utility classes.

/// @namespace GemStone::GemFire::Cache
/// This namespace contains all the GemFire .NET API classes and enumerations.

/// @namespace GemStone::GemFire::Cache::Generic
/// This namespace contains all the GemFire .NET Generics API classes and enumerations.

/// @namespace GemStone::GemFire::Cache::Internal
/// This namespace contains internal GemFire non-public .NET classes.

/// @namespace GemStone::GemFire::Cache::Template
/// This namespace contains internal GemFire .NET template classes.

/// @file gf_includes.hpp
/// Provides a commonly-used set of include directives.

/// @file AttributesFactoryM.hpp
/// Declares the AttributesFactory class.

/// @file AttributesMutatorM.hpp
/// Declares the AttributesMutator class.

/// @file CacheAttributesM.hpp
/// Declares the CacheAttributes class.

/// @file CacheAttributesFactoryM.hpp
/// Declares the CacheAttributesFactory class.

/// @file CacheableBuiltinsM.hpp
/// Declares the CacheableBuiltinKey and CacheableBuiltinArray
/// template classes and their instantiations for CacheableBoolean,
/// CacheableByte, CacheableDouble, CacheableFloat, CacheableInt16,
/// CacheableInt32, CacheableInt64, CacheableBytes, CacheableDoubleArray,
/// CacheableFloatArray, CacheableInt16Array, CacheableInt32Array, 
/// CacheableInt64Array, BooleanArray and CharArray

/// @file CacheableBuiltinsMN.hpp
/// Declared the built-in GemFire serializable types.

/// @file CacheableDateM.hpp
/// Declares the CacheableDate class.

/// @file CacheableFileNameM.hpp
/// Declares the CacheableFileName class.

/// @file CacheableHashMapM.hpp
/// Declares the CacheableHashMap class.

/// @file CacheableHashSetM.hpp
/// Declares the CacheableHashSet class.

/// @file CacheableKeyM.hpp
/// Declares the CacheableKey class.

/// @file CacheableObject.hpp
/// Declares the CacheableObject class.

/// @file CacheableObjectXml.hpp
/// Declares the CacheableObjectXml class.

/// @file CacheableStringM.hpp
/// Declares the CacheableString class.

/// @file CacheableStringArrayM.hpp
/// Declares the CacheableStringArray class.

/// @file CacheableUndefinedM.hpp
/// Declares the CacheableUndefined class.

/// @file CacheableVectorM.hpp
/// Declares the CacheableVector class.

/// @file CacheFactoryM.hpp
/// Declares the CacheFactory class.

/// @file CacheM.hpp
/// Declares the Cache class.

/// @file CacheStatisticsM.hpp
/// Declares the CacheStatistics class.

/// @file CacheStatisticsMN.hpp
/// Declares the CacheStatistics class.

/// @file DataInputM.hpp
/// Declares the DataInput class.

/// @file DataOutputM.hpp
/// Declares the DataOutput class.

/// @file DiskPolicyTypeM.hpp
/// Declares the DiskPolicyType enumeration and DiskPolicy class.

/// @file DistributedSystemM.hpp
/// Declares the DistributedSystem class.

/// @file EntryEventM.hpp
/// Declares the EntryEvent class.

/// @file ExceptionTypesM.hpp
/// Declares the GemFire exception type classes.

/// @file ExpirationActionM.hpp
/// Declares the ExpirationAction enumeration and Expiration class.

/// @file GemFireClassIdsM.hpp
/// Declares the GemFireClassIds class.

/// @file IRegionService.hpp
/// Declares the IRegionService interface.

/// @file IRegionServiceN.hpp
/// Declares the IRegionService interface.

/// @file IGemFireCache.hpp
/// Declares the IGemFireCache interface.

/// @file IGemFireCacheN.hpp
/// Declares the IGemFireCache interface.

/// @file ICacheableKey.hpp
/// Declares the ICacheableKey interface.

/// @file ICacheListener.hpp
/// Declares the ICacheListener interface.

/// @file ICacheListenerN.hpp
/// Declares the ICacheListener interface.

/// @file IPartitionResolver.hpp
/// Declares the IPartitionResolver interface.

/// @file IFixedPartitionResolver.hpp
/// Declares the IFixedPartitionResolver interface.

/// @file IPartitionResolverN.hpp
/// Declares the IPartitionResolver interface.

/// @file IFixedPartitionResolverN.hpp
/// Declares the IFixedPartitionResolver interface.

/// @file ICacheLoader.hpp
/// Declares the ICacheLoader interface.

/// @file ICacheWriter.hpp
/// Declares the ICacheWriter interface.

/// @file ICacheLoaderN.hpp
/// Declares the ICacheLoader interface.

/// @file ICacheWriterN.hpp
/// Declares the ICacheWriter interface.

/// @file IGFSerializable.hpp
/// Declares the IGFSerializable interface.

/// @file ISelectResults.hpp
/// Declares the ISelectResults interface.

/// @file LogM.hpp
/// Declares the Log class.

/// @file LogMN.hpp
/// Declares the Log class.

/// @file PropertiesM.hpp
/// Declares the Properties class.

/// @file RegionShortcutM.hpp
/// Declares the RegionShortcut enum class.

/// @file QueryM.hpp
/// Declares the Query class.

/// @file QueryServiceM.hpp
/// Declares the QueryService class.

/// @file RegionM.hpp
/// Declares the Region class.

/// @file RegionM.hpp
/// Declares the Region class.

/// @file RegionMN.hpp
/// Declares the Region class.

/// @file RegionAttributesM.hpp
/// Declares the RegionAttributes class.

/// @file RegionEntryM.hpp
/// Declares the RegionEntry class.

/// @file RegionEntryMN.hpp
/// Declares the RegionEntry class.

/// @file RegionEventM.hpp
/// Declares the RegionEvent class.

/// @file ResultSetM.hpp
/// Declares the ResultSet class.

/// @file ScopeTypeM.hpp
/// Declares the ScopeType enumeration and Scope class.

/// @file SelectResultsIteratorM.hpp
/// Declares the SelectResultsIterator class.

/// @file SerializableM.hpp
/// Declares the Serializable class.

/// @file StructSetM.hpp
/// Declares the StructSet class.

/// @file StructM.hpp
/// Declares the Struct class.

/// @file SystemPropertiesM.hpp
/// Declares the SystemProperties class.

/// @file SystemPropertiesMN.hpp
/// Declares the SystemProperties class.

/// @file Utils.hpp
/// Declares the Utils class.

/// @file UserFunctionExecutionExceptionM.hpp
/// Declares the UserFunctionExecutionException class.

/// @file UserFunctionExecutionExceptionMN.hpp
/// Declares the UserFunctionExecutionException class.

/// @file ICqStatusListenerN.hpp
/// Declares the ICqStatusListener interface.

/// @file ICqStatusListener.hpp
/// Declares the ICqStatusListener interface.

/// @file IPersistenceManagerN.hpp
/// Declares the generic IPersistenceManager interface.
