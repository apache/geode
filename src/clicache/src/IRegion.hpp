/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "ISubscriptionService.hpp"
#include <gfcpp/DataOutput.hpp>
//#include "ExceptionTypes.hpp"

using namespace System;
using namespace System::Collections::Generic;

/*
using namespace GemStone::GemFire::Cache;
using namespace GemStone::GemFire::Cache::Generic;
*/

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache 
		{
    
    namespace Generic
    {
      ref class Cache;
      ref class CacheStatistics;
      //interface class IGFSerializable;
      interface class IRegionService;

      generic<class TResult>
      interface class ISelectResults;

      generic<class TKey, class TValue>
      ref class RegionEntry;

      generic<class TKey, class TValue>
      ref class RegionAttributes;

      generic<class TKey, class TValue>
      ref class AttributesMutator;

      /// <summary>
      /// Encapsulates a concrete region of cached data.
      /// Implements generic IDictionary<TKey, TValue> interface class.
      /// </summary>
      /// <remarks>
      /// This class manages subregions and cached data. Each region
      /// can contain multiple subregions and entries for data.
      /// Regions provide a hierachical name space
      /// within the cache. Also, a region can be used to group cached
      /// objects for management purposes.
      ///
      /// Entries managed by the region are key-value pairs. A set of region attributes
      /// is associated with the region when it is created.
      ///
      /// The IRegion interface basically contains two set of APIs: Region management
      /// APIs and (potentially) distributed operations on entries. Non-distributed
      /// operations on entries  are provided by <c>RegionEntry</c>.
      ///
      /// Each <c>Cache</c> defines regions called the root regions.
      /// User applications can use the root regions to create subregions
      /// for isolated name spaces and object grouping.
      ///
      /// A region's name can be any string, except that it must not contain
      /// the region name separator, a forward slash (/).
      ///
      /// <c>Regions</c>  can be referenced by a relative path name from any region
      /// higher in the hierarchy in <see cref="IRegion.GetSubRegion" />. You can get the relative
      /// path from the root region with <see cref="IRegion.FullPath" />. The name separator
      /// is used to concatenate all the region names together from the root, starting
      /// with the root's subregions.
      /// </remarks>
      /// <see cref="RegionAttributes" />

      generic<class TKey, class TValue>
      public interface class IRegion : public System::Collections::Generic::IDictionary<TKey, TValue>
      {
        public: 

          /// <summary>
          /// Gets or sets the element with the specified key. 
          /// </summary>
          /// <remarks>
          /// This property provides the ability to access a specific element in the collection
          /// by using the following syntax: myCollection[key].
          /// You can also use the Item property to add new elements by setting the value of a key 
          /// that does not exist in the dictionary; for example, myCollection["myNonexistentKey"] = myValue
          /// However, if the specified key already exists in the dictionary, 
          /// setting the Item property overwrites the old value. In contrast, 
          /// the Add method does not modify existing elements.
          /// This property is applicable to local as well as distributed region.
          /// For local region instance - Puts/Gets a new value into an entry in this region in the local cache only.
          /// For distributed region instance - Puts/Gets a new value into an entry in this region
          /// and this operation is propogated to the Gemfire cache server to which it is connected with.
          /// </remarks>
          /// <param name="key">
          /// The key of the element to get or set.
          /// </param>
          /// <param name ="Property Value">
          /// The element with the specified key.
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// if key is null
          /// </exception>
          /// <exception cref="KeyNotFoundException">
          /// If given key was not present in the region and if region is not in secure mode, or if Pool
          /// attached with Region is not in multiusersecure mode.
          /// </exception>
          /// <exception cref="EntryNotFoundException">
          /// if given key's value is null.
          /// </exception>
          /// <exception cref="CacheWriterException">
          /// if CacheWriter aborts the operation
          /// </exception>
          /// <exception cref="CacheListenerException">
          /// if CacheListener throws an exception
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if region has been destroyed
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="OutOfMemoryException">
          /// if  there is not enough memory for the value
          /// </exception>
          /// <exception cref="CacheLoaderException">
          /// if CacheLoader throws an exception
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          virtual property TValue default[TKey]
          {
            TValue get(TKey key);
            void set(TKey key, TValue value);
          }          

          /// <summary>
          /// Returns an enumerator that iterates through the collection of the region entries.
          /// This operation is performed entirely in local cache.
          /// </summary>
          /// <remarks>
          /// The foreach statement of the C# language (for each in C++)hides the 
          /// complexity of the enumerators. Therefore, using foreach is recommended, 
          /// instead of directly manipulating the enumerator.
          /// Enumerators can be used to read the data in the collection, 
          /// but they cannot be used to modify the underlying collection.
          /// Initially, the enumerator is positioned before the first element in the collection. 
          /// At this position, Current is undefined. Therefore, you must call MoveNext to advance
          /// the enumerator to the first element of the collection before reading the value of Current.
          /// Current returns the same object until MoveNext is called. MoveNext sets Current to the next element.
          /// If MoveNext passes the end of the collection, the enumerator is positioned after the last element
          /// in the collection and MoveNext returns false. When the enumerator is at this position, subsequent
          /// calls to MoveNext also return false. If the last call to MoveNext returned false, Current is 
          /// undefined. You cannot set Current to the first element of the collection again; you must create
          /// a new enumerator instance instead.
          /// The enumerator does not have exclusive access to the collection; therefore, enumerating through a 
          /// collection is intrinsically not a thread-safe procedure. To guarantee thread safety during 
          /// enumeration, you can lock the collection during the entire enumeration. To allow the collection
          /// to be accessed by multiple threads for reading and writing, you must implement your own 
          /// synchronization.
          /// Default implementations of collections in the System.Collections.Generic namespace are not 
          /// synchronized.
          /// For both local & distributed region instances, this operation is restricted to local cache only.          
          /// </remarks>
          /// <exception cref="InvalidOperationException">
          /// if enumerator is before or after the collection and Current method is called on it.
          /// </exception>          
          /// <returns>
          /// Type: System.Collections.Generic.IEnumerator<T>. A IEnumerator<T> that 
          /// can be used to iterate through the collection.
          /// </returns>
          virtual System::Collections::Generic::IEnumerator<KeyValuePair<TKey,TValue>>^ GetEnumerator();
          
          /// <summary>
          /// Returns an enumerator that iterates through the collection of the region entries.
          /// This operation is performed entirely in local cache.
          /// </summary>
          /// <remarks>
          /// The foreach statement of the C# language (for each in C++)hides the 
          /// complexity of the enumerators. Therefore, using foreach is recommended, 
          /// instead of directly manipulating the enumerator.
          /// Enumerators can be used to read the data in the collection, 
          /// but they cannot be used to modify the underlying collection.
          /// Initially, the enumerator is positioned before the first element in the collection. 
          /// At this position, Current is undefined. Therefore, you must call MoveNext to advance
          /// the enumerator to the first element of the collection before reading the value of Current.
          /// Current returns the same object until MoveNext is called. MoveNext sets Current to the next element.
          /// If MoveNext passes the end of the collection, the enumerator is positioned after the last element
          /// in the collection and MoveNext returns false. When the enumerator is at this position, subsequent
          /// calls to MoveNext also return false. If the last call to MoveNext returned false, Current is 
          /// undefined. You cannot set Current to the first element of the collection again; you must create
          /// a new enumerator instance instead.
          /// The enumerator does not have exclusive access to the collection; therefore, enumerating through a 
          /// collection is intrinsically not a thread-safe procedure. To guarantee thread safety during 
          /// enumeration, you can lock the collection during the entire enumeration. To allow the collection
          /// to be accessed by multiple threads for reading and writing, you must implement your own 
          /// synchronization.
          /// For both local & distributed region instances, this operation is restricted to local cache only.
          /// </remarks>
          /// <exception cref="InvalidOperationException">
          /// if enumerator is before or after the collection and Current method is called on it.
          /// </exception>          
          /// <returns>
          /// Type: System.Collections.IEnumerator. An IEnumerator object that can be used to iterate 
          /// through the collection.
          /// </returns>
          virtual System::Collections::IEnumerator^ GetEnumeratorOld() = 
            System::Collections::IEnumerable::GetEnumerator;
          
          /// <summary>
          /// Determines whether the IDictionary contains an element with the specified key. 
          /// </summary>
          /// <remarks>
          /// For local region instance - This only searches in the local cache.
          /// For distributed region instance - checks to see if the key is present on the server.
          /// </remarks>
          /// <param name="key">
          /// The key to locate in the IDictionary.
          /// </param>
          /// <exception cref="ArgumentNullException">
          /// key is a null reference
          /// </exception>
          /// <returns>
          /// true if the IDictionary contains an element with the key; otherwise, false. 
          /// </returns>
          virtual bool ContainsKey(TKey key);
          
          /// <summary>
          /// Adds an element with the provided key and value to the IDictionary. 
          /// </summary>
          /// <remark>
          /// You can also use the Item property to add new elements by setting the value of a key 
          /// that does not exist in the dictionary; for example, myCollection["myNonexistentKey"] = myValue
          /// However, if the specified key already exists in the dictionary, setting the Item property 
          /// overwrites the old value. In contrast, the Add method does not modify existing elements.
          /// </remark>
          /// <remarks>
          /// <para>
          /// If remote server put fails throwing back a <c>CacheServerException</c>
          /// or security exception, then local put is tried to rollback. However,
          /// if the entry has overflowed/evicted/expired then the rollback is
          /// aborted since it may be due to a more recent notification or update
          /// by another thread.
          /// </para>
          /// <para>
          /// For local region instance - creates a new entry in this region with the specified keyvaluepair
          /// in the local cache only.
          /// For distributed region instance - The new entry is propogated to the java server to which it is 
          /// connected with.
          /// </para>
          /// </remarks>
          /// <param name="key">
          /// The object to use as the key of the element to add.
          /// </param>
          /// <param name="value">
          /// The object to use as the value of the element to add.
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// if key is null
          /// </exception>
          /// <exception cref="EntryExistsException">
          /// if an entry with this key already exists
          /// </exception>
          /// <exception cref="CacheWriterException">
          /// if CacheWriter aborts the operation
          /// </exception>
          /// <exception cref="CacheListenerException">
          /// if CacheListener throws an exception
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to a GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if region has been destroyed
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="OutOfMemoryException">
          /// if there is not enough memory for the new entry
          /// </exception>          
          virtual void Add(TKey key, TValue value);
          
          /// <summary>
          /// Adds an item to the ICollection<T>.
          /// </summary>
          /// <remarks>
          /// <para>
          /// If remote server put fails throwing back a <c>CacheServerException</c>
          /// or security exception, then local put is tried to rollback. However,
          /// if the entry has overflowed/evicted/expired then the rollback is
          /// aborted since it may be due to a more recent notification or update
          /// by another thread.
          /// </para>
          /// <para>
          /// For local region instance - creates a new entry in this region with the specified keyvaluepair
          /// in the local cache only.
          /// For distributed region instance - The new entry is propogated to the java server to which it is 
          /// connected with.
          /// </para>
          /// </remarks>
          /// <param name="keyValuePair">
          /// Type: KeyValuePair<TKey, TValue> The object to add to the ICollection<T>.
          /// </param>          
          /// <exception cref="IllegalArgumentException">
          /// if key is null
          /// </exception>
          /// <exception cref="EntryExistsException">
          /// if an entry with this key already exists
          /// </exception>
          /// <exception cref="CacheWriterException">
          /// if CacheWriter aborts the operation
          /// </exception>
          /// <exception cref="CacheListenerException">
          /// if CacheListener throws an exception
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to a GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if region has been destroyed
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="OutOfMemoryException">
          /// if there is not enough memory for the new entry
          /// </exception>          
          virtual void Add(KeyValuePair<TKey, TValue> keyValuePair);
          
          /// <summary>
          /// Creates a new entry in this region with the specified key and value,
          /// passing the callback argument to any cache writers and cache listeners
          /// that are invoked in the operation.
          /// </summary>
          /// <remarks>
          /// <para>
          /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
          /// <see cref="CacheStatistics.LastModifiedTime" /> for this region
          /// and the entry.
          /// </para>
          /// <para>
          /// For local region instance - creates a new entry in this region with the specified key and value
          /// in the local cache only.
          /// For distributed region instance - The new entry is propogated to the java server to which it is 
          /// connected with.
          /// </para><para>
          /// If remote server put fails throwing back a <c>CacheServerException</c>
          /// or security exception, then local put is tried to rollback. However,
          /// if the entry has overflowed/evicted/expired then the rollback is
          /// aborted since it may be due to a more recent notification or update
          /// by another thread.
          /// </para>
          /// </remarks>
          /// <param name="key">
          /// The key for which to create the entry in this region. The object is
          /// created before the call, and the caller should not deallocate the object.
          /// </param>
          /// <param name="value">
          /// The value for the new entry, which may be null to indicate that the new
          /// entry starts as if it had been locally invalidated.
          /// </param>
          /// <param name="callbackArg">
          /// a custome parameter to pass to the cache writer or cache listener
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// if key is null
          /// </exception>
          /// <exception cref="EntryExistsException">
          /// if an entry with this key already exists
          /// </exception>
          /// <exception cref="CacheWriterException">
          /// if CacheWriter aborts the operation
          /// </exception>
          /// <exception cref="CacheListenerException">
          /// if CacheListener throws an exception
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to a GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if region has been destroyed
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="OutOfMemoryException">
          /// if there is not enough memory for the new entry
          /// </exception>          
          /// <seealso cref="Put" />
          /// <seealso cref="Get" />
          void Add(TKey key, TValue value, Object^ callbackArg);

          /// <summary>
          /// Removes the element with the specified key from the IDictionary.
          /// </summary>
          /// <remarks>
          /// For local region instance - removes the entry with the specified key from the local cache only.
          /// For distributed region instance - remove is propogated to the Gemfire cache server.
          /// </remarks>
          /// <param name="key">
          /// The key of the element to remove.
          /// </param> 
          /// <exception cref="IllegalArgumentException">if key is null</exception>
          /// <exception cref="EntryNotFoundException">
          /// if the entry does not exist in this region locally, applicable only for local region instance.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if this region has been destroyed
          /// </exception> 
          /// <returns>
          /// true if the element is successfully removed; otherwise, false. 
          /// This method also returns false if key was not found in the original IDictionary. 
          /// </returns>
          virtual bool Remove(TKey key);

          /// <summary>
          /// Removes the entry with the specified key, passing the callback
          /// argument to any cache writers that are invoked in the operation.
          /// </summary>
          /// <remarks>
          /// <para>
          /// Removes not only the value, but also the key and entry
          /// from this region.
          /// </para>
          /// <para>
          /// For local region instance - removes the value with the specified key in the local cache only.
          /// For distributed region instance - destroy is propogated to the Gemfire cache server
          /// to which it is connected with.          
          /// </para>
          /// <para>
          /// Does not update any <c>CacheStatistics</c>.
          /// </para>
          /// </remarks>
          /// <param name="key">the key of the entry to destroy</param>
          /// <param name="callbackArg">
          /// a user-defined parameter to pass to cache writers triggered by this method
          /// </param>
          /// <exception cref="IllegalArgumentException">if key is null</exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if this region has been destroyed
          /// </exception>          
          /// <seealso cref="Invalidate" />
          /// <seealso cref="ICacheListener.AfterDestroy" />
          /// <seealso cref="ICacheWriter.BeforeDestroy" />
          /// <returns>
          /// true if the element is successfully removed; otherwise, false. 
          /// This method also returns false if key was not found in the original IDictionary. 
          /// </returns>
          bool Remove( TKey key, Object^ callbackArg );

          /// <summary>
          /// Gets the value associated with the specified key.
          /// </summary>
          /// <remark>
          /// This method combines the functionality of the ContainsKey method and the Item property.
          /// If the key is not found, then the value parameter gets the appropriate default value for the value
          /// type V; for example, zero (0) for integer types, false for Boolean types, and a null reference for
          /// reference types.
          /// For local region instance - returns the value with the specified key from the local cache only.
          /// For distributed region instance - If the value is not present locally then it is requested from
          /// the java server. If even that is unsuccessful then a local CacheLoader will be invoked
          /// if there is one.
          /// </remark>
          /// <param name="key">
          /// The key whose value to get.
          /// </param>
          /// <param name="value">When this method returns, the value associated with the specified key, if the key is
          /// found; otherwise, the default value for the type of the value parameter. 
          /// This parameter is passed uninitialized.</param>          
          /// <exception cref="IllegalArgumentException">
          /// if key is null
          /// </exception>
          /// <exception cref="CacheLoaderException">
          /// if CacheLoader throws an exception
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if this region has been destroyed
          /// </exception>
          /// <returns>
          /// true if the object that implements IDictionary contains an element with the specified key; otherwise, false.
          /// </returns>
          virtual bool TryGetValue(TKey key, TValue %value); 

          /// <summary>
          /// Determines whether the ICollection contains a specific value.
          /// </summary>
          /// <remarks>
          /// <para>          
          /// For local region instance - returns the value with the specified key from the local cache only.
          /// For distributed region instance - If the value is not present locally then it is requested from
          /// the java server. If even that is unsuccessful then a local CacheLoader will be invoked
          /// if there is one.
          /// </para>
          /// <para>
          /// The comparison of the value of the key value pair depends on the Equals function of the TValue class.
          /// If the Equals function is not overriden in the TValue class the behavior of this function is undefined. Hence, this 
          /// function won't work properly for the .NET types that uses the default implementation of the Equals method, for 
          /// e.g. arrays.
          /// </para>
          /// </remarks>
          /// <param name="keyValuePair">
          /// The KeyValuePair structure to locate in the ICollection.
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// if key is null
          /// </exception>
          /// <exception cref="CacheLoaderException">
          /// if CacheLoader throws an exception
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if this region has been destroyed
          /// </exception>
          /// <returns>
          /// true if keyValuePair is found in the ICollection; otherwise, false.
          /// </returns>
          virtual bool Contains(KeyValuePair<TKey,TValue> keyValuePair); 

          /// <summary>
          /// Removes all items from the ICollection<T>.
          /// remove all entries in the region.	
          /// </summary>
          /// For local region instance - remove all entries in the local region.
          /// For distributed region instance - remove all entries in the local region, 
	      /// and propagate the operation to server.
          virtual void Clear();

          /// <summary>
          /// Copies the elements of the ICollection to an Array, starting at a particular Array index.
          /// This operation copies entries from local region only.
          /// </summary>
          /// <param name="toArray">
          /// The one-dimensional Array that is the destination of the elements copied from ICollection. 
          /// The Array must have zero-based indexing.
          /// </param>
          /// <param name="startIdx">
          /// The zero-based index in array at which copying begins.
          /// </param>
          /// <exception cref="ArgumentNullException">
          /// if toArray is a null reference
          /// </exception>
          /// <exception cref="ArgumentOutOfRangeException">
          /// if startIdx is less than 0.
          /// </exception>
          /// <exception cref="ArgumentException">
          /// if toArray is multidimensional or The number of elements in the source ICollection is greater than 
          /// the available space from startIdx to the end of the destination array or startIdx is equal to 
          /// or greater than the length of array.
          /// </exception>
          virtual void CopyTo(array<KeyValuePair<TKey,TValue>>^ toArray, int startIdx);
          
          /// <summary>
          /// Removes a key and value from the dictionary.          
          /// </summary>
          /// <remarks>
          /// <para>
          /// Remove removes not only the value, but also the key and entry
          /// from this region.
          /// </para>
          /// <para>
          /// The Remove is propogated to the Gemfire cache server to which it is connected with.
          /// </para>
          /// <para>
          /// Does not update any <c>CacheStatistics</c>.
          /// </para>
          /// <para>
          /// The comparison of the value of the key value pair depends on the Equals function of the TValue class.
          /// If the Equals function is not overriden in the TValue class the behavior of this function is undefined. Hence, this 
          /// function won't work properly for the .NET types that uses the default implementation of the Equals method, for 
          /// e.g. arrays.
          /// </para>
          /// </remarks>
          /// <param name="keyValuePair">The KeyValuePair structure representing 
          /// the key and value to remove from the Dictionary.</param>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if this region has been destroyed
          /// </exception>
          /// <returns>true if the key and value represented by keyValuePair is successfully found and removed;
          /// otherwise, false. This method returns false if keyValuePair is not found in the ICollection.</returns> 
          /// <returns>true if entry with key and its value are removed otherwise false.</returns>
          /// <seealso cref="Invalidate" />
          /// <seealso cref="ICacheListener.AfterDestroy" />
          /// <seealso cref="ICacheWriter.BeforeDestroy" />
          virtual bool Remove(KeyValuePair<TKey,TValue> keyValuePair);

          /// <summary>
          /// Removes the entry with the specified key and value, passing the callback
          /// argument to any cache writers that are invoked in the operation.
          /// </summary>
          /// <remarks>
          /// <para>
          /// Remove removes not only the value, but also the key and entry
          /// from this region.
          /// </para>
          /// <para>
          /// The Remove is propogated to the Gemfire cache server to which it is connected with.
          /// </para>
          /// <para>
          /// Does not update any <c>CacheStatistics</c>.
          /// </para>
          /// </remarks>
          /// <param name="key">the key of the entry to Remove</param>
          /// <param name="value">the value of the entry to Remove</param>
          /// <param name="callbackArg"> the callback for user to pass in, It can also be null</param>.
          /// <exception cref="IllegalArgumentException">if key is null</exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if this region has been destroyed
          /// </exception>
          /// <returns>true if entry with key and its value are removed otherwise false.</returns>
          /// <seealso cref="Invalidate" />
          /// <seealso cref="ICacheListener.AfterDestroy" />
          /// <seealso cref="ICacheWriter.BeforeDestroy" />
          virtual bool Remove(TKey key, TValue value, Object^ callbackArg );

          /// <summary>
          /// Gets the number of elements contained in the ICollection<T>.
          /// Get the size of region. For native client regions, this will give
          /// the number of entries in the local cache and not on the servers.
          /// </summary>
          /// <returns>number of entries in the region</returns>
          virtual property int Count
          {
            int get();
          }

          /// <summary>
          /// This property throws NotImplementedException when called by 
          /// both local and distributed region instances.
          /// </summary>
          virtual property bool IsReadOnly
          {
            bool get();
          }
                   
          /// <summary>
          /// Gets an ICollection containing the keys of the IDictionary
          /// Returns all the keys for this region. This includes
          /// keys for which the entry is invalid.
          /// For local region instance - gets collection of keys from local cache only.
          /// For distributed region instance - gets collection of keys from the Gemfire cache server.
          /// </summary>
          /// <returns>collection of keys</returns>
          /// <remark>
          /// The order of the keys in the returned ICollection is unspecified, 
          /// but it is guaranteed to be the same order as the corresponding values in the ICollection
          /// returned by the Values property.
          /// </remark>
          /// <exception cref="UnsupportedOperationException">
          /// if the member type is not <c>Client</c>
          /// or region is not a Native Client region.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if there is a timeout getting the keys
          /// </exception>
          virtual property System::Collections::Generic::ICollection<TKey>^ Keys
          {
            System::Collections::Generic::ICollection<TKey>^ get() ;
          }

          /// <summary>
          /// Gets an ICollection containing the values in the IDictionary. 
          /// Returns all values in the local process for this region. No value is included
          /// for entries that are invalidated.
          /// For both local & distributed region instances, this operation is always local only.
          /// </summary>
          /// <returns>collection of values</returns>
          /// <remark>
          /// The order of the values in the returned ICollection is unspecified, 
          /// but it is guaranteed to be the same order as the corresponding keys in the ICollection 
          /// returned by the Keys property.
          /// </remark>
          virtual property System::Collections::Generic::ICollection<TValue>^ Values
          {
            System::Collections::Generic::ICollection<TValue>^ get() ;
          }
  
          /// <summary>
          /// Puts a new value into an entry in this region with the specified key,
          /// passing the callback argument to any cache writers and cache listeners
          /// that are invoked in the operation.
          /// </summary>
          /// <remarks>
          /// <para>
          /// If there is already an entry associated with the specified key in
          /// this region, the entry's previous value is overwritten.
          /// The new put value is propogated to the java server to which it is connected with.
          /// Put is intended for very simple caching situations. In general
          /// it is better to create a <c>ICacheLoader</c> object and allow the
          /// cache to manage the creation and loading of objects.
          /// For local region instance - Puts a new value into an entry in this region in the local cache only.
          /// For distributed region instance - Puts a new value into an entry in this region
          /// and this operation is propogated to the Gemfire cache server to which it is connected with.
          /// </para><para>
          /// Updates the <see cref="CacheStatistics.LastAccessedTime" /> and
          /// <see cref="CacheStatistics.LastModifiedTime" /> for this region and the entry.
          /// </para><para>
          /// If remote server put fails throwing back a <c>CacheServerException</c>
          /// or security exception, then local put is tried to rollback. However,
          /// if the entry has overflowed/evicted/expired then the rollback is
          /// aborted since it may be due to a more recent notification or update
          /// by another thread.
          /// </para>
          /// </remarks>
          /// <param name="key">
          /// a key object associated with the value to be put into this region.
          /// </param>
          /// <param name="value">the value to be put into this region</param>
          /// <param name="callbackArg">
          /// argument that is passed to the callback functions
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// if key is null
          /// </exception>
          /// <exception cref="CacheWriterException">
          /// if CacheWriter aborts the operation
          /// </exception>
          /// <exception cref="CacheListenerException">
          /// if CacheListener throws an exception
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if region has been destroyed
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="OutOfMemoryException">
          /// if  there is not enough memory for the value
          /// </exception>
          /// <seealso cref="Get" />
          /// <seealso cref="Add" />
          void Put(TKey key, TValue value, Object^ callbackArg);

          /// <summary>
          /// Returns the value for the given key, passing the callback argument
          /// to any cache loaders or that are invoked in the operation.
          /// </summary>
          /// <remarks>
          /// <para>          
          /// For local region instance - returns the value with the specified key from the local cache only.
          /// For distributed region instance - If the value is not present locally then it is requested from
          /// the java server. If even that is unsuccessful then a local CacheLoader will be invoked
          /// if there is one.
          /// </para>
          /// <para>
          /// The value returned by get is not copied, so multi-threaded applications
          /// should not modify the value directly, but should use the update methods.
          /// </para><para>
          /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
          /// <see cref="CacheStatistics.HitCount" />, <see cref="CacheStatistics.MissCount" />,
          /// and <see cref="CacheStatistics.LastModifiedTime" /> (if a new value is loaded)
          /// for this region and the entry.
          /// </para>
          /// </remarks>
          /// <param name="key">
          /// key whose associated value is to be returned -- the key
          /// object must implement the Equals and GetHashCode methods.
          /// </param>
          /// <param name="callbackArg">
          /// An argument passed into the CacheLoader if loader is used.
          /// Has to be Serializable (i.e. implement <c>IGFSerializable</c>);
          /// can be null.
          /// </param>
          /// <returns>
          /// value, or null if the value is not found and can't be loaded
          /// </returns>
          /// <exception cref="IllegalArgumentException">
          /// if key is null
          /// </exception>
          /// <exception cref="CacheLoaderException">
          /// if CacheLoader throws an exception
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if this region has been destroyed
          /// </exception>
          /// <seealso cref="Put" />
          TValue Get(TKey key, Object^ callbackArg);

          /// <summary>
          /// Invalidates this region.
          /// </summary>
          /// <remarks>
          /// <para>
          /// The invalidation will cascade to all the subregions and cached
          /// entries. The region
          /// and the entries in it will still exist.
          /// For local region instance - invalidates this region without distributing to other caches.
          /// For distributed region instance - Invalidates this region and this 
          /// operation is propogated to the Gemfire cache server to which it is connected with.
          /// </para>          
          /// <para>
          /// To remove all the
          /// entries and the region, use <see cref="DestroyRegion" />.
          /// </para><para>
          /// Does not update any <c>CacheStatistics</c>.
          /// </para>
          /// </remarks>
          /// <exception cref="CacheListenerException">
          /// if CacheListener throws an exception; if this occurs some
          /// subregions may have already been successfully invalidated
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if this region has been destroyed
          /// </exception>          
          /// <seealso cref="DestroyRegion" />
          /// <seealso cref="ICacheListener.AfterRegionInvalidate" />
          void InvalidateRegion();

          /// <summary>
          /// Invalidates this region.
          /// </summary>
          /// <remarks>
          /// <para>
          /// The invalidation will cascade to all the subregions and cached
          /// entries. The region
          /// and the entries in it will still exist.
          /// For local region instance - invalidates this region without distributing to other caches.
          /// For distributed region instance - Invalidates this region and this 
          /// operation is propogated to the Gemfire cache server to which it is connected with.
          /// </para>
          /// <para>
          /// To remove all the
          /// entries and the region, use <see cref="DestroyRegion" />.
          /// </para><para>
          /// Does not update any <c>CacheStatistics</c>.
          /// </para>
          /// </remarks>
          /// <param name="callbackArg">
          /// user-defined parameter to pass to callback events triggered by this method
          /// </param>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if this region has been destroyed
          /// </exception>
          /// <exception cref="CacheListenerException">
          /// if CacheListener throws an exception; if this occurs some
          /// subregions may have already been successfully invalidated
          /// </exception>
          /// <seealso cref="DestroyRegion" />
          /// <seealso cref="ICacheListener.AfterRegionInvalidate" />
          void InvalidateRegion(Object^ callbackArg);

          /// <summary>
          /// Destroys the whole distributed region and provides a user-defined parameter
          /// object to any <c>ICacheWriter</c> invoked in the process.
          /// </summary>
          /// <remarks>
          /// <para>
          /// Destroy cascades to all entries and subregions. After the destroy,
          /// this region object can not be used any more. Any attempt to use
          /// this region object will get a <c>RegionDestroyedException</c>
          /// The region destroy not only destroys the local region but also destroys the
          /// server region.
          /// For local region instance - destroys the whole local region only
          /// For distributed region instance - destroys the whole local region and this
          /// operation is also propogated to the Gemfire cache server to which it is connected with.
          /// </para><para>
          /// Does not update any <c>CacheStatistics</c>.
          /// </para>
          /// </remarks>
          /// <exception cref="CacheWriterException">
          /// if a CacheWriter aborts the operation; if this occurs some
          /// subregions may have already been successfully destroyed.
          /// </exception>
          /// <exception cref="CacheListenerException">
          /// if CacheListener throws an exception; if this occurs some
          /// subregions may have already been successfully invalidated
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <seealso cref="InvalidateRegion" />
          void DestroyRegion();

          /// <summary>
          /// Destroys the whole distributed region and provides a user-defined parameter
          /// object to any <c>ICacheWriter</c> invoked in the process.
          /// </summary>
          /// <remarks>
          /// <para>
          /// Destroy cascades to all entries and subregions. After the destroy,
          /// this region object can not be used any more. Any attempt to use
          /// this region object will get a <c>RegionDestroyedException</c>
          /// The region destroy not only destroys the local region but also destroys the
          /// server region.
          /// For local region instance - destroys the whole local region only
          /// For distributed region instance - destroys the whole local region and this
          /// operation is also propogated to the Gemfire cache server to which it is connected with.
          /// </para><para>
          /// Does not update any <c>CacheStatistics</c>.
          /// </para>
          /// </remarks>
          /// <param name="callbackArg">
          /// a user-defined parameter to pass to callback events triggered by this call
          /// </param>
          /// <exception cref="CacheWriterException">
          /// if a CacheWriter aborts the operation; if this occurs some
          /// subregions may have already been successfully destroyed.
          /// </exception>
          /// <exception cref="CacheListenerException">
          /// if CacheListener throws an exception; if this occurs some
          /// subregions may have already been successfully invalidated
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <seealso cref="InvalidateRegion" />
          void DestroyRegion(Object^ callbackArg);

          /// <summary>
          /// Invalidates the entry with the specified key,
          /// passing the callback argument
          /// to any cache
          /// listeners that are invoked in the operation.
          /// </summary>
          /// <remarks>
          /// <para>
          /// Invalidate only removes the value from the entry -- the key is kept intact.
          /// To completely remove the entry, call <see cref="Destroy" />.
          /// </para>
          /// <para>
          /// For both local & distributed region instaces, invalidate is not propogated to the 
          /// Gemfire cache server to which it is connected with.
          /// </para>
          /// <para>
          /// Does not update any <c>CacheStatistics</c>.
          /// </para>
          /// </remarks>
          /// <param name="key">key of the value to be invalidated</param>
          /// <exception cref="IllegalArgumentException">if key is null</exception>
          /// <exception cref="EntryNotFoundException">
          /// if this entry does not exist in this region locally
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if the region is destroyed
          /// </exception>
          /// <seealso cref="Destroy" />
          /// <seealso cref="ICacheListener.AfterInvalidate" />
          void Invalidate(TKey key);

          /// <summary>
          /// Invalidates the entry with the specified key,
          /// passing the callback argument
          /// to any cache
          /// listeners that are invoked in the operation.
          /// </summary>
          /// <remarks>
          /// <para>
          /// Invalidate only removes the value from the entry -- the key is kept intact.
          /// To completely remove the entry, call <see cref="Destroy" />.
          /// </para>
          /// <para>
          /// For both local & distributed region instaces, invalidate is not propogated to the 
          /// Gemfire cache server to which it is connected with.
          /// </para>
          /// <para>
          /// Does not update any <c>CacheStatistics</c>.
          /// </para>
          /// </remarks>
          /// <param name="key">key of the value to be invalidated</param>
          /// <param name="callbackArg">
          /// a user-defined parameter to pass to callback events triggered by this method
          /// </param>
          /// <exception cref="IllegalArgumentException">if key is null</exception>
          /// <exception cref="EntryNotFoundException">
          /// if this entry does not exist in this region locally
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if the region is destroyed
          /// </exception>
          /// <seealso cref="Destroy" />
          /// <seealso cref="ICacheListener.AfterInvalidate" />
          void Invalidate(TKey key, Object^ callbackArg);

          /// <summary>
          /// Puts a (IDictionary) generic collection of key/value pairs in this region.
          /// </summary>
          /// <remarks>
          /// <para>
          /// If there is already an entry associated with any key in the map in
          /// this region, the entry's previous value is overwritten.
          /// The new values are propogated to the java server to which it is connected with.
          /// PutAll is intended for speed up large amount of put operation into
          /// the same region.
          /// For local region instance - this method is not applicable.
          /// </para>
          /// </remarks>
          /// <param name="map">
          /// A map contains entries, i.e. (key, value) pairs. It is generic collection of key/value pairs.
          /// Value should not be null in any of the entries.
          /// </param>
          /// <exception cref="NullPointerException">
          /// if any value in the map is null
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if region has been destroyed
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="OutOfMemoryException">
          /// if  there is not enough memory for the value
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <seealso cref="Put" />
          void PutAll(IDictionary<TKey, TValue>^ map);

          /// <summary>
          /// Puts a (IDictionary) generic collection of key/value pairs in this region.
          /// </summary>
          /// <remarks>
          /// <para>
          /// If there is already an entry associated with any key in the map in
          /// this region, the entry's previous value is overwritten.
          /// The new values are propogated to the java server to which it is connected with.
          /// PutAll is intended for speed up large amount of put operation into
          /// the same region.
          /// For local region instance - this method is not applicable.
          /// </para>
          /// </remarks>
          /// <param name="map">
          /// A map contains entries, i.e. (key, value) pairs. It is generic collection of key/value pairs.
          /// Value should not be null in any of the entries.
          /// </param>
          /// <param name="timeout">The time (in seconds) to wait for the PutAll
          /// response. It should be less than or equal to 2^31/1000 i.e. 2147483.
          /// Optional.
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If timeout is more than 2^31/1000 i.e. 2147483.
          /// </exception>
          /// <exception cref="NullPointerException">
          /// if any value in the map is null
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if region has been destroyed
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="OutOfMemoryException">
          /// if  there is not enough memory for the value
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <seealso cref="Put" />
          void PutAll(IDictionary<TKey, TValue>^ map, int timeout);

          /// <summary>
          /// Puts a (IDictionary) generic collection of key/value pairs in this region.
          /// </summary>
          /// <remarks>
          /// <para>
          /// If there is already an entry associated with any key in the map in
          /// this region, the entry's previous value is overwritten.
          /// The new values are propogated to the java server to which it is connected with.
          /// PutAll is intended for speed up large amount of put operation into
          /// the same region.
          /// For local region instance - this method is not applicable.
          /// </para>
          /// </remarks>
          /// <param name="map">
          /// A map contains entries, i.e. (key, value) pairs. It is generic collection of key/value pairs.
          /// Value should not be null in any of the entries.
          /// </param>
          /// <param name="timeout">The time (in seconds) to wait for the PutAll
          /// response. It should be less than or equal to 2^31/1000 i.e. 2147483.
          /// Optional.
          /// </param>
          /// <param name="callbackArg">
          /// a user-defined parameter to pass to callback events triggered by this method
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If timeout is more than 2^31/1000 i.e. 2147483.
          /// </exception>
          /// <exception cref="NullPointerException">
          /// if any value in the map is null
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// if region has been destroyed
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// Only for Native Client regions.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="OutOfMemoryException">
          /// if  there is not enough memory for the value
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <seealso cref="Put" />
          void PutAll(IDictionary<TKey, TValue>^ map, int timeout, Object^ callbackArg);

          /// <summary>
          /// Removes all of the entries for the specified keys from this region.
          /// The effect of this call is equivalent to that of calling {@link #destroy(Object)} on
          /// this region once for each key in the specified collection.
          /// If an entry does not exist that key is skipped; 
          /// EntryNotFoundException is not thrown.
          /// For local region instance - this method is not applicable.
          /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
          /// and <see cref="CacheStatistics.HitCount" /> and
          /// <see cref="CacheStatistics.MissCount" /> for this region and the entry.
          /// </summary>
          /// <param name="keys">the collection of keys</param>
          /// <exception cref="IllegalArgumentException">
          /// If the collection of keys is null or empty. 
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server while
          /// processing the request.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if region is not connected to the cache because the client
          /// cannot establish usable connections to any of the given servers
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if operation timed out.
          /// </exception>
          /// <exception cref="UnknownException">
          /// For other exceptions.
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <seealso cref="Get"/>
          void RemoveAll(System::Collections::Generic::ICollection<TKey>^ keys);

          /// <summary>
          /// Removes all of the entries for the specified keys from this region.
          /// The effect of this call is equivalent to that of calling {@link #remove(Object)} on
          /// this region once for each key in the specified collection.
          /// If an entry does not exist that key is skipped; 
          /// EntryNotFoundException is not thrown.
          /// For local region instance - this method is not applicable.
          /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
          /// and <see cref="CacheStatistics.HitCount" /> and
          /// <see cref="CacheStatistics.MissCount" /> for this region and the entry.
          /// </summary>
          /// <param name="keys">the collection of keys</param>
          /// <param name="callbackArg">an argument that is passed to the callback functions.
          /// Optional.
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the collection of keys is null or empty. 
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server while
          /// processing the request.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if region is not connected to the cache because the client
          /// cannot establish usable connections to any of the given servers
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if operation timed out.
          /// </exception>
          /// <exception cref="UnknownException">
          /// For other exceptions.
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <seealso cref="Remove"/>
          void RemoveAll(System::Collections::Generic::ICollection<TKey>^ keys, Object^ callbackArg);

          /// <summary>
          /// Gets values for collection of keys from the local cache or server.
          /// If value for a key is not present locally then it is requested from the
          /// java server. The value returned is not copied, so multi-threaded
          /// applications should not modify the value directly,
          /// but should use the update methods.
          /// For local region instance - this method is not applicable.
          /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
          /// and <see cref="CacheStatistics.HitCount" /> and
          /// <see cref="CacheStatistics.MissCount" /> for this region and the entry.
          /// </summary>
          /// <param name="keys">the collection of keys</param>
          /// <param name="values">
          /// output parameter that provides the map of keys to
          /// respective values; when this is NULL then an
          /// <c>IllegalArgumentException</c> is thrown.
          /// </param>
          /// <param name="exceptions">
          /// output parameter that provides the map of keys
          /// to any exceptions while obtaining the key; ignored if this is NULL
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the collection of keys is null or empty,
          /// or <c>values</c> argument is null.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server while
          /// processing the request.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if region is not connected to the cache because the client
          /// cannot establish usable connections to any of the given servers
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if operation timed out.
          /// </exception>
          /// <exception cref="UnknownException">
          /// For other exceptions.
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <seealso cref="Get"/>
          void GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
            System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
            System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions);

          /// <summary>
          /// Gets values for collection of keys from the local cache or server.
          /// If value for a key is not present locally then it is requested from the
          /// java server. The value returned is not copied, so multi-threaded
          /// applications should not modify the value directly,
          /// but should use the update methods.
          /// For local region instance - this method is not applicable.
          /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
          /// and <see cref="CacheStatistics.HitCount" /> and
          /// <see cref="CacheStatistics.MissCount" /> for this region and the entry.
          /// </summary>
          /// <param name="keys">the collection of keys</param>
          /// <param name="values">
          /// output parameter that provides the map of keys to
          /// respective values; ignored if NULL; when this is NULL then at least
          /// the <c>addToLocalCache</c> parameter should be true and caching
          /// should be enabled for the region to get values into the region
          /// otherwise an <c>IllegalArgumentException</c> is thrown.
          /// </param>
          /// <param name="exceptions">
          /// output parameter that provides the map of keys
          /// to any exceptions while obtaining the key; ignored if this is NULL
          /// </param>
          /// <param name="addToLocalCache">
          /// true if the obtained values have also to be added to the local cache
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the collection of keys is null or empty. Other invalid case is when
          /// the <c>values</c> parameter is NULL, and either
          /// <c>addToLocalCache</c> is false or caching is disabled
          /// for this region.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server while
          /// processing the request.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if region is not connected to the cache because the client
          /// cannot establish usable connections to any of the given servers
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if operation timed out.
          /// </exception>
          /// <exception cref="UnknownException">
          /// For other exceptions.
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <seealso cref="Get"/>
          void GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
            System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
            System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions,
            bool addToLocalCache);

          /// <summary>
          /// Gets values for collection of keys from the local cache or server.
          /// If value for a key is not present locally then it is requested from the
          /// java server. The value returned is not copied, so multi-threaded
          /// applications should not modify the value directly,
          /// but should use the update methods.
          /// For local region instance - this method is not applicable.
          /// Updates the <see cref="CacheStatistics.LastAccessedTime" />
          /// and <see cref="CacheStatistics.HitCount" /> and
          /// <see cref="CacheStatistics.MissCount" /> for this region and the entry.
          /// </summary>
          /// <param name="keys">the collection of keys</param>
          /// <param name="values">
          /// output parameter that provides the map of keys to
          /// respective values; ignored if NULL; when this is NULL then at least
          /// the <c>addToLocalCache</c> parameter should be true and caching
          /// should be enabled for the region to get values into the region
          /// otherwise an <c>IllegalArgumentException</c> is thrown.
          /// </param>
          /// <param name="exceptions">
          /// output parameter that provides the map of keys
          /// to any exceptions while obtaining the key; ignored if this is NULL
          /// </param>
          /// <param name="addToLocalCache">
          /// true if the obtained values have also to be added to the local cache
          /// </param>
          /// <param name="callbackArg">
          /// a user-defined parameter to pass to callback events triggered by this method
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the collection of keys is null or empty. Other invalid case is when
          /// the <c>values</c> parameter is NULL, and either
          /// <c>addToLocalCache</c> is false or caching is disabled
          /// for this region.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server while
          /// processing the request.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if region is not connected to the cache because the client
          /// cannot establish usable connections to any of the given servers
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if operation timed out.
          /// </exception>
          /// <exception cref="UnknownException">
          /// For other exceptions.
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <seealso cref="Get"/>
          void GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
            System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
            System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions,
            bool addToLocalCache, Object^ callbackArg);

          /// <summary>
          /// Gets the region name.
          /// </summary>
          /// <returns>
          /// region's name
          /// </returns>
          property String^ Name
          { 
            String^ get();
          } 

          /// <summary>
          /// Gets the region's full path, which can be used to get this region object
          /// with <see cref="Cache.GetRegion" />.
          /// </summary>
          /// <returns>
          /// region's pathname
          /// </returns>
          property String^ FullPath
          {
            String^ get();
          }

          /// <summary>
          /// Gets the parent region.
          /// </summary>
          /// <returns>
          /// region's parent, if any, or null if this is a root region
          /// </returns>
          /// <exception cref="RegionDestroyedException">
          /// if the region has been destroyed
          /// </exception>
          property IRegion<TKey, TValue>^ ParentRegion
          {
            IRegion<TKey, TValue>^ get();
          }

          /// <summary>
          /// Returns the attributes for this region, which can be used to create a new
          /// region with <see cref="Cache.CreateRegion" />.
          /// </summary>
          /// <returns>
          /// region's attributes
          /// </returns>
          property RegionAttributes<TKey, TValue>^ Attributes 
          {
            RegionAttributes<TKey, TValue>^ get();
          }
          
          /// <summary>
          /// Return a mutator object for changing a subset of the
          /// region attributes.
          /// </summary>
          /// <returns>
          /// attribute mutator
          /// </returns>
          /// <exception cref="RegionDestroyedException">
          /// if the region has been destroyed
          /// </exception>
          property AttributesMutator<TKey, TValue>^ AttributesMutator
          {
            GemStone::GemFire::Cache::Generic::AttributesMutator<TKey, TValue>^ get();
          }

          /// <summary>
          /// Returns the statistics for this region.
          /// </summary>
          /// <returns>the <c>CacheStatistics</c> for this region</returns>
          /// <exception cref="StatisticsDisabledException">
          /// if statistics have been disabled for this region
          /// </exception>
          property CacheStatistics^ Statistics 
          {
            CacheStatistics^ get();
          }

          /// <summary>
          /// Returns the subregion identified by the path, null if no such subregion.
          /// </summary>
          /// <param name="path">path</param>
          /// <returns>subregion, or null if none</returns>
          /// <seealso cref="FullPath" />
          /// <seealso cref="SubRegions" />
          /// <seealso cref="ParentRegion" />
          IRegion<TKey, TValue>^ GetSubRegion( String^ path );
          
          /// <summary>
          /// Creates a subregion with the given name and attributes.
          /// </summary>
          /// <param name="subRegionName">new subregion name</param>
          /// <param name="attributes">subregion attributes</param>
          /// <returns>new subregion</returns>
          /// <seealso cref="CreateServerSubRegion" />
          IRegion<TKey, TValue>^ CreateSubRegion( String^ subRegionName, RegionAttributes<TKey, TValue>^ attributes );
          
          /// <summary>
          /// Returns the subregions of this region.
          /// </summary>
          /// <param name="recursive">if true, also return all nested subregions</param>
          /// <returns>collection of regions</returns>
          /// <exception cref="RegionDestroyedException">
          /// this region has already been destroyed
          /// </exception>
          System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^ SubRegions( bool recursive );

          /// <summary>
          /// Return the meta-object RegionEntry for the given key.
          /// For both local & distributed region instances, this operation happens in local cache only.
          /// </summary>
          /// <param name="key">key to use</param>
          /// <returns>region entry object</returns>
          /// <exception cref="IllegalArgumentException">key is null</exception>
          /// <exception cref="RegionDestroyedException">
          /// region has been destroyed
          /// </exception>
          Generic::RegionEntry<TKey, TValue>^ GetEntry( TKey key );

          /// <summary>
          /// Gets the entries in this region.
          /// For both local & distributed region instances, this operation happens in local cache only.
          /// </summary>
          /// <param name="recursive">
          /// if true, also return all nested subregion entries
          /// </param>
          /// <returns>collection of entries</returns>
          System::Collections::Generic::ICollection<Generic::RegionEntry<TKey, TValue>^>^ GetEntries(bool recursive);

          /// <summary>
          /// Gets the RegionService for this region.
          /// </summary>
          /// <returns>RegionService</returns>
          property GemStone::GemFire::Cache::Generic::IRegionService^ RegionService
          {
            GemStone::GemFire::Cache::Generic::IRegionService^ get( );
          }

          /// <summary>
          /// True if the region contains a value for the given key.
          /// This only searches in the local cache.
          /// </summary>
          /// <remark>
          /// For both local & distributed region instances this always searches only in local cache.
          /// </remark>
          /// <param name="key">key to search for</param>
          /// <returns>true if value is not null</returns>
          bool ContainsValueForKey( TKey key );

          /// <summary>
          /// True if this region has been destroyed.
          /// </summary>
          /// <returns>true if destroyed</returns>
          property bool IsDestroyed
          {
            bool get();
          }

          /// <summary>
          /// remove all entries in the region.	
          /// For local region instance - remove all entries in the local region.
          /// For distributed region instance - remove all entries in the local region, 
	        /// and propagate the operation to server.
          /// </summary>
          /// <param name="callbackArg">
          /// argument that is passed to the callback functions
          /// </param>             
          void Clear(Object^ callbackArg);

          /// <summary>
          /// Reteuns an instance of a Region<TKey, TValue> class that implements 
          /// ISubscriptionService interface
          /// This method is applicable only on distributed region & not on local region.
          /// </summary>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          ISubscriptionService<TKey>^ GetSubscriptionService();

          /// <summary>
          /// Reteuns an instance of a Region<TKey, TValue> class that executes within 
          /// a local scope of a process.
          /// This method is applicable only on distributed region & not on local region.
          /// </summary>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          IRegion<TKey, TValue>^ GetLocalView();
          
          /// <summary>
          /// Executes the query on the server based on the predicate.
          /// Valid only for a Native Client region.
          /// This method is applicable only on distributed region & not on local region.
          /// </summary>
          /// <param name="predicate">The query predicate (just the WHERE clause) or the entire query to execute</param>
          /// <exception cref="IllegalArgumentException">
          /// If the predicate is empty.
          /// </exception>
          /// <exception cref="IllegalStateException">
          /// If some error occurred.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          /// <exception cref="QueryException">
          /// If some query error occurred at the server.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="CacheClosedException">
          /// if the cache has been closed
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <returns>
          /// The SelectResults which can either be a ResultSet or a StructSet.
          /// </returns>
          generic<class TResult>
          ISelectResults<TResult>^ Query( String^ predicate );

          /// <summary>
          /// Executes the query on the server based on the predicate.
          /// Valid only for a Native Client region.
          /// This method is applicable only on distributed region & not on local region.
          /// </summary>
          /// <param name="predicate">The query predicate (just the WHERE clause) or the entire query to execute</param>
          /// <param name="timeout">The time (in seconds) to wait for the query response, optional</param>
          /// <exception cref="IllegalArgumentException">
          /// If the predicate is empty.
          /// </exception>
          /// <exception cref="IllegalStateException">
          /// If some error occurred.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          /// <exception cref="QueryException">
          /// If some query error occurred at the server.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="CacheClosedException">
          /// if the cache has been closed
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <returns>
          /// The SelectResults which can either be a ResultSet or a StructSet.
          /// </returns>
          generic<class TResult>
          ISelectResults<TResult>^ Query( String^ predicate, uint32_t timeout );

          /// <summary>
          /// Executes the query on the server based on the predicate
          /// and returns whether any result exists.
          /// Valid only for a Native Client region.
          /// This method is applicable only on distributed region & not on local region.
          /// </summary>
          /// <param name="predicate">
          /// The query predicate (just the WHERE clause)
          /// or the entire query to execute
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the predicate is empty.
          /// </exception>
          /// <exception cref="IllegalStateException">
          /// If some error occurred.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          /// <exception cref="QueryException">
          /// If some query error occurred at the server.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="CacheClosedException">
          /// if the cache has been closed
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <returns>
          /// true if the result size is non-zero, false otherwise.
          /// </returns>
          bool ExistsValue( String^ predicate );

          /// <summary>
          /// Executes the query on the server based on the predicate
          /// and returns whether any result exists.
          /// Valid only for a  Native Client region.
          /// This method is applicable only on distributed region & not on local region.
          /// </summary>
          /// <param name="predicate">
          /// The query predicate (just the WHERE clause)
          /// or the entire query to execute
          /// </param>
          /// <param name="timeout">
          /// The time (in seconds) to wait for the query response
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the predicate is empty.
          /// </exception>
          /// <exception cref="IllegalStateException">
          /// If some error occurred.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          /// <exception cref="QueryException">
          /// If some query error occurred at the server.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="CacheClosedException">
          /// if the cache has been closed
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <returns>
          /// true if the result size is non-zero, false otherwise.
          /// </returns>
          bool ExistsValue( String^ predicate, uint32_t timeout );

          /// <summary>
          /// Executes the query on the server based on the predicate
          /// and returns a single result value.
          /// Valid only for a Native Client region.
          /// This method is applicable only on distributed region & not on local region.
          /// </summary>
          /// <param name="predicate">
          /// The query predicate (just the WHERE clause)
          /// or the entire query to execute
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the predicate is empty.
          /// </exception>
          /// <exception cref="IllegalStateException">
          /// If some error occurred.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          /// <exception cref="QueryException">
          /// If some query error occurred at the server,
          /// or more than one result items are available.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="CacheClosedException">
          /// if the cache has been closed
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <returns>
          /// The single ResultSet or StructSet item,
          /// or NULL of no results are available.
          /// </returns>
          Object^ SelectValue( String^ predicate );

          /// <summary>
          /// Executes the query on the server based on the predicate
          /// and returns a single result value.
          /// Valid only for a Native Client region.
          /// This method is applicable only on distributed region & not on local region.
          /// </summary>
          /// <param name="predicate">
          /// The query predicate (just the WHERE clause)
          /// or the entire query to execute
          /// </param>
          /// <param name="timeout">
          /// The time (in seconds) to wait for the query response
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the predicate is empty.
          /// </exception>
          /// <exception cref="IllegalStateException">
          /// If some error occurred.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          /// <exception cref="QueryException">
          /// If some query error occurred at the server,
          /// or more than one result items are available.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="CacheClosedException">
          /// if the cache has been closed
          /// </exception>
          /// <exception cref="NotSupportedException">
          /// if it is called by local region instance <see cref="Region.GetLocalView" />
          /// </exception>
          /// <returns>
          /// The single ResultSet or StructSet item,
          /// or NULL of no results are available.
          /// </returns>
          Object^ SelectValue( String^ predicate, uint32_t timeout );

      };

    }
  }
}
} //namespace 
