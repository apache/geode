/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
//#include "ExceptionTypes.hpp"
using namespace System;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      generic<class TKey>
      /// <summary>
      /// This generic interface class provides all Register Interest API's for 
      /// gemfire's generic non local region (Region<TKey, TValue>).
      /// Region<TKey, TValue> class implements all methods of this interface class.
      /// LocalRegion<TKey, TValue> class does not implement this interface class.
      /// </summary>
      public interface class ISubscriptionService
      {
        public:

          /// <summary>
          /// Registers a collection of keys for getting updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// </summary>
          /// <param name="keys">a collection of keys</param>
          /// <exception cref="IllegalArgumentException">
          /// If the collection of keys is empty.
          /// </exception>
          /// <exception cref="IllegalStateException">
          /// If already registered interest for all keys.
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
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterKeys( System::Collections::Generic::ICollection<TKey>^ keys );

          /// <summary>
          /// Registers a collection of keys for getting updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// Should only be called for durable clients and with cache server version 5.5 onwards.
          /// </summary>
          /// <param name="keys">a collection of keys</param>
          /// <param name="isDurable">whether the registration should be durable</param>
          /// <param name="getInitialValues">
          /// true to populate the cache with values of the keys
          /// that were registered on the server
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the collection of keys is empty.
          /// </exception>
          /// <exception cref="IllegalStateException">
          /// If already registered interest for all keys.
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
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterKeys(System::Collections::Generic::ICollection<TKey>^ keys, bool isDurable, bool getInitialValues);

          /// <summary>
          /// Registers a collection of keys for getting updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// Should only be called for durable clients and with cache server version 5.5 onwards.
          /// </summary>
          /// <param name="keys">a collection of keys</param>
          /// <param name="isDurable">whether the registration should be durable</param>
          /// <param name="getInitialValues">
          /// true to populate the cache with values of the keys
          /// that were registered on the server
          /// </param>
          /// <param name="receiveValues">
          /// whether to act like notify-by-subscription is true
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the collection of keys is empty.
          /// </exception>
          /// <exception cref="IllegalStateException">
          /// If already registered interest for all keys.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterKeys(System::Collections::Generic::ICollection<TKey>^ keys, bool isDurable, bool getInitialValues, bool receiveValues);

          /// <summary>
          /// Unregisters a collection of keys to stop getting updates for them.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// </summary>
          /// <param name="keys">the collection of keys</param>
          /// <exception cref="IllegalArgumentException">
          /// If the collection of keys is empty.
          /// </exception>
          /// <exception cref="IllegalStateException">
          /// If no keys were previously registered.
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
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void UnregisterKeys(System::Collections::Generic::ICollection<TKey>^ keys);

          /// <summary>
          /// Register interest for all the keys of the region to get
          /// updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// </summary>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterAllKeys( );

          /// <summary>
          /// Register interest for all the keys of the region to get
          /// updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// Should only be called for durable clients and with cache server version 5.5 onwards.
          /// </summary>
          /// <param name="isDurable">whether the registration should be durable</param>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterAllKeys( bool isDurable );

          /// <summary>
          /// Register interest for all the keys of the region to get
          /// updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// Should only be called for durable clients and with cache server version 5.5 onwards.
          /// </summary>
          /// <param name="isDurable">whether the registration should be durable</param>
          /// <param name="resultKeys">
          /// if non-null then all keys on the server are returned
          /// </param>
          /// <param name="getInitialValues">
          /// true to populate the cache with values of all the keys
          /// from the server
          /// </param>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// For pools configured with locators, if no locators are available, innerException
          /// of NotConnectedException is set to NoAvailableLocatorsException.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterAllKeys(bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys,
            bool getInitialValues);

          /// <summary>
          /// Register interest for all the keys of the region to get
          /// updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// Should only be called for durable clients and with cache server version 5.5 onwards.
          /// </summary>
          /// <param name="isDurable">whether the registration should be durable</param>
          /// <param name="resultKeys">
          /// if non-null then all keys on the server are returned
          /// </param>
          /// <param name="getInitialValues">
          /// true to populate the cache with values of all the keys
          /// from the server
          /// </param>
          /// <param name="receiveValues">
          /// whether to act like notify-by-subscription is true
          /// </param>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterAllKeys(bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys,
            bool getInitialValues,
            bool receiveValues);

          /// <summary>
          /// get the interest list on this client
          /// </summary>
          System::Collections::Generic::ICollection<TKey>^ GetInterestList();

          /// <summary>
          /// get the list of interest regular expressions on this client
          /// </summary>
          System::Collections::Generic::ICollection<String^>^ GetInterestListRegex();

          /// <summary>
          /// Unregister interest for all the keys of the region to stop
          /// getting updates for them.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// </summary>
          /// <exception cref="IllegalStateException">
          /// If not previously registered all keys.
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
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void UnregisterAllKeys( );

          /// <summary>
          /// Register interest for the keys of the region that match the
          /// given regular expression to get updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// </summary>
          /// <exception cref="IllegalArgumentException">
          /// If the regular expression string is empty.
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
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterRegex(String^ regex );

          /// <summary>
          /// Register interest for the keys of the region that match the
          /// given regular expression to get updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// Should only be called for durable clients and with cache server version 5.5 onwards.
          /// </summary>
          /// <param name="regex">the regular expression to register</param>
          /// <param name="isDurable">whether the registration should be durable</param>
          /// <exception cref="IllegalArgumentException">
          /// If the regular expression string is empty.
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
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterRegex( String^ regex, bool isDurable );

          /// <summary>
          /// Register interest for the keys of the region that match the
          /// given regular expression to get updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// Should only be called for durable clients and with cache server version 5.5 onwards.
          /// </summary>
          /// <param name="regex">the regular expression to register</param>
          /// <param name="isDurable">whether the registration should be durable</param>
          /// <param name="resultKeys">
          /// if non-null then the keys that match the regular expression
          /// on the server are returned
          ///</param>
          /// <exception cref="IllegalArgumentException">
          /// If the regular expression string is empty.
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
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterRegex(String^ regex, bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys);

          /// <summary>
          /// Register interest for the keys of the region that match the
          /// given regular expression to get updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// Should only be called for durable clients and with cache server version 5.5 onwards.
          /// </summary>
          /// <param name="regex">the regular expression to register</param>
          /// <param name="isDurable">whether the registration should be durable</param>
          /// <param name="resultKeys">
          /// if non-null then the keys that match the regular expression
          /// on the server are returned
          ///</param>
          /// <param name="getInitialValues">
          /// true to populate the cache with values of the keys
          /// that were registered on the server
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the regular expression string is empty.
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
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterRegex(String^ regex, bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys, bool getInitialValues);

          /// <summary>
          /// Register interest for the keys of the region that match the
          /// given regular expression to get updates from the server.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// Should only be called for durable clients and with cache server version 5.5 onwards.
          /// </summary>
          /// <param name="regex">the regular expression to register</param>
          /// <param name="isDurable">whether the registration should be durable</param>
          /// <param name="resultKeys">
          /// if non-null then the keys that match the regular expression
          /// on the server are returned
          ///</param>
          /// <param name="getInitialValues">
          /// true to populate the cache with values of the keys
          /// that were registered on the server
          /// </param>
          /// <param name="receiveValues">
          /// whether to act like notify-by-subscription is true
          /// </param>
          /// <exception cref="IllegalArgumentException">
          /// If the regular expression string is empty.
          /// </exception>
          /// <exception cref="CacheServerException">
          /// If an exception is received from the Java cache server.
          /// </exception>
          /// <exception cref="NotConnectedException">
          /// if not connected to the GemFire system because the client cannot
          /// establish usable connections to any of the servers given to it.
          /// </exception>
          /// <exception cref="MessageException">
          /// If the message received from server could not be handled. This will
          /// be the case when an unregistered typeId is received in the reply or
          /// reply is not well formed. More information can be found in the log.
          /// </exception>
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void RegisterRegex(String^ regex, bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys, bool getInitialValues, bool receiveValues);
          
          /// <summary>
          /// Unregister interest for the keys of the region that match the
          /// given regular expression to stop getting updates for them.
          /// The regular expression must have been registered previously using
          /// a <c>RegisterRegex</c> call.
          /// Valid only for a Native Client region when client notification
          /// ( <see cref="AttributesFactory.SetClientNotificationEnabled" /> ) is true.
          /// </summary>
          /// <exception cref="IllegalArgumentException">
          /// If the regular expression string is empty.
          /// </exception>
          /// <exception cref="IllegalStateException">
          /// If this regular expression has not been registered by a previous
          /// call to <c>RegisterRegex</c>.
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
          /// <exception cref="RegionDestroyedException">
          /// If region destroy is pending.
          /// </exception>
          /// <exception cref="UnsupportedOperationException">
          /// If the region is not a Native Client region or
          /// <see cref="AttributesFactory.SetClientNotificationEnabled" /> is false.
          /// </exception>
          /// <exception cref="TimeoutException">
          /// if the operation timed out
          /// </exception>
          /// <exception cref="UnknownException">For other exceptions.</exception>
          void UnregisterRegex( String^ regex );

      };

    }

  }

}
 } //namespace 
