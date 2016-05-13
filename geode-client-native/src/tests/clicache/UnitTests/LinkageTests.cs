//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [TestFixture]
  [Category("group3")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class LinkageTests : UnitTests
  {
    protected override ClientBase[] GetClients()
    {
      return null;
    }

    [TestFixtureTearDown]
    public override void EndTests()
    {
      try
      {
        CacheHelper.Close();
      }
      finally
      {
        base.EndTests();
      }
    }

    [Test]
    public void Linkage()
    {
      AttributesFactory af = new AttributesFactory();

      #region IGFSerializable built-in types

      CacheableString cStr = new CacheableString("key1");
      CacheableInt32 cInt = new CacheableInt32(1);
      byte[] buf = System.Text.Encoding.ASCII.GetBytes("key1");
      CacheableBytes cBytes = CacheableBytes.Create(buf);
      CacheableObject cObj = CacheableObject.Create("key1");
      CacheableObjectXml cObjXml = CacheableObjectXml.Create("key1");

      #endregion

      #region All GemFire Exception types

      GemFireException ex = new GemFireException("test message");
      GemStone.GemFire.Cache.AssertionException aAssertionException =
        new GemStone.GemFire.Cache.AssertionException("AssertionException");
      IllegalArgumentException aIllegalArgumentException =
        new IllegalArgumentException("IllegalArgumentException");
      IllegalStateException aIllegalStateException =
        new IllegalStateException("IllegalStateException");
      CacheExistsException aCacheExistsException =
        new CacheExistsException("CacheExistsException");
      CacheXmlException aCacheXmlException =
        new CacheXmlException("CacheXmlException");
      TimeoutException aTimeoutException =
        new TimeoutException("TimeoutException");
      CacheWriterException aCacheWriterException =
        new CacheWriterException("CacheWriterException");
      RegionExistsException aRegionExistsException =
        new RegionExistsException("RegionExistsException");
      CacheClosedException aCacheClosedException =
        new CacheClosedException("CacheClosedException");
      LeaseExpiredException aLeaseExpiredException =
        new LeaseExpiredException("LeaseExpiredException");
      CacheLoaderException aCacheLoaderException =
        new CacheLoaderException("CacheLoaderException");
      RegionDestroyedException aRegionDestroyedException =
        new RegionDestroyedException("RegionDestroyedException");
      EntryDestroyedException aEntryDestroyedException =
        new EntryDestroyedException("EntryDestroyedException");
      NoSystemException aNoSystemException =
        new NoSystemException("NoSystemException");
      AlreadyConnectedException aAlreadyConnectedException =
        new AlreadyConnectedException("AlreadyConnectedException");
      FileNotFoundException aFileNotFoundException =
        new FileNotFoundException("FileNotFoundException");
      InterruptedException aInterruptedException =
        new InterruptedException("InterruptedException");
      UnsupportedOperationException aUnsupportedOperationException =
        new UnsupportedOperationException("UnsupportedOperationException");
      StatisticsDisabledException aStatisticsDisabledException =
        new StatisticsDisabledException("StatisticsDisabledException");
      ConcurrentModificationException aConcurrentModificationException =
        new ConcurrentModificationException("ConcurrentModificationException");
      UnknownException aUnknownException =
        new UnknownException("UnknownException");
      ClassCastException aClassCastException =
        new ClassCastException("ClassCastException");
      EntryNotFoundException aEntryNotFoundException =
        new EntryNotFoundException("EntryNotFoundException");
      GemFireIOException aGemFireIOException =
        new GemFireIOException("GemFireIOException");
      GemFireConfigException aGemFireConfigException =
        new GemFireConfigException("GemFireConfigException");
      NullPointerException aNullPointerException =
        new NullPointerException("NullPointerException");
      EntryExistsException aEntryExistsException =
        new EntryExistsException("EntryExistsException");
      NotConnectedException aNotConnectedException =
        new NotConnectedException("NotConnectedException");
      CacheProxyException aCacheProxyException =
        new CacheProxyException("CacheProxyException");
      OutOfMemoryException aOutOfMemoryException =
        new OutOfMemoryException("OutOfMemoryException");
      NotOwnerException aNotOwnerException =
        new NotOwnerException("NotOwnerException");
      WrongRegionScopeException aWrongRegionScopeException =
        new WrongRegionScopeException("WrongRegionScopeException");
      BufferSizeExceededException aBufferSizeExceededException =
        new BufferSizeExceededException("BufferSizeExceededException");
      RegionCreationFailedException aRegionCreationFailedException =
        new RegionCreationFailedException("RegionCreationFailedException");
      FatalInternalException aFatalInternalException =
        new FatalInternalException("FatalInternalException");
      DiskFailureException aDiskFailureException =
        new DiskFailureException("DiskFailureException");
      DiskCorruptException aDiskCorruptException =
        new DiskCorruptException("DiskCorruptException");
      InitFailedException aInitFailedException =
        new InitFailedException("InitFailedException");
      ShutdownFailedException aShutdownFailedException =
        new ShutdownFailedException("ShutdownFailedException");

      #endregion

      // add other types here...

      CacheHelper.InitName("foo", "playmoney");
      DistributedSystem dsys = CacheHelper.DSYS;
      Cache cache = CacheHelper.DCache;
    }
  }
}
