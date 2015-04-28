package com.gemstone.gemfire.internal.cache;
/**
 * 
 * @author ashahid
 *
 */
public interface TxEntryFactory
{
  /**
   * Creates an instance of TXEntry.
   * @return the created entry
   */
  public TXEntry createEntry(LocalRegion localRegion, KeyInfo key,
      TXStateInterface tx);
  
  public TXEntry createEntry(LocalRegion localRegion, KeyInfo key,
      TXStateInterface tx, boolean rememberReads);
  
}
