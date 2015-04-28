package com.gemstone.org.jgroups;

/**
 * GemStoneAddition - connection attempt failed due to reuse
 * of IpAddress.  This can happen on Windows & less frequently
 * on Unix
 * 
 * @author bruce
 *
 */
public class ShunnedAddressException extends RuntimeException
{
  private static final long serialVersionUID = 6638258566493306758L;
}
