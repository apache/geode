/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
//$Id: RingNodeFlowControl.java,v 1.4 2005/08/08 12:45:41 belaban Exp $

package com.gemstone.org.jgroups.protocols.ring;

import com.gemstone.org.jgroups.util.GemFireTracer;


public class RingNodeFlowControl
{
   final int initialWindow;
   final float windowReduceFactor;
   final int belowThresholdAdvanceAmount;
   final float aboveThresholdAdvanceAmount;
   private int memberCount;
   private int previousBacklog;
   private int backlog;
    protected final GemFireTracer log=GemFireTracer.getLog(this.getClass());

   public RingNodeFlowControl(int initialWindow,
                              float windowReduceFactor,
                              int belowThresholdAdvanceAmount,
                              float aboveThresholdAdvanceAmount)
   {
      this.initialWindow = initialWindow;
      this.windowReduceFactor = windowReduceFactor;
      this.belowThresholdAdvanceAmount = belowThresholdAdvanceAmount;
      this.aboveThresholdAdvanceAmount = aboveThresholdAdvanceAmount;
   }

   public RingNodeFlowControl()
   {
      this(20, 0.7F, 3, 1.0F);
   }

   public void invalidate()
   {
      previousBacklog = backlog = 0;
   }

   public int getBacklog()
   {
      return backlog;
   }

   public void setBacklog(int backlog)
   {
      if(backlog <0)
      throw new IllegalArgumentException("backlog value has to be positive");
      this.backlog = backlog;
   }

   public int getBacklogDifference()
   {
      return backlog - previousBacklog;
   }

   public int getPreviousBacklog()
   {
      return previousBacklog;
   }

   public void setPreviousBacklog()
   {
      this.previousBacklog = backlog;
   }

   public void viewChanged(int memberCount)
   {
      this.memberCount = memberCount;
   }

   public int getAllowedToBroadcast(RingToken token)
   {
      int fairWindowShare = 0;
      int windowSize = token.getWindowSize();
      if (memberCount == 0) memberCount = 1;
      int maxMessages = (windowSize / memberCount);
      if (maxMessages < 1)
         maxMessages = 1;

      int backlogAverage = token.getBacklog() + backlog - previousBacklog;
      if (backlogAverage > 0)
      {
         fairWindowShare = windowSize * backlog / backlogAverage;
      }
      fairWindowShare = (fairWindowShare < 1)?1: fairWindowShare;


      int maxAllowed = windowSize - token.getLastRoundBroadcastCount();
      if (maxAllowed < 1)
         maxAllowed = 0;


         if(log.isInfoEnabled()) log.info("fairWindowShare=" + fairWindowShare + " maxMessages="
                    + maxMessages + " maxAllowed=" + maxAllowed);

      return (fairWindowShare < maxAllowed)?Math.min(fairWindowShare, maxMessages):Math.min(maxAllowed, maxMessages);
   }

   public void updateWindow(RingToken token)
   {
      int threshold = token.getWindowThreshold();
      int window = token.getWindowSize();
      if (window < initialWindow)
      {
         window = initialWindow;
      }

      boolean congested =(token.getRetransmissionRequests().size() > 0);

      if (congested)
      {
         threshold = (int) (window * windowReduceFactor);
         window = initialWindow;
      }
      else
      {
         if (window < threshold)
         {
            window += belowThresholdAdvanceAmount;
         }
         else
         {
            window += aboveThresholdAdvanceAmount;
         }
      }
      token.setWindowSize(window);
      token.setWindowThreshold(threshold);
   }

}
