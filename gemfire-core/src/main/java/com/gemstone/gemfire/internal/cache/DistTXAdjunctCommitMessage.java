package com.gemstone.gemfire.internal.cache;


import java.util.Collections;
import java.util.Iterator;
import org.apache.logging.log4j.Logger;
import com.gemstone.gemfire.cache.CacheRuntimeException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.logging.LogService;

public class DistTXAdjunctCommitMessage extends TXCommitMessage{

  private static final Logger logger = LogService.getLogger();

  public DistTXAdjunctCommitMessage(TXId txIdent, DM dm, TXState txState) {
    super(txIdent, dm, txState);
  }

  @Override
  public void basicProcessOps() {
    Collections.sort(this.farSideEntryOps);
    Iterator it = this.farSideEntryOps.iterator();
    while (it.hasNext()) {
      try {
        RegionCommit.FarSideEntryOp entryOp = (RegionCommit.FarSideEntryOp) it
            .next();
        entryOp.processAdjunctOnly();
      } catch (CacheRuntimeException problem) {
        processCacheRuntimeException(problem);
      } catch (Exception e) {
        addProcessingException(e);
      }
    }
  }  
}
