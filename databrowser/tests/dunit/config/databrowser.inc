INCLUDE $JTESTS/dunit/impl/dunitN.inc;

hydra.BridgePrms-names       	         	= bridge;

hydra.CachePrms-names	                = server client;

hydra.RegionPrms-names                  = serverRegion1	     clientRegion;
hydra.RegionPrms-regionName     		= Customer           Customer;
hydra.RegionPrms-interestPolicy         = All                All;
hydra.RegionPrms-scope                  = default            local; 
hydra.RegionPrms-dataPolicy     		= replicate          normal; 
hydra.RegionPrms-partitionName  		= none               none;	
hydra.RegionPrms-multicastEnabled 		= true               false;
hydra.RegionPrms-poolName			    = none               edgeDescript;
hydra.RegionPrms-statisticsEnabled		= true;

admin.jmx.RecyclePrms-numberOfEntitiesInRegion 	= 10;

//Pool attributes.
hydra.PoolPrms-names				= edgeDescript;
hydra.PoolPrms-minConnections			= 10;
hydra.PoolPrms-maxConnections			= 50;
hydra.PoolPrms-subscriptionEnabled		= true;//false
hydra.PoolPrms-threadLocalConnections		= true;
hydra.PoolPrms-readTimeout			= 60000;
hydra.PoolPrms-subscriptionRedundancy		= 0;
hydra.PoolPrms-idleTimeout			= -1;
hydra.PoolPrms-statisticInterval		= 5000;

hydra.GemFirePrms-logLevel                 = fine;