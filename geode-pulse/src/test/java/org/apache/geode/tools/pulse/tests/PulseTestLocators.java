/*
 *
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
 *
 */
package org.apache.geode.tools.pulse.tests;

public class PulseTestLocators {
	public static class HtmlAttributes{
		public static final String classAttribute = "class";
		public static final String typeAttribute = "type";
		public static final String idAttribute = "id";
		public static final String nameAttribute = "name";
		public static final String placeholderAttribute = "placeholder";
		public static final String valueAttribute = "value";	
		public static final String styleAttribute ="style";
	}
	
	public static class TopNavigation {
		public static final String clusterViewLinkXpath = "//a[text()='Cluster View']";
	}

	public static class ClusterStatus {

	}

	public static class MemberDetailsView {
    public static final String gridButtonXpath = "//a[@id='btngridIcon']";
  }

  public static class TopologyView {

		public static final String radioButtonId = "radio-default";
		public static final String radioButtonXpath = "//label[text()='Topology']";
		public static final String gridButtonId = "default_grid_button";
		public static final String nodeH1Id = "h1";
		public static final String nodeH2Id = "h2";
		public static final String nodeH3Id = "h3";
		public static final String memberM1Id = "M1";
		public static final String memberM2Id = "M2";
		public static final String memberM3Id = "M3";
		public static final String treeMapButtonId = "default_treemap_button";
		
		// Host tootips
		public static final String hostNameTTXpath = ".//*[@id='_tooltip']/div/div/div[1]";
		public static final String cpuUsageTTXpath = ".//*[@id='_tooltip']/div/div/div[2]/div[1]/div[2]/div";
		public static final String memoryUsageTTXpath = ".//*[@id='_tooltip']/div/div/div[2]/div[2]/div[2]/div";
		public static final String loadAvgTTXpath = ".//*[@id='_tooltip']/div/div/div[2]/div[3]/div[2]/div";
		public static final String soketsTTXpath = ".//*[@id='_tooltip']/div/div/div[2]/div[4]/div[2]/div";
		
		// Member tooltips
		public static final String memNameTTXpath = ".//*[@id='_tooltip']/div/div/div[1]";
		public static final String memCpuUsageTTXpath = ".//*[@id='_tooltip']/div/div/div[2]/div[1]/div[2]/div";
		public static final String threadsTTXpath = ".//*[@id='_tooltip']/div/div/div[2]/div[2]/div[2]/div/text()";
		public static final String jvmPausesTTXpath = ".//*[@id='_tooltip']/div/div/div[2]/div[3]/div[2]/div";
		public static final String regionsTTXpath = ".//*[@id='_tooltip']/div/div/div[2]/div[4]/div[2]/div";
		public static final String clientsTTXpath = ".//*[@id='_tooltip']/div/div/div[2]/div[5]/div[2]/div";
		public static final String gatewaySenderTtXpath = ".//*[@id='_tooltip']/div/div/div[2]/div[6]/div[2]/div";
		public static final String portTTXpath = ".//*[@id='_tooltip']/div/div/div[2]/div[7]/div[2]/div";

		// Grid view
		public static final String idM1Xpath = ".//*[@id='M1&M1']/td[1]";
		public static final String nameM1Xpath = ".//*[@id='M1&M1']/td[2]";
		public static final String hostH1Xpath = ".//*[@id='M1&M1']/td[3]";
		public static final String idM2Xpath = ".//*[@id='M2&M2']/td[1]";
		public static final String nameM2Xpath = ".//*[@id='M2&M2']/td[2]";
		public static final String hostH2Xpath = ".//*[@id='M2&M2']/td[3]";
		public static final String idM3Xpath = ".//*[@id='M3&M3']/td[1]";
		public static final String nameM3Xpath = ".//*[@id='M3&M3']/td[2]";
		public static final String hostH3Xpath = ".//*[@id='M3&M3']/td[3]";

		// HotSpot locators 
		public static final String hotSpotId = "currentHotSpot";
		public static final String hotspotListDivId = "hotspotList";
		public static final String heapUsageXpath = "//a[text()='Heap Usage']";
		public static final String cpuUsageXpath = "//a[text()='CPU Usage']";
		public static final String graphTreeMapLblId = "//div[@id='GraphTreeMap-label']/child::node()";

    }

	public static class ServerGroups {

		public static final String radioButtonId = "radio-servergroups";
		public static final String radioButtonXpath = "//label[text()='Server Groups']";
		public static final String gridButtonId = "servergroups_grid_button";
		public static final String gridBlockId = "servergroups_grid_block";

		public static final String serverGrpsRadioId = "member_view_option_servergroups";
		
		public static final String serverGrpsRadioXpath = "//label[@for='radio-servergroups']";

		public static final String serverGrp1Id = "SG1";
		public static final String serverGrp2Id = "SG2";
		public static final String serverGrp3Id = "SG3";

		public static final String serverGrp1Xpath = ".//*[@id='SG1']/div";
		public static final String serverGrp2Xpath = ".//*[@id='SG2']/div";
		public static final String serverGrp3Xpath = ".//*[@id='SG3']/div";

		public static final String sg1M1Id = "SG1(!)M1";
		public static final String sg1M2Id = "SG1(!)M2";
		public static final String sg1M3Id = "SG1(!)M3";
		public static final String sg2M1Id = "SG2(!)M1";
		public static final String sg2M2Id = "SG2(!)M2";
		public static final String sg3M3Id = "SG3(!)M3";

		// Grid view
		public static final String idSG1M3Xpath = ".//*[@id='M3&M3']/td[2]";
		public static final String idSG1M2Xpath = ".//*[@id='M2&M2']/td[2]";
		public static final String idSG1M1Xpath = ".//*[@id='M1&M1']/td[2]";
		public static final String nameM3Xpath = ".//*[@id='M3&M3']/td[3]";
		public static final String nameM2Xpath = ".//*[@id='M2&M2']/td[3]";
		public static final String nameM1Xpath = ".//*[@id='M1&M1']/td[3]";
		
		//HotSpot locators
		public static final String hotSpotId = "currentHotSpot";
		public static final String hotspotListDivId= "hotspotList";
		public static final String heapUsageXpath= "//a[text()='Heap Usage']";
		public static final String cpuUsageXpath= "//a[text()='CPU Usage']";
		public static final String graphTreeMapLblId = "//div[@id='GraphTreeMap-label']/child::node()";		
		
	}

	public static class RedundancyZone {

		public static final String radioButtonId = "radio-redundancyzones";
		public static final String radioButtonXpathAlt = "//label[text()='Redundancy Zones']";		
		public static final String radioButtonXpath = "//label[@for='radio-redundancyzones']";
		
		public static final String gridButtonId = "redundancyzones_grid_button";

		public static final String zoneRZ1Id = "RZ1 RZ2";
		public static final String zoneRZ2Id = "RZ2";
		

		public static final String zoneRZ1RZ2Xpath = ".//*[@id='RZ1 RZ2']/div";
		public static final String zoneRZ2Xpath = ".//*[@id='RZ2']/div";

		public static final String m1RZ1RZ2Id = "RZ1 RZ2(!)M1";
		public static final String m2RZ1Id = "RZ1 RZ2(!)M2";
		public static final String m3RZ2Id = "RZ2(!)M3";
//		public static final String m3RZ2Id = "RZ2(!)M3";
//		public static final String m2RZ2Id = "RZ2(!)M2";
		// Grid
		public static final String idM2Xpath = ".//*[@id='M2&M2']/td[2]";
		public static final String idM1Xpath = ".//*[@id='M1&M1']/td[2]";
		public static final String idM3Xpath = ".//*[@id='M3&M3']/td[2]";
		
		//HotSpot locators
		public static final String hotSpotId = "currentHotSpot";
		public static final String hotspotListDivId= "hotspotList";
		public static final String heapUsageXpath= "//a[text()='Heap Usage']";
		public static final String cpuUsageXpath= "//a[text()='CPU Usage']";
		public static final String graphTreeMapLblId = "//div[@id='GraphTreeMap-label']/child::node()";
	}

	public static class DataPerspectiveView {
		public static final String downarrowButtonXpath = "//a[text()='Members']";
		public static final String dataViewButtonXpath = "//a[text()='Data']";
		public static final String gridButtonId = "data_grid_button";
	}

	public static class RegionDetailsView {

		public static final String regionNameDivId = "regionNameText";
		public static final String regionPathId = "regionPath";
		public static final String treeMapCanvasXpath = "//canvas[@id='GraphTreeMapClusterData-canvas']";
		public static final String regionTypeId = "regionType";
		public static final String regionMembersTextId = "regionMembersText";
		public static final String regionEmptyNodesId = "regionEmptyNodes";
		public static final String regionEntryCountTextId = "regionEntryCountText";
		public static final String regionDiskUsageId = "regionDiskUsage";
		public static final String regionPersistenceId = "regionPersistence";

		public static final String gridButtonXpath = "//a[@id='btngridIcon']";
		public static final String memoryUsedId = "memoryUsed";
		public static final String totalMemoryId = "totalMemory";

		public static final String inMemoryReadsId = "currentReadsPerSec";
		public static final String inMemoryWritesId = "currentWritesPerSec";
		public static final String diskReadsId = "currentDiskReadsPerSec";
		public static final String diskWritesId = "currentDiskWritesPerSec";

		public static final String memberNameId = "memberName";

	}

	public static class DataBrowser {
		public static final String rgnFilterTxtBoxId = "filterTextRegion";
		public static final String rgnNameSpanXpath = "//span[starts-with(@ID,'treeDemo_')][contains(@id,'_span')]";
		public static final String rgnNameTxtBoxXpath = "//span[starts-with(@ID,'treeDemo_')][contains(@id,'_span')]";
		public static final String rgnSpanFirstPart = "//span[@id='treeDemo_";
		public static final String rgnSpanSecondPart = "_span']";
		public static final String rgn1ChkBoxId = "treeDemo_1_check";
		public static final String queryEditorTxtBoxId = "dataBrowserQueryText";
		public static final String btnExecuteQueryId = "btnExecuteQuery";
		
		public static final String divDataRegions = "//div/ul[@id='treeDemo']/li";
		
		// History section		
		public static final String historyIcon = "historyIcon";
		public static final String historyLst = "//div[@id='detailsHistoryList']/div/div";
		public static final String queryText = ".wrapHistoryContent";
		public static final String historyDateTime = ".dateTimeHistory";
		
		//Clear button 
		
		public static final String btnClearXpath = "//input[@value='Clear']";

	}

}
