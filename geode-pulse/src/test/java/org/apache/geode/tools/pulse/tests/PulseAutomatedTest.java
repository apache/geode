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
/**
* This test class contains automated tests for Pulse application related to
* 1. Different grid data validations for example - Topology, Server Group, Redundancy Zone
* 2. Data Browser
* 3. 
* 
*
* @version 1.0
* @since GemFire   2014-04-02
*/
package org.apache.geode.tools.pulse.tests;

import static org.junit.Assert.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import org.apache.geode.test.junit.categories.UITest;

@Category(UITest.class)
public class PulseAutomatedTest extends PulseAbstractTest {

	@BeforeClass
	public static void beforeClassSetup() throws Exception {
		setUpServer("pulseUser", "12345", "/pulse-auth.json");
	}

	@Test
	public void serverGroupGridDataValidation() {
		navigateToServerGroupGridView();
		validateServerGroupGridData();
	}

	@Test
	public void redundancyZonesGridDataValidation() {
		navigateToRedundancyZonesGridView();
		validateRedundancyZonesGridData();
	}

	@Test
	public void topologyGridDataValidation() {
		navigateToTopologyGridView();
		validateTopologyGridData();
	}

	@Test
	public void dataViewGridDataValidation() {
		navigateToDataPrespectiveGridView();
		validateDataPrespectiveGridData();
	}

	@Test
	public void regionDetailsGridDataValidation() {
		navigateToRegionDetailsGridView();
		validateRegionDetailsGridData();

	}

	@Test
	public void regionDetailsNavigationTest() {
		navigateToRegionDetailsView();
		assertEquals("/R2", getTextUsingId(PulseTestLocators.RegionDetailsView.regionNameDivId));
	}

	@Test
	public void regionName() {
		navigateToRegionDetailsView();
		assertEquals(getPropertyValue("region.R2.name"), getTextUsingId(PulseTestLocators.RegionDetailsView.regionNameDivId));
	}

	@Test
	public void regionPath() {
		navigateToRegionDetailsView();
		assertEquals(getPropertyValue("region.R2.fullPath"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionPathId));
	}

	@Test
	public void regionType() {
		navigateToRegionDetailsView();
		assertEquals(getPropertyValue("region.R2.regionType"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionTypeId));
	}

	@Test
	public void regionMembers() {
		navigateToRegionDetailsView();
		assertEquals(getPropertyValue("region.R2.memberCount"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionMembersTextId));
	}

	@Test
	public void regionEmptyNodes() {
		navigateToRegionDetailsView();
		assertEquals(getPropertyValue("region.R2.emptyNodes"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionEmptyNodesId));
	}

	@Test
	public void regionEntryCount() {
		navigateToRegionDetailsView();
		assertEquals(getPropertyValue("region.R2.systemRegionEntryCount"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionEntryCountTextId));
	}

	@Test
	public void regionDiskUsage() {
		navigateToRegionDetailsView();
		assertEquals(getPropertyValue("region.R2.diskUsage"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionDiskUsageId));
	}

	@Test
	public void regionPersistence() {
		navigateToRegionDetailsView();
		assertEquals(getPersistanceEnabled(getPropertyValue("region.R2.persistentEnabled")),
				getTextUsingId(PulseTestLocators.RegionDetailsView.regionPersistenceId));
	}

	@Ignore("WIP")
	@Test
	public void regionMemoryUsage() {
		navigateToRegionDetailsView();
		// need to check the respective property values
	}

	@Test
	public void regionInMemoryRead() {
		navigateToRegionDetailsView();
		assertEquals(getPropertyValue("region.R2.getsRate"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.inMemoryReadsId));

	}

	@Test
	public void regionInMemoryWrites() {
		navigateToRegionDetailsView();
		assertEquals(getPropertyValue("region.R2.putsRate"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.inMemoryWritesId));
	}

	@Test
	public void regionDiskRead() {
		navigateToRegionDetailsView();
		assertEquals(getPropertyValue("region.R2.diskReadsRate"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.diskReadsId));
	}

	@Test
	public void regionDiskWrites() {
		navigateToRegionDetailsView();
		assertEquals(getPropertyValue("region.R2.diskWritesRate"),
				getTextUsingId(PulseTestLocators.RegionDetailsView.diskWritesId));
	}

	@Test
	public void clickHostShowsMemberTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH1Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM1Id);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH2Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM2Id);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH3Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM3Id);
	}

	@Test
	@Ignore("Issue with highlighting")
	public void verifyHostTooltipsOfTopologyGraphTest() {		
		for (int i = 1; i <=3; i++) {
			clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
			mouseClickAndHoldOverElementById("h" + i);
			verifyTextPresrntByXpath(PulseTestLocators.TopologyView.hostNameTTXpath, getPropertyValue("member.M" + i + ".host"));
			verifyTextPresrntByXpath(PulseTestLocators.TopologyView.cpuUsageTTXpath, "0%");
			verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memoryUsageTTXpath, getPropertyValue("member.M" + i
					+ ".UsedMemory"));
			verifyTextPresrntByXpath(PulseTestLocators.TopologyView.loadAvgTTXpath, getPropertyValue("member.M" + i
					+ ".loadAverage"));
			verifyTextPresrntByXpath(PulseTestLocators.TopologyView.soketsTTXpath, getPropertyValue("member.M" + i
					+ ".totalFileDescriptorOpen"));
			mouseReleaseById("h" + i);
			driver.navigate().refresh();
		}
	}

	@Ignore("Issues with member tooltip xpath")
	@Test
	public void verifyMemberTooltipsOfTopologyGraphTest() {

		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH1Id);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH1Id);
		mouseClickAndHoldOverElementById(PulseTestLocators.TopologyView.memberM1Id);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memNameTTXpath, getPropertyValue("member.M1.member"));
		//verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memCpuUsageTTXpath, getPropertyValue("member.M1.cpuUsage") + "%");
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.jvmPausesTTXpath, getPropertyValue("member.M1.JVMPauses"));
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.regionsTTXpath, getPropertyValue("member.M1.totalRegionCount"));
		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH2Id);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH2Id);
		mouseClickAndHoldOverElementById(PulseTestLocators.TopologyView.memberM2Id);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memNameTTXpath, getPropertyValue("member.M2.member"));
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memCpuUsageTTXpath, getPropertyValue("member.M2.cpuUsage") + "%");
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.jvmPausesTTXpath, getPropertyValue("member.M2.JVMPauses"));
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.regionsTTXpath, getPropertyValue("member.M2.totalRegionCount"));

		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH3Id);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH3Id);
		mouseClickAndHoldOverElementById(PulseTestLocators.TopologyView.memberM3Id);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memNameTTXpath, getPropertyValue("member.M3.member"));
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.memCpuUsageTTXpath, getPropertyValue("member.M3.cpuUsage") + "%");
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.jvmPausesTTXpath, getPropertyValue("member.M3.JVMPauses"));
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.regionsTTXpath, getPropertyValue("member.M3.totalRegionCount"));

	}

	@Test
	public void VerifyRGraphTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH1Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH2Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.nodeH3Id);
	}

	@Test
	public void clickMembersOfTopologyGraphTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH1Id);
		clickElementUsingId(PulseTestLocators.TopologyView.memberM1Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH2Id);
		clickElementUsingId(PulseTestLocators.TopologyView.memberM2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.nodeH3Id);
		clickElementUsingId(PulseTestLocators.TopologyView.memberM3Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}

	@Test
	public void clickTreeMapViewShowingTreeMapTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM1Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM2Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM3Id);
	}

	@Test
	public void verifyMembersPresentInTreeMapTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM1Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM2Id);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM3Id);
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM1Id, "M1");
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM2Id, "M2");
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM3Id, "M3");
	}

	@Test
	public void clickMemberNavigatingToCorrespondingRegionTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM1Id);
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM1Id, "M1");
		clickElementUsingId(PulseTestLocators.TopologyView.memberM1Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM2Id);
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM2Id, "M2");
		clickElementUsingId(PulseTestLocators.TopologyView.memberM2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.memberM3Id);
		verifyTextPresrntById(PulseTestLocators.TopologyView.memberM3Id, "M3");
		clickElementUsingId(PulseTestLocators.TopologyView.memberM3Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}

	@Test
	public void clickGridButtonShowsGridTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.idM1Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM1Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH1Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.idM2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.idM3Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM3Xpath);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH3Xpath);
	}

	@Test
	public void verifyMembersPresentInGridTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM1Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.nameM1Xpath, "M1");

		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM2Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.nameM2Xpath, "M2");

		verifyElementPresentByXpath(PulseTestLocators.TopologyView.nameM3Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.nameM3Xpath, "M3");
	}

	@Test
	public void verifyHostNamesInGridTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH1Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.hostH1Xpath, "h1");
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH2Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.hostH2Xpath, "h2");
		verifyElementPresentByXpath(PulseTestLocators.TopologyView.hostH3Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.TopologyView.hostH3Xpath, "h3");
	}

	@Test
	public void clickOnGridMemNameNavigatingToCorrespondingRegionTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.nameM1Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");

		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.nameM2Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");

		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.gridButtonId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.nameM3Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}

	@Test
	public void verifyMembersPresentInSvrGrpTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrpsRadioXpath);
		verifyElementPresentById(PulseTestLocators.ServerGroups.serverGrp1Id);
		verifyElementPresentById(PulseTestLocators.ServerGroups.serverGrp2Id);
		verifyElementPresentById(PulseTestLocators.ServerGroups.serverGrp3Id);

		verifyElementPresentById(PulseTestLocators.ServerGroups.sg1M1Id);
		verifyElementPresentById(PulseTestLocators.ServerGroups.sg1M2Id);
		verifyElementPresentById(PulseTestLocators.ServerGroups.sg1M3Id);

		verifyElementPresentById(PulseTestLocators.ServerGroups.sg2M1Id);
		verifyElementPresentById(PulseTestLocators.ServerGroups.sg2M2Id);

		verifyElementPresentById(PulseTestLocators.ServerGroups.sg3M3Id);
	}

	@Test
	public void expandAndCloseServerGroupsTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		//waitForElement(findElementByXpath(PulseTestLocators.ServerGroups.serverGrpsRadioXpath));
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrpsRadioXpath);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp1Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp1Id, "style", "width: 720px; height: 415px;");
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp1Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp1Id, "style", "width: 239.667px; height: 399px;");

		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp2Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp2Id, "style", "width: 720px; height: 415px;");
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp2Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp2Id, "style", "width: 239.667px; height: 399px;");

		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp3Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp3Id, "style", "width: 720px; height: 415px;");
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrp3Xpath);
		verifyElementAttributeById(PulseTestLocators.ServerGroups.serverGrp3Id, "style", "width: 239.667px; height: 399px;");
	}

	@Test
	public void verifyMembersInServGrpTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrpsRadioXpath);

		verifyTextPresrntById(PulseTestLocators.ServerGroups.serverGrp1Id, "SG1");
		verifyTextPresrntById(PulseTestLocators.ServerGroups.serverGrp2Id, "SG2");
		verifyTextPresrntById(PulseTestLocators.ServerGroups.serverGrp3Id, "SG3");

		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg1M1Id, "M1");
		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg1M2Id, "M2");
		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg1M3Id, "M3");

		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg2M1Id, "M1");
		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg2M2Id, "M2");

		verifyTextPresrntById(PulseTestLocators.ServerGroups.sg3M3Id, "M3");
	}

	@Test
	public void memberNavigationFromServGrpTest() {
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.serverGrpsRadioXpath);
		clickElementUsingId(PulseTestLocators.ServerGroups.sg1M1Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.sg1M2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.sg1M3Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.sg2M1Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.sg2M2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.sg3M3Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}

	@Test
	public void clickServGrpGridButtonShowsGridTest() {
		navigateToServerGroupGridView();
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.idSG1M3Xpath);
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.idSG1M2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.idSG1M1Xpath);
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.nameM3Xpath);
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.nameM2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.ServerGroups.nameM1Xpath);

	}

	@Test
	public void memberNavigationFromServGrpGridTest() {
		navigateToServerGroupGridView();
		clickElementUsingXpath(PulseTestLocators.ServerGroups.idSG1M3Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
		navigateToServerGroupGridView();
		clickElementUsingXpath(PulseTestLocators.ServerGroups.idSG1M1Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		navigateToServerGroupGridView();
		clickElementUsingXpath(PulseTestLocators.ServerGroups.idSG1M2Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
	}

	@Test
	public void verifyZonePresentTest() {
		navigateToRedundancyZonesTreeView();
		verifyElementPresentByXpath(PulseTestLocators.RedundancyZone.zoneRZ1RZ2Xpath);
		verifyElementPresentById(PulseTestLocators.RedundancyZone.zoneRZ2Id);
	}

	@Test
	public void expandAndCloseRdncyZoneTest() {
		navigateToRedundancyZonesTreeView();
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.zoneRZ1RZ2Xpath);
		verifyElementAttributeById(PulseTestLocators.RedundancyZone.zoneRZ1Id, "style", "width: 720px; height: 415px;");
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.zoneRZ1RZ2Xpath);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.zoneRZ2Xpath);
		verifyElementAttributeById(PulseTestLocators.RedundancyZone.zoneRZ2Id, "style", "width: 720px; height: 415px;");

	}

	@Test
	public void clickRZMembersNavigationTest() {
		navigateToRedundancyZonesTreeView();		
		clickElementUsingId(PulseTestLocators.RedundancyZone.m1RZ1RZ2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.m2RZ1Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.m3RZ2Id);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}

	@Test
	public void clickRZGridShowingGridTest() {
		navigateToRedundancyZonesGridView();
		verifyElementPresentByXpath(PulseTestLocators.RedundancyZone.idM2Xpath);
		verifyElementPresentByXpath(PulseTestLocators.RedundancyZone.idM1Xpath);
		verifyElementPresentByXpath(PulseTestLocators.RedundancyZone.idM3Xpath);
		verifyTextPresrntByXpath(PulseTestLocators.RedundancyZone.idM2Xpath, "M2");
		verifyTextPresrntByXpath(PulseTestLocators.RedundancyZone.idM1Xpath, "M1");
		verifyTextPresrntByXpath(PulseTestLocators.RedundancyZone.idM3Xpath, "M3");
	}

	@Test
	public void clickRZGridMembersNavigationTest() {
		navigateToRedundancyZonesGridView();
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.idM2Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M2");
		navigateToRedundancyZonesGridView();
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.idM1Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M1");
		navigateToRedundancyZonesGridView();
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.idM3Xpath);
		verifyTextPresrntById(PulseTestLocators.RegionDetailsView.memberNameId, "M3");
	}


	@Test
	public void verifySortingOptionsTest(){
		clickElementUsingXpath(PulseTestLocators.TopNavigation.clusterViewLinkXpath);
		clickElementUsingId(PulseTestLocators.TopologyView.treeMapButtonId);
		verifyElementPresentById(PulseTestLocators.TopologyView.hotSpotId);
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);			
		verifyElementPresentByLinkText("Heap Usage");
		verifyElementPresentByLinkText("CPU Usage");		
	}
	
	/* 
	 * HotSpot test scripts - 
	 */
	//--- Topology view
	
	@Test
	public void testHotSpotOptPrsntOnTopologyView(){
		navigateToTopologyTreeView();
	    assertEquals(PulseTestData.Topology.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.TopologyView.hotSpotId));	
	}
	
	@Test
	public void testHotSpotOptionsTopologyView(){	
		navigateToTopologyTreeView();
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);
		assertEquals(PulseTestData.Topology.hotSpotHeapLbl, getTextUsingXpath(PulseTestLocators.TopologyView.heapUsageXpath));
		assertEquals(PulseTestData.Topology.hotSpotCPULbl, getTextUsingXpath(PulseTestLocators.TopologyView.cpuUsageXpath));
	}
	
	@Test
	public void testCpuUsageNavigationOnTopologyView(){
		navigateToTopologyTreeView();
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.cpuUsageXpath);
		assertEquals(PulseTestData.Topology.hotSpotCPULbl, getTextUsingId(PulseTestLocators.TopologyView.hotSpotId));
	}
	
	@Test
	public void testHeapUsageNavigationOnTopologyView(){
		navigateToTopologyTreeView();
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.heapUsageXpath);
		assertEquals(PulseTestData.Topology.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.TopologyView.hotSpotId));
	}

	@Test
	public void testSortingUsingCpuUsageOnTopologyView(){
		navigateToTopologyTreeView();
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.cpuUsageXpath);
		assertMemberSortingByCpuUsage();
	}
	
	@Test
	public void testSortingUsingHeapUsageOnTopologyView(){
		navigateToTopologyTreeView();
		clickElementUsingId(PulseTestLocators.TopologyView.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.TopologyView.heapUsageXpath);
		assertMemberSortingByHeapUsage();
	}
	
	//--- Server Group view
	
	@Test
	public void testHotSpotOptPrsntOnServerGroupView(){
		navigateToServerGroupTreeView();
	    assertEquals(PulseTestData.ServerGroups.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.ServerGroups.hotSpotId));	
	}
	
	@Test
	public void testHotSpotOptionsServerGroupView(){	
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.hotSpotId);
		assertEquals(PulseTestData.ServerGroups.hotSpotHeapLbl, getTextUsingXpath(PulseTestLocators.ServerGroups.heapUsageXpath));
		assertEquals(PulseTestData.ServerGroups.hotSpotCPULbl, getTextUsingXpath(PulseTestLocators.ServerGroups.cpuUsageXpath));		
	}
	
	@Test
	public void testCpuUsageNavigationOnServerGroupView(){
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.cpuUsageXpath);
		assertEquals(PulseTestData.ServerGroups.hotSpotCPULbl, getTextUsingId(PulseTestLocators.ServerGroups.hotSpotId));
	}
	
	@Test
	public void testHeapUsageNavigationOnServerGroupView(){
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.heapUsageXpath);
		assertEquals(PulseTestData.ServerGroups.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.ServerGroups.hotSpotId));
	}	

	@Test
	public void testSortingUsingHeapUsageOnServerGroupView(){
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.heapUsageXpath);
		assertMemberSortingBySgHeapUsage();
	}
	
	@Test
	public void testSortingUsingCpuUsageOnServerGroupView(){
		navigateToServerGroupTreeView();
		clickElementUsingId(PulseTestLocators.ServerGroups.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.ServerGroups.cpuUsageXpath);
		assertMemberSortingBySgCpuUsage();
	}
	
	//--- Redundancy Zone view
	
	@Test
	public void testHotSpotOptPrsntOnRedundancyZoneView(){
		navigateToRedundancyZonesTreeView();
	    assertEquals(PulseTestData.RedundancyZone.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.RedundancyZone.hotSpotId));	
	}
	
	
	@Test
	public void testHotSpotOptionsRedundancyZoneView(){	
		// navigate to Redundancy Zones - Tree View
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.hotSpotId);
		assertEquals(PulseTestData.RedundancyZone.hotSpotHeapLbl, getTextUsingXpath(PulseTestLocators.RedundancyZone.heapUsageXpath));
		assertEquals(PulseTestData.RedundancyZone.hotSpotCPULbl, getTextUsingXpath(PulseTestLocators.RedundancyZone.cpuUsageXpath));		
	}
	
	@Test
	public void testCpuUsageNavigationOnRedundancyZoneView(){
		// navigate to Redundancy Zones - Tree View
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.cpuUsageXpath);
		assertEquals(PulseTestData.RedundancyZone.hotSpotCPULbl, getTextUsingId(PulseTestLocators.RedundancyZone.hotSpotId));
	}
	
	@Test
	public void testHeapUsageNavigationOnRedundancyZoneView(){
		// navigate to Redundancy Zones - Tree View
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.heapUsageXpath);
		assertEquals(PulseTestData.RedundancyZone.hotSpotHeapLbl, getTextUsingId(PulseTestLocators.RedundancyZone.hotSpotId));
	}
	
	@Test
	public void testSortingUsingHeapUsageOnRedundancyView(){
		// navigate to Redundancy Zones - Tree View
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.heapUsageXpath);
		assertMemberSortingByRzHeapUsage();
	}
	
	@Test
	public void testSortingUsingCpuUsageOnRedundancyView(){
		// navigate to Redundancy Zones - Tree View
		navigateToRedundancyZonesTreeView();
		clickElementUsingId(PulseTestLocators.RedundancyZone.hotSpotId);
		clickElementUsingXpath(PulseTestLocators.RedundancyZone.cpuUsageXpath);
		assertMemeberSortingByRzCpuUsage();
	}	
	
	@Test
	public void testDataBrowserFilterFeature(){
		// navigate to Data browser page
		loadDataBrowserpage();		
		List<WebElement> regionLst = getRegionsFromDataBrowser();		
		String []regionNames = new String[regionLst.size()];		
		for(int regionIndex = 0; regionIndex < regionLst.size(); regionIndex++){
			regionNames[regionIndex] = findElementByXpath(PulseTestLocators.DataBrowser.rgnSpanFirstPart + (regionIndex + 1 ) + PulseTestLocators.DataBrowser.rgnSpanSecondPart).getText();
		}	
		// type each region name in region filter and verify respective region(s) are displayed in region list
		for (String region : regionNames) {
				findElementById(PulseTestLocators.DataBrowser.rgnFilterTxtBoxId).clear();
				findElementById(PulseTestLocators.DataBrowser.rgnFilterTxtBoxId).sendKeys(region);				
				
				List<WebElement> regionLst1 = getRegionsFromDataBrowser();									
				
				for(int regionIndex = 1; regionIndex <= regionLst1.size(); regionIndex++){
					assertEquals(region,  findElementByXpath(PulseTestLocators.DataBrowser.rgnSpanFirstPart + regionIndex + PulseTestLocators.DataBrowser.rgnSpanSecondPart).getText());
				}
		}
	}
	
	@Test
	public void testDataBrowserFilterPartialRegionName(){
		// navigate to Data browser page
		loadDataBrowserpage();		
		findElementById(PulseTestLocators.DataBrowser.rgnFilterTxtBoxId).clear();
		
		// type partial region name in region filter and verify that all the regions that contains that  text displays
		findElementById(PulseTestLocators.DataBrowser.rgnFilterTxtBoxId).sendKeys(PulseTestData.DataBrowser.partialRgnName);
		List<WebElement> regionLst = getRegionsFromDataBrowser();		
		
		for(int regionIndex = 0; regionIndex < regionLst.size(); regionIndex++){			
			assertTrue(findElementByXpath(PulseTestLocators.DataBrowser.rgnSpanFirstPart + 
					(regionIndex + 1 ) + 
					PulseTestLocators.DataBrowser.rgnSpanSecondPart).
					getText().
					contains(PulseTestData.DataBrowser.partialRgnName));
		}	
	}
	
	@Test
	public void testDataBrowserClearButton(){
		// navigate to Data browser page
		loadDataBrowserpage();		
		
		sendKeysUsingId(PulseTestLocators.DataBrowser.queryEditorTxtBoxId, PulseTestData.DataBrowser.query1Text);		
		String editorTextBeforeClear = getTextUsingId(PulseTestLocators.DataBrowser.queryEditorTxtBoxId);
		clickElementUsingXpath(PulseTestLocators.DataBrowser.btnClearXpath);
		String editorTextAfterClear = getTextUsingId(PulseTestLocators.DataBrowser.queryEditorTxtBoxId);
		
		assertFalse(PulseTestData.DataBrowser.query1Text.equals(editorTextAfterClear));
	}
	
	@Ignore("WIP") // Data Browser's Query History not showing any data on button click, therefore this test is failing
	@Test
	public void testDataBrowserHistoryQueue(){
		// navigate to Data browser page
		loadDataBrowserpage();	
						
		List<WebElement> numOfReg = driver.findElements(By.xpath(PulseTestLocators.DataBrowser.divDataRegions));
		   
	    for(int i = 1;  i <= numOfReg.size(); i ++){
	    	if(getTextUsingId("treeDemo_" + i + "_span").equals( PulseTestData.DataBrowser.regName)){	  	    		
	    		searchByIdAndClick("treeDemo_" + i + "_check"); 	//driver.findElement(By.id("treeDemo_" + i + "_check")).click();
	    	}
	    }	
		
		sendKeysUsingId(PulseTestLocators.DataBrowser.queryEditorTxtBoxId, PulseAbstractTest.QUERY_TYPE_ONE);
		clickElementUsingId(PulseTestLocators.DataBrowser.btnExecuteQueryId);
			
		//Get required datetime format and extract date and hours from date time.
	    DateFormat dateFormat = new SimpleDateFormat(PulseTestData.DataBrowser.datePattern);
	    String queryDateTime = dateFormat.format(System.currentTimeMillis());
	    String queryTime[] = queryDateTime.split(":");
		System.out.println("Query Time from System: " + queryTime[0]);

	    
	    clickElementUsingId(PulseTestLocators.DataBrowser.historyIcon);	    
	    List<WebElement> historyLst = driver.findElements(By.xpath(PulseTestLocators.DataBrowser.historyLst));
		String queryText       = findElementByXpath(PulseTestLocators.DataBrowser.historyLst)
						.findElement(By.cssSelector(PulseTestLocators.DataBrowser.queryText)).getText();
  	String historyDateTime = findElementByXpath(PulseTestLocators.DataBrowser.historyLst)
						.findElement(By.cssSelector(PulseTestLocators.DataBrowser.historyDateTime)).getText();
	  System.out.println("Query Text from History Table: " + queryText);
		System.out.println("Query Time from History Table: " + historyDateTime);
  	    //verify the query text, query datetime in history panel
	    assertTrue(PulseAbstractTest.QUERY_TYPE_ONE.equals(queryText));
	    assertTrue(historyDateTime.contains(queryTime[0]));
	   
	}	
}
