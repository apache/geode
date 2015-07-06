package com.pivotal.jvsd;

import com.pivotal.jvsd.fx.VSDChartWindow;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import javax.swing.JMenuItem;

/**
 *
 * @author Vince Ford
 */
public class ChartMenuActionListener implements ActionListener {

	ArrayList<VSDChartWindow> chartWindows = null;

	ChartMenuActionListener(ArrayList<VSDChartWindow> chartWindows) {
		this.chartWindows = chartWindows;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		JMenuItem menuitem = (JMenuItem) e.getSource();
//		for (JFrame win : chartWindows) {
//			if (win.getTitle().equals(menuitem.getText())) {
//                //silly but works as for some reason the windows dont' get
//				//pulled forward on on some platforms unless visibility changes
//				win.setVisible(false);
//				win.setVisible(true);
//				//set focus
//				win.isActive();
//				win.toFront();
//				break;
//			}
//		}
	}

}
