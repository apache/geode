/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.sequence;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: dsmith
 * Date: Oct 29, 2010
 * Time: 3:59:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class Lifeline {
    private List<LifelineState> states;
    private final String name;
    private final Comparable diagramName;
    private int x;
    private int width;

    public String getName() {
        return name;
    }

    public int getX() {
        return x;
    }

    public int getWidth() {
        return width;
    }

    public void addState(LifelineState state) {
        this.states.add(state);
    }
    
    public Lifeline(Comparable diagramName, String name) {
      this.name = name;
      this.states = new ArrayList<LifelineState>();
      this.diagramName = diagramName;
    }

    public void resize(int x, int lineWidth, long Ybase, double Yscale) {
        for(LifelineState state : states) {
            state.resize(Yscale, Ybase);
        }

        this.x = x;
        width = lineWidth;
    }

    public void paint(Graphics2D g, StateColorMap colorMap) {
        Rectangle boundary = g.getClipBounds();
        if(x > boundary.getMaxX() || x + width < boundary.getMinX()) {
            //no need to paint if this line isn't displayed
            return;
        }
        //TODO - we need to clip these to the visible states
        for(LifelineState state : states) {
            state.paint(g, colorMap);
        }
    }

    public List<LifelineState> getStates() {
        return states;
    }

    public LifelineState getStateAt(int y) {
        for(LifelineState state : states) {
            if(state.getStartY() < y && state.getStartY() + state.getHeight() > y) {
                return state;
            }
        }
        return null;
    }

    public Comparable getDiagramName() {
      return diagramName;
    }
}
