package com.lishid.kijiji.contest.util;

/**
 * This class represents an integer that can be modified. <br>
 * <br>
 * This really help HashMap implementations where the value is an integer and needs to be incremented
 * 
 * @author lishid
 */
public class MutableInteger {
    public int value;
    
    public MutableInteger(int value) {
        this.value = value;
    }
    
    public MutableInteger useAsNewInteger(int value) {
        this.value = value;
        return this;
    }
    
    public MutableInteger add(int add) {
        this.value += add;
        return this;
    }
}
