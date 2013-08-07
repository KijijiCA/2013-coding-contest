package com.lishid.kijiji.contest.util;

import java.nio.charset.Charset;

/**
 * MutableString is like a Java String, but it re-uses the char array given <br>
 * <br>
 * Internal fields are marked public for direct access, resulting in blazing fast string operations
 * 
 * @author lishid
 */
public class MutableString implements Comparable<MutableString> {
    private static final Charset US_ASCII = Charset.forName("US-ASCII");
    public byte[] data;
    public int start;
    public int length;
    public int end;
    private int hash;
    
    /**
     * Create a MutableString from a Java String. The backing array is newly allocated.
     * 
     * @param string
     */
    public MutableString(String string) {
        char[] array = string.toCharArray();
        byte[] buffer = new byte[array.length];
        for (int i = 0; i < array.length; i++) {
            buffer[i] = (byte) array[i];
        }
        useAsNewString(buffer, 0, string.length());
    }
    
    /**
     * Create a MutableString from a backing array.
     * 
     * @param data
     * @param start
     * @param length
     */
    public MutableString(byte[] data, int offset, int length) {
        useAsNewString(data, offset, length);
    }
    
    public MutableString useAsNewString(byte[] data, int offset, int length) {
        this.data = data;
        this.start = offset;
        this.length = length;
        this.end = start + length;
        this.hash = 0;
        return this;
    }
    
    @Override
    public String toString() {
        return new String(data, start, length, US_ASCII);
    }
    
    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && length > 0) {
            for (int i = start; i < end; i++) {
                h = 31 * h + data[i];
            }
            hash = h;
        }
        return h;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MutableString)) {
            return false;
        }
        MutableString another = (MutableString) obj;
        if (another.length != this.length) {
            return false;
        }
        // Chars comparison
        for (int i = this.start, j = another.start; i < length; i++, j++) {
            if (this.data[i] != another.data[j]) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public int compareTo(MutableString another) {
        int len1 = data.length;
        int len2 = another.data.length;
        int lim = Math.min(len1, len2);
        byte v1[] = data;
        byte v2[] = another.data;
        
        int start1 = start;
        int start2 = another.start;
        int end = start1 + lim;
        while (start1 < end) {
            byte c1 = v1[start1];
            byte c2 = v2[start2];
            if (c1 != c2) {
                return c1 - c2;
            }
            start1++;
            start2++;
        }
        
        return len1 - len2;
    }
    
    public boolean isEmpty() {
        return length == 0;
    }
    
    public MutableString clone() {
        return new MutableString(data, start, length);
    }
    
    /**
     * Read an integer from the buffer.
     * This is static as there's no need to create an object just to read the buffer though.
     * 
     * @param data
     * @param start
     * @param length
     * @return
     */
    public static int toPositiveInteger(byte[] data, int start, int length) {
        int end = start + length;
        int result = 0;
        for (int i = start; i < end; i++) {
            byte c = data[i];
            if (c >= '0' && c <= '9') {
                result = result * 10 + (c - '0');
            }
            else if (c != ' ') {
                return 0;
            }
        }
        return result;
    }
    
    // Backup and restore allows to use the same object for a temporary purpose and revert back
    
    public byte[] backup_data;
    public int backup_start;
    public int backup_length;
    public int backup_end;
    private int backup_hash;
    
    /**
     * Backup this string so as to be able to use it as a temporary string
     */
    public void backup() {
        backup_data = data;
        backup_start = start;
        backup_length = length;
        backup_end = end;
        backup_hash = hash;
    }
    
    /**
     * Restore the backup to get the original string
     */
    public void restore() {
        data = backup_data;
        start = backup_start;
        length = backup_length;
        end = backup_end;
        hash = backup_hash;
    }
}
