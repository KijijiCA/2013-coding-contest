package com.lishid.kijiji.contest.util;

/**
 * Used to read lines from a char array
 * 
 * @author lishid
 */
public class ByteArrayReader {
    private byte[] buffer;
    private int offset = 0;
    private int end;
    
    public ByteArrayReader(byte[] buffer, int offset, int length) {
        this.buffer = buffer;
        this.offset = offset;
        this.end = offset + length;
    }

    private static byte CR = '\r';
    private static byte LF = '\n';
    
    /**
     * Reads a line of text. A line is considered to be terminated by any one
     * of a line feed ('\n'), a carriage return ('\r'), or a carriage return
     * followed immediately by a line feed.
     * 
     * @return A String containing the contents of the line, not including
     *         any line-termination characters, or null if the end of the
     *         char array has been reached
     */
    public MutableString readLine() {
        if (offset >= end) {
            return null;
        }
        
        int startIndex = offset;
        int endIndex;
        boolean foundCR = false;
        for (endIndex = startIndex; endIndex < end; endIndex++) {
            byte c = buffer[endIndex];
            if (c == CR) {
                foundCR = true;
            }
            else if (c == LF) {
                offset = endIndex + 1;
                break;
            }
            else if (foundCR) {
                offset = endIndex;
                break;
            }
        }
        if (endIndex == end) {
            offset = end;
        }
        if (foundCR) {
            endIndex--;
        }
        
        if (endIndex == startIndex) {
            return readLine();
        }
        
        return new MutableString(buffer, startIndex, endIndex - startIndex);
    }
    
    /**
     * Gets the underlying buffer
     * 
     * @return the underlying buffer
     */
    public byte[] getBuffer() {
        return buffer;
    }
}
