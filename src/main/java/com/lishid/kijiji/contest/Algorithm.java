package com.lishid.kijiji.contest;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.lishid.kijiji.contest.util.MutableInteger;
import com.lishid.kijiji.contest.util.MutableString;

public class Algorithm {
    
    private static SuffixFilter suffixFilter = new SuffixFilter();
    
    /**
     * The map process, reading lines and returning key-value pairs as Street name and Ticket amount
     * 
     * @param line the line from the csv file
     * @param result the result object to put values in
     */
    public static void map(MutableString line, MapResult result) {
        if (line == null)
            return;
        
        int wordEnd = line.end;
        int col = 0;
        // Keep track of the indices of the two fields and process them at the end
        // This is done to recycle the MutableString variable "line".
        int addressStart = 0;
        int addressLength = 0;
        int fineAmountStart = 0;
        int fineAmountLength = 0;
        // Read from back to front as we don't really care about the first 6 columns
        for (int i = line.end - 1; i >= line.start; i--) {
            if (line.data[i] == ',') {
                // Column 11 => Address
                if (col == 3) {
                    addressStart = i + 1;
                    addressLength = wordEnd - i - 1;
                }
                // Column 7 => Fine Amount
                else if (col == 6) {
                    fineAmountStart = i + 1;
                    fineAmountLength = wordEnd - i - 1;
                    // Skip the rest of the columns
                    break;
                }
                wordEnd = i;
                col++;
            }
        }
        result.value = MutableString.toPositiveInteger(line.data, fineAmountStart, fineAmountLength);
        // MutableString "line" can now be recycled as it is no longer used
        result.key = findRoadName(line.useAsNewString(line.data, addressStart, addressLength));
    }
    
    /**
     * Object to collect key-value pairs
     */
    public static class MapResult {
        public MutableString key;
        public int value;
    }
    
    /**
     * Combine key-value pairs together by adding values together for the same key if it existed in the map
     */
    public static void combine(MutableString key, int value, Map<MutableString, MutableInteger> map) {
        MutableInteger previousValue = map.get(key);
        if (previousValue != null) {
            previousValue.add(value);
        }
        else {
            map.put(key, new MutableInteger(value));
        }
    }
    
    /**
     * Combine key-value pairs together by adding values together for the same key if it existed in the map
     */
    private static void reduce(MutableString key, MutableInteger value, Map<MutableString, MutableInteger> map) {
        MutableInteger previousValue = map.get(key);
        if (previousValue != null) {
            previousValue.add(value.value);
        }
        else {
            map.put(key, value);
        }
    }
    
    /**
     * Combine two maps
     * 
     * @param input to take values from
     * @param result to put the final results
     */
    public static void reduce(Map<MutableString, MutableInteger> input, Map<MutableString, MutableInteger> result) {
        for (Entry<MutableString, MutableInteger> entry : input.entrySet()) {
            reduce(entry.getKey(), entry.getValue(), result);
        }
    }
    
    /**
     * This method has been optimized to cut off the beginning and end of the given un-formatted address.
     * Using direct access to the backed array of the MutableString, this method can quickly scan through characters
     * without much overhead for splitting string or creating temporary copies of any data
     */
    private static MutableString findRoadName(MutableString address) {
        int startIndex = address.start;
        int endIndex = address.end;
        int wordStart = address.start;
        
        // Step 1, skip all non-alphabetic words in the beginning of the address
        // Note that this will ignore numbered street names (such as 3RD), but the sample data
        // only contains 4 different numbered street names which are all trivial in amount
        boolean alphabetic = true;
        boolean wordStarted = false;
        for (startIndex = address.start; startIndex < endIndex; startIndex++) {
            if (address.data[startIndex] == ' ') {
                // Found alphabetic word
                if (wordStarted && alphabetic) {
                    break;
                }
                // Reset
                wordStarted = false;
                alphabetic = true;
                wordStart = startIndex + 1;
            }
            else {
                wordStarted = true;
                if (alphabetic && (address.data[startIndex] < 'A' || address.data[startIndex] > 'Z')) {
                    alphabetic = false;
                }
            }
        }
        
        // To avoid creating a new object, use the same object as temporary string
        address.backup();
        
        // Step 2, find first filtered suffix
        int endWordStart = startIndex + 1;
        for (endIndex = startIndex + 1; endIndex < address.end; endIndex++) {
            if (address.data[endIndex] == ' ') {
                boolean foundSuffix = suffixFilter.isWordFilteredSuffix(address.useAsNewString(address.data, endWordStart, endIndex - endWordStart));
                address.restore();
                if (foundSuffix) {
                    break;
                }
                endWordStart = endIndex + 1;
            }
        }
        
        // Construct new word by only cutting off the front and back
        int length = endWordStart - wordStart - 1;
        if (length == address.length) {
            return address;
        }
        
        // MutableString roadName is no longer needed, recycle as new string
        return address.useAsNewString(address.data, wordStart, length);
    }
    
    private static class SuffixFilter {
        private Set<MutableString> filteredSuffixes = new HashSet<MutableString>();
        
        public SuffixFilter() {
            addAllFilteredSuffix();
        }
        
        public boolean isWordFilteredSuffix(MutableString word) {
            return filteredSuffixes.contains(word);
        }
        
        private void addAllFilteredSuffix() {
            addFilteredSuffix("EAST", "WEST", "NORTH", "SOUTH");
            addFilteredSuffix("E", "W", "N", "S");
            addFilteredSuffix("AVENUE", "AVE", "AV");
            addFilteredSuffix("BOULEVARD", "BULVD", "BLVD", "BL");
            addFilteredSuffix("CIRCLE", "CIRCL", "CRCL", "CIRC", "CIR", "CR", "CIRCUIT");
            addFilteredSuffix("COURT", "CRT", "CRCT", "CT", "CTR");
            addFilteredSuffix("CRESCENT", "CRES", "CRE");
            addFilteredSuffix("DRIVE", "DR");
            addFilteredSuffix("EXPRESSWAY");
            addFilteredSuffix("GARDENS", "GRNDS", "GDNS", "GARDEN", "GDN");
            addFilteredSuffix("GATE", "GT");
            addFilteredSuffix("GREEN", "GRN");
            addFilteredSuffix("GROVE", "GRV");
            addFilteredSuffix("HEIGHTS", "HTS");
            addFilteredSuffix("HILL", "HILLS");
            addFilteredSuffix("LANE", "LN");
            addFilteredSuffix("LAWN", "LWN");
            addFilteredSuffix("LINE");
            addFilteredSuffix("MALL", "MEWS");
            addFilteredSuffix("PARKWAY", "PKWY", "PARK", "PARKING", "PK");
            addFilteredSuffix("PATHWAY", "PTWY", "PATH");
            addFilteredSuffix("PLACE", "PL");
            addFilteredSuffix("PROMENADE", "RAMP");
            addFilteredSuffix("ROAD", "RD", "ROADWAY", "RDWY");
            addFilteredSuffix("SQUARE", "SQ");
            addFilteredSuffix("STREET", "STR", "ST");
            addFilteredSuffix("TERRACE", "TER", "TERR", "TR");
            addFilteredSuffix("TRAIL", "TRL");
            addFilteredSuffix("VIEW", "WALKWAY", "WALK");
            addFilteredSuffix("WAYS", "WAY", "WY");
            addFilteredSuffix("WOODS");
        }
        
        private void addFilteredSuffix(String... words) {
            for (int i = 0; i < words.length; i++) {
                filteredSuffixes.add(new MutableString(words[i]));
            }
        }
    }
}
