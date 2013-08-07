package ca.kijiji.contest;

import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ParkingTicketsStatsTest {

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStatsTest.class);

    // Download the file from the following URL and extract into src/test/resources
    // http://www1.toronto.ca/City_Of_Toronto/Information_&_Technology/Open_Data/Data_Sets/Assets/Files/parking_tickets_data_2012.zip
    private static final String PARKING_TAGS_DATA_2012_CSV_PATH = "/Parking_Tags_Data_2012.csv";
    private static SortedMap<String, Integer> streets;
    private static boolean testCasePassed = false;
    private static boolean streetsProperlySorted = false;
    private static boolean valuesDecreasing = true;
    private static boolean streetsHasDuplicateValues = false;
    private static boolean streetsHandlesGetUnknown = false;
    private static boolean streetsTotalSumValid = false;
    private static int iterCount = 0;
    private static int totalAmount = 0;
    private static long durationWarmup1 = -1;
    private static long durationWarmup2 = -1;
    private static long durationAverage = -1;

    @BeforeClass
    public static void setup() throws Exception {
        durationWarmup1 = loadStreets();
        LOG.info("Duration of warm up 1 = {} ms", durationWarmup1);

        Thread averagerThread = new Thread(new Averager());
        averagerThread.start();
        // wait 60s max
        for (int i = 0; i < 60 && averagerThread.isAlive(); i++) {
            Thread.sleep(1000);
        }
        if (averagerThread.isAlive()) {
            averagerThread.interrupt();
        }
        analyzeOutput();
    }

    static class Averager implements Runnable {
        public void run() {
            try {
                if (durationWarmup1 < 30000) {
                    durationWarmup2 = loadStreets();
                    LOG.info("Duration of warm up 2 = {} ms", durationWarmup2);

                    if (durationWarmup2 < 10000) {
                        durationAverage = calculateAverageTime();
                        LOG.info("Average time = {} ms", durationAverage);
                    } else {
                        LOG.info("Warmup 2 too slow, don't bother averaging");
                    }
                } else {
                    LOG.info("Warmup 1 too slow, don't bother averaging");
                }
            } catch (Exception e) {
                LOG.info("Method doesn't support multiple calls", e);
            }
        }
    }

    private static long calculateAverageTime() throws Exception {
        long[] durations = new long[10];
        for (int i = 0; i < durations.length; i++) {
            durations[i] = loadStreets();
        }
        Arrays.sort(durations);
        long[] middleDurations = new long[6];
        System.arraycopy(durations, 2, middleDurations, 0, middleDurations.length);
        int sum = 0;
        for (long d : middleDurations) {
            sum += d;
        }
        return (long) (sum / middleDurations.length);
    }

    private static long loadStreets() throws Exception {
        long startTime = System.currentTimeMillis();
        InputStream parkingTicketsStream = ParkingTicketsStatsTest.class.getResourceAsStream(PARKING_TAGS_DATA_2012_CSV_PATH);
        streets = ParkingTicketsStats.sortStreetsByProfitability(parkingTicketsStream);
        return System.currentTimeMillis() - startTime;
    }

    private static void analyzeOutput() {
        Iterator<Map.Entry<String, Integer>> iter = streets.entrySet().iterator();
        int previous = Integer.MAX_VALUE;
        while (iter.hasNext()) {
            Map.Entry<String, Integer> entry = iter.next();
            Integer value = entry.getValue();
            valuesDecreasing = valuesDecreasing && value <= previous;
            streetsHasDuplicateValues = streetsHasDuplicateValues || value == previous;
            previous = value;
            iterCount++;
            totalAmount += value;
        }
    }

    @Test
    public void testSortStreetsByProfitability() throws Exception {

        // Watch out for some nasty business in the data!
        // Luckily, there is a 5% margin of error on each number asserted below, so don't bother finding the
        // most accurate solution. Just build the best implementation which solves the problem reasonably well and
        // satisfies the test case.
        //
        // Generally, addresses follow the format: NUMBER NAME SUFFIX DIRECTION
        // with
        // NUMBER (optional) = digits, ranges of digit (e.g. 1531-1535), letters, characters like ! or ? or % or /
        // NAME (required) = the name you need to extract, mostly uppercase letters, sometimes spaces (e.g. ST CLAIR), rarely numbers (e.g. 16TH)
        // SUFFIX (optional) = the type of street such as ST, STREET, AV, AVE, COURT, CRT, CT, RD ...
        // DIRECTION (optional) = one of EAST, WEST, E, W, N, S
        //
        // NOTE: the street name should be extracted from the field location2 only.

        assertThat(streets.get("KING"), closeTo(2570710));
        assertThat(streets.get("ST CLAIR"), closeTo(1871510));
        assertThat(streets.get(streets.firstKey()), closeTo(3781095));
        testCasePassed = true;
    }

    @Test
    public void testStreetsProperlySorted() throws Exception {
        assertTrue(valuesDecreasing);
        assertThat(iterCount, greaterThan(100)); // reasonable amount of entries
        streetsProperlySorted = true;
    }

    @Test
    public void testStreetsHasDuplicateValues() throws Exception {
        assertTrue(streetsHasDuplicateValues);
    }

    @Test
    public void testHandleGetUnknown() {
        assertNull(streets.get("SOMETHING THAT DOESNT EXIST"));
        streetsHandlesGetUnknown = true;
    }

    @Test
    public void testTotalSum() {
        assertThat(totalAmount, closeTo(111485565));
        streetsTotalSumValid = true;
    }

    private Matcher<Integer> closeTo(int num) {
        int low = (int) (num * 0.95);
        int high = (int) (num * 1.05);
        return allOf(greaterThan(low), lessThan(high));
    }

    @AfterClass
    public static void report() throws Exception {
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("../2013-coding-contest-results.csv", true)));
        String branch = System.getProperty("user");
        String result = String.format("%s,%d,%d,%d,%b,%b,%b,%b,%b", branch, durationWarmup1, durationWarmup2, durationAverage,
                testCasePassed, streetsProperlySorted, streetsHasDuplicateValues, streetsTotalSumValid, streetsHandlesGetUnknown);
        out.println(result);
        out.close();
    }
}