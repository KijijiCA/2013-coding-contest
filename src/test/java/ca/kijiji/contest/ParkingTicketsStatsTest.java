package ca.kijiji.contest;

import org.hamcrest.Matcher;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.SortedMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ParkingTicketsStatsTest {

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStatsTest.class);

    // Download the file from the following URL and extract into src/test/resources
    // http://www1.toronto.ca/City_Of_Toronto/Information_&_Technology/Open_Data/Data_Sets/Assets/Files/parking_tickets_data_2012.zip
    private static final String PARKING_TAGS_DATA_2012_CSV_PATH = "/Parking_Tags_Data_2012.csv";
    private static final int MIN_AMOUNT_THRESHOLD = 1000;

    @Test
    public void testSortStreetsByProfitability() throws Exception {
        long startTime = System.currentTimeMillis();

        InputStream parkingTicketsStream = this.getClass().getResourceAsStream(PARKING_TAGS_DATA_2012_CSV_PATH);
        SortedMap<String, Integer> streets = ParkingTicketsStats.sortStreetsByProfitability(parkingTicketsStream);

        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Duration of computation = {} ms", duration);

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
    }

    private Matcher<Integer> closeTo(int num) {
        int low = (int) (num * 0.95);
        int high = (int) (num * 1.05);
        return allOf(greaterThan(low), lessThan(high));
    }
}