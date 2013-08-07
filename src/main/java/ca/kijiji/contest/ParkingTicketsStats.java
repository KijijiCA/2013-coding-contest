package ca.kijiji.contest;

import java.io.InputStream;
import java.util.SortedMap;

import com.lishid.kijiji.contest.MapReduceProcessor;

public class ParkingTicketsStats {
    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) throws Exception {
        return new MapReduceProcessor().sortStreetsByProfitability(parkingTicketsStream);
    }
}