package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ParkingTicketStats
 *
 * Principles used in this implementation:
 * - Mechanical sympathy
 *   - registers have fastest access, use all HyperThreads
 *   - parallel processing
 *   - balancing of work between workers, and between producer and consumer
 *   - prevent cache overflows (limit read-ahead, work queue size)
 *   - no unnecessary copying of data, or allocation of objects
 *   - primitive collections using contiguous memory
 * @author Keith Kim <keith.karmakaze@gmail.com>
 */
public class ParkingTicketsStats {

	static final int BITS = 23;
	static final int SIZE = 1 << BITS;
	static final ConcurrentStringIntHashMap map = new ConcurrentStringIntHashMap(BITS);

	static volatile byte[] data;

	static final Pattern namePattern = Pattern.compile("([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)");

	// 4-cores with HyperThreading sets nThreads = 8
	static final int nWorkers = Runtime.getRuntime().availableProcessors();

	// use small blocking queue size to limit read-ahead for higher cache hits
	static final ArrayBlockingQueue<int[]> byteArrayQueue = new ArrayBlockingQueue<int[]>(2 * nWorkers - 1, false);
	static final int[] END_OF_WORK = new int[0];

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	if (data != null) {
    		map.clear();
    	}

    	try {
			final int available = parkingTicketsStream.available();

    		data = new byte[available];

        	Parallel.Task worker = new Parallel.Task() {
    			public void run(int instance) {
    				doWork();
    			}};

        	List<Parallel.Task> workers = new ArrayList<Parallel.Task>();
        	for (int i = 0; i < nWorkers; i++) {
        		workers.add(worker);
        	}

        	Parallel parallel = new Parallel(workers);

        	// workers are running, send the data
        	produceWork(parkingTicketsStream, available);

    		// wait for workers to finish processing last unit
        	parallel.waitForCompletion();
    	}
    	catch (IOException e) {
			e.printStackTrace();
		}

    	final SortedMap<String, Integer> sorted = new TreeMap<String, Integer>(new Comparator<String>() {
			public int compare(String o1, String o2) {
				int c = map.get(o2) - map.get(o1);
				if (c != 0) return c;
				return o2.compareTo(o1);
			}});

    	final Map<String, Integer> syncSorted = Collections.synchronizedMap(sorted);

    	final int nGatherers = 2;

    	Parallel.Task task = new Parallel.Task() {
			public void run(int instance) {
	    		int start = SIZE * instance / nGatherers;
	    		int end = SIZE * (instance + 1) / nGatherers;

    			map.putRangeTo(start, end, syncSorted);
			}};

    	Parallel.Task[] tasks = new Parallel.Task[] { task, task };

    	Parallel par = new Parallel(tasks);
    	par.waitForCompletion();

        return sorted;
    }

    static final void produceWork(InputStream parkingTicketsStream, int available) {
    	try {
			int read_end = 0;
			int block_start = 0;
			int block_end = 0;
			for (int read_amount = 1 * 1024 * 1024; (read_amount = parkingTicketsStream.read(data, read_end, read_amount)) > 0; ) {
				read_end += read_amount;
				block_start = block_end;
				block_end = read_end;

				// don't offer the first (header) row
				if (block_start == 0) {
					while (data[block_start++] != '\n') {}
				}

				// partially read line will be processed on next iteration
				if (read_end < available) {
					while (data[--block_end] != '\n') {}
	    			block_end++;
				}

				// subdivide block to minimize latency and improve work balancing
				int sub_end = block_start;
				int sub_start;
				for (int k = 1; k <= nWorkers; k++) {
					sub_start = sub_end;
					sub_end = block_start + (block_end - block_start) * k / nWorkers;

					if (read_amount < available || k < nWorkers) {
						while (data[--sub_end] != '\n') {}
						sub_end++;
					}

	    			for (;;) {
						try {
							byteArrayQueue.put(new int[] {sub_start, sub_end});
							break;
						}
						catch (InterruptedException e) {
							e.printStackTrace();
						}
	    			}
				}

				if (available - read_end < read_amount) {
					read_amount = available - read_end;
				}
			}
    	}
    	catch (IOException e) {
			e.printStackTrace();
		}

		// inform each worker that there's no more work
		for (int t = 0; t < nWorkers; t++) {
			try {
				byteArrayQueue.put(END_OF_WORK);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
    }

    /**
     * worker parallel worker takes blocks of bytes (with no broken lines) and processes them
     */
    static final void doWork() {
		Matcher nameMatcher = namePattern.matcher("");

		// local access faster than volatile fields
		final byte[] data = ParkingTicketsStats.data;

		for (;;) {
			int[] block_start_end;
			for (;;) {
				try {
					block_start_end = byteArrayQueue.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
					break;
				}
				catch (InterruptedException e) {
					e.printStackTrace();
					continue;
				}
			}

			if (block_start_end == END_OF_WORK) {
				break;
			}
			final int block_start = block_start_end[0];
			final int block_end = block_start_end[1];

			// process block as fields
			// save fields 4 (set_fine_amount) and 7 (location2)
			int start = block_start;
			int column = 0;
			int fine = 0;
			String location = null;
			// process block
			while (start < block_end) {
				// position 'end' at delimiter of current column
				int end = start; while (end < block_end && data[end] != ',' && data[end] != '\n') { end++; }

				if (column == 4) {
					// position 'start' at start of number
					while ((data[start] < '0' || data[start] > '9') && start < end) start++;

					while (start < end && (data[start] >= '0' && data[start] <= '9')) {
						fine = fine * 10 + (data[start] - '0');
						start++;
					}
				}
				else if (column == 7) {
					if (fine > 0) {
						location = new String(data, start, end - start);

			    		nameMatcher.reset(location);
			    		if (nameMatcher.find()) {
			    			final String name = nameMatcher.group();
			    			map.adjustOrPutValue(name, fine);
		    			}
					}
				}

				column++;
				if (end < block_end && data[end] == '\n') {
					fine = 0;
					location = null;
					column = 0;
				}
				start = end + 1;
			}
		}
    }
}