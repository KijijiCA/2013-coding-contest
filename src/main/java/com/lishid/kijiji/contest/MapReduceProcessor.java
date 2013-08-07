package com.lishid.kijiji.contest;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import com.lishid.kijiji.contest.mapred.MapTask;
import com.lishid.kijiji.contest.mapred.ReduceTask;
import com.lishid.kijiji.contest.mapred.TaskTracker;
import com.lishid.kijiji.contest.mapred.MapTask.MapperResultCollector;
import com.lishid.kijiji.contest.mapred.ReduceTask.ReducerResultCollector;
import com.lishid.kijiji.contest.util.ByteArrayReader;
import com.lishid.kijiji.contest.util.LargeChunkReader;
import com.lishid.kijiji.contest.util.ParkingTicketTreeMap;

/**
 * This implementation uses the technique "MapReduce" to parallelize work by first performing independent
 * Map operations, then independent Reduce operations, and finally merging the result. <br>
 * More information on each step are located at {@link MapTask#performTask()} and {@link ReduceTask#performTask()} <br>
 * <br>
 * An optimization technique used here is avoiding the use of String operations as much as possible since it
 * allocates temporary char[], which can be optimized by reusing the same char[] the buffered reading process used. <br>
 * <br>
 * In fact, after optimizing even more, it is obvious that object allocation reduction is key to eliminate slow speeds.
 * From this idea, MutableInteger is created to combat the issue where HashMap values of Integer are continuously created.
 * 
 * @author lishid
 */
public class MapReduceProcessor {
    
    // Tweakable variables
    private static final int AVAILABLE_CORES = Runtime.getRuntime().availableProcessors();
    /** The size of the buffer to use for all mappers */
    private static final int MAPPER_CHUNK_SIZE = 1 << 18;
    /** The number of reducers to run, also the number of partitions the mappers will split the results into */
    private static final int PARTITIONS = AVAILABLE_CORES;
    
    public SortedMap<String, Integer> sortStreetsByProfitability(InputStream inputStream) throws Exception {
        // Leave a core free for IO until inputs have been fully read
        TaskTracker taskTracker = new TaskTracker(AVAILABLE_CORES - 1);
        
        MapperResultCollector[] mapperResults = map(taskTracker, inputStream);
        ReducerResultCollector[] reducerResults = reduce(taskTracker, mapperResults);
        
        taskTracker.shutdown();
        
        SortedMap<String, Integer> result = mergeAndSort(reducerResults);
        
        return result;
    }
    
    private MapperResultCollector[] map(TaskTracker taskTracker, InputStream inputStream) throws Exception {
        List<MapperResultCollector> resultCollectors = new ArrayList<MapperResultCollector>();
        
        // Read the stream in large chunks. Individual line splitting will be done
        // on worker threads so as to parallelize as much work as possible
        LargeChunkReader reader = new LargeChunkReader(inputStream);
        int read = 1;
        while (read > 0) {
            byte[] buffer = new byte[MAPPER_CHUNK_SIZE];
            read = reader.readChunk(buffer);
            if (read > 0) {
                ByteArrayReader arrayReader = new ByteArrayReader(buffer, 0, read);
                MapTask task = new MapTask(taskTracker, arrayReader, PARTITIONS);
                taskTracker.startTask(task);
                resultCollectors.add(task.getFutureResult());
            }
        }
        taskTracker.setThreads(AVAILABLE_CORES);
        reader.close();
        
        taskTracker.waitForTasksAndReset();
        
        List<MapperResultCollector> validResults = new ArrayList<MapperResultCollector>();
        for (MapperResultCollector collector : resultCollectors) {
            if (collector.partitionedResult != null) {
                validResults.add(collector);
            }
        }
        return validResults.toArray(new MapperResultCollector[validResults.size()]);
    }
    
    private ReducerResultCollector[] reduce(TaskTracker taskTracker, MapperResultCollector[] input) throws Exception {
        ReducerResultCollector[] resultCollectors = new ReducerResultCollector[PARTITIONS];
        
        for (int i = 0; i < PARTITIONS; i++) {
            ReduceTask task = new ReduceTask(taskTracker, input, i);
            taskTracker.startTask(task);
            resultCollectors[i] = task.getFutureResult();
        }
        
        taskTracker.waitForTasksAndReset();
        return resultCollectors;
    }
    
    private SortedMap<String, Integer> mergeAndSort(ReducerResultCollector[] input) {
        SortedMap<String, Integer> sortedResult = null;
        
        for (int i = 0; i < input.length; i++) {
            if (sortedResult == null) {
                sortedResult = new ParkingTicketTreeMap(input[i].result);
            }
            else {
                sortedResult.putAll(input[i].result);
            }
        }
        
        return sortedResult;
    }
}
