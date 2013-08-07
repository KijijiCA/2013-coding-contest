package com.lishid.kijiji.contest.mapred;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.lishid.kijiji.contest.Algorithm;
import com.lishid.kijiji.contest.mapred.MapTask.MapperResultCollector;
import com.lishid.kijiji.contest.util.MutableInteger;
import com.lishid.kijiji.contest.util.MutableString;

public class ReduceTask extends MapReduceTask {
    MapperResultCollector[] mapperResults;
    int partition;
    ReducerResultCollector resultCollector;
    
    public ReduceTask(TaskTracker taskTracker, MapperResultCollector[] mapperResults, int partition) {
        super(taskTracker);
        this.mapperResults = mapperResults;
        this.partition = partition;
        resultCollector = new ReducerResultCollector();
    }
    
    public ReducerResultCollector getFutureResult() {
        return resultCollector;
    }
    
    /**
     * The reducer task: <br>
     * Take all mapper results for the same reducer partition and reduce(combine) it. <br>
     * Also convert MutableString and MutableInteger to String and Integer
     */
    @Override
    public void performTask() throws Exception {
        Map<MutableString, MutableInteger> result = null;
        for (MapperResultCollector mapperResult : mapperResults) {
            if (result == null) {
                result = mapperResult.partitionedResult[partition];
            }
            else {
                Algorithm.reduce(mapperResult.partitionedResult[partition], result);
            }
        }
        resultCollector.result = new HashMap<String, Integer>();
        for (Entry<MutableString, MutableInteger> entry : result.entrySet()) {
            resultCollector.result.put(entry.getKey().toString(), entry.getValue().value);
        }
    }
    
    public static class ReducerResultCollector {
        public Map<String, Integer> result;
    }
}
