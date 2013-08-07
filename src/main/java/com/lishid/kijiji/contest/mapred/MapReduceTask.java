package com.lishid.kijiji.contest.mapred;

/**
 * An abstract task that reports status to a TaskTracker when the task finishes
 * 
 * @author lishid
 */
public abstract class MapReduceTask implements Runnable {
    private TaskTracker taskTracker;
    
    public MapReduceTask(TaskTracker taskTracker) {
        this.taskTracker = taskTracker;
    }
    
    /**
     * Runs the map-reduce task
     */
    public void run() {
        try {
            performTask();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        taskTracker.finishTask();
    }
    
    public abstract void performTask() throws Exception;
}
