package com.lishid.kijiji.contest.mapred;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TaskTracker {
    private ThreadPoolExecutor threadPool;
    private int startedTasks;
    private int finishedTasks;
    private boolean waitingForWake;
    
    /**
     * TaskTracker uses a fixed-size thread pool to run MapReduceTask and provides an easy way of waiting until all tasks finish
     */
    public TaskTracker(int threads) {
        this.threadPool = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        
        reset();
    }
    
    /**
     * Shuts down the underlying executor service
     */
    public void shutdown() {
        threadPool.shutdown();
    }
    
    /**
     * Change the number of allowed threads
     * 
     * @param threads
     */
    public void setThreads(int threads) {
        threadPool.setMaximumPoolSize(threads);
        threadPool.setCorePoolSize(threads);
    }
    
    /**
     * Blocks current thread until all tasks are finished, then reset the task tracker.
     */
    public void waitForTasksAndReset() throws InterruptedException {
        while (true) {
            synchronized (this) {
                if (startedTasks != finishedTasks) {
                    waitingForWake = true;
                    this.wait();
                }
                else {
                    waitingForWake = false;
                    break;
                }
            }
        }
        reset();
    }
    
    /**
     * Find out how many tasks are queued or running. Note that this is not synchronized for a performance boost
     * 
     * @return number of tasks that have not sent in termination signals
     */
    public int getUnfinishedTasks() {
        return startedTasks - finishedTasks;
    }
    
    /**
     * Start a new task. Note: This is not synchronized. All tasks should be started from the same thread.
     */
    public void startTask(MapReduceTask task) {
        startedTasks++;
        threadPool.submit(task);
    }
    
    /**
     * Called from individual tasks to indicate they finished execution.
     */
    public void finishTask() {
        synchronized (this) {
            finishedTasks++;
            // Wake the main thread up if needed
            if (waitingForWake && startedTasks == finishedTasks) {
                notify();
            }
        }
    }
    
    private void reset() {
        startedTasks = 0;
        finishedTasks = 0;
        waitingForWake = false;
    }
}
