package ca.kijiji.contest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Parallel {
	public interface Task {
		public void run(int instance);
	}

	List<Thread> threads = new ArrayList<Thread>();

	Parallel(Task[] tasks) {
		this(Arrays.asList(tasks));
	}

	Parallel(Iterable<Task> tasks) {
		int i = 0;
		for (final Task task : tasks) {
			final int instance = i;
			Thread t = new Thread() {
				public void run() {
					task.run(instance);
				}
			};
			t.start();
			threads.add(t);
			i++;
		}
	}

	public void waitForCompletion() {
    	for (Thread thread : threads) {
	    	try { thread.join(); } catch (InterruptedException e) {}
    	}
	}
}
