package ca.kijiji.contest;

import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class Producer<E> {
	Producer(ConcurrentLinkedQueue<E> queue) {}
}
