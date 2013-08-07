package ca.kijiji.contest;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class ConcurrentStringIntHashMap extends AbstractMap<String, Integer> {

	// 23-bit indices (8M possible entries)
	final int BITS;
	final int UNUSED_BITS;
	final int SIZE;
	final int MASK;

	final String[] keys;
	final AtomicIntegerArray vals;

	ConcurrentStringIntHashMap(int BITS) {
		this.BITS = BITS;
		UNUSED_BITS = 32 - BITS;
		SIZE = 1 << BITS;
		MASK = SIZE - 1;
		keys = new String[SIZE];
		vals = new AtomicIntegerArray(SIZE);
	}

	public int hash(String k) {
		int h = 0;
		for (char c : k.toCharArray()) {
			int i = (c == ' ') ? 0 : ((int)c - 64) & 0x00FF;
			h = h * 443 + i;
			h = (h & MASK) ^ (h >>> BITS);
		}
		return h;
	}

	public void adjustOrPutValue(final String k, final int d) {
		int i = hash(k);

		if (vals.getAndAdd(i, d) == 0) {
			keys[i] = k;
		}
		// use code below instead of if() above to show hash collisions
//		if (vals.getAndAdd(i, d) != 0) {
//			synchronized (keys) {
//				String k0 = keys[i];
//				if (!k.equals(k0)) {
//					println("Key hash clash: first "+ k0 +" and "+ k);
//				}
//			}
//		}
//		else {
//			keys[i] = k;
//		}
	}

	public int get(final String k) {
		int i = hash(k);
		return vals.get(i);
	}

	@Override
	public void clear() {
		Arrays.fill(keys, null);

    	for (int h = 0; h < SIZE; h++) {
    		vals.set(h, 0);
    	}
	}

	public void putRangeTo(int start, int end, Map<String, Integer> dest) {
		String[] keys;
		synchronized (this.keys) {
			keys = this.keys;
		}
		for (int i = start; i < end; i++) {
			String key = keys[i];
			if (key != null) {
				dest.put(key, vals.get(i));
			}
		}
	}

	@Override
	public Set<java.util.Map.Entry<String, Integer>> entrySet() {
		return null;
	}
}
