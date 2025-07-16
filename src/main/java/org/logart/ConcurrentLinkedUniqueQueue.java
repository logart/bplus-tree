package org.logart;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConcurrentLinkedUniqueQueue<T> extends ConcurrentLinkedQueue<T> {
    private final Set<T> presentKeys = ConcurrentHashMap.newKeySet();

    @Override
    public T poll() {
        boolean removed;
        T poll;
        do {
            poll = super.poll();
            if (poll == null) {
                return null;
            }
            removed = presentKeys.remove(poll);
            // if another thread already removed this element, we have to retry
        } while (!removed);

        return poll;
    }

    @Override
    public boolean offer(T t) {
        if (presentKeys.contains(t)) {
            // do not allow duplicates
            return false;
        }
        presentKeys.add(t);
        return super.offer(t);
    }
}
