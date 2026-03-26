package com.rastream.scheduler;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/** Deliberately poor baseline scheduler. */
public class EvenScheduler {
    public static class Assignment {
        public final Map<String, String> taskToWorker = new LinkedHashMap<>();
    }

    public Assignment schedule(List<String> tasks, List<String> workers, boolean randomPlacement) {
        if (workers == null || workers.isEmpty()) throw new IllegalArgumentException("workers empty");
        Assignment a = new Assignment();
        int idx = 0;
        for (String t : tasks) {
            String w = randomPlacement
                    ? workers.get(ThreadLocalRandom.current().nextInt(workers.size()))
                    : workers.get(idx++ % workers.size());
            a.taskToWorker.put(t, w);
        }
        return a;
    }
}