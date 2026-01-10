package com.sbbsystems.statefun.tasks.graph.v2;

import com.google.common.collect.Iterators;

import java.util.*;

import static com.sbbsystems.statefun.tasks.graph.v2.GraphEntry.isGroup;
import static java.util.Objects.isNull;

public class GraphEntryIterator implements Iterator<GraphEntry> {
    private Iterator<GraphEntry> iterators = Collections.emptyIterator();
    private GraphEntry current;
    private final Map<String, GraphEntry> mapOfEntries;

    public static GraphEntryIterator from(GraphEntry entry, Map<String, GraphEntry> mapOfEntries) {
        return new GraphEntryIterator(entry, mapOfEntries);
    }

    private GraphEntryIterator(GraphEntry entry, Map<String, GraphEntry> mapOfEntries) {
        this.current = entry;
        this.mapOfEntries = mapOfEntries;
    }

    @Override
    public boolean hasNext() {
        if (iterators.hasNext()) {
            return true;
        }
        else {
            return !isNull(current) && (!isNull(current.getNextId()) || !current.isEmpty(mapOfEntries));
        }
    }

    @Override
    public GraphEntry next() {
        while (isGroup(current)) {
            for (var headId : current.getIdsInGroup()) {
                GraphEntry head = mapOfEntries.get(headId);
                iterators = Iterators.concat(iterators, GraphEntryIterator.from(head, mapOfEntries));
            }

            String nextId = current.getNextId();
            current = Objects.isNull(nextId) ? null : mapOfEntries.get(nextId);
        }

        if (iterators.hasNext()) {
            return iterators.next();
        }

        if (isNull(current)) {
            throw new NoSuchElementException();
        }

        var value = current;
        current = mapOfEntries.get(current.getNextId());
        return value;
    }
}
