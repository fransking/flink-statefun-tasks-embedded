package com.sbbsystems.statefun.tasks.graph.v2;

import com.google.common.collect.Iterators;

import java.util.*;

import static java.util.Objects.isNull;

public class EntryIterator  implements Iterator<Entry> {
    private Iterator<Entry> iterators = Collections.emptyIterator();
    private Entry current;
    private final Map<String, Entry> mapOfEntries;

    public static EntryIterator from(Entry entry, Map<String, Entry> mapOfEntries) {
        return new EntryIterator(entry, mapOfEntries);
    }

    private EntryIterator(Entry entry, Map<String, Entry> mapOfEntries) {
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
    public Entry next() {
        while (!isNull(current) && current.isGroup()) {
            for (var headId : current.getIdsInGroup()) {
                Entry head = mapOfEntries.get(headId);
                iterators = Iterators.concat(iterators, EntryIterator.from(head, mapOfEntries));
            }

            String nextId = current.getNextId();
            current = Objects.isNull(nextId) ? null : mapOfEntries.getOrDefault(nextId, null);
        }

        if (iterators.hasNext()) {
            return iterators.next();
        }

        if (isNull(current)) {
            throw new NoSuchElementException();
        }

        var value = current;
        current = mapOfEntries.getOrDefault(current.getNextId(), null);
        return value;
    }
}
