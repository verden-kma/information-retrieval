package ukma.ir.data_stuctores;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

public class TST<Value> implements Serializable {
    private int n;              // size
    private Node<Value> root;   // root of TST

    private static class Node<Value> implements Serializable {
        private char c;                        // character
        private Node<Value> left, mid, right;  // left, middle, and right subtries
        private Value val;                     // value associated with string
    }

    /**
     * Initializes an empty string symbol table.
     */
    public TST() {
    }

    /**
     * Returns the number of key-value pairs in this symbol table.
     *
     * @return the number of key-value pairs in this symbol table
     */
    public int size() {
        return n;
    }

    /**
     * Does this symbol table contain the given key?
     *
     * @param key the key
     * @return {@code true} if this symbol table contains {@code key} and
     * {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    public boolean contains(String key) {
        if (key == null) {
            throw new IllegalArgumentException("argument to contains() is null");
        }
        return get(key) != null;
    }

    /**
     * Returns the value associated with the given key.
     *
     * @param key the key
     * @return the value associated with the given key if the key is in the symbol table
     * and {@code null} if the key is not in the symbol table
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    public Value get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("calls get() with null argument");
        }
        if (key.length() == 0) throw new IllegalArgumentException("key must have length >= 1");
        Node<Value> x = get(root, key, 0);
        if (x == null) return null;
        return x.val;
    }

    // return subtrie corresponding to given key
    private Node<Value> get(Node<Value> x, String key, int d) {
        if (x == null) return null;
        if (key.length() == 0) throw new IllegalArgumentException("key must have length >= 1");
        char c = key.charAt(d);
        if (c < x.c) return get(x.left, key, d);
        else if (c > x.c) return get(x.right, key, d);
        else if (d < key.length() - 1) return get(x.mid, key, d + 1);
        else return x;
    }

    /**
     * Inserts the key-value pair into the symbol table, overwriting the old value
     * with the new value if the key is already in the symbol table.
     * If the value is {@code null}, this effectively deletes the key from the symbol table.
     *
     * @param key the key
     * @param val the value
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    public void put(String key, Value val) {
        if (key == null) {
            throw new IllegalArgumentException("calls put() with null key");
        }
        if (!contains(key)) n++;
        else if (val == null) n--;       // delete existing key
        root = put(root, key, val, 0);
    }

    private Node<Value> put(Node<Value> x, String key, Value val, int d) {
        char c = key.charAt(d);
        if (x == null) {
            x = new Node<>();
            x.c = c;
        }
        if (c < x.c) x.left = put(x.left, key, val, d);
        else if (c > x.c) x.right = put(x.right, key, val, d);
        else if (d < key.length() - 1) x.mid = put(x.mid, key, val, d + 1);
        else x.val = val;
        return x;
    }

    @SuppressWarnings("unchecked")
    public Set<Map.Entry<String, Value>> entrySet() {
        Set<Entry> set = new TreeSet<>();
        SetExecutor se = new SetExecutor(set);
        collect(root, new StringBuilder(), se);
        return (Set<Map.Entry<String, Value>>) se.getSet();
    }

    /**
     * Returns the string in the symbol table that is the longest prefix of {@code query},
     * or {@code null}, if no such string.
     *
     * @param query the query string
     * @return the string in the symbol table that is the longest prefix of {@code query},
     * or {@code null} if no such string
     * @throws IllegalArgumentException if {@code query} is {@code null}
     */
    public String longestPrefixOf(String query) {
        if (query == null) {
            throw new IllegalArgumentException("calls longestPrefixOf() with null argument");
        }
        if (query.length() == 0) return null;
        int length = 0;
        Node<Value> x = root;
        int i = 0;
        while (x != null && i < query.length()) {
            char c = query.charAt(i);
            if (c < x.c) x = x.left;
            else if (c > x.c) x = x.right;
            else {
                i++;
                if (x.val != null) length = i;
                x = x.mid;
            }
        }
        return query.substring(0, length);
    }

    /**
     * Returns all of the keys in the symbol table that match {@code pattern},
     * where . symbol is treated as a wildcard character.
     *
     * @param pattern the pattern
     * @return all of the keys in the symbol table that match {@code pattern},
     * as a collection, where . is treated as a wildcard character.
     */
    public Collection<String> keysThatMatch(String pattern) {
        Queue<String> queue = new ArrayDeque<>();
        collect(root, new StringBuilder(), 0, pattern, queue);
        return queue;
    }

    private void collect(Node<Value> x, StringBuilder prefix, int i, String pattern, Queue<String> queue) {
        if (x == null) return;
        char c = pattern.charAt(i);
        if (c == '.' || c < x.c) collect(x.left, prefix, i, pattern, queue);
        if (c == '.' || c == x.c) {
            if (i == pattern.length() - 1 && x.val != null) queue.add(prefix.toString() + x.c);
            if (i < pattern.length() - 1) {
                collect(x.mid, prefix.append(x.c), i + 1, pattern, queue);
                prefix.deleteCharAt(prefix.length() - 1);
            }
        }
        if (c == '.' || c > x.c) collect(x.right, prefix, i, pattern, queue);
    }

    /**
     * Returns all keys in the symbol table as a {@code Collection}.
     * To iterate over all of the keys in the symbol table named {@code st},
     * use the foreach notation: {@code for (Key key : st.keys())}.
     *
     * @return all keys in the symbol table as a {@code Collection}
     */
    public Collection<String> keys() {
        Queue<String> queue = new ArrayDeque<>();
        collect(root, new StringBuilder(), queue);
        return queue;
    }

    /**
     * Returns all of the keys in the set that start with {@code prefix}.
     *
     * @param prefix the prefix
     * @return all of the keys in the set that start with {@code prefix},
     * as a collection
     * @throws IllegalArgumentException if {@code prefix} is {@code null}
     */
    public Collection<String> keysWithPrefix(String prefix) {
        if (prefix == null) {
            throw new IllegalArgumentException("calls keysWithPrefix() with null argument");
        }
        Queue<String> queue = new ArrayDeque<>();
        Node<Value> x = get(root, prefix, 0);
        if (x == null) return queue;
        if (x.val != null) queue.add(prefix);
        collect(x.mid, new StringBuilder(prefix), queue);
        return queue;
    }

    // all keys in subtrie rooted at x with given prefix
//    private void collect(Node<Value> x, StringBuilder prefix, Queue<String> queue) {
//        if (x == null) return;
//        collect(x.left, prefix, queue);
//        if (x.val != null) queue.add(prefix.toString() + x.c);
//        collect(x.mid, prefix.append(x.c), queue);
//        prefix.deleteCharAt(prefix.length() - 1);
//        collect(x.right, prefix, queue);
//    }

    private void collect(Node<Value> x, StringBuilder prefix, Queue<String> queue) {
        collect(x, prefix, new QueueExecutor(queue));
    }

    private void collect(Node<Value> x, StringBuilder prefix, Executor exec) {
        if (x == null) return;
        collect(x.left, prefix, exec);
        if (x.val != null) exec.take(x, prefix.toString() + x.c);
        collect(x.mid, prefix.append(x.c), exec);
        prefix.deleteCharAt(prefix.length() - 1);
        collect(x.right, prefix, exec);
    }

    private abstract class Executor {
        abstract void take(Node<Value> n, String prefix);
    }

    private class SetExecutor extends Executor {
        private Set<Entry> set;

        SetExecutor(Set<Entry> s) {
            set = s;
        }

        @Override
        void take(Node<Value> n, String prefix) {
            set.add(new Entry(prefix, n.val));
        }

        Set<? extends Map.Entry<String, Value>> getSet() {
            return set;
        }
    }

    private class QueueExecutor extends Executor {
        private Queue<String> queue;

        QueueExecutor(Queue<String> q) {
            queue = q;
        }

        @Override
        void take(Node<Value> n, String prefix) {
            queue.add(prefix);
        }
    }

    private class Entry implements Map.Entry<String, Value>, Comparable<Entry> {
        private final String key;
        private Value value;

        Entry(String k, Value v) {
            key = k;
            value = v;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Value getValue() {
            return value;
        }

        @Override
        public Value setValue(Value v) {
            return value = v;
        }

        @Override
        public int compareTo(Entry entry) {
            return key.compareTo(entry.key);
        }
    }
}