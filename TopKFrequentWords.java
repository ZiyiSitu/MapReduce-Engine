/**
 * Definition of OutputCollector:
 * class OutputCollector<K, V> {
 *     public void collect(K key, V value);
 *         // Adds a key/value pair to the output buffer
 * }
 * Definition of Document:
 * class Document {
 *     public int id;
 *     public String content;
 * }
 */

class Pair {
    String s;
    int cnt;
    public Pair(String s, int cnt) {
        this.s = s;
        this.cnt = cnt;
    }
}

public class TopKFrequentWords {

    public static class Map {
        public void map(String _, Document value,
                        OutputCollector<String, Integer> output) {
            // Output the results into output buffer.
            // Ps. output.collect(String key, int value);
            String[] strs = value.content.split("\\s+");
            for (String s : strs) {
                output.collect(s, 1);
            }
        }
    }

    public static class Reduce {
        Queue<Pair> minheap;
        int k;
        Comparator<Pair> cmp = new Comparator<Pair>() {
            @Override
            public int compare(Pair a, Pair b) {
                if (a.cnt != b.cnt) {
                    return a.cnt - b.cnt;
                }
                return b.s.compareTo(a.s);
            }
        };
        
        public void setup(int k) {
            minheap = new PriorityQueue<>(k, cmp);
            this.k = k;
        }

        public void reduce(String key, Iterator<Integer> values) {
            int sum = 0;
            while (values.hasNext()) {
                values.next();
                sum++;
            }
            Pair p = new Pair(key, sum);
            if (minheap.size() < k) {
                minheap.offer(p);
            } else {
                if (cmp.compare(p, minheap.peek()) > 0) {
                    minheap.poll();
                    minheap.offer(p);
                }
            }
        }

        public void cleanup(OutputCollector<String, Integer> output) {
            // Output the top k pairs <word, times> into output buffer.
            // Ps. output.collect(String key, Integer value);
            int n = minheap.size();
            Pair[] tmp = new Pair[n];
            for (int i = 0; i < n; i++) {
                tmp[i] = minheap.poll();
            }
            
            for (int i = n - 1; i >= 0; i--) {
                output.collect(tmp[i].s, tmp[i].cnt);
            }
        }
    }
}