/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

public class CalculateAverage_sachinhejip {

    /*
     * Notes:
     * Baseline: 2.20.37
     * Plain reading the entire file: 0.01.1
     * + iterating over every byte: 0.01.23
     * + Reading using input streams + parsing but no Record creation : 0.41.28
     * + allocating data ArrayList: 0.01.51 (Abandoned)
     * + Allocating Xms can take time for first run
     * + adding all buffers to data (with 20G allocation): OOM (Abandoned)
     * + Program without sort (no floating point math) : 1.02.48
     * + Program without sort (no FP, ByteBuffer) : 1.20.62 (worse)
     * + Working program with sort :
     * Reading using memory mapped file + parsing but no Record creation : 0.28.00
     * + Working program without sort : 1.20 (worse than the one with input streams ... hmmm)
     * Reading using memory mapped files + threads + parsing but no Record creation : 0.04.32 (after OS caches)
     * + Working program without sort : OOM even with 2G
     * + No merge and no print : 0.23 - 0.18 (20G Xmx)
     * + No merge and no print : 0.16.6 (2G Xmx)
     * + Merge and print : 0.16.49 (2G Xmx) - however sorting is not correct
     * Also changed to store strings in the loop (reduced MBB.get and removed byte)
     * * Merge and print (correct sort but incorrect math) : 0.17.12 (2G Xmx)
     * * Correct response : 0.18.42 (2G Xmx)
     * * Correct response without Xmx : 0.18.69
     * * TreeMap : 0.20.63 (2G Xmx)
     * * Node instead of TreeMap : 0.18.7
     *  * calculating ints in loop : 0.20 :(
     *  * buffer for name : 0.19.8
     *
     *
     * Ideas:
     * - Large memory allocation in Java 17
     */
    private static final String FILE = "./measurements.txt";
    public static final int MAX_READ = Integer.MAX_VALUE;
    // private static final String FILE = "/home/sachin/dev_work/java/1brc/src/test/resources/samples/measurements-20.txt";
    // private static final String FILE = "./test.txt";
    // public static final int MAX_READ = 20000;
    public static final String MARKED_CITY = "Abha";

    private static List<Shard> shards = new ArrayList<>();

    private static class Shard implements Runnable {
        private final String name;
        private final RandomAccessFile file;
        private final int position;
        private final long start;
        private final int length;

        private boolean isDone = false;
        private Node node = new Node((byte) -1); // root

        // private TreeMap<String, Record> map = new TreeMap<>(Comparator.comparing(k -> k));

        public Shard(String name, RandomAccessFile file, long start, int position, int length) {
            this.name = name;
            this.file = file;
            this.start = start;
            this.position = position;
            this.length = length;
        }

        @Override
        public String toString() {
            return "Shard{" +
                    "name='" + name + '\'' +
                    ", position=" + position +
                    ", start=" + start +
                    ", length=" + length +
                    ", isDone=" + isDone +
                    '}';
        }

        @Override
        public void run() {
            byte[] recordNameBytes = new byte[2000];
            try {
                MappedByteBuffer buf = file.getChannel().map(FileChannel.MapMode.READ_ONLY, start, length);
                if (start > 0) {
                    int i = position;
                    for (; i >= 0; i--) {
                        if (buf.get(i) == '\n') {
                            break;
                        }
                    }
                    if (i == -1) {
                        throw new IllegalStateException();
                    }
                    buf.position(i + 1);
                }
                int recordStart = buf.position();
                int nameEnd = -1;
                // int nStart = -1;
                // int nEnd = -1;
                // int fStart = -1;
                int n = 0;
                int f = 0;
                int mult = 1;
                STATE state = STATE.IN_NAME;
                Node current = node;
                while (buf.hasRemaining()) {
                    byte b = buf.get();
                    // System.out.print(b + ",");
                    // System.out.println(b + " > " + current);
                    if (b == '\n') {
                        if (state != STATE.IN_F) {
                            throw new IllegalArgumentException();
                        }
                        // if (nameEnd == -1 || nStart == -1 || nEnd == -1 || fStart == -1) {
                        // throw new RuntimeException();
                        // }
                        // current.setRecord(buf, recordStart, nameEnd, nStart, nEnd, fStart, buf.position() - 1);
                        // byte[] bytes = new byte[nameEnd - recordStart];
                        // buf.get(recordStart, bytes, 0, bytes.length);
                        String recordName = new String(recordNameBytes, 0, nameEnd - recordStart);
                        current.setRecord(recordName, mult * n, mult * f);
                        // byte[] bytes = new byte[nameEnd - recordStart];
                        // buf.get(recordStart, bytes, 0, bytes.length);
                        // String recordName = new String(bytes);
                        // Record mapRecord = map.get(recordName);
                        // if (mapRecord != null) {
                        // mapRecord.update(buf, nStart, nEnd, fStart, buf.position() - 1);
                        // }
                        // else {
                        // Record record = new Record(recordName);
                        // record.update(buf, nStart, nEnd, fStart, buf.position() - 1);
                        // map.put(record.name, record);
                        // }
                        // if (recordName.equals(MARKED_CITY)) {
                        // byte[] recordBytes = new byte[buf.position() - 1 - recordStart];
                        // buf.get(recordStart, recordBytes, 0, recordBytes.length);
                        // System.out.println("record = " + current.record + " n,f " + n + "." + f + " for recordBytes = " + new String(recordBytes));
                        // }
                        // records.update(name, buf, recordStart, nameEnd, nStart, nEnd, fStart, buf.position() - 1);
                        // byte[] name = new byte[nameEnd - recordStart];
                        // buf.get(recordStart, name, 0, name.length);
                        recordStart = buf.position();
                        nameEnd = -1;
                        // nStart = -1;
                        // nEnd = -1;
                        // fStart = -1;
                        n = 0;
                        f = 0;
                        mult = 1;
                        state = STATE.IN_NAME;
                        // System.out.println(new String(name) + " > " + current.record);
                        current = node;
                        continue;
                    }
                    else if (b == ';') {
                        if (state == STATE.IN_NAME) {
                            nameEnd = buf.position() - 1;
                            // nStart = buf.position();
                            state = STATE.IN_N;
                            continue;
                        }
                        else {
                            throw new IllegalArgumentException();
                        }
                    }
                    else if (b == '.') {
                        if (state == STATE.IN_N) {
                            // nEnd = buf.position() - 1;
                            // fStart = buf.position();
                            state = STATE.IN_F;
                            continue;
                        }
                    }
                    if (state == STATE.IN_N) {
                        if (b == '-') {
                            mult = -1;
                        }
                        else {
                            n = n * 10 + (b - '0');
                        }
                    }
                    if (state == STATE.IN_F) {
                        f = b - '0';
                    }
                    if (state == STATE.IN_NAME) {
                        recordNameBytes[buf.position() - 1 - recordStart] = b;
                        current = current.update(b);
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            isDone = true;
        }
    }

    public static void main(String[] args) throws IOException {
        // TreeMap<String, Record> obj = readUsingThreadsAndMemoryMappedFile();
        Node obj = readUsingThreadsAndMemoryMappedFile();
        obj.printResults(System.out);
        // printResults(obj, System.out);
    }

    private static void printResults(TreeMap<String, Record> map, PrintStream out) {
        out.print("{");
        boolean[] first = { true };
        map.forEach((key, value) -> {
            if (!first[0]) {
                out.print(", ");
            }
            // System.out.println("record : " + value);
            value.print(out);
            first[0] = false;
        });
        out.print("}");
    }

    private static Node readUsingThreadsAndMemoryMappedFile() throws IOException {
        // private static TreeMap<String, Record> readUsingThreadsAndMemoryMappedFile() throws IOException {
        var OVERLAP = 200;
        try (RandomAccessFile f = new RandomAccessFile(FILE, "r")) {
            FileChannel channel = f.getChannel();
            long fileSize = channel.size();
            int newDataInBuf = 0;
            long readPosInFile = 0;
            int shardCount = 0;
            while (readPosInFile < fileSize) {
                long oldReadPosInFile = readPosInFile;
                readPosInFile = oldReadPosInFile - OVERLAP;
                readPosInFile = readPosInFile < 0 ? 0 : readPosInFile;
                newDataInBuf = (int) (oldReadPosInFile - readPosInFile);
                int read = (int) Math.min(fileSize - readPosInFile, MAX_READ);
                Shard shard = new Shard(String.valueOf(shardCount), f, readPosInFile, newDataInBuf, read);
                shardCount++;
                var th = new Thread(shard, shard.name);
                // System.out.println("shard = " + shard);
                shards.add(shard);
                th.start();
                // if (true)
                // break;
                readPosInFile += read;
            }
            int done = 0;
            var shardsSize = shards.size();
            while (done < shardsSize) {
                try {
                    Thread.sleep(1);
                    done = shards.stream().mapToInt(s -> s.isDone ? 1 : 0).sum();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        Node node = shards.get(0).node;
        for (Shard shard : shards) {
            node.merge(shard.node);
        }
        return node;
        // TreeMap<String, Record> map = shards.get(0).map;
        // for (int i = 1; i < shards.size(); i++) {
        // TreeMap<String, Record> shardMap = shards.get(i).map;
        // for (String s : shardMap.keySet()) {
        // Record shardRecord = shardMap.get(s);
        // // if (map.computeIfPresent(s, (k, r) -> r.update(shardRecord)) == null) {
        // // map.put(s, shardRecord);
        // // }
        // // if (s.equals(MARKED_CITY)) {
        // // System.out.println("shardRecord = " + shardRecord);
        // // }
        // if (map.containsKey(s)) {
        // Record mapRecord = map.get(s);
        // // if (s.equals(MARKED_CITY)) {
        // // System.out.println("mapRecord = " + shardRecord);
        // // }
        // mapRecord.update(shardRecord);
        // // if (s.equals(MARKED_CITY)) {
        // // System.out.println("Map's updated mapRecord = " + mapRecord);
        // // }
        // }
        // else {
        // map.put(s, shardRecord);
        // }
        // }
        // }
        // return map;
    }

    private static class Node {
        Record record = null;
        Node[] childNodes = new Node[256];

        public Node(byte b) {

        }

        Node update(byte b) {
            int mask = 128 + b;
            // System.out.println(this.index + " > update (" + index + " " + b + ") " + mask + " childNodes[mask] " + childNodes[mask]);
            if (childNodes[mask] == null) {
                childNodes[mask] = new Node(b);
            }
            return childNodes[mask];
        }

        public String toString() {
            return "Node : " + record;
        }

        void setRecord(String name, int n, int f) {
            if (record == null) {
                record = new Record(name);
            }
            record.update(n, f);
        }

        void setRecord(MappedByteBuffer buf, int nameStart, int nameEnd, int nStart, int nEnd, int fStart, int end) {
            // boolean printRecord = false;
            if (record == null) {
                byte[] b = new byte[nameEnd - nameStart];
                buf.get(nameStart, b, 0, b.length);
                record = new Record(new String(b));
            }
            else {
                // System.out.println("Found repeat");
                // if (record.name.equals(MARKED_CITY)) {
                // System.out.println("Node.setRecord - " + record);
                // printRecord = true;
                // }
            }

            record.update(buf, nStart, nEnd, fStart, end);
            // if (printRecord) {
            // System.out.println("Node.setRecord - " + record);
            // }
        }

        Node merge(Node node) {
            if (node != null && node != this) {
                if (this.record == null) {
                    this.record = node.record;
                }
                else {
                    // if (this.record.name.equals(MARKED_CITY)) {
                    // System.out.println("Node.merge " + this.record);
                    // System.out.println("Node.merge : " + node.record);
                    // }
                    this.record.update(node.record);
                    // if (this.record.name.equals(MARKED_CITY)) {
                    // System.out.println("Node.merge " + this.record);
                    // }
                }
                for (int i = 0; i < this.childNodes.length; i++) {
                    Node myChildNode = this.childNodes[i];
                    Node otherChildNode = node.childNodes[i];
                    if (myChildNode == null) {
                        this.childNodes[i] = otherChildNode;
                    }
                    else {
                        myChildNode.merge(otherChildNode);
                    }
                }
            }
            return this;
        }

        private void collectRecords(List<Record> collector) {
            if (record != null) {
                collector.add(record);
            }
            for (Node childNode : childNodes) {
                if (childNode != null) {
                    childNode.collectRecords(collector);
                }
            }
        }

        public void printResults(PrintStream out) {
            out.print('{');
            List<Record> records = new ArrayList<>(100000);
            collectRecords(records);
            // records.sort(Comparator.comparing(r -> r.name));
            // boolean first = true;
            // for (Record r : records) {
            // if (!first) {
            // out.print(", ");
            // }
            // r.print(out);
            // first = false;
            // }
            // streaming is faster by a bit (1-2 seconds)
            boolean[] first = { true };
            records.stream().sorted(Comparator.comparing(r -> r.name)).forEach(r -> {
                if (!first[0]) {
                    out.print(", ");
                }
                r.print(out);
                first[0] = false;
            });
            // this.printResultsImpl(out, true);
            out.print('}');
        }

        private boolean printResultsImpl(PrintStream out, boolean first) {
            if (record != null) {
                if (!first) {
                    out.print(", ");
                }
                record.print(out);
                first = false;
            }
            for (Node childNode : childNodes) {
                if (childNode != null) {
                    first &= childNode.printResultsImpl(out, first);
                }
            }
            return first;
        }
    }

    private static class Record {
        String name;
        int sumN = 0;
        int sumF = 0;
        int count = 0;
        int minN = Integer.MAX_VALUE;
        int minF = 9;
        int maxN = Integer.MIN_VALUE;
        int maxF = -9;

        public Record() {
        }

        public Record(String name) {
            this.name = name;
        }

        private Record update(int n, int f) {
            count += 1;
            sumN = sumN + n;
            sumF = sumF + f;
            if (n < minN || (n == minN && f < minF)) {
                minN = n;
                minF = f;
            }
            if (n > maxN || (n == maxN && f > maxF)) {
                maxN = n;
                maxF = f;
            }
            return this;
        }

        private Record update(ByteBuffer buf, int numStart, int numEnd, int fStart, int fEnd) {
            int mult = 1;
            if (buf.get(numStart) == '-') {
                mult = -1;
                numStart = numStart + 1;
            }
            int n = 0;
            int p10 = 1;
            for (int i = numEnd - 1; i >= numStart; i--) {
                n += (buf.get(i) - '0') * p10;
                p10 = p10 * 10;
            }
            int f = buf.get(fStart) - '0';
            n = mult * n;
            f = mult * f;
            return update(n, f);
        }

        private Record update(Record record) {
            if (record == null) {
                return this;
            }
            this.count += record.count;
            this.sumN += record.sumN;
            this.sumF += record.sumF;
            if (record.minN < this.minN || (record.minN == this.minN && record.minF < this.minF)) {
                this.minN = record.minN;
                this.minF = record.minF;
            }
            if (record.maxN > this.maxN || (record.maxN == this.maxN && record.maxF > this.maxF)) {
                this.maxN = record.maxN;
                this.maxF = record.maxF;
            }
            return this;
        }

        @Override
        public String toString() {
            return name + "=" + minN + "." + minF + "/" + sumN + "." + sumF + "/" + count + "/" + maxN + "." + maxF;
        }

        void print(PrintStream os) {
            os.print(name);
            os.print("=");
            os.print(minN);
            os.print(".");
            os.print(minF < 0 ? -1 * minF : minF);
            os.print("/");
            if (count > 1) {
                sumN += sumF / 10;
                sumF = sumF % 10;
                int meanN = sumN / count;
                int carry = (sumN % count) * 10 + sumF;
                int meanF = carry / count;
                int meanC = (carry % count);
                if (meanC > (count / 2)) {
                    meanF += 1;
                    if (meanF == 10) {
                        meanF = 0;
                        meanN += 1;
                    }
                }
                os.print(meanN);
                os.print(".");
                os.print(meanF);
                os.print("/");
            }
            else {
                os.print(sumN);
                os.print(".");
                os.print(sumF);
                os.print("/");
            }
            os.print(maxN);
            os.print(".");
            os.print(maxF < 0 ? -1 * maxF : maxF);
        }
    }

    enum STATE {
        IN_NAME,
        IN_N,
        IN_F
    }
}
