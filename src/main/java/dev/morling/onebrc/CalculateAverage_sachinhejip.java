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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

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
     *
     * Ideas:
     * - Try to keep sorted records Tree?
     * - Large memory allocation in Java 17
     */
    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./test.txt";
    public static final int MAX_READ = Integer.MAX_VALUE;
    // public static final String MARKED_CITY = "Abidjan";
    // private static final String FILE = "/home/sachin/dev_work/java/1brc/src/test/resources/samples/measurements-20.txt";
    // public static final int MAX_READ = 20000;

    private static List<Shard> shards = new ArrayList<>();

    private static class Shard implements Runnable {
        private final String name;
        private final RandomAccessFile file;
        private final int position;
        private final long start;
        private final int length;

        private boolean isDone = false;
        private Node node = new Node((byte) -1); // root

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
                int recordStart = 0;
                int nameEnd = -1;
                int nStart = -1;
                int nEnd = -1;
                int fStart = -1;
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
                        if (nameEnd == -1 || nStart == -1 || nEnd == -1 || fStart == -1) {
                            throw new RuntimeException();
                        }
                        current.setRecord(buf, recordStart, nameEnd, nStart, nEnd, fStart, buf.position() - 1);
                        // records.update(name, buf, recordStart, nameEnd, nStart, nEnd, fStart, buf.position() - 1);
                        // byte[] name = new byte[nameEnd - recordStart];
                        // buf.get(recordStart, name, 0, name.length);
                        recordStart = buf.position();
                        nameEnd = nStart = nEnd = fStart = -1;
                        state = STATE.IN_NAME;
                        // System.out.println(new String(name) + " > " + current.record);
                        current = node;
                        continue;
                    }
                    else if (b == ';') {
                        if (state == STATE.IN_NAME) {
                            nameEnd = buf.position() - 1;
                            nStart = buf.position();
                            state = STATE.IN_N;
                            continue;
                        }
                        else {
                            throw new IllegalArgumentException();
                        }
                    }
                    else if (b == '.') {
                        if (state == STATE.IN_N) {
                            nEnd = buf.position() - 1;
                            fStart = buf.position();
                            state = STATE.IN_F;
                            continue;
                        }
                    }
                    if (state == STATE.IN_NAME) {
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

    // private static class RecordKey {
    // private final String name;
    // ByteBuffer buf;
    // int position;
    // int len;
    //
    // public RecordKey(String name, ByteBuffer buf, int position, int len) {
    // this.name = name;
    // this.buf = buf;
    // this.position = position;
    // this.len = len;
    // }
    //
    // @Override
    // public boolean equals(Object o) {
    // if (this == o)
    // return true;
    // if (o == null || getClass() != o.getClass())
    // return false;
    // RecordKey recordKey = (RecordKey) o;
    // return position == recordKey.position && len == recordKey.len && Objects.equals(name, recordKey.name);
    // }
    //
    // @Override
    // public int hashCode() {
    // return Objects.hash(name, position, len);
    // }
    //
    // public void print(PrintStream os) {
    // byte[] bytes = new byte[len];
    // buf.get(bytes, position, len);
    // os.print(new String(bytes));
    // }
    // }
    //
    private static class Record {
        String name;
        int sumN = 0;
        int sumF = 0;
        int count = 0;
        int minN = Integer.MAX_VALUE;
        int minF = 0;
        int maxN = Integer.MIN_VALUE;
        int maxF = 9;

        public Record(String name) {
            this.name = name;
        }

        private void update(ByteBuffer buf, int numStart, int numEnd, int fStart, int fEnd) {
            count += 1;
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
            sumN = sumN + n;
            sumF = sumF + f;
            if (n < minN) {
                minN = n;
                minF = f;
            }
            else if (n == minN && f < minF) {
                minF = f;
            }
            if (n > maxN) {
                maxN = n;
                maxF = f;
            }
            else if (n == maxN && f > maxF) {
                maxF = f;
            }
        }

        private void update(Record record) {
            if (record == null) {
                return;
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
                // sumN = sumN / 10;
                // // sumF = sumF % 10; // don't care about the fractional part - it is always zero
                // int meanN = sumN / count;
                // os.print(meanN);
                // if (count >= 10) {
                // os.print(".0/");
                // }
                // else {
                // os.print("." + this.sumF / 10 + "/");
                // }
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

    // private static class Records {
    // Map<RecordKey, Record> records = new HashMap<>(100000);
    //
    // // ends are not inclusive
    // void update(String name, ByteBuffer bb, int start, int nameEnd, int numberStart, int numberEnd, int fractionStart, int end) {
    // Record record = findOrCreate(name, bb, start, nameEnd);
    // record.update(bb, numberStart, numberEnd, fractionStart, end);
    // }
    //
    // Record findOrCreate(String name, ByteBuffer bb, int start, int nameEnd) {
    // // System.out.println("key : " + name + " " + start + " " + nameEnd);
    // RecordKey recordKey = getRecordKey(name, bb, start, nameEnd);
    // Record r = records.get(recordKey);
    // if (r == null) {
    // r = new Record(recordKey);
    // records.put(recordKey, r);
    // }
    // return r;
    // }
    //
    // // COPY VERSION
    // // static ByteBuffer getByteBuffer(ByteBuffer bb, int start, int end) {
    // // byte[] buf = new byte[end - start];
    // // bb.get(start, buf, 0, buf.length);
    // // return ByteBuffer.wrap(buf);
    // // }
    //
    // // REF VERSION
    // RecordKey getRecordKey(String name, ByteBuffer bb, int start, int end) {
    // return new RecordKey(name, bb, start, end - start);
    // }
    // }

    enum STATE {
        IN_NAME,
        IN_N,
        IN_F
    }

    public static void main(String[] args) throws IOException {
        Node obj = readUsingThreadsAndMemoryMappedFile();
        // Records records = readUsingMemoryMappedFile();
        // Records records = readUsingInputStream();
        // var entries = records.records.entrySet();
        // int size = entries.size();
        // int i = 0;
        // System.out.print("{");
        // for (var entry : entries) {
        // entry.getValue().print(System.out);
        // if (i + 1 < size) {
        // System.out.print(", ");
        // }
        // i++;
        // }
        // System.out.print("}");
        obj.printResults(System.out);
    }

    private static Node readUsingThreadsAndMemoryMappedFile() throws IOException {
        var OVERLAP = 200;
        try (RandomAccessFile f = new RandomAccessFile(FILE, "r")) {
            FileChannel channel = f.getChannel();
            long fileSize = channel.size();
            int newDataInBuf = 0;
            long readPosInFile = 0;
            while (readPosInFile < fileSize) {
                long oldReadPosInFile = readPosInFile;
                readPosInFile = oldReadPosInFile - OVERLAP;
                readPosInFile = readPosInFile < 0 ? 0 : readPosInFile;
                newDataInBuf = (int) (oldReadPosInFile - readPosInFile);
                int read = (int) Math.min(fileSize - readPosInFile, MAX_READ);
                Shard shard = new Shard(String.valueOf(readPosInFile), f, readPosInFile, newDataInBuf, read);
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
                    Thread.sleep(10);
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
    }

    // private static Records readUsingMemoryMappedFile() throws IOException {
    // Records records = new Records();
    // try (RandomAccessFile f = new RandomAccessFile(FILE, "r")) {
    // FileChannel channel = f.getChannel();
    // long size = channel.size();
    // long position = 0;
    // while (position < size) {
    // // System.out.println(position + " " + size);
    // int read = (int) Math.min(size - position, MAX_READ);
    // // System.out.println(read);
    // ByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY, position, read);
    // String name = position + "-" + read;
    // // byte[] buf = new byte[read];
    // // mbb.get(buf);
    // // System.out.println("Raed : " + new String(buf));
    // int recordStart = 0;
    // int nameEnd = -1;
    // int nStart = -1;
    // int nEnd = -1;
    // int fStart = -1;
    // STATE state = STATE.IN_NAME;
    // for (int i = 0; i < read; i++) {
    // byte b = mbb.get(i);
    // // System.out.println((char) b);
    // if (b == '\n') {
    // if (state != STATE.IN_F) {
    // throw new IllegalArgumentException();
    // }
    // if (nameEnd == -1 || nStart == -1 || nEnd == -1 || fStart == -1) {
    // throw new RuntimeException();
    // }
    // records.update(name, mbb, recordStart, nameEnd, nStart, nEnd, fStart, i);
    // recordStart = i + 1;
    // nameEnd = nStart = nEnd = fStart = -1;
    // state = STATE.IN_NAME;
    // }
    // else if (b == ';') {
    // if (state == STATE.IN_NAME) {
    // nameEnd = i;
    // nStart = i + 1;
    // state = STATE.IN_N;
    // }
    // else {
    // byte[] bytes = new byte[i - recordStart];
    // mbb.get(i, bytes);
    // throw new IllegalArgumentException(new String(bytes));
    // }
    // }
    // else if (b == '.') {
    // if (state == STATE.IN_N) {
    // nEnd = i;
    // if (i + 1 < read) {
    // state = STATE.IN_F;
    // fStart = i + 1;
    // }
    // }
    // }
    // }
    // if (state == STATE.IN_F) {
    // records.update(name, mbb, recordStart, nameEnd, nStart, nEnd, fStart, read);
    // }
    // // System.out.println("end : " + recordStart);
    // position += recordStart;
    // // System.out.println("position : " + position);
    // }
    // }
    // return records;
    // }

    // private static Records readUsingInputStream() throws IOException {
    // Records records = new Records();
    // try (FileInputStream in = new FileInputStream(FILE)) {
    // // try (ByteArrayInputStream in = new ByteArrayInputStream(test)) {
    // byte[] buf = new byte[4 * 1024 * 1024];
    // ByteBuffer bb = ByteBuffer.wrap(buf);
    // // byte[] buf = new byte[20];
    // int recordStart = 0;
    // int nameEnd = -1;
    // int nStart = -1;
    // int nEnd = -1;
    // int fStart = -1;
    // int prevRemain = 0;
    // boolean done = false;
    // STATE state = STATE.IN_NAME;
    // while (!done) {
    // // System.out.println("Reading with previousRemain " + prevRemain);
    // int read = in.read(buf, prevRemain, buf.length - prevRemain);
    // bb.position(0);
    // bb.limit(buf.length);
    // if (read == -1) {
    // if (prevRemain == 0) {
    // break;
    // }
    // read = 0;
    // done = true;
    // }
    // read += prevRemain;
    // prevRemain = 0;
    // if (read > 0) {
    // // System.out.println("$" + new String(buf, 0, read) + "$$");
    // for (int i = 0; i < read; i++) {
    // byte b = buf[i];
    // // System.out.println((char) b);
    // if (b == '\n') {
    // if (state != STATE.IN_F) {
    // throw new IllegalArgumentException();
    // }
    // if (nameEnd == -1 || nStart == -1 || nEnd == -1 || fStart == -1) {
    // throw new RuntimeException();
    // }
    // records.update("", bb, recordStart, nameEnd, nStart, nEnd, fStart, i);
    // recordStart = i + 1;
    // nameEnd = nStart = nEnd = fStart = -1;
    // state = STATE.IN_NAME;
    // }
    // else if (b == ';') {
    // if (state == STATE.IN_NAME) {
    // nameEnd = i;
    // nStart = i + 1;
    // state = STATE.IN_N;
    // }
    // else {
    // throw new IllegalArgumentException(new String(buf, recordStart, i));
    // }
    // }
    // else if (b == '.') {
    // if (state == STATE.IN_N) {
    // nEnd = i;
    // state = STATE.IN_F;
    // fStart = i + 1;
    // }
    // }
    // }
    // if (recordStart != read) {
    // // System.out.println("In end with incomplete record: " + recordStart);
    // }
    // if (done) {
    // records.update("", bb, recordStart, nameEnd, nStart, nEnd, fStart, read + 1);
    // break;
    // }
    // System.arraycopy(buf, recordStart, buf, 0, read - recordStart);
    // prevRemain = read - recordStart;
    // recordStart = 0;
    // nameEnd = nStart = nEnd = fStart = -1;
    // state = STATE.IN_NAME;
    // }
    // }
    // }
    // return records;
    // }

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

        void setRecord(MappedByteBuffer buf, int nameStart, int nameEnd, int nStart, int nEnd, int fStart, int end) {
            boolean printRecord = false;
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

    byte[] test = """
            Hamburg;12.0
            Bulawayo;8.9
            Palembang;38.8
            St. John's;15.2
            Cracow;12.6
            Bridgetown;26.9
            Istanbul;6.2
            Roseau;34.4
            Conakry;31.2
            Hamburg;12.0
            Bulawayo;8.9
            Palembang;38.8
            St. John's;15.2
            Cracow;12.6
            Bridgetown;26.9
            Istanbul;6.2
            Roseau;34.4
            Conakry;31.2
            Hamburg;12.0
            Bulawayo;8.9
            Palembang;38.8
            St. John's;15.2
            Cracow;12.6
            Bridgetown;26.9
            Istanbul;6.2
            Roseau;34.4
            Conakry;31.2
            Istanbul;23.0""".getBytes(StandardCharsets.UTF_8);
}
