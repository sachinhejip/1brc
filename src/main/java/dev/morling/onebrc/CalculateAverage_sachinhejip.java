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
import java.nio.MappedByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CalculateAverage_sachinhejip {

    /**
     * Notes:
     *      Baseline: 2.20.37
     *      Plain reading the entire file: 0.01.1
     *          + iterating over every byte: 0.01.23
     *          + allocating data ArrayList: 0.01.51 (Abandoned)
     *          + Allocating Xms can take time for first run
     *          + adding all buffers to data (with 20G allocation): OOM (Abandoned)
     *          + Program without sort (no floating point math) : 1.02.48
     *          + Working program with sort :
     */
    private static final String FILE = "./measurements.txt";

    private static class RecordKey {
        byte[] name;

        public RecordKey(byte[] name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return new String(name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            RecordKey recordKey = (RecordKey) o;
            return Arrays.equals(name, recordKey.name);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(name);
        }

        private void print(PrintStream os) {
            os.print(new String(name));
        }
    }

    private static class Record {
        RecordKey key;
        int sumN = 0;
        int sumF = 0;
        int count = 0;
        int minN = Integer.MAX_VALUE;
        int minF = 0;
        int maxN = Integer.MIN_VALUE;
        int maxF = 9;

        public Record(RecordKey key) {
            this.key = key;
        }

        private void update(byte[] buf, int numStart, int numEnd, int fStart, int fEnd) {
            count += 1;
            int mult = 1;
            if (buf[numStart] == '-') {
                mult = -1;
                numStart = numStart + 1;
            }
            int n = 0;
            int p10 = 1;
            for (int i = numEnd - 1; i >= numStart; i--) {
                n += (buf[i] - '0') * p10;
                p10 = p10 * 10;
            }
            int f = buf[fStart] - '0';
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

        @Override
        public String toString() {
            return key.toString() + "=" + minN + "." + minF + "/" + sumN + "." + sumF + "/" + maxN + "." + maxF;
        }

        void print(PrintStream os) {
            key.print(os);
            os.print("=");
            os.print(minN);
            os.print(".");
            os.print(minF < 0 ? -1 * minF : minF);
            os.print("/");
            sumN = sumN / 10;
            // sumF = sumF % 10; // don't care about the fractional part - it is always zero
            int meanN = sumN / count;
            os.print(meanN);
            os.print(".0/");
            os.print(maxN);
            os.print(".");
            os.print(maxF < 0 ? -1 * maxF : maxF);
        }
    }

    private static class Records {
        static Map<RecordKey, Record> records = new HashMap<>(1000000);

        // ends are not inclusive
        static void update(byte[] buf, int start, int nameEnd, int numberStart, int numberEnd, int fractionStart, int end) {
            Record record = findOrCreate(buf, start, nameEnd);
            record.update(buf, numberStart, numberEnd, fractionStart, end);
        }

        static Record findOrCreate(byte[] buf, int start, int nameEnd) {
            byte[] key = new byte[nameEnd - start];
            for (int i = 0; i < key.length; i++) {
                key[i] = buf[start + i];
            }
            RecordKey rk = new RecordKey(key);
            Record r = records.get(rk);
            if (r == null) {
                r = new Record(rk);
                records.put(rk, r);
            }
            return r;
        }
    }

    enum STATE {
        IN_NAME,
        IN_N,
        IN_F
    }

    public static void main(String[] args) throws IOException {
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

        try (FileInputStream in = new FileInputStream(FILE)) {
            // try (ByteArrayInputStream in = new ByteArrayInputStream(test)) {
            byte[] buf = new byte[4 * 1024 * 1024];
            // byte[] buf = new byte[20];
            int recordStart = 0;
            int nameEnd = -1;
            int nStart = -1;
            int nEnd = -1;
            int fStart = -1;
            int prevRemain = 0;
            boolean done = false;
            STATE state = STATE.IN_NAME;
            while (!done) {
                // System.out.println("Reading with previousRemain " + prevRemain);
                int read = in.read(buf, prevRemain, buf.length - prevRemain);
                if (read == -1) {
                    if (prevRemain == 0) {
                        break;
                    }
                    read = 0;
                    done = true;
                }
                read += prevRemain;
                prevRemain = 0;
                if (read > 0) {
                    // System.out.println("$" + new String(buf, 0, read) + "$$");
                    for (int i = 0; i < read; i++) {
                        byte b = buf[i];
                        // System.out.println((char) b);
                        if (b == '\n') {
                            if (state != STATE.IN_F) {
                                throw new IllegalArgumentException();
                            }
                            if (nameEnd == -1 || nStart == -1 || nEnd == -1 || fStart == -1) {
                                throw new RuntimeException();
                            }
                            Records.update(buf, recordStart, nameEnd, nStart, nEnd, fStart, i);
                            recordStart = i + 1;
                            nameEnd = nStart = nEnd = fStart = -1;
                            state = STATE.IN_NAME;
                        }
                        else if (b == ';') {
                            if (state == STATE.IN_NAME) {
                                nameEnd = i;
                                nStart = i + 1;
                                state = STATE.IN_N;
                            }
                            else {
                                throw new IllegalArgumentException(new String(buf, recordStart, i));
                            }
                        }
                        else if (b == '.') {
                            if (state == STATE.IN_N) {
                                nEnd = i;
                                state = STATE.IN_F;
                                fStart = i + 1;
                            }
                        }
                    }
                    if (recordStart != read) {
                        // System.out.println("In end with incomplete record: " + recordStart);
                    }
                    if (done) {
                        Records.update(buf, recordStart, nameEnd, nStart, nEnd, fStart, read + 1);
                        break;
                    }
                    System.arraycopy(buf, recordStart, buf, 0, read - recordStart);
                    prevRemain = read - recordStart;
                    recordStart = 0;
                    nameEnd = nStart = nEnd = fStart = -1;
                    state = STATE.IN_NAME;
                }
            }
        }
        // TODO: Optimize output
        Set<Map.Entry<RecordKey, Record>> entries = Records.records.entrySet();
        int size = entries.size();
        int i = 0;
        System.out.print("{");
        for (var entry : entries) {
            entry.getValue().print(System.out);
            if (i + 1 < size) {
                System.out.print(", ");
            }
            i++;
        }
        System.out.print("}");
    }
}
