/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.utils;

public class RunningStat  {

    private long m_n=0;
    private double m_oldM, m_newM, m_oldS, m_newS;
    private String name;
    public static int printFreq = 20_000_000;
    private boolean disable;
    private long count=0;

    public RunningStat()
    { this.name = "" ; }

    public RunningStat(String name)
    {
        this.name = name;
    }

    public RunningStat(String name, boolean disable)
    {
        this.name = name;
        this.disable = disable;
    }

    public RunningStat(String name, int printFreq, boolean disable)
    {
        this.name = name;
        this.printFreq = printFreq;
        this.disable = disable;
    }

    public RunningStat(String name, int printFreq) {
        this(name, printFreq, false);
    }

    public void clear()  {
        m_n = 0;
    }

    public void pushLatency(long startMs) {
        push(System.currentTimeMillis() - startMs);
    }

    public void push(long x)  {
        if(disable)
            return;

        m_n++;

        // See Knuth TAOCP vol 2, 3rd edition, page 232
        if (m_n == 1)  {
            m_oldM = m_newM = x;
            m_oldS = 0;
        }  else  {
            m_newM = m_oldM + (x - m_oldM)/m_n;
            m_newS = m_oldS + (x - m_oldM)*(x - m_newM);

            // set up for next iteration
            m_oldM = m_newM;
            m_oldS = m_newS;
        }
        if(++count==printFreq) {
            System.err.printf("  ***> %s - %,f\n", name, mean());
            count=0;
        }
    }

    public long numDataValues()  {
        return m_n;
    }

    public double mean()  {
        return (m_n > 0) ? m_newM : 0.0;
    }

    public double variance()  {
        return ( (m_n > 1) ? m_newS/(m_n - 1) : 0.0 );
    }

//    double StandardDeviation() const {
//        return sqrt( Variance() );
//    }


//    /* simple test to try out RunningStat */
//    public static void main(String[] args) {
//        RunningStat rs = new RunningStat();
//
//        rs.push(10);
//        rs.push(20);
////        rs.push(10);
////        rs.push(10);
////        rs.push(10);
//        System.err.println(rs.mean());
////        System.err.println(rs.variance());
//    }


//    public static void main(String[] args) {
//        /* currrentTimeInMillis V/S  nanoTime */
//        {
//            long startTime = System.currentTimeMillis();
//            for (int i = 0; i < 10_000_000; i++) {
//                long test = System.nanoTime();
//            }
//            long endTime = System.currentTimeMillis();
//
//            System.out.println("Total Nano time: " + (endTime - startTime));
//        }
//        {
//            long startTime = System.currentTimeMillis();
//            for (int i = 0; i < 10_000_000; i++) {
//                long test = System.currentTimeMillis();
//            }
//            long endTime = System.currentTimeMillis();
//
//            System.out.println("Total CTM time: " + (endTime - startTime));
//
//        }
//    }
}