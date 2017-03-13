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

    private int m_n=0;
    private double m_oldM, m_newM, m_oldS, m_newS;

    public RunningStat()
    { }

    void clear()  {
        m_n = 0;
    }

    void push(double x)  {
        m_n++;

        // See Knuth TAOCP vol 2, 3rd edition, page 232
        if (m_n == 1)  {
            m_oldM = m_newM = x;
            m_oldS = 0.0;
        }  else  {
            m_newM = m_oldM + (x - m_oldM)/m_n;
            m_newS = m_oldS + (x - m_oldM)*(x - m_newM);

            // set up for next iteration
            m_oldM = m_newM;
            m_oldS = m_newS;
        }
    }

    int numDataValues()  {
        return m_n;
    }

    double mean()  {
        return (m_n > 0) ? m_newM : 0.0;
    }

    double variance()  {
        return ( (m_n > 1) ? m_newS/(m_n - 1) : 0.0 );
    }

//    double StandardDeviation() const {
//        return sqrt( Variance() );
//    }

    public static void main(String[] args) {
        RunningStat rs = new RunningStat();

        rs.push(10);
        rs.push(20);
//        rs.push(10);
//        rs.push(10);
//        rs.push(10);
        System.err.println(rs.mean());
//        System.err.println(rs.variance());
    }
}