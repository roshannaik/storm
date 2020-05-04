/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.streams.processors;

import org.apache.storm.streams.operations.CombinerAggregator;

public class MergeAggregateProcessor<T, A, R> extends BaseProcessor<A> implements BatchProcessor {
    private final CombinerAggregator<T, A, R> aggregator;
    private A state;

    public MergeAggregateProcessor(CombinerAggregator<T, A, R> aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    protected void execute(A input) {
        if (state == null) {
            state = aggregator.init();
        }
        state = aggregator.merge(state, input);
        mayBeForwardAggUpdate(() -> aggregator.result(state));
    }

    @Override
    public void finish() {
        if (state != null) {
            context.forward(aggregator.result(state));
            state = null;
        }
    }

}
