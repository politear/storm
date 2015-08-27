/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.elasticsearch.bolt;

import java.util.Map;

import org.apache.storm.elasticsearch.common.EsConfig;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public abstract class AbstractEsBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractEsBolt.class);

    protected static Client client;

    protected OutputCollector collector;
    private EsConfig esConfig;

    public AbstractEsBolt(EsConfig esConfig) {
        this.esConfig = esConfig;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.collector = outputCollector;
            synchronized (AbstractEsBolt.class) {
                if (client == null) {
                    client = new ElasticSearchClient(esConfig).construct();
                }
            }
        } catch (Exception e) {
            LOG.warn("unable to initialize EsBolt ", e);
        }
    }

    @Override
    public abstract void execute(Tuple tuple);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @VisibleForTesting
    static Client getClient() {
        return AbstractEsBolt.client;
    }

    @VisibleForTesting
    static void replaceClient(Client client) {
        AbstractEsBolt.client = client;
    }
}