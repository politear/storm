/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.storm.kafka.spout;

import info.batey.kafka.unit.KafkaUnitRule;
import kafka.producer.KeyedMessage;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.stream.IntStream;
import static org.mockito.Mockito.*;
import static org.apache.storm.kafka.spout.builders.SingleTopicKafkaSpoutConfiguration.*;

public class SingleTopicKafkaSpoutTest {

    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule();

    void populateTopicData(String topicName, int msgCount) {
        kafkaUnitRule.getKafkaUnit().createTopic(topicName);

        IntStream.range(0, msgCount).forEach(value -> {
            KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(
                    topicName, Integer.toString(value),
                    Integer.toString(value));

            kafkaUnitRule.getKafkaUnit().sendMessages(keyedMessage);
        });
    }
    
    /*
     * Asserts that the next possible offset to commit or the committed offset is the provided offset.
     * An offset that is ready to be committed is not guarenteed to be already committed.
     */
    private void assertOffsetCommitted(int offset, KafkaSpout.OffsetEntry entry) {

        boolean currentOffsetMatch = entry.getCommittedOffset() == offset;
        OffsetAndMetadata nextOffset = entry.findNextCommitOffset();
        boolean nextOffsetMatch =  nextOffset != null && nextOffset.offset() == offset;
        assertTrue("Next offset: " +
                        entry.findNextCommitOffset() +
                        " OR current offset: " +
                        entry.getCommittedOffset() +
                        " must equal desired offset: " +
                        offset,
                currentOffsetMatch | nextOffsetMatch);
    }

    @Test
    public void shouldContinueWithSlowDoubleAcks() throws Exception {
        int messageCount = 20;
        populateTopicData(SingleTopicKafkaSpoutConfiguration.topic, messageCount);
        int kafkaPort = kafkaUnitRule.getKafkaPort();

        TopologyContext topology = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        Map conf = mock(Map.class);

        KafkaSpout<String, String> spout = new KafkaSpout<>(getKafkaSpoutConfig(getKafkaSpoutStreams(), kafkaPort));
        spout.open(conf, topology, collector);
        spout.activate();

        //play 1st tuple
        ArgumentCaptor<Object> messageIdToDoubleAck = ArgumentCaptor.forClass(Object.class);
        spout.nextTuple();
        verify(collector).emit(anyObject(), anyObject(), messageIdToDoubleAck.capture());
        spout.ack(messageIdToDoubleAck.getValue());

        IntStream.range(0, messageCount/2).forEach(value -> {
            spout.nextTuple();
        });

        spout.ack(messageIdToDoubleAck.getValue());

        IntStream.range(0, messageCount).forEach(value -> {
            spout.nextTuple();
        });

        ArgumentCaptor<Object> remainingIds = ArgumentCaptor.forClass(Object.class);

        verify(collector, times(messageCount)).emit(
                eq(SingleTopicKafkaSpoutConfiguration.stream),
                anyObject(),
                remainingIds.capture());
        remainingIds.getAllValues().iterator().forEachRemaining(spout::ack);

        spout.acked.values().forEach(item -> {
            assertOffsetCommitted(messageCount - 1, (KafkaSpout.OffsetEntry) item);
        });
    }

    @Test
    public void shouldEmitAllMessages() throws Exception {
        int messageCount = 10;
        populateTopicData(SingleTopicKafkaSpoutConfiguration.topic, messageCount);
        int kafkaPort = kafkaUnitRule.getKafkaPort();

        TopologyContext topology = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        Map conf = mock(Map.class);

        KafkaSpout<String, String> spout = new KafkaSpout<>(getKafkaSpoutConfig(getKafkaSpoutStreams(), kafkaPort));
        spout.open(conf, topology, collector);
        spout.activate();

        IntStream.range(0, messageCount).forEach(value -> {
            spout.nextTuple();
            ArgumentCaptor<Object> messageId = ArgumentCaptor.forClass(Object.class);
            verify(collector).emit(
                    eq(SingleTopicKafkaSpoutConfiguration.stream),
                    eq(new Values(SingleTopicKafkaSpoutConfiguration.topic,
                            Integer.toString(value),
                            Integer.toString(value))),
            messageId.capture());
            spout.ack(messageId.getValue());
            reset(collector);
        });

        spout.acked.values().forEach(item -> {
            assertOffsetCommitted(messageCount - 1, (KafkaSpout.OffsetEntry) item);
        });
    }

    @Test
    public void shouldReplayInOrderFailedMessages() throws Exception {
        int messageCount = 10;
        populateTopicData(SingleTopicKafkaSpoutConfiguration.topic, messageCount);
        int kafkaPort = kafkaUnitRule.getKafkaPort();

        TopologyContext topology = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        Map conf = mock(Map.class);

        KafkaSpout<String, String> spout = new KafkaSpout<>(getKafkaSpoutConfig(getKafkaSpoutStreams(), kafkaPort));
        spout.open(conf, topology, collector);
        spout.activate();

        //play and ack 1 tuple
        ArgumentCaptor<Object> messageIdAcked = ArgumentCaptor.forClass(Object.class);
        spout.nextTuple();
        verify(collector).emit(anyObject(), anyObject(), messageIdAcked.capture());
        spout.ack(messageIdAcked.getValue());
        reset(collector);

        //play and fail 1 tuple
        ArgumentCaptor<Object> messageIdFailed = ArgumentCaptor.forClass(Object.class);
        spout.nextTuple();
        verify(collector).emit(anyObject(), anyObject(), messageIdFailed.capture());
        spout.fail(messageIdFailed.getValue());
        reset(collector);

        //pause so that failed tuples will be retried
        Thread.sleep(200);


        //allow for some calls to nextTuple() to fail to emit a tuple
        IntStream.range(0, messageCount + 5).forEach(value -> {
            spout.nextTuple();
        });

        ArgumentCaptor<Object> remainingMessageIds = ArgumentCaptor.forClass(Object.class);

        //1 message replayed, messageCount - 2 messages emitted for the first time
        verify(collector, times(messageCount - 1)).emit(
                eq(SingleTopicKafkaSpoutConfiguration.stream),
                anyObject(),
                remainingMessageIds.capture());
        remainingMessageIds.getAllValues().iterator().forEachRemaining(spout::ack);

        spout.acked.values().forEach(item -> {
            assertOffsetCommitted(messageCount - 1, (KafkaSpout.OffsetEntry) item);
        });
    }

    @Test
    public void shouldReplayOutOfOrderFailedMessages() throws Exception {
        int messageCount = 10;
        populateTopicData(SingleTopicKafkaSpoutConfiguration.topic, messageCount);
        int kafkaPort = kafkaUnitRule.getKafkaPort();

        TopologyContext topology = mock(TopologyContext.class);
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        Map conf = mock(Map.class);

        KafkaSpout<String, String> spout = new KafkaSpout<>(getKafkaSpoutConfig(getKafkaSpoutStreams(), kafkaPort));
        spout.open(conf, topology, collector);
        spout.activate();

        //play 1st tuple
        ArgumentCaptor<Object> messageIdToFail = ArgumentCaptor.forClass(Object.class);
        spout.nextTuple();
        verify(collector).emit(anyObject(), anyObject(), messageIdToFail.capture());
        reset(collector);

        //play 2nd tuple
        ArgumentCaptor<Object> messageIdToAck = ArgumentCaptor.forClass(Object.class);
        spout.nextTuple();
        verify(collector).emit(anyObject(), anyObject(), messageIdToAck.capture());
        reset(collector);

        //ack 2nd tuple
        spout.ack(messageIdToAck.getValue());
        //fail 1st tuple
        spout.fail(messageIdToFail.getValue());

        //pause so that failed tuples will be retried
        Thread.sleep(200);

        //allow for some calls to nextTuple() to fail to emit a tuple
        IntStream.range(0, messageCount + 5).forEach(value -> {
            spout.nextTuple();
        });

        ArgumentCaptor<Object> remainingIds = ArgumentCaptor.forClass(Object.class);
        //1 message replayed, messageCount - 2 messages emitted for the first time
        verify(collector, times(messageCount - 1)).emit(
                eq(SingleTopicKafkaSpoutConfiguration.stream),
                anyObject(),
                remainingIds.capture());
        remainingIds.getAllValues().iterator().forEachRemaining(spout::ack);

        spout.acked.values().forEach(item -> {
            assertOffsetCommitted(messageCount - 1, (KafkaSpout.OffsetEntry) item);
        });
    }
}