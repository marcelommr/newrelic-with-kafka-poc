/*
 *  Copyright 2020 New Relic Corporation. All rights reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.example.kafkaconsumer;

import com.newrelic.api.agent.ConcurrentHashMapHeaders;
import com.newrelic.api.agent.HeaderType;
import com.newrelic.api.agent.Headers;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.TransportType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;

@Slf4j
@Component
public class KafkaConsumer implements CommandLineRunner {
    private static final String W3C_TRACE_PARENT_HEADER = "traceparent";
    private static final String W3C_TRACE_STATE_HEADER = "tracestate";
    private static final String NEWRELIC_HEADER = "newrelic";

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Override
    public void run(String... args) throws Exception {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singletonList("example-topic"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                processRecord(record);
            }
        }
    }

    /**
     * Starts a New Relic transaction and processes the headers on each Kafka record.
     *
     * @param consumerRecord Kafka record
     */
    @Trace(dispatcher = true)
    private static void processRecord(ConsumerRecord<String, String> consumerRecord) {
        NewRelic.setTransactionName("Custom", "KafkaConsumerService/processRecord");

        //Add value message
        HashMap<String, String> metricMap = new HashMap<String, String>();
        metricMap.put("ValueRecord", consumerRecord.value());
        NewRelic.getAgent().getInsights().recordCustomEvent("MetricKafkaConsumer", metricMap);

        System.out.printf("%nConsuming Kafka Record:%n\ttopic = %s, key = %s, value = %s, offset = %d%n", consumerRecord.topic(), consumerRecord.key(),
                consumerRecord.value(),
                consumerRecord.offset());

        acceptDistributedTraceHeadersFromKafkaRecord(consumerRecord);
    }

    /**
     * This method illustrates usage of New Relic Java agent APIs for propagating distributed tracing headers over Kafka records.
     * NewRelic.getAgent().getTransaction().acceptDistributedTraceHeaders(TransportType, Headers) is used to accept distributed tracing headers from an
     * incoming request and link the requests together into a single distributed trace.
     *
     * @param record Kafka record
     */
    @Trace
    private static void acceptDistributedTraceHeadersFromKafkaRecord(ConsumerRecord<String, String> record) {
        // ConcurrentHashMapHeaders provides a concrete implementation of com.newrelic.api.agent.Headers
        Headers distributedTraceHeaders = ConcurrentHashMapHeaders.build(HeaderType.MESSAGE);

        // Iterate through each Kafka record header and insert the W3C Trace Context headers into the distributedTraceHeaders map
        for (Header kafkaRecordHeader : record.headers()) {
            String kafkaRecordHeaderValue = new String(kafkaRecordHeader.value(), StandardCharsets.UTF_8);
            System.out.printf("\tKafka record header: key = %s, value = %s%n", kafkaRecordHeader.key(), kafkaRecordHeaderValue);

            headerRecord(record.value(), distributedTraceHeaders, kafkaRecordHeader, kafkaRecordHeaderValue, NEWRELIC_HEADER);
            headerRecord(record.value(), distributedTraceHeaders, kafkaRecordHeader, kafkaRecordHeaderValue, W3C_TRACE_PARENT_HEADER);
            headerRecord(record.value(), distributedTraceHeaders, kafkaRecordHeader, kafkaRecordHeaderValue, W3C_TRACE_STATE_HEADER);

            // Accept distributed tracing headers to link this request to the originating request
            NewRelic.getAgent().getTransaction().acceptDistributedTraceHeaders(TransportType.Kafka, distributedTraceHeaders);

        }
    }

    @Trace
    private static void headerRecord(String messageValue, Headers distributedTraceHeaders, Header kafkaRecordHeader, String kafkaRecordHeaderValue, String w3cTraceStateHeader) {
        if (kafkaRecordHeader.key().equals(w3cTraceStateHeader)) {
            distributedTraceHeaders.addHeader(w3cTraceStateHeader, kafkaRecordHeaderValue);

            //Add value header
            HashMap<String, String> metricMap = new HashMap<>();
            metricMap.put("RecordMessageValue", messageValue);
            metricMap.put("HeaderRecordKey", w3cTraceStateHeader);
            metricMap.put("HeaderRecordValue", kafkaRecordHeaderValue);
            NewRelic.getAgent().getInsights().recordCustomEvent("MetricHeaderKafkaConsumer", metricMap);
        }
    }

}
