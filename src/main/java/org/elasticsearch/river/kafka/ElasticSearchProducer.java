/*
 * Copyright 2014 Mariam Hakobyan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.river.kafka;

import kafka.message.MessageAndMetadata;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Map;
import java.util.Set;

/**
 * An ElasticSearch base producer, which creates an index, mapping in the EL. Also, creates index/delete document
 * requests against ElasticSearch, and executes them with Bulk API.
 *
 * @author Mariam Hakobyan
 */
public abstract class ElasticSearchProducer {

    private static final ESLogger logger = ESLoggerFactory.getLogger(ElasticSearchProducer.class.getName());

    protected final ObjectReader reader = new ObjectMapper().reader(new TypeReference<Map<String, Object>>() {
    });

    private Client client;
    protected BulkProcessor bulkProcessor;

    protected RiverConfig riverConfig;
    protected Stats stats;

    public ElasticSearchProducer(final Client client, final RiverConfig riverConfig, final KafkaConsumer kafkaConsumer,
            final Stats stats) {
        this.client = client;
        this.riverConfig = riverConfig;
        this.stats = stats;

        createBulkProcessor(kafkaConsumer);
    }

    private void createBulkProcessor(final KafkaConsumer kafkaConsumer) {
        bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                stats.flushCount.incrementAndGet();
                logger.info("Index: {}: Going to execute bulk request composed of {} actions.",
                        riverConfig.getIndexName(), request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                logger.info("Index: {}: Executed bulk composed of {} actions.", riverConfig.getIndexName(),
                        request.numberOfActions());

                for (BulkItemResponse item : response.getItems()) {
                    if (item.isFailed()) {
                        stats.failed.incrementAndGet();
                    } else {
                        stats.succeeded.incrementAndGet();
                    }
                }

                // Commit the kafka messages offset, only when messages have been successfully
                // inserted into ElasticSearch
                kafkaConsumer.getConsumerConnector().commitOffsets();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                stats.failed.addAndGet(request.numberOfActions());
                logger.warn("Index: {}: Error executing bulk.", failure, riverConfig.getIndexName());
            }
        }).setBulkActions(riverConfig.getBulkSize()).setFlushInterval(riverConfig.getFlushInterval())
                .setConcurrentRequests(riverConfig.getConcurrentRequests()).build();
    }

    /**
     * For the given messages executes the specified operation type and adds the results bulk processor queue, for
     * processing later when the size of bulk actions is reached.
     *
     * @param messageAndMetadata given message
     */
    public abstract void addMessagesToBulkProcessor(final MessageAndMetadata messageAndMetadata);

    public void closeBulkProcessor() {
        bulkProcessor.close();
    }

    /**
     * support active type by parameter frequency
     * 
     * @return type
     * @author luanmingming
     */
    public String getType() {
        String type = riverConfig.getIndexName();
        if (riverConfig.getFrequency() == RiverConfig.FrequencyType.NOT_FREQUENCY) {
            return type;
        }
        switch (riverConfig.getFrequency()) {
            case ONE_MONTH:
                type = TimeInterval.monthInterval();
                break;
            case ONE_DAY:
                type = TimeInterval.dayInterval();
                break;
            case ONE_HOUR:
                type = TimeInterval.hourInterval();
                break;
            case TEN_MINUTE:
                type = TimeInterval.minuteInterval();
                break;
        }
        return type;
    }
}
