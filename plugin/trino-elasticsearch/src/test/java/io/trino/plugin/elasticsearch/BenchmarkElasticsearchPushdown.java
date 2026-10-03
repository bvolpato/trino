/*
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
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonMapperProvider;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.elasticsearch.ElasticsearchServer.ELASTICSEARCH_7_IMAGE;
import static io.trino.plugin.elasticsearch.ElasticsearchServer.ELASTICSEARCH_8_IMAGE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Manual benchmark for Elasticsearch TopN and aggregation pushdown.
 *
 * Run with:
 * {@code ./mvnw -pl :trino-elasticsearch -Dtest=BenchmarkElasticsearchPushdown -Delasticsearch.benchmark.enabled=true test}
 *
 * Set {@code -Delasticsearch.benchmark.images=elasticsearch:7.17.27} to run one image,
 * or tune the document count, warmups, and repetitions with the corresponding
 * {@code elasticsearch.benchmark.*} system properties. The reported processed input bytes
 * come from the page source and do not represent Elasticsearch wire bytes.
 */
@EnabledIfSystemProperty(named = "elasticsearch.benchmark.enabled", matches = "true")
final class BenchmarkElasticsearchPushdown
{
    private static final Logger LOG = Logger.get(BenchmarkElasticsearchPushdown.class);
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();

    private static final String SCHEMA = "pushdown_benchmark";
    private static final String INDEX = "pushdown_benchmark";
    private static final String ALIAS = "pushdown_benchmark_all";
    private static final List<String> ALIAS_BACKING_INDICES = ImmutableList.of("pushdown_benchmark_alias_a", "pushdown_benchmark_alias_b");
    private static final int ALIAS_BACKING_DOCUMENT_COUNT = 1_000;
    private static final int SHARD_COUNT = 3;
    private static final int DEFAULT_DOCUMENT_COUNT = 100_000;
    private static final int DEFAULT_GROUP_COUNT = 100;
    private static final int DEFAULT_HIGH_CARDINALITY_GROUP_COUNT = 5_000;
    private static final int DEFAULT_BATCH_SIZE = 1_000;
    private static final int DEFAULT_PAYLOAD_BYTES = 128;
    private static final int DEFAULT_WARMUP_ITERATIONS = 2;
    private static final int DEFAULT_MEASUREMENT_ITERATIONS = 7;

    @Test
    void benchmarkPushdown()
            throws Exception
    {
        int documentCount = positiveProperty("elasticsearch.benchmark.documents", DEFAULT_DOCUMENT_COUNT);
        int groupCount = positiveProperty("elasticsearch.benchmark.groups", DEFAULT_GROUP_COUNT);
        int highCardinalityGroupCount = positiveProperty("elasticsearch.benchmark.high-cardinality-groups", DEFAULT_HIGH_CARDINALITY_GROUP_COUNT);
        int batchSize = positiveProperty("elasticsearch.benchmark.batch-size", DEFAULT_BATCH_SIZE);
        int payloadBytes = positiveProperty("elasticsearch.benchmark.payload-bytes", DEFAULT_PAYLOAD_BYTES);
        int warmups = nonNegativeProperty("elasticsearch.benchmark.warmups", DEFAULT_WARMUP_ITERATIONS);
        int iterations = positiveProperty("elasticsearch.benchmark.iterations", DEFAULT_MEASUREMENT_ITERATIONS);
        List<String> images = benchmarkImages();

        for (String image : images) {
            benchmarkImage(image, documentCount, groupCount, highCardinalityGroupCount, batchSize, payloadBytes, warmups, iterations);
        }
    }

    private static void benchmarkImage(
            String image,
            int documentCount,
            int groupCount,
            int highCardinalityGroupCount,
            int batchSize,
            int payloadBytes,
            int warmups,
            int iterations)
            throws Exception
    {
        LOG.info("ES_BENCHMARK_ENV image=%s java_version=%s processors=%s max_heap_bytes=%s " +
                        "documents=%s shards=%s alias_backing_indices=%s alias_shards=%s groups=%s " +
                        "high_cardinality_groups=%s payload_bytes=%s warmups=%s iterations=%s",
                image,
                System.getProperty("java.version"),
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().maxMemory(),
                documentCount,
                SHARD_COUNT,
                ALIAS_BACKING_INDICES.size() + 1,
                (ALIAS_BACKING_INDICES.size() + 1) * SHARD_COUNT,
                groupCount,
                Math.min(documentCount, highCardinalityGroupCount),
                payloadBytes,
                warmups,
                iterations);

        try (ElasticsearchServer server = new ElasticsearchServer(image);
                RestClient client = server.getClient()) {
            createIndex(client, INDEX);
            loadDocuments(client, INDEX, documentCount, groupCount, highCardinalityGroupCount, batchSize, payloadBytes, 0);
            for (int index = 0; index < ALIAS_BACKING_INDICES.size(); index++) {
                String backingIndex = ALIAS_BACKING_INDICES.get(index);
                createIndex(client, backingIndex);
                loadDocuments(
                        client,
                        backingIndex,
                        ALIAS_BACKING_DOCUMENT_COUNT,
                        groupCount,
                        highCardinalityGroupCount,
                        batchSize,
                        payloadBytes,
                        documentCount + (long) index * ALIAS_BACKING_DOCUMENT_COUNT);
            }
            createAlias(client);

            try (DistributedQueryRunner queryRunner = ElasticsearchQueryRunner.builder(server)
                    .addConnectorProperties(Map.of("elasticsearch.default-schema-name", SCHEMA))
                    .amendSession(session -> session.setSchema(SCHEMA))
                    .build()) {
                Session pushdownEnabled = Session.builder(queryRunner.getDefaultSession())
                        .setSystemProperty("allow_pushdown_into_connectors", "true")
                        .build();
                Session pushdownDisabled = Session.builder(queryRunner.getDefaultSession())
                        .setSystemProperty("allow_pushdown_into_connectors", "false")
                        .build();

                assertThat(queryRunner.execute(pushdownEnabled, "SELECT count(*) FROM " + INDEX).getOnlyValue())
                        .as("loaded Elasticsearch document count")
                        .isEqualTo((long) documentCount);
                assertThat(queryRunner.execute(pushdownEnabled, "SELECT count(*) FROM " + ALIAS).getOnlyValue())
                        .as("loaded Elasticsearch alias document count")
                        .isEqualTo((long) documentCount + (long) ALIAS_BACKING_INDICES.size() * ALIAS_BACKING_DOCUMENT_COUNT);

                for (BenchmarkQuery query : benchmarkQueries()) {
                    benchmarkQuery(queryRunner, image, query, pushdownEnabled, pushdownDisabled, warmups, iterations);
                }
                for (BenchmarkQuery query : aliasBenchmarkQueries()) {
                    benchmarkQuery(queryRunner, image, query, pushdownEnabled, pushdownDisabled, warmups, iterations);
                }
            }
        }
    }

    private static void createIndex(RestClient client, String indexName)
            throws IOException
    {
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(
                """
                {
                    "settings": {
                        "number_of_shards": %s,
                        "number_of_replicas": 0
                    },
                    "mappings": {
                        "properties": {
                            "id": {"type": "long"},
                            "group_key": {"type": "keyword"},
                            "high_cardinality_key": {"type": "keyword"},
                            "value": {"type": "double"},
                            "nullable_value": {"type": "double"},
                            "payload": {"type": "keyword"}
                        }
                    }
                }
                """.formatted(SHARD_COUNT));
        client.performRequest(request);
    }

    private static void loadDocuments(
            RestClient client,
            String indexName,
            int documentCount,
            int groupCount,
            int highCardinalityGroupCount,
            int batchSize,
            int payloadBytes,
            long idOffset)
            throws IOException
    {
        String payloadValue = "x".repeat(payloadBytes);
        int highCardinalityGroups = Math.min(documentCount, highCardinalityGroupCount);

        for (int batchStart = 0; batchStart < documentCount; batchStart += batchSize) {
            StringBuilder bulkBody = new StringBuilder(batchSize * (payloadBytes + 180));
            int batchEnd = Math.min(documentCount, batchStart + batchSize);
            for (int id = batchStart; id < batchEnd; id++) {
                long documentId = idOffset + id;
                bulkBody.append("{\"index\":{\"_id\":\"").append(documentId).append("\"}}\n")
                        .append("{\"id\":").append(documentId)
                        .append(",\"group_key\":\"g").append(id % groupCount).append("\"")
                        .append(",\"high_cardinality_key\":\"c").append(id % highCardinalityGroups).append("\"")
                        .append(",\"value\":").append(1 + documentId % 101).append(".25")
                        .append(",\"payload\":\"").append(payloadValue).append("\"");
                if (documentId % 10 != 0) {
                    bulkBody.append(",\"nullable_value\":").append(documentId % 97).append(".5");
                }
                bulkBody.append("}\n");
            }

            Request request = new Request("POST", "/" + indexName + "/_bulk");
            request.setEntity(new StringEntity(bulkBody.toString(), ContentType.create("application/x-ndjson")));
            JsonNode response = JSON_MAPPER.readTree(EntityUtils.toString(client.performRequest(request).getEntity()));
            assertThat(response.path("errors").asBoolean())
                    .as("Elasticsearch bulk response: %s", response)
                    .isFalse();
        }

        client.performRequest(new Request("POST", "/" + indexName + "/_refresh"));
    }

    private static void createAlias(RestClient client)
            throws IOException
    {
        Request request = new Request("POST", "/_aliases");
        request.setJsonEntity(
                """
                {
                    "actions": [
                        {"add": {"index": "%s", "alias": "%s"}},
                        {"add": {"index": "%s", "alias": "%s"}},
                        {"add": {"index": "%s", "alias": "%s"}}
                    ]
                }
                """.formatted(INDEX, ALIAS, ALIAS_BACKING_INDICES.get(0), ALIAS, ALIAS_BACKING_INDICES.get(1), ALIAS));
        client.performRequest(request);
    }

    private static void benchmarkQuery(
            DistributedQueryRunner queryRunner,
            String image,
            BenchmarkQuery query,
            Session pushdownEnabled,
            Session pushdownDisabled,
            int warmups,
            int iterations)
    {
        for (int iteration = 0; iteration < warmups; iteration++) {
            runPair(queryRunner, image, query, pushdownEnabled, pushdownDisabled, iteration, false);
        }

        List<Sample> pushdownSamples = new ArrayList<>();
        List<Sample> fallbackSamples = new ArrayList<>();
        for (int iteration = 0; iteration < iterations; iteration++) {
            Pair pair = runPair(queryRunner, image, query, pushdownEnabled, pushdownDisabled, iteration, true);
            pushdownSamples.add(pair.pushdown());
            fallbackSamples.add(pair.fallback());
        }

        Summary pushdownSummary = summarize(pushdownSamples);
        Summary fallbackSummary = summarize(fallbackSamples);
        assertThat(pushdownSummary.medianInputPositions())
                .as("median processed input positions for pushdown in %s on %s", query.name(), image)
                .isLessThan(fallbackSummary.medianInputPositions());

        LOG.info("ES_BENCHMARK_SUMMARY image=%s query=%s mode=pushdown iterations=%s " +
                        "median_wall_ms=%.3f p90_wall_ms=%.3f median_engine_elapsed_ms=%.3f " +
                        "p90_engine_elapsed_ms=%.3f median_cpu_ms=%.3f median_processed_input_positions=%s " +
                        "median_processed_input_bytes=%s output_rows=%s",
                image,
                query.name(),
                iterations,
                pushdownSummary.medianWallMillis(),
                pushdownSummary.p90WallMillis(),
                pushdownSummary.medianEngineElapsedMillis(),
                pushdownSummary.p90EngineElapsedMillis(),
                pushdownSummary.medianCpuMillis(),
                pushdownSummary.medianInputPositions(),
                pushdownSummary.medianProcessedInputBytes(),
                pushdownSummary.outputRows());
        LOG.info("ES_BENCHMARK_SUMMARY image=%s query=%s mode=fallback iterations=%s " +
                        "median_wall_ms=%.3f p90_wall_ms=%.3f median_engine_elapsed_ms=%.3f " +
                        "p90_engine_elapsed_ms=%.3f median_cpu_ms=%.3f median_processed_input_positions=%s " +
                        "median_processed_input_bytes=%s output_rows=%s",
                image,
                query.name(),
                iterations,
                fallbackSummary.medianWallMillis(),
                fallbackSummary.p90WallMillis(),
                fallbackSummary.medianEngineElapsedMillis(),
                fallbackSummary.p90EngineElapsedMillis(),
                fallbackSummary.medianCpuMillis(),
                fallbackSummary.medianInputPositions(),
                fallbackSummary.medianProcessedInputBytes(),
                fallbackSummary.outputRows());
        LOG.info("ES_BENCHMARK_RATIO image=%s query=%s wall_speedup=%.3f processed_input_position_reduction=%.3f",
                image,
                query.name(),
                fallbackSummary.medianWallMillis() / pushdownSummary.medianWallMillis(),
                (double) fallbackSummary.medianInputPositions() / pushdownSummary.medianInputPositions());
    }

    private static Pair runPair(
            DistributedQueryRunner queryRunner,
            String image,
            BenchmarkQuery query,
            Session pushdownEnabled,
            Session pushdownDisabled,
            int iteration,
            boolean reportSamples)
    {
        Sample pushdown;
        Sample fallback;
        MaterializedResult pushdownResult;
        MaterializedResult fallbackResult;
        boolean pushdownFirst = (iteration % 2) == 0;
        if (pushdownFirst) {
            Execution first = execute(queryRunner, pushdownEnabled, query.sql());
            Execution second = execute(queryRunner, pushdownDisabled, query.sql());
            pushdown = first.sample();
            pushdownResult = first.result();
            fallback = second.sample();
            fallbackResult = second.result();
        }
        else {
            Execution first = execute(queryRunner, pushdownDisabled, query.sql());
            Execution second = execute(queryRunner, pushdownEnabled, query.sql());
            fallback = first.sample();
            fallbackResult = first.result();
            pushdown = second.sample();
            pushdownResult = second.result();
        }

        assertThat(pushdownResult.getMaterializedRows())
                .as("pushdown and fallback results for %s on %s", query.name(), image)
                .isEqualTo(fallbackResult.getMaterializedRows());
        assertThat(pushdown.outputRows())
                .as("result row count for %s on %s", query.name(), image)
                .isEqualTo(fallback.outputRows());

        if (reportSamples) {
            LOG.info("ES_BENCHMARK_SAMPLE image=%s query=%s mode=pushdown iteration=%s " +
                            "wall_ms=%.3f engine_elapsed_ms=%.3f cpu_ms=%.3f " +
                            "processed_input_positions=%s processed_input_bytes=%s output_rows=%s",
                    image,
                    query.name(),
                    iteration,
                    pushdown.wallMillis(),
                    pushdown.engineElapsedMillis(),
                    pushdown.cpuMillis(),
                    pushdown.inputPositions(),
                    pushdown.processedInputBytes(),
                    pushdown.outputRows());
            LOG.info("ES_BENCHMARK_SAMPLE image=%s query=%s mode=fallback iteration=%s " +
                            "wall_ms=%.3f engine_elapsed_ms=%.3f cpu_ms=%.3f " +
                            "processed_input_positions=%s processed_input_bytes=%s output_rows=%s",
                    image,
                    query.name(),
                    iteration,
                    fallback.wallMillis(),
                    fallback.engineElapsedMillis(),
                    fallback.cpuMillis(),
                    fallback.inputPositions(),
                    fallback.processedInputBytes(),
                    fallback.outputRows());
        }
        return new Pair(pushdown, fallback);
    }

    private static Execution execute(DistributedQueryRunner queryRunner, Session session, String sql)
    {
        long startNanos = System.nanoTime();
        MaterializedResultWithPlan resultWithPlan = queryRunner.executeWithPlan(session, sql);
        double wallMillis = (System.nanoTime() - startNanos) / 1_000_000.0;
        QueryStats queryStats = queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(resultWithPlan.queryId())
                .getQueryStats();
        MaterializedResult result = resultWithPlan.result();
        return new Execution(
                result,
                new Sample(
                        wallMillis,
                        queryStats.getElapsedTime().roundTo(TimeUnit.NANOSECONDS) / 1_000_000.0,
                        queryStats.getTotalCpuTime().roundTo(TimeUnit.NANOSECONDS) / 1_000_000.0,
                        queryStats.getProcessedInputPositions(),
                        queryStats.getProcessedInputDataSize().toBytes(),
                        result.getRowCount()));
    }

    private static Summary summarize(List<Sample> samples)
    {
        List<Sample> sortedByWallTime = samples.stream()
                .sorted(Comparator.comparingDouble(Sample::wallMillis))
                .toList();
        List<Sample> sortedByEngineElapsed = samples.stream()
                .sorted(Comparator.comparingDouble(Sample::engineElapsedMillis))
                .toList();
        return new Summary(
                median(samples.stream().map(Sample::wallMillis).toList()),
                percentile90(sortedByWallTime.stream().map(Sample::wallMillis).toList()),
                median(samples.stream().map(Sample::engineElapsedMillis).toList()),
                percentile90(sortedByEngineElapsed.stream().map(Sample::engineElapsedMillis).toList()),
                median(samples.stream().map(Sample::cpuMillis).toList()),
                Math.round(median(samples.stream().map(Sample::inputPositions).map(Long::doubleValue).toList())),
                Math.round(median(samples.stream().map(Sample::processedInputBytes).map(Long::doubleValue).toList())),
                samples.getFirst().outputRows());
    }

    private static double median(List<Double> values)
    {
        List<Double> sorted = values.stream().sorted().toList();
        int middle = sorted.size() / 2;
        if (sorted.size() % 2 == 1) {
            return sorted.get(middle);
        }
        return (sorted.get(middle - 1) + sorted.get(middle)) / 2;
    }

    private static double percentile90(List<Double> sortedValues)
    {
        return sortedValues.get((int) Math.ceil(sortedValues.size() * 0.9) - 1);
    }

    private static List<BenchmarkQuery> benchmarkQueries()
    {
        return ImmutableList.of(
                new BenchmarkQuery("topn_10", "SELECT id, payload FROM " + INDEX + " ORDER BY id DESC LIMIT 10"),
                new BenchmarkQuery("topn_1000", "SELECT id, payload FROM " + INDEX + " ORDER BY id DESC LIMIT 1000"),
                new BenchmarkQuery("topn_1500", "SELECT id, payload FROM " + INDEX + " ORDER BY id DESC LIMIT 1500"),
                new BenchmarkQuery("global_metrics", "SELECT COUNT(*), COUNT(nullable_value), SUM(value), AVG(value), MIN(value), MAX(value) FROM " + INDEX),
                new BenchmarkQuery("group_low_cardinality", "SELECT group_key, COUNT(*), SUM(value) FROM " + INDEX + " GROUP BY group_key ORDER BY group_key"),
                new BenchmarkQuery(
                        "group_high_cardinality",
                        "SELECT high_cardinality_key, COUNT(*), SUM(value) FROM " + INDEX + " GROUP BY high_cardinality_key ORDER BY high_cardinality_key"));
    }

    private static List<BenchmarkQuery> aliasBenchmarkQueries()
    {
        return ImmutableList.of(
                new BenchmarkQuery("alias_topn_1500", "SELECT id, payload FROM " + ALIAS + " ORDER BY id DESC LIMIT 1500"),
                new BenchmarkQuery(
                        "alias_global_metrics",
                        "SELECT COUNT(*), COUNT(nullable_value), SUM(value), AVG(value), MIN(value), MAX(value) FROM " + ALIAS));
    }

    private static List<String> benchmarkImages()
    {
        String images = System.getProperty("elasticsearch.benchmark.images", ELASTICSEARCH_7_IMAGE + "," + ELASTICSEARCH_8_IMAGE);
        List<String> benchmarkImages = Arrays.stream(images.split(","))
                .map(String::trim)
                .filter(image -> !image.isEmpty())
                .toList();
        assertThat(benchmarkImages).isNotEmpty();
        return benchmarkImages;
    }

    private static int positiveProperty(String name, int defaultValue)
    {
        int value = Integer.getInteger(name, defaultValue);
        assertThat(value).as(name).isPositive();
        return value;
    }

    private static int nonNegativeProperty(String name, int defaultValue)
    {
        int value = Integer.getInteger(name, defaultValue);
        assertThat(value).as(name).isNotNegative();
        return value;
    }

    private record BenchmarkQuery(String name, String sql) {}

    private record Sample(
            double wallMillis,
            double engineElapsedMillis,
            double cpuMillis,
            long inputPositions,
            long processedInputBytes,
            int outputRows) {}

    private record Execution(MaterializedResult result, Sample sample) {}

    private record Pair(Sample pushdown, Sample fallback) {}

    private record Summary(
            double medianWallMillis,
            double p90WallMillis,
            double medianEngineElapsedMillis,
            double p90EngineElapsedMillis,
            double medianCpuMillis,
            long medianInputPositions,
            long medianProcessedInputBytes,
            int outputRows) {}
}
