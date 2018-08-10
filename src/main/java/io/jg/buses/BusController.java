package io.jg.buses;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.*;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class BusController {

    private BusRepository busRepository;
    private BigQuery bigQuery;
    private Publisher publisher;
    private String appToken;

    public BusController(BusRepository busRepository, BigQuery bigQuery, Publisher publisher, String appToken) {
        this.busRepository = busRepository;
        this.bigQuery = bigQuery;
        this.publisher = publisher;
        this.appToken = appToken;
    }

    void batchLoad() {
        List<Map<String, Object>> buses = busRepository.getBuses(appToken);

        TableId tableId = TableId.of("buses", "buses");
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);

        for (Map<String, Object> segment : buses) {
            String rowId = segment.get("segmentid").toString() + ":" + segment.get("_last_updt");
            builder.addRow(rowId, segment);
        }

        InsertAllResponse response = bigQuery.insertAll(builder.build());
        if (response.hasErrors()) {
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                log.error(entry.toString());
            }
        }
    }

    void publishEvents() {
        List<Map<String, Object>> buses = busRepository.getBuses(appToken);
        List<ApiFuture<String>> futures = new ArrayList<>();
        ObjectMapper om = new ObjectMapper();

        String json = null;
        try {
            for (Map<String, Object> event : buses) {
                json = om.writeValueAsString(event);
                log.info("publishing event for segment: " + event.get("segmentid"));
                futures.add(publisher.publish(PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(json)).build()));
            }
        } catch (JsonProcessingException e) {
            log.error("invalid json: " + json, e);
        } finally {
            try {
                List<String> messageIds = ApiFutures.allAsList(futures).get();
                for (String messageId : messageIds) {
                    log.info("published event with id: " + messageId);
                }
            } catch (Exception e) {
                log.error("error retrieving message ids.", e);
            }
        }
    }

    long segmentCount() {
        String query = "SELECT count(*) from buses.buses";
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

        long count = 0l;
        Iterable<FieldValueList> fields = null;
        try {
            fields = bigQuery.query(queryConfig).iterateAll();
        } catch (InterruptedException e) {
            log.error("error processing count query.", e);
            return count;
        }

        for (FieldValueList row : fields) {
            for (FieldValue val : row) {
                count = val.getLongValue();
            }
        }
        return count;
    }
}
