package io.jg.buses;

import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class BusController {

    private BusRepository busRepository;
    private BigQuery bigQuery;
    private String appToken;

    public BusController(BusRepository busRepository, BigQuery bigQuery, String appToken) {
        this.busRepository = busRepository;
        this.bigQuery = bigQuery;
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
