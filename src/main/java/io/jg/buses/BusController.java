package io.jg.buses;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.datastore.*;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
@Slf4j
public class BusController {

    private BusRepository busRepository;
    private Publisher publisher;
    private String appToken;
    private Datastore datastore;

    public BusController(BusRepository busRepository, Publisher publisher, Datastore datastore, String appToken) {
        this.busRepository = busRepository;
        this.publisher = publisher;
        this.datastore = datastore;
        this.appToken = appToken;
    }

    void publishEvents() {
        List<Map<String, Object>> buses = busRepository.getBuses(appToken);

        Map<String, Object> busMap = new HashMap<>();
        for (Map<String, Object> bus : buses) {
            busMap.put(bus.get("segmentid").toString(), bus);
        }

        Set<String[]> latest = getLatest();
        Set<String> dupeKeys = new HashSet<>();
        for (String[] keys : latest) {
            Map<String, Object> bus = (Map<String, Object>) busMap.get(keys[0]);
            if (bus != null && keys[1].equals(bus.get("_last_updt"))) {
                dupeKeys.add(keys[0]);
            }
        }

        for (String dupeKey : dupeKeys) {
            busMap.remove(dupeKey);
        }

        log.info("publishing: " + busMap.size() + " events.");

        List<ApiFuture<String>> futures = new ArrayList<>();
        ObjectMapper om = new ObjectMapper();

        String json = null;
        Collection<Object> deduped = busMap.values();
        try {
            for (Object bus : deduped) {
                log.info("publishing event for segment: " + bus);
                json = om.writeValueAsString(bus);
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

    Set<String[]> getLatest() {
        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind("segment")
                .build();

        QueryResults<Entity> res = datastore.run(query);
        Set<String[]> buses = new HashSet<>();

        String segmentid = null;
        String update = null;
        while (res.hasNext()) {
            Entity bus = res.next();
            try {
                segmentid = bus.getString("segmentid");
                update = bus.getString("_last_updt");
                buses.add(new String[]{segmentid, update});
            } catch (DatastoreException e) {
                log.info("bus is missing segmentid and/or _last_updt.", bus.toString());
            }
        }
        return buses;
    }
}
