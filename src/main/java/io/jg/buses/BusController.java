package io.jg.buses;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.*;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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

            //fix the timezones on the buses
            bus.put("_last_updt", fixTimezone(bus.get("_last_updt").toString()));

            busMap.put(bus.get("segmentid").toString(), bus);
        }

        Set<Object[]> latest = getLatest();
        Set<String> dupeKeys = new HashSet<>();
        for (Object[] keys : latest) {
            Map<String, Object> bus = (Map<String, Object>) busMap.get(keys[0]);

            if (bus == null) {
                continue;
            }

            if (sameDate((Timestamp) keys[1], bus.get("_last_updt").toString())) {
                dupeKeys.add(keys[0].toString());
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

    private boolean sameDate(Timestamp timestamp, String s) {
        Date gd = timestamp.toDate();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            return sdf.parse(s).compareTo(gd) == 0;
        } catch (ParseException e) {
            log.error("invalid date: " + s);
            return false;
        }
    }

    @GetMapping("/")
    public ResponseEntity<List<Map<String, Object>>> latest() {
        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind("segment")
                .build();

        QueryResults<Entity> res = datastore.run(query);
        List<Map<String, Object>> buses = new ArrayList<>();
        while (res.hasNext()) {
            Entity bus = res.next();
            Map<String, Object> m = new HashMap<>();

            m.put("segmentid", bus.getKey().getName());
            m.put("_direction", bus.getString("_direction"));
            m.put("fromst", bus.getString("_fromst"));
            m.put("_last_updt", bus.getTimestamp("_last_updt"));
            m.put("_length:", bus.getValue("_length"));
            m.put("_lif", bus.getLatLng("_lif"));
            m.put("_lit", bus.getLatLng("_lit"));
            m.put("_strheading", bus.getString("_strheading"));
            m.put("_tost", bus.getString("_tost"));
            m.put("_traffic", bus.getLong("_traffic"));
            m.put("street", bus.getString("street"));
            buses.add(m);
        }

        return new ResponseEntity<>(buses, HttpStatus.OK);
    }

    Set<Object[]> getLatest() {
        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind("segment")
                .build();

        QueryResults<Entity> res = datastore.run(query);
        Set<Object[]> buses = new HashSet<>();

        String segmentid = null;
        Timestamp update = null;
        while (res.hasNext()) {
            Entity bus = res.next();
            try {
                segmentid = bus.getString("segmentid").trim();
                update = bus.getTimestamp("_last_updt");
                buses.add(new Object[]{segmentid, update});
            } catch (DatastoreException e) {
                log.info("bus is missing segmentid and/or _last_updt.", bus.toString());
            }
        }
        return buses;
    }

    String fixTimezone(String date) {
        Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("America/Chicago"));
        try {
            Date d = sdf.parse(date);
            utcCal.setTime(d);
            return Timestamp.of(utcCal.getTime()).toString();
        } catch (ParseException e) {
            log.error("invalid date: " + date, e);
            return Timestamp.now().toString();
        }
    }
}
