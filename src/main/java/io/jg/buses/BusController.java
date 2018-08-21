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
import org.threeten.bp.format.DateTimeParseException;

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

    List<Map<String, Object>> getBusData() {
        return busRepository.getBuses(appToken);
    }

    void publishEvents() {
        Map<String, Object> busMap = dedupe(getBusData());
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

    Map<String, Object> dedupe(List<Map<String, Object>> buses) {
        Map<String, Object> busMap = new HashMap<>();
        for (Map<String, Object> bus : buses) {

            //fix the timezones on the buses
            bus.put("_last_updt", fixTimezone(bus.get("_last_updt").toString()));

            //is the event within the past 24 hours? If not, ignore it.
            if (isRecent(bus.get("_last_updt").toString())) {
                busMap.put(bus.get("segmentid").toString(), bus);
            }
        }

        Set<Object[]> latest = getLatest();
        Set<String> dupeKeys = new HashSet<>();
        for (Object[] keys : latest) {
            Map<String, Object> bus = (Map<String, Object>) busMap.get(keys[0]);

            if (bus == null) {
                continue;
            }

            if (sameDate((Timestamp) keys[1], bus.get("_last_updt"))) {
                dupeKeys.add(keys[0].toString());
            }
        }

        for (String dupeKey : dupeKeys) {
            busMap.remove(dupeKey);
        }

        return busMap;
    }

    boolean isRecent(String s) {
        if (s == null) {
            return false;
        }

        Timestamp timestamp;
        try {
            timestamp = Timestamp.parseTimestamp(s);
        } catch (DateTimeParseException e) {
            log.error("invalid date format: " + s);
            return false;
        }

        long now = Timestamp.now().getSeconds();
        long difference = now - timestamp.getSeconds();
        return difference < 60 * 60 * 24;
    }

    private boolean sameDate(Timestamp timestamp1, Object o) {
        if (timestamp1 == null && o == null) {
            return true;
        }

        if (timestamp1 == null || o == null) {
            return false;
        }

        Timestamp timestamp2 = Timestamp.parseTimestamp(o.toString());

        return timestamp1.compareTo(timestamp2) == 0;
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

        String segmentid;
        Timestamp update;
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
