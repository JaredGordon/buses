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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
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
    private String mapToken;
    private Datastore datastore;

    public BusController(BusRepository busRepository, Publisher publisher, Datastore datastore, String appToken, String mapToken) {
        this.busRepository = busRepository;
        this.publisher = publisher;
        this.datastore = datastore;
        this.appToken = appToken;
        this.mapToken = mapToken;
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

        Set<String[]> latest = getLatest();
        Set<String> dupeKeys = new HashSet<>();
        for (String[] keys : latest) {
            Map<String, Object> bus = (Map<String, Object>) busMap.get(keys[0]);

            if (bus == null) {
                continue;
            }

            if (sameDate(keys[1], bus.get("_last_updt").toString())) {
                dupeKeys.add(keys[0]);
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

    boolean sameDate(String s1, String s2) {
        if (s1 == null && s2 == null) {
            return true;
        }

        if (s1 == null || s2 == null) {
            return false;
        }

        Timestamp timestamp1 = Timestamp.parseTimestamp(s1);
        Timestamp timestamp2 = Timestamp.parseTimestamp(s2);

        return timestamp1.compareTo(timestamp2) == 0;
    }

    private QueryResults<Entity> query(String kind, String orderBy, int rows) {
        StructuredQuery.OrderBy ob = StructuredQuery.OrderBy.desc(orderBy);
        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind(kind).setLimit(rows).addOrderBy(ob)
                .build();

        return datastore.run(query);
    }

    @GetMapping("/buses")
    @ResponseBody
    public String busLocations() {
        QueryResults<Entity> res = query("segment", "_last_updt", 250);
        List<Map<String, Object>> buses = busesFromResults(res);

        StringBuilder sb = new StringBuilder();
        sb.append(header());

        for (Map<String, Object> bus : buses) {
            sb.append("%7C");
            sb.append(bus.get("_lit").toString().replaceAll("\\s+", ""));
        }

        sb.append(footer());

        return sb.toString();
    }

    private String header() {
        return "<html><head></head>" +
                "<body style=\"margin: 0px; background: #0e0e0e;\">" +
                "<style> .marginauto { margin: 10px auto 20px; display: block; } </style>" +
                "<img class=\"marginauto\" style=\"-webkit-user-select: none;\"" +
                " src=\"https://maps.googleapis.com/maps/api/staticmap?maptype=hybrid&size=600x800&scale=4&markers=size:tiny%7Ccolor:0xFFFF00";
    }

    private String footer() {
        return "&key=" +
                mapToken +
                "\" width=\"600\" height=\"641\"></body></html>";
    }

    @GetMapping("/traffic")
    public String traffic() {
        QueryResults<Entity> res = query("traffic", "_last_updt", 100);
        List<Map<String, Object>> buses = busesFromResults(res);

        StringBuilder sb = new StringBuilder();
        sb.append(header());

        for (Map<String, Object> bus : buses) {
            int traffic = Integer.parseInt(bus.get("_traffic").toString());

            String color;
            if (traffic < 10) {
                color = "color:red";
            } else if (traffic < 15) {
                color = "color:orange";
            } else if (traffic < 20) {
                color = "color:yellow";
            } else {
                color = "color:green";
            }
            sb.append("&markers=size:tiny");
            sb.append("%7C");
            sb.append(color);
            sb.append("%7C");
            sb.append(bus.get("_lit").toString().replaceAll("\\s+", ""));
        }

        sb.append(footer());

        return sb.toString();
    }

    @GetMapping("/")
    public List<Map<String, Object>> latest() {
        return busesFromResults(query("segment", "_last_updt", 1000));
    }

    private List<Map<String, Object>> busesFromResults(QueryResults<Entity> queryResults) {
        List<Map<String, Object>> buses = new ArrayList<>();
        while (queryResults.hasNext()) {
            Entity bus = queryResults.next();
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
        return buses;
    }

    Set<String[]> getLatest() {
        Query<Entity> query = Query.newEntityQueryBuilder()
                .setKind("segment")
                .build();

        QueryResults<Entity> res = datastore.run(query);
        Set<String[]> buses = new HashSet<>();

        String segmentid;
        String update;
        while (res.hasNext()) {
            Entity bus = res.next();
            try {
                segmentid = bus.getString("segmentid").trim();
                update = bus.getTimestamp("_last_updt").toString();
                buses.add(new String[]{segmentid, update});
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
