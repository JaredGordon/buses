package io.jg.buses;

import feign.RequestLine;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface BusRepository {

    @RequestLine("GET /")
    public List<Map<String, Object>> getBuses();
}
