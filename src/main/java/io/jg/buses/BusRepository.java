package io.jg.buses;

import feign.RequestLine;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Set;

@Repository
public interface BusRepository {

    @RequestLine("GET /")
    public Set<Map<String, Object>> getBuses();
}
