package com.example.simbajdbc;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@SpringBootApplication
@RequiredArgsConstructor
public class SimbaJdbcApplication implements CommandLineRunner {

    private final JdbcTemplate jdbcTemplate;

    public static void main(String[] args) {
        SpringApplication.run(SimbaJdbcApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        String SQL = "SELECT * FROM sampledb.elb_logs limit 10";
        List<ElbLog> elbLogs = jdbcTemplate.query(SQL, new ElbLogMapper());

        System.out.println("test");
    }

    public class ElbLogMapper implements RowMapper<ElbLog> {
        public ElbLog mapRow(ResultSet rs, int rowNum) throws SQLException {
            ElbLog elbLog = new ElbLog();

            elbLog.setUrl(rs.getString("url"));
            elbLog.setElbName(rs.getString("elb_name"));
            elbLog.setRequestPort(rs.getInt("request_port"));

            //TODO...

            return elbLog;
        }
    }
}
