-- Active: 1736532502233@@127.0.0.1@3306@senamhi
USE senamhi;

CREATE TABLE IF NOT EXISTS alert_rules (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(120) NOT NULL,
  station_id INT NULL,                 /* NULL => aplica a todas */
  pollutant VARCHAR(10) NOT NULL,      /* pm25|pm10|so2|no2|o3|co */
  operator VARCHAR(2) NOT NULL,        /* gt|ge|lt|le */
  threshold DOUBLE NOT NULL,
  time_window VARCHAR(10) NULL,        /* ej. '1h' (MVP: opcional) */
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_rules_station FOREIGN KEY (station_id)
    REFERENCES stations(id) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS alert_events (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  rule_id INT NOT NULL,
  station_id INT NOT NULL,
  ts DATETIME NOT NULL,
  pollutant VARCHAR(10) NOT NULL,
  value DOUBLE NULL,
  operator VARCHAR(2) NOT NULL,
  threshold DOUBLE NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_rule_station_ts (rule_id, station_id, ts),
  KEY idx_station_ts (station_id, ts),
  CONSTRAINT fk_events_rule FOREIGN KEY (rule_id)
    REFERENCES alert_rules(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_events_station FOREIGN KEY (station_id)
    REFERENCES stations(id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;
