-- Active: 1736532502233@@127.0.0.1@3306@senamhi
-- Crea BD (cámbiale el nombre si quieres)
CREATE DATABASE IF NOT EXISTS senamhi
  CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE senamhi;

-- (Opcional) Crea usuario local
-- CREATE USER 'senamhi'@'localhost' IDENTIFIED BY 'tu_password_segura';
-- GRANT ALL PRIVILEGES ON senamhi.* TO 'senamhi'@'localhost';
-- FLUSH PRIVILEGES;

-- Tabla de estaciones (evita duplicar nombres)
CREATE TABLE IF NOT EXISTS stations (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(150) NOT NULL,
  UNIQUE KEY uq_station_name (name)
) ENGINE=InnoDB;

-- Tabla de mediciones
-- Guardamos el timestamp combinado (fecha+hora) y los contaminantes
CREATE TABLE IF NOT EXISTS measurements (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  station_id INT NOT NULL,
  ts DATETIME NOT NULL,
  pm2_5 DOUBLE NULL,
  pm10  DOUBLE NULL,
  so2   DOUBLE NULL,
  no2   DOUBLE NULL,
  o3    DOUBLE NULL,
  co    DOUBLE NULL,
  -- Evita duplicar por estación y momento
  UNIQUE KEY uq_station_ts (station_id, ts),
  -- Índices útiles para consultas/alertas
  KEY idx_ts (ts),
  CONSTRAINT fk_measurements_station
    FOREIGN KEY (station_id) REFERENCES stations(id)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;
