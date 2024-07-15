-- GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' IDENTIFIED BY 'my_secret_password';

-- Crear la base de datos y usarla
CREATE DATABASE IF NOT EXISTS my_sqldatabase CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE my_sqldatabase;

-- Crear la tabla de dimensiones de continente
CREATE TABLE dim_continent (
    id INT AUTO_INCREMENT PRIMARY KEY,
    continent VARCHAR(255) UNIQUE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Crear la tabla de dimensiones de ubicación
CREATE TABLE dim_location (
    id INT AUTO_INCREMENT PRIMARY KEY,
    continent_id INT,
    country VARCHAR(255),
    population INT, 
    FOREIGN KEY (continent_id) REFERENCES dim_continent(id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Crear la tabla de dimensiones de mes
CREATE TABLE dim_month (
    id INT PRIMARY KEY,
    month VARCHAR(255)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Crear la tabla de dimensiones de año
CREATE TABLE dim_year (
    id INT AUTO_INCREMENT PRIMARY KEY,
    year INT
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Crear la tabla de dimensiones de día
CREATE TABLE dim_day (
    id INT  PRIMARY KEY
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Crear la tabla de dimensiones de fecha
CREATE TABLE dim_date (
    id INT AUTO_INCREMENT PRIMARY KEY,
    month_id INT,
    year_id INT,
    day_id INT,
    FOREIGN KEY (month_id) REFERENCES dim_month(id),
    FOREIGN KEY (year_id) REFERENCES dim_year(id),
    FOREIGN KEY (day_id) REFERENCES dim_day(id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Crear la tabla de hechos
CREATE TABLE hechos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    location_id INT,
    date_id INT,
    total_deaths INT,
    total_vaccinations BIGINT,
    total_covid_cases INT,
    new_cases INT,
    FOREIGN KEY (location_id) REFERENCES dim_location(id),
    FOREIGN KEY (date_id) REFERENCES dim_date(id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
