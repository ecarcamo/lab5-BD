CREATE TABLE pais_poblacion (
   id SERIAL PRIMARY KEY,
   mongo_id VARCHAR(50),
   continente VARCHAR(50),
   pais VARCHAR(100),
   poblacion BIGINT,
   costo_bajo_hospedaje NUMERIC(10,2),
   costo_promedio_comida NUMERIC(10,2),
   costo_bajo_transporte NUMERIC(10,2),
   costo_promedio_entretenimiento NUMERIC(10,2)
);

CREATE TABLE pais_envejecimiento (
   id_pais INT PRIMARY KEY,
   nombre_pais VARCHAR(100),
   capital VARCHAR(100),
   continente VARCHAR(50),
   region VARCHAR(100),
   poblacion NUMERIC,
   tasa_de_envejecimiento NUMERIC(5,2)
);


-- Esto no lo tienen que pegar, es solo para verificar que funcionen.
select count(*) from pais_poblacion;
select count(*) from pais_envejecimiento;

SELECT p.pais, e.nombre_pais
FROM pais_poblacion p
JOIN pais_envejecimiento e
ON p.pais = e.nombre_pais
LIMIT 10;

DROP TABLE dw_paises;
SELECT COUNT(*) FROM dw_paises;

select current_database()