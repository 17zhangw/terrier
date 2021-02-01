CREATE TABLE noisepage_forecast_clusters(cluster_id INT, query_id INT, db_id INT, ts TIMESTAMP);
CREATE TABLE noisepage_forecast_intervals(query_id INT, interval INT, arrival_rate REAL, ts TIMESTAMP);
CREATE TABLE noisepage_forecast_parameters(query_id INT, parameters VARCHAR, ts TIMESTAMP);
