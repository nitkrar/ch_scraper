-- Insert the current staged HSCA snapshot into the durable tables.
--
-- Expected staging inputs:
--   cqc_hsca_locations_staging
--   cqc_hsca_dual_registrations_staging
--
-- This file is intentionally NOT executed by bootstrap.py because it
-- requires those staging tables to exist. WP2's ODS processor owns the
-- staging load and then calls this file after clearing the old snapshot
-- in Python. The caller is also responsible for recreating
-- cqc_hsca_dual_registrations before this script runs, because DuckDB's
-- FK enforcement is more reliable when that child table is dropped and
-- recreated around the parent-table snapshot refresh.

INSERT INTO cqc_hsca_locations BY NAME
SELECT * FROM cqc_hsca_locations_staging;

INSERT INTO cqc_hsca_dual_registrations BY NAME
SELECT * FROM cqc_hsca_dual_registrations_staging;
