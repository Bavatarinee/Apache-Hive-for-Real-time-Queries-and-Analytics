-- ============================================================
-- Script: 01_create_database.hql
-- Purpose: Create the Hive database for real-time analytics
-- Author:  Apache Hive Analytics Project
-- Created: 2024
-- ============================================================

-- Drop and recreate database (comment out DROP in production)
DROP DATABASE IF EXISTS hive_analytics CASCADE;
CREATE DATABASE hive_analytics
  COMMENT 'Apache Hive Real-time Queries and Analytics Database'
  WITH DBPROPERTIES (
    'creator'     = 'Analytics Team',
    'created_on'  = '2024-01-01',
    'version'     = '1.0',
    'environment' = 'production'
  );

USE hive_analytics;

-- Show database info
DESCRIBE DATABASE EXTENDED hive_analytics;
