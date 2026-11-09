-- AtlasFlow read-only query pack
-- Run after the Gold shipment_control_tower table has been created and populated.
-- Execute from the repository root with: python scripts/run-trino-query-pack.py

-- 1. Overall control-tower KPIs
SELECT
    COUNT(DISTINCT shipment_id) AS total_shipments,
    COUNT(DISTINCT CASE WHEN risk_level = 'HIGH' THEN shipment_id END) AS high_risk_shipments,
    COUNT(DISTINCT CASE WHEN inventory_shortage_flag THEN shipment_id END) AS inventory_shortage_shipments,
    SUM(COALESCE(active_disruption_count, 0)) AS disruption_links
FROM atlasflow.gold.shipment_control_tower;

-- 2. Shipment distribution by risk level
SELECT risk_level, COUNT(DISTINCT shipment_id) AS shipment_count
FROM atlasflow.gold.shipment_control_tower
GROUP BY risk_level
ORDER BY shipment_count DESC;

-- 3. Prioritized shipment attention queue
SELECT
    risk_level, shipment_id, order_id, customer_name, carrier_name,
    shipment_status, warehouse_region, destination_country,
    tracking_delay_minutes, inventory_shortage_flag,
    active_disruption_count, expected_disruption_delay_minutes,
    primary_risk_reason
FROM atlasflow.gold.shipment_control_tower
WHERE risk_level IN ('HIGH', 'MEDIUM')
ORDER BY
    CASE risk_level WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
    COALESCE(tracking_delay_minutes, 0) DESC, shipment_id;

-- 4. High-risk shipment concentration by carrier
SELECT carrier_name, COUNT(DISTINCT shipment_id) AS high_risk_shipments
FROM atlasflow.gold.shipment_control_tower
WHERE risk_level = 'HIGH'
GROUP BY carrier_name
ORDER BY high_risk_shipments DESC, carrier_name;

-- 5. Inventory shortage exposure by warehouse region
SELECT warehouse_region, COUNT(DISTINCT shipment_id) AS affected_shipments
FROM atlasflow.gold.shipment_control_tower
WHERE inventory_shortage_flag = TRUE
GROUP BY warehouse_region
ORDER BY affected_shipments DESC, warehouse_region;

-- 6. Tracking delay profile by carrier
SELECT
    carrier_name,
    COUNT(DISTINCT shipment_id) AS shipments_with_tracking_delay,
    MAX(tracking_delay_minutes) AS maximum_delay_minutes,
    AVG(CAST(tracking_delay_minutes AS DOUBLE)) AS average_delay_minutes
FROM atlasflow.gold.shipment_control_tower
WHERE COALESCE(tracking_delay_minutes, 0) > 0
GROUP BY carrier_name
ORDER BY maximum_delay_minutes DESC, carrier_name;
