from datetime import datetime, timezone
from pyspark.sql import Row
from iot_stream_analytics.processor import compute_telemetry_metrics, extract_out_of_bounds_anomalies
from iot_stream_analytics.router import classify_and_route_events


def test_compute_telemetry_metrics_calculates_lag(spark_session):
    """Verifies that network lag and ingestion metrics are computed correctly."""
    # Enforcing timezone awareness guarantees consistent Unix timestamp conversions
    event_time = datetime(2026, 10, 26, 12, 0, 0, tzinfo=timezone.utc)
    kafka_time = datetime(2026, 10, 26, 12, 0, 5, tzinfo=timezone.utc)
    ingest_time = datetime(2026, 10, 26, 12, 0, 8, tzinfo=timezone.utc)

    mock_row = Row(
        sample=Row(device_id="D1", sensor_id="S1", event_timestamp=event_time, temperature=20.0, power=100.0),
        kafka_timestamp=kafka_time,
        ingestion_timestamp=ingest_time
    )
    input_df = spark_session.createDataFrame([mock_row])

    # Test the pure core transformation directly
    result_df = compute_telemetry_metrics(input_df)
    result_row = result_df.collect()[0]

    assert result_row["network_lag_sec"] == 5.0
    assert result_row["ingestion_lag_sec"] == 3.0


def test_extract_out_of_bounds_anomalies_isolates_spikes(spark_session):
    """Verifies that safety threshold breaches are accurately filtered as alerts."""
    mock_records = [
        Row(device_id="DEV-CLEAN", temperature=23.0, power=400.0),
        Row(device_id="DEV-HOT", temperature=92.5, power=500.0),  # Temp > 85.0
        Row(device_id="DEV-SURGE", temperature=23.0, power=1600.0)  # Power > 1500.0
    ]
    input_df = spark_session.createDataFrame(mock_records)

    # Test the pure core filter directly
    alerts_df = extract_out_of_bounds_anomalies(input_df)
    alert_ids = [row["device_id"] for row in alerts_df.collect()]

    assert len(alert_ids) == 2
    assert "DEV-HOT" in alert_ids
    assert "DEV-SURGE" in alert_ids
    assert "DEV-CLEAN" not in alert_ids


def test_classify_and_route_events_identifies_corrupted_data(spark_session):
    """Verifies that structural anomalies are successfully routed to the Dead Letter Queue (DLQ)."""
    mock_records = [
        Row(kafka_key="CLEAN", kafka_value="{}", sample=Row(temperature=20.0, power=100.0)),
        Row(kafka_key="CORRUPT", kafka_value="{}", sample=None),  # Severe JSON parsing malformation
        Row(kafka_key="NEGATIVE", kafka_value="{}", sample=Row(temperature=20.0, power=-10.0))  # Impossible metric
    ]
    input_df = spark_session.createDataFrame(mock_records)

    # Test the pure core router directly
    routed_df = classify_and_route_events(input_df, dlq_topic="dlq.topic", refined_topic="refined.topic")
    routed_rows = {row["key"]: row["topic"] for row in routed_df.collect()}

    assert routed_rows["CLEAN"] == "refined.topic"
    assert routed_rows["CORRUPT"] == "dlq.topic"
    assert routed_rows["NEGATIVE"] == "dlq.topic"
