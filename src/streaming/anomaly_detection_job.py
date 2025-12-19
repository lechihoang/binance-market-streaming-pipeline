"""Anomaly Detection Job."""

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    current_timestamp,
    expr,
    from_json,
    lit,
    struct,
    to_json,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming.base_spark_job import BaseSparkJob

# Import metrics utilities for production monitoring
from src.utils.metrics import (
    record_error,
    record_message_processed,
    track_latency,
)

# Import KafkaConnector from shared module
from src.utils.kafka import KafkaConnector


class AnomalyDetectionJob(BaseSparkJob):
    WHALE_THRESHOLD = 100000.0
    VOLUME_SPIKE_THRESHOLD = 1000000.0
    PRICE_SPIKE_THRESHOLD = 2.0

    def __init__(self):
        super().__init__(job_name="AnomalyDetectionJob")
        
        # Override default graceful shutdown timeout to 90s
        # to allow checkpoint completion before forced termination
        self.graceful_shutdown.graceful_shutdown_timeout = 90

    @staticmethod
    def _get_trade_schema() -> StructType:
        original_data_schema = StructType([
            StructField("E", LongType(), False),
            StructField("s", StringType(), False),
            StructField("p", StringType(), False),
            StructField("q", StringType(), False),
            StructField("m", BooleanType(), False),
            StructField("t", LongType(), True)
        ])
        return StructType([
            StructField("original_data", original_data_schema, False),
            StructField("symbol", StringType(), False),
            StructField("ingestion_timestamp", LongType(), False)
        ])

    @staticmethod
    def _get_aggregation_schema() -> StructType:
        return StructType([
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("window_duration", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("open", DoubleType(), False),
            StructField("high", DoubleType(), False),
            StructField("low", DoubleType(), False),
            StructField("close", DoubleType(), False),
            StructField("volume", DoubleType(), False),
            StructField("quote_volume", DoubleType(), False),
            StructField("trade_count", LongType(), False),
            StructField("vwap", DoubleType(), True),
            StructField("price_change_pct", DoubleType(), True),
            StructField("buy_sell_ratio", DoubleType(), True),
            StructField("large_order_count", LongType(), True),
            StructField("price_stddev", DoubleType(), True)
        ])

    def detect_whale_alerts(self, df: DataFrame) -> DataFrame:
        self.logger.info(f"Detecting whale alerts with threshold: ${self.WHALE_THRESHOLD:,.2f}")
        trade_schema = self._get_trade_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), trade_schema).alias("trade"))
        trades_df = parsed_df.select(
            (col("trade.original_data.E") / 1000).cast(TimestampType()).alias("timestamp"),
            col("trade.symbol").alias("symbol"),
            col("trade.original_data.p").cast(DoubleType()).alias("price"),
            col("trade.original_data.q").cast(DoubleType()).alias("quantity"),
            when(col("trade.original_data.m") == True, lit("SELL")).otherwise(lit("BUY")).alias("side"),
        ).withColumn("value", col("price") * col("quantity"))
        trades_df = trades_df.withWatermark("timestamp", "1 minute")
        whale_trades = trades_df.filter(col("value") > self.WHALE_THRESHOLD)
        alerts_df = whale_trades.select(
            col("timestamp"), col("symbol"),
            lit("WHALE_ALERT").alias("alert_type"), lit("HIGH").alias("alert_level"),
            to_json(struct(col("price"), col("quantity"), col("value"), col("side"))).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("Whale alert detection configured")
        return alerts_df

    def detect_volume_spikes(self, df: DataFrame) -> DataFrame:
        self.logger.info(f"Detecting volume spikes with quote_volume threshold: ${self.VOLUME_SPIKE_THRESHOLD:,.0f}")
        agg_schema = self._get_aggregation_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), agg_schema).alias("agg"))
        aggs_df = parsed_df.select(
            col("agg.window_start").alias("window_start"), col("agg.window_end").alias("window_end"),
            col("agg.window_duration").alias("window_duration"), col("agg.symbol").alias("symbol"),
            col("agg.volume").alias("volume"), col("agg.quote_volume").alias("quote_volume"),
            col("agg.trade_count").alias("trade_count")
        ).filter(col("window_duration") == "1m")
        aggs_df = aggs_df.withWatermark("window_start", "1 minute")
        volume_spikes = aggs_df.filter(col("quote_volume") > self.VOLUME_SPIKE_THRESHOLD)
        alerts_df = volume_spikes.select(
            col("window_start").alias("timestamp"), col("symbol"),
            lit("VOLUME_SPIKE").alias("alert_type"), lit("MEDIUM").alias("alert_level"),
            to_json(struct(col("volume"), col("quote_volume"), col("trade_count"))).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("Volume spike detection configured")
        return alerts_df

    def detect_price_spikes(self, df: DataFrame) -> DataFrame:
        self.logger.info(f"Detecting price spikes with threshold: {self.PRICE_SPIKE_THRESHOLD}%")
        agg_schema = self._get_aggregation_schema()
        parsed_df = df.select(from_json(col("value").cast("string"), agg_schema).alias("agg"))
        aggs_df = parsed_df.select(
            col("agg.window_start").alias("window_start"), col("agg.window_end").alias("window_end"),
            col("agg.window_duration").alias("window_duration"), col("agg.symbol").alias("symbol"),
            col("agg.open").alias("open"), col("agg.close").alias("close"),
            col("agg.price_change_pct").alias("price_change_pct")
        ).filter(col("window_duration") == "1m")
        aggs_df = aggs_df.withWatermark("window_start", "1 minute")
        price_spikes = aggs_df.filter(spark_abs(col("price_change_pct")) > self.PRICE_SPIKE_THRESHOLD)
        alerts_df = price_spikes.select(
            col("window_start").alias("timestamp"), col("symbol"),
            lit("PRICE_SPIKE").alias("alert_type"), lit("HIGH").alias("alert_level"),
            to_json(struct(col("open"), col("close"), col("price_change_pct"))).alias("details"),
            expr("uuid()").alias("alert_id"), current_timestamp().alias("created_at")
        )
        self.logger.info("Price spike detection configured")
        return alerts_df

    def _create_alert(self, alert_id: str, timestamp: datetime, symbol: str, alert_type: str, alert_level: str, details: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "alert_id": alert_id,
            "timestamp": timestamp,
            "symbol": symbol, 
            "alert_type": alert_type, 
            "alert_level": alert_level,
            "details": details, 
            "created_at": datetime.now(timezone.utc)
        }

    def _write_alerts_to_sinks(self, alerts: List[Dict[str, Any]], batch_id: int) -> None:
        """Write alerts to multiple sinks using StorageWriter batch method."""
        if not alerts:
            self.logger.debug(f"Batch {batch_id}: No alerts to write")
            return
        self.logger.info(f"Batch {batch_id}: Writing {len(alerts)} alerts to sinks")

        # Write to Kafka (for downstream consumers)
        try:
            kafka_conn = KafkaConnector(bootstrap_servers=self.kafka_bootstrap_servers, client_id="anomaly_detection_job")
            for alert in alerts:
                kafka_conn.send(topic=self.kafka_topic_alerts, value=alert, key=alert["symbol"])
            kafka_conn.close()
            self.logger.debug(f"Batch {batch_id}: Wrote {len(alerts)} alerts to Kafka")
        except Exception as e:
            self.logger.error(f"Batch {batch_id}: Failed to write to Kafka: {str(e)}")

        # Collect all alerts for batch write to 3-tier storage
        alert_records = []
        for alert in alerts:
            alert_data = {
                'alert_id': alert.get('alert_id'),
                'timestamp': alert.get('timestamp'),
                'symbol': alert.get('symbol'),
                'alert_type': alert.get('alert_type'),
                'alert_level': alert.get('alert_level'),
                'created_at': alert.get('created_at'),
                'details': alert.get('details', '{}'),
            }
            alert_records.append(alert_data)
        
        # Write all alerts to all 3 tiers via StorageWriter batch method
        batch_result = self.storage_writer.write_alerts_batch(alert_records)
        success_count = batch_result.success_count
        failure_count = batch_result.failure_count
        
        # Log tier-level results
        for tier, succeeded in batch_result.tier_results.items():
            if not succeeded:
                self.logger.warning(f"Batch {batch_id}: {tier} tier write failed")
        
        self.logger.info(f"Batch {batch_id}: StorageWriter completed - {success_count} succeeded, {failure_count} failed")

    def _process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Process a batch of alerts and write to sinks."""
        # Check shutdown before starting - raise exception to prevent offset commit
        if self.graceful_shutdown.shutdown_requested:
            self.logger.info(f"Batch {batch_id}: Aborting due to shutdown request")
            raise InterruptedError("Shutdown requested, aborting batch to prevent offset commit")
        
        try:
            # Track batch processing latency using utils metrics
            with track_latency("spark_anomaly_detection", "batch_processing"):
                self._log_memory_metrics(batch_id=batch_id, alert_threshold_pct=80.0)
                is_empty = batch_df.isEmpty()
                if self.should_stop(is_empty):
                    self.logger.info(f"Batch {batch_id}: Stopping query due to auto-stop condition")
                    if self.query:
                        self.query.stop()
                    return
                if is_empty:
                    self.logger.info(f"Batch {batch_id} is EMPTY, skipping writes")
                    return
                
                self.graceful_shutdown.mark_batch_start(batch_id)
                start_time = time.time()
                records = batch_df.collect()
                record_count = len(records)
                
                # Check shutdown again after collect (can be slow)
                if self.graceful_shutdown.shutdown_requested:
                    self.logger.info(f"Batch {batch_id}: Aborting after collect due to shutdown")
                    self.graceful_shutdown.mark_batch_end(batch_id)
                    raise InterruptedError("Shutdown requested, aborting batch to prevent offset commit")
                
                self.logger.info(f"Batch {batch_id}: Processing {record_count} alerts")
                alerts = []
                for row in records:
                    details_dict = json.loads(row.details) if row.details else {}
                    alert = self._create_alert(
                        alert_id=row.alert_id,
                        timestamp=row.timestamp, 
                        symbol=row.symbol, 
                        alert_type=row.alert_type, 
                        alert_level=row.alert_level, 
                        details=details_dict
                    )
                    alerts.append(alert)
                    
                    # Record message processed metric for each alert
                    record_message_processed(
                        service="spark_anomaly_detection",
                        topic="alerts",
                        status="success"
                    )
                
                self._write_alerts_to_sinks(alerts, batch_id)
                duration = time.time() - start_time
                watermark = str(records[0].timestamp) if records else None
                self._log_batch_metrics(batch_id, duration, record_count, watermark)
                self.graceful_shutdown.mark_batch_end(batch_id)
        except InterruptedError:
            # Re-raise InterruptedError to prevent checkpoint commit
            raise
        except Exception as e:
            # Record error metric for batch processing failure
            record_error(
                service="spark_anomaly_detection",
                error_type="batch_processing_error",
                severity="error"
            )
            self.logger.error(f"Batch {batch_id}: Error in _process_batch: {str(e)}", exc_info=True)
            self.graceful_shutdown.mark_batch_end(batch_id)

    def _create_stream_reader(self, topic: str) -> DataFrame:
        self.logger.info(f"Creating stream reader for topic: {topic}")
        try:
            df = (self.spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                  .option("subscribe", topic)
                  .option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", str(self.kafka_max_rate_per_partition * 10))
                  .load())
            self.logger.info(f"Stream reader created for topic: {topic}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to create stream reader for {topic}: {str(e)}")
            raise

    def run(self) -> None:
        try:
            self.spark = self._create_spark_session()
            self.storage_writer = self._init_storage_writer()

            self.logger.info("Anomaly Detection Job starting...")
            self.logger.info("Detecting 3 anomaly types:")
            self.logger.info(f"  - Whale alerts (threshold: ${self.WHALE_THRESHOLD:,.0f})")
            self.logger.info(f"  - Volume spikes (threshold: ${self.VOLUME_SPIKE_THRESHOLD:,.0f})")
            self.logger.info(f"  - Price spikes ({self.PRICE_SPIKE_THRESHOLD}% change)")
            self.logger.info("Writing to 3-tier storage: Redis (hot), PostgreSQL (warm), MinIO (cold)")

            raw_trades_stream = self._create_stream_reader(self.kafka_topic_raw_trades)
            aggregations_stream = self._create_stream_reader(self.kafka_topic_processed_aggregations)

            whale_alerts = self.detect_whale_alerts(raw_trades_stream)
            volume_spikes = self.detect_volume_spikes(aggregations_stream)
            price_spikes = self.detect_price_spikes(aggregations_stream)

            all_alerts = whale_alerts.union(volume_spikes).union(price_spikes)

            self.start_time = time.time()
            self.logger.info("Anomaly Detection Job started successfully (micro-batch mode)")
            self.logger.info(f"Auto-stop config: max_runtime={self.max_runtime_seconds}s, empty_batch_threshold={self.empty_batch_threshold}")
            self.logger.info(f"Graceful shutdown timeout: {self.graceful_shutdown.graceful_shutdown_timeout}s")

            # Trigger interval increased to 60s to match actual processing time
            query = (all_alerts.writeStream.foreachBatch(self._process_batch).outputMode("append")
                    .trigger(processingTime='60 seconds')
                    .option("checkpointLocation", self.spark_checkpoint_location).start())
            self.query = query
            query.awaitTermination(timeout=self.max_runtime_seconds)

            if self.graceful_shutdown.shutdown_requested:
                self.logger.info("Anomaly Detection Job shutdown - incomplete batch will replay on restart")
            else:
                self.logger.info("Anomaly Detection Job completed successfully")
        except Exception as e:
            # Record error metric for job failure
            record_error(
                service="spark_anomaly_detection",
                error_type="job_failure",
                severity="critical"
            )
            self.logger.error(f"Job failed with error: {str(e)}", exc_info=True)
            raise
        finally:
            self._cleanup()


def run_anomaly_detection_job():
    job = AnomalyDetectionJob()
    job.run()


if __name__ == "__main__":
    run_anomaly_detection_job()
