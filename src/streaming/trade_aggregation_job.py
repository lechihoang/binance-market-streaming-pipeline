"""Trade Aggregation Job."""

import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, sum as spark_sum, avg, min as spark_min, 
    max as spark_max, first, last, stddev, when, from_json, to_json, 
    struct, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, BooleanType, TimestampType
)

from src.streaming.base_spark_job import BaseSparkJob

# Import metrics utilities for production monitoring
from src.utils.metrics import (
    record_error,
    record_message_processed,
    track_latency,
)


class TradeAggregationJob(BaseSparkJob):
    def __init__(self):
        super().__init__(job_name="TradeAggregationJob")

    def create_stream_reader(self) -> DataFrame:
        """Create Kafka stream reader for raw_trades topic."""
        self.logger.info(f"Creating stream reader for topic: {self.kafka_topic_raw_trades}")
        self.logger.info(f"Kafka bootstrap servers: {self.kafka_bootstrap_servers}")
        
        try:
            # Use "earliest" so checkpoint can track progress
            # When job restarts, it continues from last checkpoint offset (not from beginning)
            # This ensures no data is lost when job is temporarily stopped
            df = (self.spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                  .option("subscribe", self.kafka_topic_raw_trades)
                  .option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", 
                         str(self.kafka_max_rate_per_partition * 10))
                  .load())
            
            self.logger.info("Stream reader created successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to create stream reader: {str(e)}", 
                            extra={"topic": self.kafka_topic_raw_trades,
                                   "error": str(e)})
            raise
    
    def parse_trades(self, df: DataFrame) -> DataFrame:
        """Parse JSON trade messages and extract fields."""
        # Define schema for Binance connector EnrichedMessage format
        # Must match fields from EnrichedMessage in binance_kafka_connector/models.py
        # Note: spark.sql.caseSensitive=true is enabled to distinguish e/E and t/T fields
        original_data_schema = StructType([
            StructField("e", StringType(), True),   # Event type ("trade")
            StructField("E", LongType(), True),     # Event time (milliseconds)
            StructField("s", StringType(), True),   # Symbol
            StructField("t", LongType(), True),     # Trade ID
            StructField("p", StringType(), True),   # Price
            StructField("q", StringType(), True),   # Quantity
            StructField("T", LongType(), True),     # Trade time (milliseconds)
            StructField("m", BooleanType(), True),  # Is buyer maker
        ])
        
        # Full EnrichedMessage schema matching binance_kafka_connector/models.py
        trade_schema = StructType([
            StructField("original_data", original_data_schema, True),
            StructField("ingestion_timestamp", LongType(), True),
            StructField("source", StringType(), True),
            StructField("data_version", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("stream_type", StringType(), True),
            StructField("topic", StringType(), True)
        ])
        
        self.logger.info("Parsing trade messages with Binance connector schema")
        
        try:
            # Parse JSON from Kafka value
            parsed_df = df.select(
                from_json(col("value").cast("string"), trade_schema).alias("trade"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            # Extract fields, convert event_time to timestamp, and add watermark
            # Use getField() to avoid case-insensitive ambiguity between 'E' and 'e'
            original_data_col = col("trade").getField("original_data")
            extracted_df = parsed_df.select(
                (original_data_col.getField("E") / 1000).cast(TimestampType()).alias("event_time"),
                col("trade").getField("symbol").alias("symbol"),
                original_data_col.getField("p").cast(DoubleType()).alias("price"),
                original_data_col.getField("q").cast(DoubleType()).alias("quantity"),
                original_data_col.getField("m").alias("is_buyer_maker"),
                original_data_col.getField("t").alias("trade_id"),
                col("topic"),
                col("partition"),
                col("offset")
            ).withWatermark("event_time", "1 minute")
            
            self.logger.info("Trade parsing configured with 1 minute watermark")
            return extracted_df
            
        except Exception as e:
            self.logger.error(f"Failed to parse trades: {str(e)}", 
                            extra={"error": str(e)})
            raise

    def aggregate_trades(self, df: DataFrame) -> DataFrame:
        """Create 1-minute OHLCV candles with derived metrics from parsed trades."""
        return (df
            .groupBy(
                window(col("event_time"), "1 minute").alias("window"),
                col("symbol")
            )
            .agg(
                count("*").alias("trade_count"),
                spark_sum("quantity").alias("volume"),
                spark_sum(col("price") * col("quantity")).alias("quote_volume"),
                avg("price").alias("avg_price"),
                spark_min("price").alias("low"),
                spark_max("price").alias("high"),
                first("price").alias("open"),
                last("price").alias("close"),
                stddev("price").alias("price_stddev"),
                spark_sum(when(col("is_buyer_maker") == False, 1).otherwise(0)).alias("buy_count"),
                spark_sum(when(col("is_buyer_maker") == True, 1).otherwise(0)).alias("sell_count")
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                lit("1m").alias("window_duration"),
                col("symbol"),
                col("open"),
                col("high"),
                col("low"),
                col("close"),
                col("volume"),
                col("quote_volume"),
                col("trade_count"),
                col("avg_price"),
                col("price_stddev"),
                col("buy_count"),
                col("sell_count")
            )
            # Derived metrics
            .withColumn("vwap", col("quote_volume") / col("volume"))
            .withColumn("price_change_pct", ((col("close") - col("open")) / col("open")) * 100)
            .withColumn("buy_sell_ratio", when(col("sell_count") > 0, col("buy_count") / col("sell_count")).otherwise(lit(None)))
        )

    def write_to_sinks(self, batch_df: DataFrame, batch_id: int) -> None:
        """Write batch to multiple sinks using StorageWriter for 3-tier storage."""
        start_time = time.time()
        
        # Check shutdown before starting
        if self.graceful_shutdown.shutdown_requested:
            self.logger.info(f"Batch {batch_id}: Aborting due to shutdown request")
            raise InterruptedError("Shutdown requested, aborting batch to prevent offset commit")
        
        # Check if batch is empty
        is_empty = batch_df.isEmpty()
        
        if self.should_stop(is_empty):
            self.logger.info(f"Batch {batch_id}: Stopping query due to auto-stop condition")
            if self.query:
                self.query.stop()
            return
        
        if is_empty:
            self.logger.info(f"Batch {batch_id} is EMPTY, skipping writes")
            return
        
        # Log memory usage
        self._log_memory_metrics(batch_id=batch_id, alert_threshold_pct=80.0)
        
        # Collect data for writing
        records = batch_df.collect()
        record_count = len(records)
        
        self.logger.info(f"Writing batch {batch_id} with {record_count} records to sinks")
        
        # Check shutdown again after collect (can be slow)
        if self.graceful_shutdown.shutdown_requested:
            self.logger.info(f"Batch {batch_id}: Aborting after collect due to shutdown")
            raise InterruptedError("Shutdown requested, aborting batch to prevent offset commit")
        
        # Write to Kafka
        try:
            kafka_df = batch_df.select(
                col("symbol").cast("string").alias("key"),
                to_json(struct(*[col(c) for c in batch_df.columns])).alias("value")
            )
            
            kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("topic", self.kafka_topic_processed_aggregations) \
                .option("kafka.enable.idempotence", str(self.kafka_enable_idempotence).lower()) \
                .option("kafka.acks", self.kafka_acks) \
                .option("kafka.max.in.flight.requests.per.connection", 
                       str(self.kafka_max_in_flight_requests)) \
                .save()
            
            self.logger.debug(f"Batch {batch_id}: Wrote to Kafka topic {self.kafka_topic_processed_aggregations}")
        except Exception as e:
            self.logger.error(f"Batch {batch_id}: Failed to write to Kafka: {str(e)}")
        
        # Write to 3-tier storage
        aggregation_records = [
            {
                'timestamp': row.window_start,
                'symbol': row.symbol,
                'interval': row.window_duration,
                'open': float(row.open) if row.open else None,
                'high': float(row.high) if row.high else None,
                'low': float(row.low) if row.low else None,
                'close': float(row.close) if row.close else None,
                'volume': float(row.volume) if row.volume else None,
                'quote_volume': float(row.quote_volume) if hasattr(row, 'quote_volume') and row.quote_volume else 0,
                'trades_count': int(row.trade_count) if hasattr(row, 'trade_count') and row.trade_count else 0,
                'buy_count': int(row.buy_count) if hasattr(row, 'buy_count') and row.buy_count is not None else 0,
                'sell_count': int(row.sell_count) if hasattr(row, 'sell_count') and row.sell_count is not None else 0,
            }
            for row in records
        ]
        
        batch_result = self.storage_writer.write_aggregations_batch(aggregation_records)
        
        # Log tier-level results
        for tier, succeeded in batch_result.tier_results.items():
            if not succeeded:
                self.logger.warning(f"Batch {batch_id}: {tier} tier write failed")
        
        # Record metrics
        for _ in range(batch_result.success_count):
            record_message_processed(
                service="spark_trade_aggregation",
                topic="processed_aggregations",
                status="success"
            )
        
        self.logger.info(
            f"Batch {batch_id}: StorageWriter completed - "
            f"{batch_result.success_count} succeeded, {batch_result.failure_count} failed"
        )
        
        # Log processing metrics
        duration = time.time() - start_time
        watermark = str(records[0].window_start) if records and hasattr(records[0], 'window_start') else None
        watermark_str = f", watermark={watermark}" if watermark else ""
        
        self.logger.info(
            f"Batch {batch_id}: processed {record_count} records in {duration:.2f}s{watermark_str}"
        )

    def run(self) -> None:
        """Run the Trade Aggregation streaming job."""
        try:
            # Create Spark session (using inherited method)
            self.spark = self._create_spark_session()
            
            # Initialize StorageWriter for 3-tier storage (using inherited method)
            self.storage_writer = self._init_storage_writer()
            
            # Create stream reader
            raw_stream = self.create_stream_reader()
            
            # Parse trades and aggregate into 1-minute candles
            trades_df = self.parse_trades(raw_stream)
            enriched_df = self.aggregate_trades(trades_df)
            
            # Track start time for timeout-based auto-stop
            self.start_time = time.time()
            
            self.logger.info("Trade Aggregation Job started successfully (micro-batch mode)")
            self.logger.info(f"Reading from topic: {self.kafka_topic_raw_trades}")
            self.logger.info("Writing to 3-tier storage: Redis (hot), PostgreSQL (warm), MinIO (cold)")
            self.logger.info(f"Auto-stop config: max_runtime={self.max_runtime_seconds}s, empty_batch_threshold={self.empty_batch_threshold}")
            self.logger.info(f"Graceful shutdown timeout: {self.graceful_shutdown.graceful_shutdown_timeout}s")
            
            # Write to multiple sinks using foreachBatch
            # Trigger interval increased to 60s to match actual processing time (~40-50s per batch)
            # This prevents "falling behind" warnings and allows proper batch completion
            query = (enriched_df
                    .writeStream
                    .foreachBatch(self.write_to_sinks)
                    .outputMode("update")
                    .trigger(processingTime='60 seconds')
                    .option("checkpointLocation", self.spark_checkpoint_location)
                    .start())
            
            self.query = query
            
            # Use awaitTermination with timeout for micro-batch processing
            # The query will also stop if should_stop() returns True in write_to_sinks
            # If shutdown requested during batch, exception is raised and offset not committed
            query.awaitTermination(timeout=self.max_runtime_seconds)
            
            if self.graceful_shutdown.shutdown_requested:
                self.logger.info("Trade Aggregation Job shutdown - incomplete batch will replay on restart")
            else:
                self.logger.info("Trade Aggregation Job completed successfully")
            
        except Exception as e:
            # Record error metric for job failure
            record_error(
                service="spark_trade_aggregation",
                error_type="job_failure",
                severity="critical"
            )
            self.logger.error(f"Job failed with error: {str(e)}", 
                            extra={"error": str(e)}, 
                            exc_info=True)
            raise
        finally:
            # Use inherited cleanup method
            self._cleanup()


def main():
    job = TradeAggregationJob()
    job.run()


if __name__ == "__main__":
    main()
