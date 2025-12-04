# Airflow Orchestration - Setup và Usage Guide

## Tổng quan

Hệ thống orchestration sử dụng Apache Airflow để quản lý các data pipeline trong dự án. Thiết kế tối giản với cấu trúc rõ ràng, dễ bảo trì, và tích hợp data quality tests.

### Kiến trúc

```
┌─────────────────────────────────────────────────────────────┐
│                     Airflow Orchestration                    │
│                                                              │
│  ┌────────────┐      ┌────────────┐      ┌────────────┐   │
│  │Streaming   │      │ Anomaly    │      │  Custom    │   │
│  │Pipeline    │      │ Detection  │      │  DAGs      │   │
│  │            │      │            │      │            │   │
│  │ TaskGroup1 │      │ TaskGroup1 │      │ TaskGroup1 │   │
│  │  ├─ Run    │      │  ├─ Run    │      │  ├─ Run    │   │
│  │  └─ Test   │      │  └─ Test   │      │  └─ Test   │   │
│  └────────────┘      └────────────┘      └────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │           Data Quality Test Framework                 │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐          │  │
│  │  │  Health  │  │  Field   │  │Complete- │          │  │
│  │  │  Checks  │  │Validation│  │ness Check│          │  │
│  │  └──────────┘  └──────────┘  └──────────┘          │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Các thành phần chính

- **DAGs**: Định nghĩa workflows với task groups
- **Task Groups**: Nhóm các tasks liên quan (run + test)
- **Data Quality Tests**: Health checks, field validation, completeness checks
- **Metrics & Logging**: Thu thập metrics và logs chi tiết

## Deployment

### Prerequisites

- Docker và Docker Compose đã cài đặt
- Ports 8080 (Airflow UI) và 5432 (PostgreSQL) available
- Kafka, Redis, và các services khác đã chạy (nếu cần)

### Bước 1: Khởi động Airflow

```bash
# Từ project root
docker-compose up -d

# Kiểm tra services đã chạy
docker-compose ps
```

Services sẽ được khởi động:
- **PostgreSQL**: Metadata database (port 5432)
- **Airflow Webserver**: Web UI (port 8080)
- **Airflow Scheduler**: Task scheduler

### Bước 2: Truy cập Airflow UI

1. Mở browser và truy cập: http://localhost:8080
2. Login credentials:
   - Username: `admin`
   - Password: `admin`

### Bước 3: Verify DAGs

1. Trong Airflow UI, click vào tab "DAGs"
2. Bạn sẽ thấy các DAGs:
   - `streaming_pipeline_dag`: Pipeline chính cho streaming data
   - `anomaly_detection_dag`: Pipeline phát hiện anomalies

### Bước 4: Kiểm tra cấu hình

Các environment variables được cấu hình trong `docker-compose.yml`:

```yaml
environment:
  # Executor configuration
  AIRFLOW__CORE__EXECUTOR: LocalExecutor
  
  # Auto-discovery settings
  AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: 30  # Scan for new DAGs every 30s
  
  # External service connections
  KAFKA_BOOTSTRAP_SERVERS: kafka:29092
  REDIS_HOST: redis:6379
  DUCKDB_PATH: /opt/airflow/data/streaming.duckdb
```

## Sử dụng Airflow

### Trigger DAG thủ công

**Cách 1: Từ UI**

1. Vào trang DAGs
2. Tìm DAG muốn chạy
3. Click nút "Play" (▶️) bên phải tên DAG
4. Click "Trigger DAG" trong popup

**Cách 2: Từ CLI**

```bash
# Trigger streaming pipeline
docker exec airflow-scheduler airflow dags trigger streaming_pipeline_dag

# Trigger anomaly detection
docker exec airflow-scheduler airflow dags trigger anomaly_detection_dag
```

### Xem DAG execution

1. Click vào tên DAG
2. Chọn tab "Graph" để xem task dependencies
3. Click vào task để xem:
   - **Log**: Chi tiết logs
   - **XCom**: Metrics và data được share giữa tasks
   - **Task Instance Details**: Thông tin execution

### Xem logs

**Cách 1: Từ UI**

1. Vào DAG → chọn DAG run
2. Click vào task muốn xem
3. Click "Log" button

**Cách 2: Từ Docker logs**

```bash
# Scheduler logs
docker logs airflow-scheduler

# Webserver logs
docker logs airflow-webserver

# Follow logs real-time
docker logs -f airflow-scheduler
```

**Cách 3: Từ log files**

```bash
# Logs được mount tại ./logs/
ls -la logs/

# Xem log của specific task
cat logs/streaming_pipeline_dag/task_id/execution_date/1.log
```

### Xem metrics

Metrics được lưu trong XCom và có thể xem từ UI:

1. Click vào task đã complete
2. Click tab "XCom"
3. Xem các keys:
   - `validation_metrics`: Kết quả field validation
   - `completeness_metrics`: Kết quả completeness check
   - `{service}_health`: Kết quả health checks

Example metrics structure:

```json
{
  "timestamp": "2024-01-01T12:00:00",
  "total_records": 1000,
  "valid_records": 995,
  "violations": 5,
  "status": "passed"
}
```

## Thêm DAG mới

### Bước 1: Tạo DAG file

Tạo file mới trong thư mục `dags/`:

```bash
# Ví dụ: tạo my_pipeline_dag.py
touch dags/my_pipeline_dag.py
```

### Bước 2: Viết DAG definition

Sử dụng template (xem `dags/example_dag_template.py`) hoặc viết từ đầu:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# Define helper functions inline
def my_task_function(**context):
    """Your task logic here."""
    # Do something
    result = {"status": "success", "records": 100}
    
    # Push metrics to XCom
    context['task_instance'].xcom_push(key='metrics', value=result)
    return result

# DAG definition
with DAG(
    dag_id='my_pipeline',
    default_args={
        'owner': 'data-engineering',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['custom', 'production'],
) as dag:
    
    # Task Group with run + test pattern
    with TaskGroup('my_task_group') as group:
        run_task = PythonOperator(
            task_id='run_processing',
            python_callable=my_task_function,
        )
        
        test_task = PythonOperator(
            task_id='test_quality',
            python_callable=my_task_function,
        )
        
        run_task >> test_task
```

### Bước 3: Đợi auto-discovery

Airflow sẽ tự động phát hiện DAG mới sau 30 giây (theo `DAG_DIR_LIST_INTERVAL`).

Không cần restart services!

### Bước 4: Verify trong UI

1. Refresh trang DAGs
2. DAG mới sẽ xuất hiện trong list
3. Nếu có syntax error, sẽ hiển thị trong "Import Errors"

## Data Quality Tests

### Health Checks

Kiểm tra connectivity của external services:

```python
from dags.data_quality import health_check_service

# In your DAG
health_check = PythonOperator(
    task_id='check_kafka_health',
    python_callable=health_check_service,
    op_kwargs={
        'service_name': 'kafka',
        'host': 'kafka',
        'port': 29092,
        'max_retries': 3,
    },
)
```

Features:
- Exponential backoff retry (1s, 2s, 4s, ...)
- Logs mỗi attempt
- Push metrics to XCom khi success
- Raise exception khi fail sau max_retries

### Field Validation

Validate schema và data types:

```python
from dags.data_quality import validate_fields

# In your DAG
validation = PythonOperator(
    task_id='validate_trade_data',
    python_callable=validate_fields,
    op_kwargs={
        'data': [...],  # Your data
        'schema': {
            'symbol': str,
            'price': float,
            'quantity': float,
            'timestamp': int,
        },
    },
)
```

Features:
- Check field existence
- Check data types
- Log violating records (first 10)
- Push metrics to XCom
- Raise exception nếu có violations

### Completeness Checks

Kiểm tra missing values:

```python
from dags.data_quality import check_completeness

# In your DAG
completeness = PythonOperator(
    task_id='check_completeness',
    python_callable=check_completeness,
    op_kwargs={
        'data': [...],  # Your data
        'required_fields': ['symbol', 'price', 'quantity'],
    },
)
```

Features:
- Count null và empty values
- Calculate completeness percentage
- Log incomplete records (first 10)
- Push metrics to XCom
- Raise exception nếu có incomplete records

## Troubleshooting

### DAG không xuất hiện trong UI

**Kiểm tra:**

1. File có trong thư mục `dags/`?
   ```bash
   ls -la dags/
   ```

2. Có syntax errors?
   ```bash
   # Test import DAG
   docker exec airflow-scheduler python -c "from dags.my_dag import dag"
   ```

3. Check "Import Errors" trong Airflow UI

4. Xem scheduler logs:
   ```bash
   docker logs airflow-scheduler | grep -i error
   ```

### Task fails với connection error

**Kiểm tra:**

1. Services đang chạy?
   ```bash
   docker-compose ps
   ```

2. Environment variables đúng?
   ```bash
   docker exec airflow-scheduler env | grep KAFKA
   ```

3. Network connectivity?
   ```bash
   docker exec airflow-scheduler ping kafka
   ```

### Logs không hiển thị

**Kiểm tra:**

1. Log volume được mount?
   ```bash
   ls -la logs/
   ```

2. Permissions đúng?
   ```bash
   chmod -R 755 logs/
   ```

### DAG chạy chậm

**Tối ưu:**

1. Giảm `dag_dir_list_interval` nếu cần faster discovery
2. Tăng parallelism trong `airflow.cfg`
3. Sử dụng CeleryExecutor cho distributed execution (advanced)

## Monitoring

### Health checks

```bash
# Check scheduler health
docker exec airflow-scheduler airflow jobs check --job-type SchedulerJob

# Check database connection
docker exec postgres pg_isready -U airflow

# Check all services
docker-compose ps
```

### Metrics summary

```bash
# View recent DAG runs
docker exec airflow-scheduler airflow dags list-runs -d streaming_pipeline_dag

# View task instances
docker exec airflow-scheduler airflow tasks list streaming_pipeline_dag
```

### Log monitoring

```bash
# Follow scheduler logs
docker logs -f airflow-scheduler

# Search for errors
docker logs airflow-scheduler 2>&1 | grep -i error

# View specific DAG logs
docker logs airflow-scheduler 2>&1 | grep streaming_pipeline_dag
```

## Stopping và Cleanup

### Stop services

```bash
# Stop all services (giữ data)
docker-compose stop

# Stop và remove containers (giữ volumes)
docker-compose down
```

### Cleanup data

```bash
# Remove all data (cẩn thận!)
docker-compose down -v

# Remove specific volumes
docker volume rm <volume_name>
```

### Restart services

```bash
# Restart all
docker-compose restart

# Restart specific service
docker-compose restart airflow-scheduler
```

## Best Practices

### 1. DAG Design

- ✅ Sử dụng TaskGroups để organize tasks
- ✅ Mỗi TaskGroup có run task và test task (nếu cần)
- ✅ Set dependencies rõ ràng
- ✅ Sử dụng meaningful task IDs
- ✅ Add tags để categorize DAGs

### 2. Error Handling

- ✅ Set retries và retry_delay trong default_args
- ✅ Sử dụng on_failure_callback để handle errors
- ✅ Log chi tiết errors với stack traces
- ✅ Push metrics to XCom trước khi fail

### 3. Testing

- ✅ Test DAG syntax trước khi deploy
- ✅ Add data quality tests cho critical steps
- ✅ Verify metrics được push to XCom
- ✅ Test failure propagation

### 4. Logging

- ✅ Log với timestamp và task context
- ✅ Use structured logging
- ✅ Log important events (start, complete, errors)
- ✅ Avoid logging sensitive data

### 5. Configuration

- ✅ Sử dụng environment variables
- ✅ Không hardcode connections
- ✅ Document tất cả configuration options
- ✅ Use Airflow Variables cho dynamic config

## Advanced Topics

### Custom Operators

Tạo custom operators cho reusable logic:

```python
from airflow.models import BaseOperator

class MyCustomOperator(BaseOperator):
    def __init__(self, my_param, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.my_param = my_param
    
    def execute(self, context):
        # Your logic here
        pass
```

### Dynamic DAG Generation

Generate DAGs programmatically:

```python
for symbol in ['BTC', 'ETH', 'BNB']:
    dag_id = f'process_{symbol.lower()}'
    
    with DAG(dag_id=dag_id, ...) as dag:
        # Define tasks
        pass
    
    globals()[dag_id] = dag
```

### Sensors

Wait for conditions before proceeding:

```python
from airflow.sensors.base import BaseSensorOperator

class MyCustomSensor(BaseSensorOperator):
    def poke(self, context):
        # Return True when condition is met
        return check_condition()
```

## Support

Để được hỗ trợ:

1. Check documentation này
2. Xem Airflow logs để debug
3. Check "Import Errors" trong UI
4. Review DAG code và configuration
5. Liên hệ team nếu cần help

## References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [TaskGroups Guide](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#taskgroups)
