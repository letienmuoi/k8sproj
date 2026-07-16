# Hệ thống xử lý dữ liệu hỗ trợ tiền xử lý cho AI

Project này xây dựng một hệ thống xử lý dữ liệu nhằm hỗ trợ bước tiền xử lý dữ liệu cho các bài toán AI. Dữ liệu đầu vào là các file dạng text. Hệ thống sẽ ingest dữ liệu, xử lý theo từng bước, làm sạch và chuẩn hóa dần dữ liệu qua nhiều job, đồng thời lưu lại dữ liệu sau mỗi giai đoạn theo mô hình Medallion Pattern.

## Mục tiêu

- Ingest dữ liệu text từ nhiều nguồn file.
- Tách dữ liệu đầu vào thành từng dòng để xử lý luồng.
- Sử dụng Kafka làm message broker trung gian giữa các bước xử lý.
- Sử dụng Apache Flink để xử lý dữ liệu streaming theo từng job.
- Sau mỗi job, dữ liệu đầu ra được đẩy sang một Kafka topic mới để tiếp tục xử lý.
- Lưu dữ liệu sau mỗi bước xử lý vào Apache Iceberg để phục vụ truy vết, tái xử lý và phân tích.
- Sử dụng MinIO làm lớp lưu trữ bền vững cho Persistent Volume trong Kubernetes.
- Làm mịn dữ liệu theo từng tầng để tạo dữ liệu chất lượng hơn cho AI.

## Kiến trúc tổng quan

Luồng xử lý đã được implement:

```text
Text Files
    |
    v
Apache NiFi
    |
    | ingest data, split từng dòng
    v
Kafka Topic: raw-data
    |
    +--------------------------+
    | Apache Flink SQL Job     |
    | - chuẩn hóa Unicode      |
    | - trim/lowercase         |
    | - gộp khoảng trắng       |
    | - loại dòng rỗng         |
    +--------------------------+
       |          |          |
       v          v          v
   Iceberg     Iceberg     Kafka Topic
   Bronze      Silver      refined-data
       \          |          /
        \         v         /
         +---- Iceberg Gold
```

Sau mỗi bước xử lý, dữ liệu cũng được ghi xuống Apache Iceberg:

```text
Kafka Topic / Flink Output
    |
    v
Apache Iceberg Table
    |
    v
MinIO Bucket / Persistent Volume
```

## Các thành phần chính

### Apache NiFi

Apache NiFi chịu trách nhiệm ingestion dữ liệu từ các file text đầu vào. NiFi đọc file, tách nội dung thành từng dòng, sau đó đẩy từng record vào Kafka topic đầu tiên.

### Apache Kafka

Kafka đóng vai trò là hệ thống trung gian truyền dữ liệu giữa các bước xử lý. Mỗi bước xử lý có thể đọc từ một topic và ghi dữ liệu đầu ra sang topic tiếp theo.

Ví dụ:

- `raw-data`: dữ liệu thô sau khi ingest từ NiFi.
- `processed-data-level-1`: dữ liệu sau bước xử lý đầu tiên.
- `processed-data-level-2`: dữ liệu sau bước xử lý tiếp theo.
- `refined-data`: dữ liệu đã được làm sạch và làm mịn hơn để phục vụ AI.

### Apache Flink

Apache Flink chạy các job xử lý dữ liệu streaming. Mỗi job đọc dữ liệu từ một Kafka topic, thực hiện một bước xử lý cụ thể, sau đó ghi kết quả sang Kafka topic mới.

Các bước xử lý có thể bao gồm:

- Làm sạch dữ liệu.
- Chuẩn hóa định dạng.
- Loại bỏ dòng không hợp lệ.
- Trích xuất metadata.
- Biến đổi dữ liệu phục vụ huấn luyện hoặc suy luận AI.

### Apache Iceberg

Apache Iceberg được sử dụng làm storage table format để lưu dữ liệu sau từng giai đoạn xử lý. Việc lưu dữ liệu ở mỗi bước giúp hệ thống có thể truy vết, phân tích chất lượng dữ liệu, tái xử lý khi cần và quản lý dữ liệu theo từng tầng.

### MinIO

MinIO được sử dụng làm lớp lưu trữ bền vững cho hệ thống khi triển khai trên Kubernetes. MinIO cung cấp object storage tương thích S3 và được cấu hình với PersistentVolume/PersistentVolumeClaim để dữ liệu không bị mất khi pod bị restart hoặc được schedule lại.

Trong project này, MinIO có thể được dùng để lưu:

- Dữ liệu đầu vào đã ingest.
- Dữ liệu trung gian sau từng bước xử lý.
- Warehouse của Apache Iceberg.
- Dữ liệu theo các tầng Bronze, Silver và Gold.

## Medallion Pattern

Project áp dụng Medallion Pattern để tổ chức dữ liệu theo nhiều tầng chất lượng:

- Bronze: dữ liệu thô sau khi được ingest từ file text.
- Silver: dữ liệu đã qua các bước làm sạch, chuẩn hóa và kiểm tra chất lượng.
- Gold: dữ liệu đã được làm mịn, sẵn sàng phục vụ các pipeline AI hoặc phân tích nâng cao.

Mỗi tầng dữ liệu được lưu vào một Iceberg table riêng biệt:

- `medallion.bronze_lines`: dữ liệu gốc từ Kafka.
- `medallion.silver_lines`: dữ liệu đã làm sạch và chuẩn hóa.
- `medallion.gold_ai_ready`: dữ liệu tinh gọn sẵn sàng cho pipeline AI.

Các Iceberg table sử dụng MinIO làm storage backend để lưu file Parquet, metadata và snapshot, giúp hệ thống quản lý version, schema, lineage và khả năng tái xử lý.

## Kết quả kỳ vọng

Hệ thống cung cấp một pipeline xử lý dữ liệu có khả năng mở rộng, phù hợp cho các bài toán tiền xử lý dữ liệu AI. Dữ liệu được xử lý tuần tự qua nhiều bước, chất lượng được cải thiện dần, đồng thời vẫn đảm bảo khả năng lưu trữ, theo dõi và tái xử lý thông qua Apache Iceberg và MinIO.

## Trạng thái implementation

Project hiện có một MVP chạy end-to-end trên Docker Desktop Kubernetes:

- NiFi tự động tạo flow `GetFile -> SplitText -> PublishKafka_2_6` qua REST API.
- Kafka chạy KRaft single-node và tự tạo topic `raw-data`, `refined-data`.
- Flink Application Mode chạy một SQL streaming job với checkpoint 15 giây.
- Iceberg REST Catalog lưu registry bằng SQLite trên PersistentVolume.
- MinIO lưu toàn bộ data/metadata Iceberg trong bucket `iceberg-warehouse`.
- PersistentVolume riêng cho MinIO, Kafka, NiFi, Flink checkpoint và Iceberg catalog.

Image của project chứa Flink job cùng Kafka connector, Iceberg runtime, S3 client và Hadoop compatibility runtime:

```text
docker.io/mualanhlung017/ai-data-pipeline:1.0.2
docker.io/mualanhlung017/ai-data-pipeline:latest
```

Digest của release `1.0.2`:

```text
sha256:157988ae188ac6d0d5a9689cfec09392f137f7b35b2187a07a94ad44106566a5
```

Kafka, NiFi, MinIO và Iceberg REST chạy bằng image upstream riêng. Đây là cách triển khai đúng cho Kubernetes: mỗi server là một workload độc lập, còn image custom của project được dùng cho Flink JobManager và TaskManager.

## Cấu trúc source code

```text
.
|-- Dockerfile
|-- pom.xml
|-- kustomization.yaml
|-- examples/
|   `-- sample.txt
|-- scripts/
|   `-- nifi_bootstrap.py
|-- src/
|   |-- main/java/vn/muoilt/pipeline/
|   `-- test/java/vn/muoilt/pipeline/
`-- k8s/
    |-- minio-deployment.yaml
    |-- iceberg-pv.yaml
    |-- iceberg-rest.yaml
    |-- kafka.yaml
    |-- nifi.yaml
    `-- flink.yaml
```

## Triển khai trên Kubernetes

Yêu cầu:

- Docker Desktop đang chạy.
- Kubernetes của Docker Desktop đã được bật.
- Context hiện tại là `docker-desktop`.
- Máy nên có tối thiểu 8 GB RAM cấp cho Docker Desktop.

Triển khai toàn bộ stack:

```powershell
kubectl config use-context docker-desktop
kubectl apply -k .

kubectl rollout status deployment/kafka -n data-platform --timeout=180s
kubectl rollout status deployment/iceberg-rest -n data-platform --timeout=180s
kubectl rollout status deployment/nifi -n data-platform --timeout=300s
kubectl rollout status deployment/flink-jobmanager -n data-platform --timeout=300s
kubectl rollout status deployment/flink-taskmanager -n data-platform --timeout=300s
```

Kiểm tra trạng thái:

```powershell
kubectl get pods,pvc -n data-platform
```

## Chạy thử pipeline

Đưa file text mẫu vào thư mục input của NiFi:

```powershell
$nifiPod = kubectl get pod -n data-platform -l app=nifi -o jsonpath='{.items[0].metadata.name}'
$fileName = "sample-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds()).txt"
kubectl cp examples/sample.txt "data-platform/${nifiPod}:/opt/nifi/input/${fileName}"
```

Đọc dữ liệu đã chuẩn hóa từ Kafka:

```powershell
$kafkaPod = kubectl get pod -n data-platform -l app=kafka -o jsonpath='{.items[0].metadata.name}'
kubectl exec -n data-platform $kafkaPod -- /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server kafka:9092 `
  --topic refined-data `
  --from-beginning `
  --max-messages 3 `
  --timeout-ms 30000
```

Output có dạng:

```json
{"original_line":"  HELLO    AI DATA PIPELINE","normalized_line":"hello ai data pipeline","character_count":22,"processed_at":"2026-07-16T01:04:16.844Z"}
```

Liệt kê Iceberg table qua REST Catalog:

```powershell
kubectl run iceberg-check --rm -i --restart=Never -n data-platform `
  --image=curlimages/curl:8.12.1 --command -- `
  curl -sS http://iceberg-rest:8181/v1/namespaces/medallion/tables
```

Liệt kê file Parquet, metadata và snapshot trong MinIO:

```powershell
kubectl run minio-check --rm -i --restart=Never -n data-platform `
  --image=quay.io/minio/mc:latest `
  --env='MC_HOST_local=http://minioadmin:minioadmin123@minio:9000' `
  --command -- mc ls --recursive local/iceberg-warehouse/medallion
```

## Truy cập giao diện

Docker Desktop Kubernetes có thể không expose NodePort trực tiếp ra Windows. Dùng `port-forward` trong các terminal riêng:

```powershell
kubectl port-forward -n data-platform svc/nifi 8080:8080
kubectl port-forward -n data-platform svc/flink-jobmanager 8081:8081
kubectl port-forward -n data-platform svc/minio 9000:9000 9001:9001
```

- NiFi: `http://localhost:8080/nifi`
- Flink: `http://localhost:8081`
- MinIO Console: `http://localhost:9001`
- MinIO username: `minioadmin`
- MinIO password: `minioadmin123`

## Build image

```powershell
docker build -t mualanhlung017/ai-data-pipeline:1.0.2 .
docker push mualanhlung017/ai-data-pipeline:1.0.2
```

Docker build chạy unit test bằng Maven trước khi tạo runtime image.

## Lưu ý production

Cấu hình hiện tại dành cho local development và demo. Kafka, NiFi, MinIO, Iceberg REST và Flink đang chạy single replica; NiFi dùng HTTP không authentication; secret MinIO là credential mặc định; PV dùng `hostPath` của Docker Desktop. Trước khi đưa lên production cần thay credential, bật TLS/authentication, dùng StorageClass hỗ trợ multi-node, cấu hình replication/high availability và quản lý secret bằng giải pháp chuyên dụng.
