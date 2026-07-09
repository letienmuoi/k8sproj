# Hệ thống xử lý dữ liệu hỗ trợ tiền xử lý cho AI

Project này xây dựng một hệ thống xử lý dữ liệu nhằm hỗ trợ bước tiền xử lý dữ liệu cho các bài toán AI. Dữ liệu đầu vào là các file dạng text. Hệ thống sẽ ingest dữ liệu, xử lý theo từng bước, làm sạch và chuẩn hóa dần dữ liệu qua nhiều job, đồng thời lưu lại dữ liệu sau mỗi giai đoạn theo mô hình Medallion Pattern.

## Mục tiêu

- Ingest dữ liệu text từ nhiều nguồn file.
- Tách dữ liệu đầu vào thành từng dòng để xử lý luồng.
- Sử dụng Kafka làm message broker trung gian giữa các bước xử lý.
- Sử dụng Apache Flink để xử lý dữ liệu streaming theo từng job.
- Sau mỗi job, dữ liệu đầu ra được đẩy sang một Kafka topic mới để tiếp tục xử lý.
- Lưu dữ liệu sau mỗi bước xử lý vào Apache Iceberg để phục vụ truy vết, tái xử lý và phân tích.
- Làm mịn dữ liệu theo từng tầng để tạo dữ liệu chất lượng hơn cho AI.

## Kiến trúc tổng quan

Luồng xử lý chính:

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
    v
Flink Job 1
    |
    | xử lý bước 1
    v
Kafka Topic: processed-data-level-1
    |
    v
Flink Job 2
    |
    | xử lý bước 2
    v
Kafka Topic: processed-data-level-2
    |
    v
Flink Job N
    |
    v
Kafka Topic: refined-data
```

Sau mỗi bước xử lý, dữ liệu cũng được ghi xuống Apache Iceberg:

```text
Kafka Topic / Flink Output
    |
    v
Apache Iceberg Table
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

## Medallion Pattern

Project áp dụng Medallion Pattern để tổ chức dữ liệu theo nhiều tầng chất lượng:

- Bronze: dữ liệu thô sau khi được ingest từ file text.
- Silver: dữ liệu đã qua các bước làm sạch, chuẩn hóa và kiểm tra chất lượng.
- Gold: dữ liệu đã được làm mịn, sẵn sàng phục vụ các pipeline AI hoặc phân tích nâng cao.

Mỗi tầng dữ liệu có thể được lưu vào các Iceberg table riêng biệt, giúp hệ thống dễ quản lý version, schema, lineage và khả năng tái xử lý.

## Kết quả kỳ vọng

Hệ thống cung cấp một pipeline xử lý dữ liệu có khả năng mở rộng, phù hợp cho các bài toán tiền xử lý dữ liệu AI. Dữ liệu được xử lý tuần tự qua nhiều bước, chất lượng được cải thiện dần, đồng thời vẫn đảm bảo khả năng lưu trữ, theo dõi và tái xử lý thông qua Apache Iceberg.
