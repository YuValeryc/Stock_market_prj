# Stock Market Prediction & Monitoring System

System giám sát và dự đoán giá chứng khoán theo thời gian thực (Real-time Stock Market Monitoring System) được xây dựng dựa trên kiến trúc Lambda (Lambda Architecture).

##  Giới thiệu
Dự án này cung cấp một luồng dữ liệu end-to-end (End-to-End Pipeline) để xử lý dữ liệu chứng khoán:
1. **Ingestion**: Thu thập dữ liệu giao dịch giả lập.
2. **Processing**: Xử lý song song dữ liệu nóng (real-time) và dữ liệu lịch sử (batch).
3. **Serving**: API cung cấp dữ liệu tức thời và dự báo.
4. **Visualization**: Dashboard theo dõi biến động giá.

##  Kiến trúc Hệ thống (Lambda Architecture)

Mô hình gồm các thành phần chính:

*   **Ingestion Layer**:
    *   **Kafka**: Message queue để nhận luồng dữ liệu giá chứng khoán từ Producer.
    *   **Zookeeper**: Quản lý cluster Kafka.

*   **Speed Layer (Xử lý Real-time)**:
    *   **Spark Streaming**: Đọc dữ liệu từ Kafka, xử lý và cập nhật ngay lập tức vào Redis.
    *   **Redis**: In-memory database lưu trữ trạng thái giá mới nhất để truy xuất cực nhanh.

*   **Batch Layer (Xử lý Lô/Lịch sử)**:
    *   **Spark**: Đọc dữ liệu từ Kafka (hoặc lưu trữ khác), xử lý và lưu trữ lâu dài vào HDFS.
    *   **HDFS (Hadoop Distributed File System)**: Lưu trữ dữ liệu lịch sử dưới dạng Parquet.

*   **Serving Layer**:
    *   **FastAPI**: Backend cung cấp API RESTful.
        *   `GET /latest/{code}`: Lấy giá hiện tại từ Redis.
        *   `POST /predict`: Dự đoán giá đóng cửa (tích hợp MLflow).

*   **Machine Learning Ops**:
    *   **MLflow**: Quản lý vòng đời mô hình học máy (Tracking, Registry).
    *   **PostgreSQL**: Metadata store cho MLflow và Airflow.

*   **Orchestration**:
    *   **Airflow**: Lên lịch các tác vụ Batch hoặc huấn luyện lại mô hình (đã tích hợp sẵn).

*   **Frontend**:
    *   Giao diện HTML/JS đơn giản sử dụng **Chart.js** để vẽ biểu đồ realtime.

##  Cách sử dụng

### 1. Yêu cầu
*   Docker & Docker Compose

### 2. Chạy dự án
Khởi động toàn bộ hệ thống bằng lệnh:
```bash
docker-compose up -d
```
*Lưu ý: Lần đầu chạy có thể mất thời gian để build các image.*

### 3. Truy cập Dịch vụ

| Dịch vụ | URL | Mô tả |
| :--- | :--- | :--- |
| **Frontend Dashboard** | `http://localhost:3000` | Xem biểu đồ giá realtime |
| **API Documentation** | `http://localhost:8000/docs` | Swagger API Docs |
| **Spark Master** | `http://localhost:8080` | Quản lý Spark Cluster |
| **MLflow UI** | `http://localhost:5000` | Quản lý Model ML |
| **Airflow Webserver** | `http://localhost:8081` | Quản lý Workflows (User/Pass: `admin`/`admin`) |

### 4. Luồng hoạt động cơ bản
1.  **Producer** tự động gửi dữ liệu (giả lập từ CSV) vào Kafka topic.
2.  **Speed Layer** đọc Kafka -> Ghi giá mới nhất vào Redis.
3.  **Frontend** poll API mỗi 2 giây -> API đọc Redis -> Cập nhật biểu đồ.
4.  **Batch Layer** chạy ngầm định kỳ để lưu trữ dữ liệu vào HDFS.

##  Cấu trúc thư mục
*   `api/`: Mã nguồn FastAPI backend.
*   `frontend/`: Mã nguồn giao diện Dashboard.
*   `processor/`: Mã nguồn Spark jobs (Batch & Speed layers).
*   `producer/`: Script giả lập đẩy dữ liệu vào Kafka.
*   `ml/`: Docker context cho MLflow server.
*   `airflow/`: DAGs và cấu hình cho Airflow.
