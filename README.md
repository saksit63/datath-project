# End-to-End Movie ETL Pipeline

Technology used: *Python, Pandas, Apache Airflow, Google Cloud Composer, Google Cloud Storage, Google Cloud Bigquery, Looker Studio*

![Data Pipeline Diagram](https://github.com/saksit63/movie-project/blob/main/img/movie_workflow.png)

## Process
 1. นำข้อมูลการซื้อขายสินค้าจากฐานข้อมูล MySQL และข้อมูลการแลกเปลี่ยนเงินตราจาก API ไปยัง Google Cloud Storage (GCS)
 2. นำข้อมูลทั้งสองไปประมวลผล และ merge ข้อมูลจากทั้งสองที่เข้าด้วยกัน โดยใช้ Pandas
 4. นำข้อมูลที่ประมวลผลแล้วเข้าไปยัง Google Bigquery
 5. สร้าง Dashbord โดยใช้ Looker Studio เพื่อช่วยในการวิเคราะห์ตลาด การวางแผนกลยุทธ์ และการติดตามแนวโน้มการขาย
 6. กระบวนการทั้งหมดถูกจัดการด้วย Apache Airflow และ Google Cloud Composer

## Source code
DAG file: [movie_project.py](https://github.com/saksit63/movie-project/blob/main/dags/movie_project.py)

PySpark file: [pyspark_script.py](https://github.com/saksit63/movie-project/blob/main/include/python/pyspark_script.py)


## Result
Airflow:

![Airlofw](https://github.com/saksit63/movie-project/blob/main/result/airflow_dag.png)

Bigquery:

![Bigquery](https://github.com/saksit63/movie-project/blob/main/result/bigquery.png)

Dashbord: 

![Dashboard](https://github.com/saksit63/movie-project/blob/main/result/movie_dashboard.png)


