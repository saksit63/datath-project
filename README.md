# End-to-End Retail ETL Pipeline

Technology used: *Python, Pandas, Apache Airflow, Google Cloud Composer, Google Cloud Storage, Google Cloud Bigquery, Looker Studio*

![Data Pipeline Diagram](https://github.com/saksit63/datath-project/blob/main/img/workflow.png)

## Process
 1. นำข้อมูลการซื้อขายสินค้าจากฐานข้อมูล MySQL และข้อมูลการแลกเปลี่ยนเงินตราจาก API ไปแปลงข้อมูล โดยใช้ pandas จากนั้นจึงนำข้อมูลทั้งสองไปยัง Google Cloud Storage (GCS)
 2. นำข้อมูลทั้งสองจาก GCS ไปประมวลผล และ merge ข้อมูลจากทั้งสองที่เข้าด้วยกัน โดยใช้ Pandas แล้วนำข้อมลไปยัง GCS
 4. นำข้อมูลที่ประมวลผลแล้วจาก GCS เข้าไปยัง Google Bigquery
 5. สร้าง Dashbord โดยใช้ Looker Studio เพื่อช่วยในการวิเคราะห์ตลาด การวางแผนกลยุทธ์ และการติดตามแนวโน้มการขาย
 6. กระบวนการทั้งหมดถูกจัดการด้วย Apache Airflow และ Google Cloud Composer

## Source code
DAG file: [movie_project.py](https://github.com/saksit63/datath-project/blob/main/dags/datath_project.py)


## Result
Airflow:

![Airlofw](https://github.com/saksit63/datath-project/blob/main/result/airflow_dag.png)

Bigquery:

![Bigquery](https://github.com/saksit63/datath-project/blob/main/result/bigquery_datath.png)

Dashbord: 

![Dashboard1](https://github.com/saksit63/datath-project/blob/main/result/dashboard_1.png)

![Dashboard2](https://github.com/saksit63/datath-project/blob/main/result/dashboard_2.png)


