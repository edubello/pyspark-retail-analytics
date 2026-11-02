# 🛒 Retail Analytics Lakehouse with PySpark & Delta Lake

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5%2B-orange?logo=apachespark)](https://spark.apache.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%2FSilver%2FGold)-green)]()
[![Code Style](https://img.shields.io/badge/Code%20Style-Modular%20%26%20Clean-black)]()

---

## Executive Summary / Sumário Executivo

**[EN]**  
This repository presents an end-to-end **Retail Data Lakehouse** solution built using **PySpark**. The project demonstrates enterprise-grade data engineering patterns, including modular package architecture (`src/`), Medallion architecture (Bronze, Silver, Gold), automated data quality checks, optimization techniques (broadcast joins, partitioning), and business-oriented analytical modeling.

**[PT]**  
Este repositório apresenta uma solução completa de **Data Lakehouse Varejista** construída com **PySpark**. O projeto demonstra padrões de engenharia de dados de nível corporativo, incluindo arquitetura modular de pacotes (`src/`), arquitetura em Medalhão (Bronze, Silver, Gold), validações automatizadas de qualidade de dados, técnicas de otimização (broadcast joins, particionamento) e modelagem analítica voltada a negócios.

---

##  Medallion Architecture / Arquitetura em Medalhão



[ Raw Data / Dados Brutos ] (CSV / JSON)

▼

[ 🥉 BRONZE LAYER ] --> Raw Ingestion & Schema Enforcement (ingestion.py)

▼

[ 🥈 SILVER LAYER ] --> Cleaning, Deduplication & Quality Validation (transformations.py) 

▼

[ 🥇 GOLD LAYER ] --> Business Aggregations & Analytical KPIs (analytics.py)

▼ 

[ Power BI / BI Dashboards ]