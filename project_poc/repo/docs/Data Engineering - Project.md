This document outlines the simplified requirements for a Proof of Concept (POC) demonstrating the capability to integrate data from disparate sources, transform it, and present it in a business intelligence (BI) report, mirroring the flow of the larger project.

- 2 datasource  
- 10 countries in East South Asian

### **1\. 📂 Data Sources and Acquisition**

The POC will utilize two distinct data sources to establish a robust ETL/ELT pipeline.

#### **1.1. Source 1: File Download (Representing Development Indices)**

* Data Set: Sample Human Development Index (HDI) Data. [https://hdr.undp.org/data-center](https://hdr.undp.org/data-center)  
* Source Organization: United Nations Development Programme (UNDP).  
* Acquisition Method: File Download (CSV or Excel).  
* Relevance: The HDI is a key index created by the United Nations (UNDP) and is central to global policy documents and SDG frameworks.  
* Sample Data Fields (for POC):  
  * Country\_Code  
  * Country\_Name  
  * Year  
  * HDI\_Value  
  * Life\_Expectancy  
  * Education\_Years

#### **1.2. Source 2: API (Representing Economic/Macro Data)**

* Data Set: World Development Indicators (WDI).[https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD](https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD)  
* Source Organization: World Bank Group.  
* Acquisition Method: API Call (JSON/XML output).  
* Relevance: The WDI is the best fit for integration with the Orange Index. It maintains over 10,000 indicators and informs $100+ billion/year in lending.  
* Sample Data Fields (for POC):  
  * Country\_Code  
  * Year  
  * Indicator\_Code  
  * FDI\_Net\_Inflows (Foreign Direct Investment Net Inflows)  
  * GDP\_Per\_Capita

### **2\. ⚙️ Data Transformation (T)**

The transformation process will focus on cleansing, standardization, and integration to create an analysis-ready dataset.

#### **2.1. Cleansing and Standardization**

* Data Typing: Ensure all value columns (HDI\_Value, FDI\_Net\_Inflows, etc.) are converted to a consistent Decimal/Float type.  
* Missing Values: Handle NULL or missing values by either imputation or removal for key analytical columns.  
* Key Normalization: Standardize the format of join keys (Country\_Code and Year) across both sources.

#### **2.2. Integration (Join)**

* Operation: Perform an INNER JOIN between the cleaned HDI data (File) and WDI data (API).  
* Join Key: The join must be performed on the composite key of Country\_Code AND Year to ensure data integrity and accuracy.

#### **2.3. Feature Engineering**

* Calculated Field: Create a new analytical metric, e.g., the FDI\_to\_HDI\_Ratio, using the formula:  
  $$FDI\\\_to\\\_HDI\\\_Ratio \= \\frac{FDI\\\_Net\\\_Inflows}{HDI\\\_Value}$$  
* Categorization: Create a categorical field, HDI\_Level, based on HDI\_Value for simplified grouping (e.g., High, Medium, Low).

### **3\. 💾 Data Storage (L)**

* Target Platform: The transformed, integrated dataset must be loaded into a Data Lake (e.g., Google Cloud Storage/S3) or a Data Warehouse (e.g., BigQuery) (select one).

### **4\. 📊 Reporting and Export**

#### **4.1. Business Intelligence (BI) Dashboard (Optional)**

* Tool: Use a standard BI tool (e.g., Looker Studio, Tableau, Power BI) connected to the stored integrated dataset.  
* Dashboard Content:  
  * Trend Chart: Line chart showing the trend of HDI\_Value and FDI\_Net\_Inflows over Year for a selected country.  
  * Correlation Plot: Scatter plot visualizing the relationship between HDI\_Value (X-axis) and FDI\_Net\_Inflows (Y-axis).  
  * Detail Table: A table showing all integrated fields (Country\_Name, Year, HDI\_Value, FDI\_Net\_Inflows, HDI\_Level).  
* Filters: Must include interactive filters for Country\_Name and Year.

#### **4.2. Data Export**

* Requirement: Users must be able to export the filtered data from the Detail Table view of the BI report.  
* Format: Export must be available in CSV and/or Excel (.xlsx) format.

