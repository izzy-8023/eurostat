# eurostat - Healthcare
## 1. Introduction
## 2. Metadata
### 2.1 Table_of_Contents
- When interested in working with from Eurostat navigation tree , a classification of Eurostat datasets into hierarchical categories, it is possible to retrieve a TXT or XML representation named "table of contents" (TOC)

- Please consult API - Detailed guidelines - Catalogue API - TOC for the information provided by each format
- https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/catalogue-api/toc
- Download link

- XML

- https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/xml
 - XML table of contents is multilingual 

- TXT
- One text file is available per language:
- english : https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en  
- french : https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=fr 
- german : https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=de 

### 2.2 DCAT - Data Catalogue Vocabulary
- The catalogue of datasets can be retrieved as a set of RDF files expressed following Data Catalogue Vocabulary - Application Profile (DCAT-AP).

- This data catalogue is sent by Eurostat to https://data.europa.eu/en portal on a daily basis.

- Download link
- FULL
- Retrieve the full catalogue

- https://ec.europa.eu/eurostat/api/dissemination/catalogue/dcat/ESTAT/FULL

- UPDATES
- Updates are the list of modified datasets since last update.

 - Eurostat datasets are updated twice a day, at 11:00 and at 23:00 in Europe/Brussels time zone

- http://ec.europa.eu/eurostat/api/dissemination/catalogue/dcat/ESTAT/UPDATES

- ### 2.3 RSS feed
- RSS (Rich Site Summary) is a type of web feed that allows users to access updates to online content in a standardised, computer-readable format.

- It allows to be informed about the last changes carried out to data products and code lists published by Eurostat.

- Here are the URL’s of the RSS feeds:

- English version: https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/en/statistics-update.rss 
- German version: https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/de/statistics-update.rss 
- French version: https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/fr/statistics-update.rss 
- For further details, please refer to API - Detailed guidelines - Catalogue API - RSS


### 2.4 Eurostat metabase.txt.gz
The metabase file includes the structure definition of all available Eurostat datasets via the API i.e. the dimensions codes used and their related positions codes in a delimiter-separated format separated by tabulations (TAB). One line of this file as the structure "Dataset Code TAB Dimension Code TAB Position Code" .

This list is alphabetically sorted on dataset code then dimension code and finally on position code list order.

https://ec.europa.eu/eurostat/api/dissemination/catalogue/metabase.txt.gz

Eurostat metabase is refreshed twice a day, at 11:00 and at 23:00 in Europe/Brussels time zone


## 3. Datasets
### Size
- Loading catalog from eurostat_catalog.json...
- Filtering datasets with keyword 'HLTH'...
- Found 490 datasets to process.

- --- Total Size Calculation Summary ---
- Datasets targeted:                 490
- Successfully processed and sized:  490
- Failed or size unavailable:        0
 
- Total calculated size from successful downloads: 6844.20 MB (6.68 GB)
