# Technical Test for Investment Analytics Management

## Table of Contents

- [General project requirements](https://github.com/SilvanaJ90/DataEngineerAssessment#general-project-requirements)
- [General functionalities](https://github.com/SilvanaJ90/DataEngineerAssessment#general-functionalities)
- [Authors](https://github.com/SilvanaJ90/DataEngineerAssessment?tab=readme-ov-file#author)


## General project requirements

Investment commercial managers oversee a large number of clients, and the information they have about these clients comes from various sources and with unconventional codes, making it difficult for them to extract knowledge from the available information and generate new investment opportunities in a more automated way. Therefore, it is necessary to develop an analytical tool that allows them to visualize at a minimum:
- The portfolio of each client and what percentage each macro-asset and asset represents in the total portfolio, considering the latest available date.
- The portfolio by banking sector and what percentage each macro-asset represents, considering the latest available date.
- The portfolio by risk profile and what percentage each macro-asset represents, considering the latest available date.
- The month-to-month evolution of the average ABA (Assets Under Management) of the total portfolio. It is desirable to be able to select start and end dates to determine the period to be analyzed.


## General functionalities

## Design a System or Analytical Pipeline:

This diagram shows the architecture of our analytical pipeline:

![This is an image](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/img/structure_analysis.png)

## ETL
 Transform and clean

- [To transform and clean the data in the csv files use the following script](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/transform_and_clean_data.py)

## Data Warehouse

Postgres database, an O.R.M storage was implemented with sqlalchemy

- [To create the database use the following command ](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/setup_postgres_dev.sql)

- Insert Data:

- [to insert the data from the csv files into the database use the following script](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/insert_data.py)

## SQL Queries Development: 

- The portfolio of each client and what percentage each macro-asset and asset represents in the total portfolio as of the latest available date
[Query 1](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/query_1.py)

result:
```
    id_sistema_cliente     macroactivo cod_activo         aba  porcentaje_macroactivo  porcentaje_activo
0                  100            FICs       1018      650061              100.000000              100.0
1           1002203023      Renta Fija       1001   667131500              100.000000              100.0
2           1004870235  Renta Variable       1004     7679200              100.000000              100.0
3          10014876058  Renta Variable       1012     8704400              100.000000              100.0
4          10020203023      Renta Fija       1000   370308560               27.053822              100.0
5          10020203023      Renta Fija       1001   655560500               47.893618              100.0
6          10020203023            FICs       1008   157349050               11.495530              100.0
7          10020203023            FICs       1019   119282276                8.714466              100.0
8          10020203023            FICs       1020    66284271                4.842564              100.0
9          10021382511  Renta Variable       1004     2320000              100.000000              100.0
10         10026419826  Renta Variable       1005       95300              100.000000              100.0
11         10032184607  Renta Variable       1002    12870000               24.204078              100.0
12         10032184607  Renta Variable       1003     7097500               13.347975              100.0
13         10032184607  Renta Variable       1011     9490000               17.847451              100.0
14         10032184607  Renta Variable       1014     7387600               13.893554              100.0
15         10032184607  Renta Variable       1016     6048840               11.375804              100.0
16         10032184607  Renta Variable       1017    10200000               19.182719              100.0
17         10032184607            FICs       1019       78918                0.148418              100.0
18         10036346714  Renta Variable       1004     1160000              100.000000              100.0
19         10038643094  Renta Variable       1005       95300                0.109916              100.0
20         10038643094            FICs       1018    86607034               99.890084              100.0
21         10039538018            FICs       1007      333878              100.000000              100.0
22         10039553126            FICs       1018      650735              100.000000              100.0
23         10039616197  Renta Variable       1004     2320000                6.318874               50.0
24         10039616197  Renta Variable       1004     2320000                6.318874               50.0
25         10039616197  Renta Variable       1005       95300                0.259564               50.0
26         10039616197  Renta Variable       1005       95300                0.259564               50.0
27         10039616197  Renta Variable       1012    15942400               43.421562               50.0
28         10039616197  Renta Variable       1012    15942400               43.421562               50.0
29         10041560001  Renta Variable       1004     1160000              100.000000              100.0
30         10066848163            FICs       1007     2060595              100.000000              100.0
31         10071747544            FICs       1007      371731              100.000000              100.0
32         10079142847  Renta Variable       1004     3447520               30.612731              100.0
33         10079142847  Renta Variable       1014     3861700               34.290499              100.0
34         10079142847  Renta Variable       1017     3952500               35.096770              100.0
35         10079414250  Renta Variable       1004     1160000              100.000000              100.0
36         10098522488  Renta Variable       1004     3480000              100.000000              100.0
37        100800000000            FICs       1019   535000147               50.000000               50.0
38        100800000000            FICs       1019   535000147               50.000000               50.0
39        100830000000  Renta Variable       1005      158800              100.000000              100.0
40        100890000000            FICs       1008    22489664               13.067129              100.0
41        100890000000            FICs       1019   149619024               86.932871              100.0
42        100900000000            FICs       1007   104552953               69.949985              100.0
43        100900000000            FICs       1019    22457602               15.025008               50.0
44        100900000000            FICs       1019    22457602               15.025008               50.0
45        100901000000            FICs       1019  1077997925              100.000000              100.0
46        100902000000            FICs       1019  1071851480              100.000000              100.0
47       1001030000000  Renta Variable       1004     1160000              100.000000              100.0
48       1001050000000  Renta Variable       1004     2320000              100.000000              100.0
49       1001140000000  Renta Variable       1004     2320000              100.000000              100.0
```

- The portfolio by bank and the percentage of each macro-asset as of the latest available date.
[Query 2](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/queries/query_2.py)

result:
```
  cod_banca         banca     macroactivo   total_aba  porcentaje_macroactivo
0        EG      Empresas            FICs  1077997925              100.000000
1        PF  Preferencial            FICs  1221319637               99.086330
2        PF  Preferencial  Renta Variable    11261720                0.913670
3        PN      Personal            FICs    87336687               41.827870
4        PN      Personal  Renta Variable   121463540               58.172130
5        PR       Privada            FICs   515024285               33.423746
6        PR       Privada      Renta Fija  1025869060               66.576254
7        PY         Pymes            FICs  1070000294               99.985161
8        PY         Pymes  Renta Variable      158800                0.014839
```

-  The portfolio by risk profile and the percentage of ach macro-asset as of the latest available date.
[Query 3](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/queries/query_3.py)

result:
```
  cod_perfil_riesgo perfil_riesgo     macroactivo   total_aba  porcentaje_macroactivo
0              1466   SIN DEFINIR            FICs    87257769               59.380448
1              1466   SIN DEFINIR  Renta Variable    59689200               40.619552
2              1467   CONSERVADOR            FICs  3369317856              100.000000
3              1468      MODERADO            FICs   515103203               32.063932
4              1468      MODERADO      Renta Fija  1025869060               63.857876
5              1468      MODERADO  Renta Variable    65515660                4.078192
6              1469      AGRESIVO  Renta Variable     7679200              100.000000
```

- The month-on-month evolution of the average ABA (Assets Under Management) of the total portfolio
[Query 4](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/queries/query_4.py)

result:
```
   ingestion_year  ingestion_month  promedio_aba
0            2023               11  8.222355e+07
1            2023               12  1.001967e+08
2            2024                1  1.105484e+08
3            2024                2  1.025973e+08
4            2024                3  1.094799e+08
5            2024                4  1.217252e+08
6            2024                5  1.224560e+08
```
## Data Visualisation
[To visualise the data use the following script and enter the browser with the ip given by the server](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/queries/data_visualisation.py)


- The portfolio of each client and what percentage each macro-asset and asset represents in the total portfolio as of the latest available date
![This is an image](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/img/Query1.png)

- The portfolio by bank and the percentage of each macro-asset as of the latest available date.
![This is an image](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/img/Query2.png)

-  The portfolio by risk profile and the percentage of ach macro-asset as of the latest available date.
![This is an image](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/img/Query3.png)

- The month-on-month evolution of the average ABA (Assets Under Management) of the total portfolio
![This is an image](https://github.com/SilvanaJ90/DataEngineerAssessment/blob/main/img/Query4.png)

## Technical Conclusions:


## Technology Implemented:

<p align="left"> <a href="https://pandas.pydata.org/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/2ae2a900d2f041da66e950e4d48052658d850630/icons/pandas/pandas-original.svg" alt="pandas" width="40" height="40"/> </a> <a href="https://www.postgresql.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/postgresql/postgresql-original-wordmark.svg" alt="postgresql" width="40" height="40"/> </a> <a href="https://www.python.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> </a> </p>

## Author

- Silvana Jaramillo
 <a href="https://linkedin.com/in/silvana-jaramillo" target="blank"><img align="center" src="https://raw.githubusercontent.com/rahuldkjain/github-profile-readme-generator/master/src/images/icons/Social/linked-in-alt.svg" alt="silvana-jaramillo" height="30" width="40" /></a>
