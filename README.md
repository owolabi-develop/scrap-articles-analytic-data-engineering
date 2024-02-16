## Data engineering project
The project scrap data daily from multiple website extract the data to folder called lastestarticles with almost 50 to 
100 csv files then transform it to a single Json file call transform.json in the transfer folder and then upload to s3 for further processing 

## Tech stack
1. docker
2. airflow
3. python
4. aws s3 bucket to store raw data
5. snowflake

## Architecture diagram 
<img width="1882" alt="project1" src="https://github.com/owolabi-develop/scrap-articles-analytic/assets/94055941/a0968e43-fe0e-4406-997f-182d82e82cc4">

 ## Snowflake visualization dashboard 
![project1_analytic](https://github.com/owolabi-develop/scrap-articles-analytic/assets/94055941/21962efb-13b6-4123-9eb7-8850ba8bb169)
