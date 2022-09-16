# Sync_Bigquery_to_Postgres
Sync bigquery tables to postgres tables


1. create service account that has requisite access to biguery, then copt the key to the file service_account.json in the root folder 
2. fill some env vars in the file env_vars.sh, and run . ./env_vars.sh to set the env vars
3. install pipenv ; run pipenv shell to activate the env ; run pipenv install to install needed packages.  
4. Now you are ready to go . run python3 main.py python main.py --table-name [] --primarykey [] to start to sync. 

For more details, check the blog : 
[https://yyfhust.github.io/2022/03/08/Sync-BigQuery-to-Postgres.html](https://yyfhust.github.io/2022/03/08/Sync-BigQuery-to-Postgres.html)