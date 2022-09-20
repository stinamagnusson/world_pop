import os
from google.cloud import bigquery

"""Create a BigQuery dataset"""
# Construct a BigQuery client object.
client = bigquery.Client()
# TODO(developer): Set dataset_id to the ID of the dataset to create.
dataset_id = "{}.bq_world_pop_transf".format(client.project)
# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

# TODO(developer): Specify the geographic location where the dataset should reside.
dataset.location = "europe-west1"
dataset = client.create_dataset(dataset)
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

"""Create BigQuery table"""
# Construct a BigQuery client object.
client = bigquery.Client()
# TODO(developer): Set table_id to the ID of the table to create.
table_id = "composed-arbor-362511.bq_world_pop_transf.world_pop_table_transf"
table = bigquery.Table(table_id)
# Make an API request
table = client.create_table(table)
print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


"""Read csv file from Cloud Storage into BigQuery"""
#def csv_loader(): #data, context
client = bigquery.Client()
        #dataset_id = os.environ['bq_world_pop']
dataset_id = 'bq_world_pop2'
dataset_ref = client.dataset(dataset_id=dataset_id)
job_config = bigquery.LoadJobConfig()

"""Autodetect schema
job_config.autodetect = True
"""
# All columns as strings
job_config.schema = [
                bigquery.SchemaField('Rank', 'STRING'),
                bigquery.SchemaField('CCA3', 'STRING'),
                bigquery.SchemaField('Country', 'STRING'),
                bigquery.SchemaField('Capital', 'STRING'),
                bigquery.SchemaField('Continent', 'STRING'),
                bigquery.SchemaField('_2022_Population', 'STRING'),
                bigquery.SchemaField('_2020_Population', 'STRING'),
                bigquery.SchemaField('_2015_Population', 'STRING'),
                bigquery.SchemaField('_2010_Population', 'STRING'),
                bigquery.SchemaField('_2000_Population', 'STRING'),
                bigquery.SchemaField('_1990_Population', 'STRING'),
                bigquery.SchemaField('_1980_Population', 'STRING'),
                bigquery.SchemaField('_1970_Population', 'STRING'),
                bigquery.SchemaField('Area_km2', 'STRING'),
                bigquery.SchemaField('Density_per_km2', 'STRING'),
                bigquery.SchemaField('Growth_Rate', 'STRING'),
                bigquery.SchemaField('World_Population_Percentage', 'STRING'),
                ]
job_config.skip_leading_rows = 1
job_config.source_format = bigquery.SourceFormat.CSV
# get the URI for uploaded CSV in GCS from 'data'
uri = 'gs://world_pop/world_population.csv'
        #uri = 'gs://' + os.environ['world_pop'] + '/' + data['name']
        #load_job = client.load_table_from_uri(uri, dataset_ref.table(os.environ['world_pop_table']), job_config=job_config)
load_job = client.load_table_from_uri(uri, dataset_ref.table('world_pop_table2'), job_config=job_config)

        #print('Starting job {}'.format(load_job.job_id))
        #print('Function=csv_loader, Version=' + os.environ['VERSION'])
        
        #print('File: {}'.format(data['name']))
        
load_job.result()  # wait for table load to complete.
print('Job finished.')
        #destination_table = client.get_table(dataset_ref.table(os.environ['world_pop_table']))
destination_table = client.get_table(dataset_ref.table('world_pop_table2'))
print('Loaded {} rows.'.format(destination_table.num_rows))


"""Transform integer columns to integer, float columns to float and save to
new dataset"""
# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the destination table.
table_id = "composed-arbor-362511.bq_world_pop_transf.world_pop_table_transf"
job_config = bigquery.QueryJobConfig(destination=table_id)

sql = """
    SELECT
        CAST(Rank as INTEGER) AS Rank,	
        CCA3,
        Country,			
        Capital,			
        Continent,			
        CAST(_2022_Population as INTEGER) AS _2022_Population,
        CAST(_2020_Population as INTEGER) AS _2020_Population,	
        CAST(_2015_Population as INTEGER) AS _2015_Population,	
        CAST(_2010_Population as INTEGER) AS _2010_Population,	
        CAST(_2000_Population as INTEGER) AS _2000_Population,	
        CAST(_1990_Population as INTEGER) AS _1990_Population,	
        CAST(_1980_Population as INTEGER) AS _1980_Population,
        CAST(_1970_Population as INTEGER) AS _1970_Population,
        CAST(Area_km2 as INTEGER) AS Area_km2,			
        CAST(Density_per_km2 as FLOAT64) AS Density_per_km2,			
        CAST(Growth_Rate as FLOAT64) AS Growth_Rate,			
        CAST(World_Population_Percentage as FLOAT64) AS World_Population_Percentage
    FROM bq_world_pop2.world_pop_table2
"""
# Start the query
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.
print("Query results loaded to the table {}".format(table_id))