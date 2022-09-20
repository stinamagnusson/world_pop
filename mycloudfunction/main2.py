# Description of file
import os
from google.cloud import bigquery
#def csv_loader(): #data, context
client = bigquery.Client()
        #dataset_id = os.environ['bq_world_pop']
dataset_id = 'bq_world_pop2'
dataset_ref = client.dataset(dataset_id=dataset_id)
job_config = bigquery.LoadJobConfig()
job_config.autodetect = True
"""job_config.schema = [
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
                ]"""
job_config.skip_leading_rows = 1
job_config.source_format = bigquery.SourceFormat.CSV
# get the URI for uploaded CSV in GCS from 'data'
uri = 'gs://world_pop/world_population.csv'
        #uri = 'gs://' + os.environ['world_pop'] + '/' + data['name']
# lets do this
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


# fortsätta följa Load CSV to data
#csv_loader()