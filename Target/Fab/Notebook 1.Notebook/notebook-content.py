# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4a517c49-3850-4d27-85ab-77489cc91401",
# META       "default_lakehouse_name": "config",
# META       "default_lakehouse_workspace_id": "01b6a600-f06e-4981-892f-f4f3f25e610d",
# META       "known_lakehouses": [
# META         {
# META           "id": "4a517c49-3850-4d27-85ab-77489cc91401"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import json
from notebookutils import mssparkutils

# -------- 1) Load JSON from OneLake through Spark --------
df = spark.read.option("multiline", "true").json("abfss://MRSHit@onelake.dfs.fabric.microsoft.com/config.Lakehouse/Files/config.json")
config = df.first().asDict()

# -------- 2) Fetch workspace ID --------
workspace_id = spark.conf.get("trident.workspace.id")

# -------- 3) Build exit JSON --------
pipeline_parameters = {


    "sql_server": "6d939131-0282-44ff-b191-9e8d5465519b",
    "sql_database": "AIF",
    "warehouse_name": "2cf8babd-af80-45ea-af39-18dc1767433a",
    "workspace_id": workspace_id,
    "conn_string": "3o2azv6enfeuvkxt6ht2yj3wqy-qboct6qvhhxu7glkx3iceuuu7m.datawarehouse.fabric.microsoft.com",
    "table_NAME" : "Fabric",
    "table_Schema" : "dbo"


}

# -------- 4) Return to pipeline --------
mssparkutils.notebook.exit(json.dumps(pipeline_parameters))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
