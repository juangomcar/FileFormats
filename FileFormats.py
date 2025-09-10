#!/usr/bin/env python
# coding: utf-8

# # Universidad de la Sabana
# ## Big Data Tools
# ### Coterminal - Ingeniería Informática
# ### Prof. Hugo Franco

# In[1]:


import pandas as pd
import os

from IPython.display import display


# - We'll define the paths for the files selected for the analysis.
# 
# 

# In[2]:


csv_path = 'result_retrieve_left-and-right_x_50_2016_modified.csv'
parquet_path = 'result_retrieve_left-and-right_x_50_2016_modified.parquet'


# - Now, we'll load the two files.

# In[ ]:


# --- Load the modified files ---
print("Loading modified files into new DataFrames...")

df_csv = pd.read_csv(csv_path)
df_parquet = pd.read_parquet(parquet_path)

display(df_csv.head())
display(df_parquet.head())


# - Let's compare the disk size and the shape (rows, columns) of the two file formats. You'll notice that Parquet is significantly more efficient for storage.

# In[7]:


# --- Compare file size and DataFrame shape ---

# Get file sizes
csv_size_bytes = os.path.getsize(csv_path)
parquet_size_bytes = os.path.getsize(parquet_path)

# Get DataFrame shapes
csv_rows, csv_cols = df_csv.shape
parquet_rows, parquet_cols = df_parquet.shape

# Print comparison
print("--- File and DataFrame Comparison ---")

print("\nCSV File:")
print(f"- File Path: {csv_path}")
print(f"- Size on disk: {csv_size_bytes / 1024:.2f} KB")
print(f"- Shape: {csv_rows} rows, {csv_cols} columns")

print("\nParquet File:")
print(f"- File Path: {parquet_path}")
print(f"- Size on disk: {parquet_size_bytes / 1024:.2f} KB")
print(f"- Shape: {parquet_rows} rows, {parquet_cols} columns")

# Highlight the size difference
size_difference = (csv_size_bytes - parquet_size_bytes) / csv_size_bytes * 100
print(f"\nNote: The Parquet file is {size_difference:.2f}% smaller than the CSV file.")


# - The .describe() method provides a powerful statistical summary of the data. Using include='all' gives us statistics for both numerical and text-based columns.

# In[8]:


# --- Obtain a statistical description of the DataFrame ---
# (We only need to run this on one DataFrame, as they contain identical data)

print("--- Statistical Description ---")
df_parquet.info()
display(df_parquet.describe(include='all'))


# This is the core analysis step. We group the data by the specified categories and calculate the average value_x, value_y, and value_z for each group.

# In[9]:


# --- Create average values for x, y, and z columns ---

# Define the columns to group by and the columns to aggregate
grouping_cols = ['fact_id', 'side', 'joint', 'variable']
value_cols = ['value_x', 'value_y', 'value_z']

print(f"Grouping by {grouping_cols} and calculating the mean of {value_cols}...")

# Perform the groupby and aggregation.
# .reset_index() converts the grouped columns back into regular columns.
df_agg = df_parquet.groupby(grouping_cols)[value_cols].mean().reset_index()

# Rename columns for clarity in the database
df_agg.rename(columns={
    'value_x': 'avg_x',
    'value_y': 'avg_y',
    'value_z': 'avg_z'
}, inplace=True)

print("\nPreview of the final data to be loaded:")
display(df_agg.head())


# Handling Missing Data:
# •The value_x, value_y, and value_z columns have some missing entries. First, calculate and print the total number of missing values for each of these three columns.
# •Create a new, cleaned DataFrame by dropping all rows that have missing values in any of those three columns (value_x, value_y, or value_z).
# •Verify your work by checking for missing values again in the new DataFrame.3.Data Filtering and Subsetting:•From your cleaned DataFrame (from Question 2), remove the columns sd_x, sd_y, sd_z, md_x, md_y, and md_z, as they are not needed for this analysis.
# •Create a new DataFrame that contains only the data for the 'Hip' joint. How many rows remain in this new 'Hip' DataFrame?
# 
# File Format Comparison:
# •Take the final 'Hip' DataFrame from Question 3 and save it to two new files: hip_data.csv and hip_data.parquet.
# •Using Python's os library, get the size of each file on disk.
# •Calculate and print the percentage difference in size, showing how much smaller the Parquet file is compared to the CSV.
# 
# Advanced Pandas Aggregation:
# •Using the full cleaned DataFrame (from Question 2, before filtering for the 'Hip' joint), group the data by side and variable.
# •For each group, calculate the standard deviation (std) of value_x, value_y, and value_z.
# •Display the resulting aggregated DataFrame. Which variable shows the highest standard deviation for value_x on the 'L' (Left) side?6.Finding a Maximum Value:
# •Using the full cleaned DataFrame, find the fact_id that corresponds to the single highest value_y measurement recorded in the entire dataset. (Hint: You might find the .idxmax() method useful).

# ## Interpretación de Resultados
# 
# Al comparar los dos formatos de archivo (CSV y Parquet) con el mismo conjunto de datos, lo primero que se nota es la diferencia de tamaño en disco. El CSV ocupa alrededor de 5 MB, mientras que el Parquet apenas llega a unos 230 KB. Esto significa que el archivo Parquet es aproximadamente un 95% más liviano que el CSV, lo que representa una ventaja enorme en contextos de Big Data, donde no trabajamos con un par de megas, sino con gigas o incluso teras de información. Con un formato más ligero, los procesos de carga, transferencia y almacenamiento son mucho más rápidos y eficientes.
# 
# En cuanto a la estructura del dataset, ambos archivos mantienen la misma cantidad de columnas (22), aunque después de la transformación en Parquet se observan algunas diferencias en la cantidad de filas que se usan para el análisis. Esto demuestra que, a pesar de la compresión y de ser más “moderno”, Parquet no pierde información ni cambia el formato de los datos, sino que simplemente lo organiza de forma más optimizada. Además, al aplicar métodos como `.info()` y `.describe(include='all')`, se confirma que la integridad de los datos se conserva y que se pueden obtener estadísticas básicas sin ningún problema.
# 
# La parte de la agregación de valores (promedios de `value_x`, `value_y` y `value_z`) también es importante porque muestra cómo, a partir de un dataset bastante grande, podemos generar resúmenes más compactos y fáciles de interpretar. Agrupar por categorías como `fact_id`, `side`, `joint` y `variable` permite organizar mejor la información y sacar conclusiones más claras sin tener que revisar fila por fila.
# 
# ---
# 
# ### Conclusión
# 
# En conclusión, el experimento muestra que el formato Parquet es mucho más adecuado que el CSV para trabajar en proyectos de Big Data. No solo ocupa mucho menos espacio, sino que también es más rápido de procesar y mantiene la misma calidad de la información. El CSV es útil por su simplicidad y compatibilidad, pero cuando se trata de eficiencia y escalabilidad, Parquet es claramente la mejor opción. Esto me deja claro que elegir el formato correcto no es un detalle menor, sino una decisión clave que puede ahorrar recursos y facilitar el análisis de datos a gran escala.
# 
