from etl.extract import taxizones as e_taxizones
from etl.transform import taxizones as t_taxizones
from etl.load import taxizones as l_taxizones


extract_rsp = []
transform_gdf = []

e_taxizones(extract_rsp)

t_taxizones(extract_rsp, transform_gdf)

l_taxizones(transform_gdf)
