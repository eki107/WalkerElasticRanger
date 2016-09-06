import pandas as pd
import json
import aggwalk

with open("marketing_results.json", 'r') as f:
    elastic_result = f.read()

agg = json.loads(elastic_result)['aggregations']
a = aggwalk.tablify(agg)
df = pd.DataFrame(a)

print(df.head())
