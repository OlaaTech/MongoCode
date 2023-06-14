import pandas as pd
import json

# load the json file
with open('input.json', 'r', encoding="utf8") as f:
    data = json.load(f)

# normalize the nested data and convert it to a pandas dataframe
df = pd.json_normalize(data)

# save the dataframe to a csv file
df.to_csv('output.csv', index=False)
