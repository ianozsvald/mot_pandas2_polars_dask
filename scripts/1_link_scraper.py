#!/usr/bin/env python
# coding: utf-8

import pandas as pd

dfs = pd.read_html(
    "https://www.data.gov.uk/dataset/e3939ef8-30c7-4ca8-9c7c-ad9475cc9b2f/anonymised-mot-tests-and-results",
    extract_links="body",
)

data_links_df = pd.DataFrame.from_records(
    dfs[0]["Link to the data"], columns=["text", "link"]
).sort_values(by="text", ignore_index=True)
print(data_links_df)

data_links_df.link.to_csv("data/data_links.csv", header=False, index=False)

supporting_documents_df = pd.DataFrame.from_records(
    dfs[1]["Link to the document"], columns=["text", "link"]
).sort_values(by="text", ignore_index=True)
print(supporting_documents_df)

supporting_documents_df.link.to_csv(
    "data/supporting_document_links.csv", header=False, index=False
)
