"""
Main Class/File that returns pandas-dataframe friendly array of dictionaries from elasticsearch agg. result
Algorithm walks recursively through the tree of aggregations, matches if it is a aggregation or bucket and saves result
to an array

Node = aggregation, column name
Branch = bucket, column value

PROJECT                                             <----- Node / Aggregation / Column Name
    - "FC":                                         <----- Branch / Term or Filter / Value
        CAMPAIGN_ID                                 <----- Node
            - "002"                                 <----- Branch
                MKT_REGISTRATIONS                   <----- Node
                    - "3"                           <----- Leaf
            - "003"
                MKT_REGISTRATIONS
                    - "1"
            - "004"
                MKT_REGISTRATIONS
                    - "12"



PROJECT CAMPAIGN_ID MKT_REGISTRATIONS
-------------------------------------
"FC"    "002"       3
"FC"    "003"       1
"FC"    "004"       12

Example:
    Lets say, you queried elasticsearch and have string result in "elasticsearch_result_body" variable,
    to create pandas dataframe from it, you call:
    >>> import pandas as pd, json, aggwalk
    >>> pd.DataFrame(aggwalk.tablify(json.loads(elasticsearch_result_body)["aggregation"]))

Todo:
    - Error Handling
    - Add possibility to return list of ordered dictionaries
"""


def has_buckets(sub_dict: dict) -> bool:
    """Helper function to make code more readable
    Check if current node has buckets (a.k.a contains internal-nodes), return boolean

    Args:
        sub_dict (dict): node

    Returns:
        bool: True if node has buckets, False if it has not
    """
    if isinstance(sub_dict, dict):
        if 'buckets' in sub_dict.keys():
            return True
    return False


def has_values(sub_dict: dict) -> bool:
    """Helper function to make code more readable
    Check if current node has values (a.k.a contains branches to leaf-nodes), return boolean

    Args:
        sub_dict (dict): current node

    Returns:
        bool: True if node has buckets, False if it has not
    """
    if isinstance(sub_dict, dict):
        if 'value' in sub_dict.keys():
            return True
    return False


def walk(sub_tree: dict, row: dict) -> dict:
    """Recursive function that walks 'aggregation map' from elasticsearch

    Args:
        sub_tree (dict): Current node with entire subsection of the tree
        row (dict, optional): (in)complete dictionary of a result, current line in the "table"

    Yields:
        dict: 1 row of a result in format: {"column1": "value1", "column2": "value2"}

    Todo:
        * key_as_string
        * test multiple leaves in one node

    """

    # go through all the junk in the tree, find relevant parts (buckets or values)
    for node in sub_tree:
        # does this node contain buckets?
        if has_buckets(sub_tree[node]) and len(sub_tree[node]["buckets"]) > 0:

            # are buckets terms?
            if isinstance(sub_tree[node]["buckets"], list):
                for branch in sub_tree[node]["buckets"]:
                    row.update({node: branch["key"]})
                    yield from walk(branch, row)

            # are buckets filters?
            elif isinstance(sub_tree[node]["buckets"], dict):
                for branch in sub_tree[node]["buckets"]:
                    row.update({node: branch})
                    yield from walk(sub_tree[node]["buckets"][branch], row)

        # does this node contain value?
        elif has_values(sub_tree[node]):
            yield {**row, **{node: sub_tree[node]["value"]}}


def tablify(agg_sub) -> list:
    """main point of the module
    Calls the algorithm to walk entire tree and transforms it to a key-valued rows

    Args:
        - agg_sub (dict): part of the elasticsearch result labeled 'aggregations'
    """
    return list(walk(agg_sub, {}))

