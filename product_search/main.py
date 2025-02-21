"""
Helper module for AWS Kendra operations.
Provides functionality for index management and product retrieval.
"""

import boto3
import time
from logging_config import logger
from collections import defaultdict
from rapidfuzz.fuzz import ratio, token_set_ratio, partial_ratio
from rapidfuzz.process import extract
from typing import List, Dict

# Initialize Kendra client
kendra = boto3.client('kendra', region_name='ap-southeast-2')
kendra_name = "product-desc"


def get_list(input_string):
    """Convert comma-separated string to list of stripped values."""
    return [word.strip() for word in input_string.split(",") if word.strip()]


def check_index_response(response):
    """Check index response and return matching index ID if found."""
    for index in response.get('IndexConfigurationSummaryItems', []):
        if index['Name'] == kendra_name:
            return index['Id']
    return None


def get_index_id_by_name(client, index_name):
    """Retrieve Kendra index ID by name."""
    try:
        response = client.list_indices()
        return check_index_response(response)
    except Exception as e:
        logger.error(f"Error listing indices: {e}")
        return None


kendra_index_id = get_index_id_by_name(kendra, kendra_name)


def create_equals_filter(key, value):
    """Create an equals filter for Kendra."""
    return {
        'EqualsTo': {
            'Key': key.lower(),
            'Value': {'StringValue': value}
        }
    }


def create_contains_filter(key, value):
    """Create a contains filter for Kendra."""
    return {
        'ContainsAny': {
            'Key': key.lower(),
            'Value': {'StringListValue': get_list(value)}
        }
    }


def create_gender_base_filter(gender=''):
    """Create base gender filter."""
    if not gender:
        return create_equals_filter('gender', 'male female')

    return {
        'OrAllFilters': [
            create_equals_filter('gender', gender),
            create_equals_filter('gender', 'male female')
        ]
    }


def create_single_attribute_filter(key, value):
    """Create filter for a single attribute."""
    if not value:
        return None

    if key == "std_product_type":
        return create_contains_filter(key, value)
    
    if key == "category":
        return create_equals_filter(key, value)

    # return create_equals_filter(key, value)
    return None


def create_dynamic_filters(extracted_attributes):
    """Create filters from extracted attributes."""
    filters = []
    if not extracted_attributes:
        return filters

    for key, value in extracted_attributes.items():
        filter_item = create_single_attribute_filter(key, value)
        if filter_item:
            filters.append(filter_item)

    return filters


def combine_attribute_filters(gender_filter, dynamic_filters):
    """Combine all attribute filters."""
    if not dynamic_filters:
        return gender_filter

    return {
        'AndAllFilters': [
            gender_filter,
            {'OrAllFilters': dynamic_filters}
        ]
    }


def create_query_params(index_id, query_text, attribute_filter, top_n , facets = None):
    facets_list = []
    for facet in facets:
        facets_list.append({
            'DocumentAttributeKey': facet,
            'MaxResults': 10
        })
    
    """Create parameters for Kendra query."""
    return {
        "IndexId": index_id,
        "QueryText": query_text,
        "AttributeFilter": attribute_filter,
        "PageSize": top_n,
        "PageNumber": 1,
        "Facets": facets_list,
        "QueryResultTypeFilter": 'DOCUMENT'
    }


def process_single_result(item):
    """Process a single result item."""
    try:
        if item.get('DocumentId') and item.get("ScoreAttributes" , {}).get("ScoreConfidence" , "") in ["VERY_HIGH" , "HIGH", "MEDIUM"]:
            print("Score Confidence",item.get("ScoreAttributes" , {}).get("ScoreConfidence" , ""))
            return item.get('DocumentId')
    except Exception as e:
        logger.error(f"Error parsing content: {e}")
    return None


def extract_product_ids(results):
    """Extract product IDs from results."""
    product_ids = []
    print("results" , len(results))
    for item in results:
        product_id = process_single_result(item)
        if product_id:
            product_ids.append(product_id)
    return product_ids


def extract_product_details(kendra_res):
    """Extract product details from results."""
    query_id = kendra_res.get('QueryId')
    product_details = []
    for item in kendra_res["ResultItems"]:
        product_id = process_single_result(item)
        if product_id:
            product_details.append({
                'product_id': product_id,
                'result_id': item["Id"]
            })
    return {
        "product_details": product_details,
        "query_id": query_id
    }


def validate_index():
    """Validate Kendra index existence."""
    if not kendra_index_id:
        raise Exception(
            f"Couldn't find the kendra_id for kendra name: {kendra_name}"
        )


def get_query_text(user_message, extracted_attributes):
    """Get query text from message or attributes."""
    if extracted_attributes and "query_text" in extracted_attributes:
        return extracted_attributes["query_text"]
    return user_message


def get_doc_title(document):
    return document.get("DocumentTitle", {}).get("Text" , "")

def get_doc_with_title(documents):
    res_docs = []
    for doc in documents:
        product_id = doc.get("DocumentId" , "")
        if(product_id):
            doc_title = get_doc_title(doc)
            res_docs.append({
                "product_id":product_id,
                "doc_title":doc_title
            })
    return res_docs


def group_and_combine(products):
    """
    Groups products by their actual ID and combines their titles in order of the sequence number.

    Args:
        products (list): List of dictionaries with "product_id" and "doc_title" keys.

    Returns:
        dict: A dictionary where the key is the actual ID and the value is the combined title.
    """
    grouped_products = defaultdict(list)

    # Split product_id and group by actual ID
    for product in products:
        if len(product["product_id"].split("__"))==1:
            actual_id = product["product_id"].split("__")[0]
            sequence = "0"
        else:
            actual_id, sequence = product["product_id"].split("__")
        grouped_products[actual_id].append((int(sequence), product["doc_title"]))

    # Combine titles in order of sequence
    combined_results = {}
    for actual_id, items in grouped_products.items():
        # Sort by sequence number
        sorted_items = sorted(items, key=lambda x: x[0])
        # Combine titles
        combined_title = " ".join(title for _, title in sorted_items)
        combined_results[actual_id] = combined_title

    return combined_results


def retrieve_kendra(
    query_text,
    extracted_attributes=None,
    gender='',
    top_n=5
):
    """
    Retrieve products from Kendra index.

    Args:
        query_text: User's search query
        extracted_attributes: Dictionary of additional search attributes
        gender: Gender filter value
        top_n: Number of results to return

    Returns:
        List of product IDs
        kendra_query params
    """
    start_time = time.time()

    try:
        validate_index()

        start_time = time.time()
        query_text = get_query_text(query_text, extracted_attributes)
        gender_filter = create_gender_base_filter(gender)
        dynamic_filters = create_dynamic_filters(extracted_attributes)
        attribute_filter = combine_attribute_filters(
            gender_filter,
            dynamic_filters
        )

        logger.info("user_gender: %s", gender)
        logger.info("attribute_filter: %s", attribute_filter)
        logger.info("query_text: %s", query_text)

    
        kendra_params = create_query_params(
            kendra_index_id,
            query_text,
            attribute_filter,
            top_n,
            ['category', 'brand']
        )

        response = kendra.query(**kendra_params)
        results = response.get('ResultItems', [])
        
        
        # fuzzywuzzy
        doc_with_titles = get_doc_with_title(results)
        grouped_titles = group_and_combine(doc_with_titles)
        
#         product_ids = extract_product_ids(results)
        
        # product_details_res = extract_product_details(response)
        # logger.info("product_details: %s", product_details_res)
        
        logger.info("Kendra retrieve time: %s", time.time() - start_time)
        print("Kendra retrieve::",grouped_titles )
        return grouped_titles, kendra_params

    except Exception as e:
        logger.error("Error in retrieve_kendra: %s", str(e))
        raise




def fuzzy_search(query: str, products: Dict, threshold: int = 20) -> List[Dict]:
    """
    Perform fuzzy search on a product database.
    
    Args:
        query (str): Search query.
        products (List[Dict]): List of product dictionaries.
        threshold (int): Minimum similarity score for a match.
        
    Returns:
        List[Dict]: List of matched products with scores.
    """
    results = []
    for product_id , combined_title in products.items():
        # Concatenate all product fields for comprehensive matching
        combined_text = combined_title
        similarity_score = partial_ratio(query.lower(), combined_text.lower())
        # print("-----------------------------------------------")
        # print("product_details:" , combined_title)
        # print("similarity_score::" , similarity_score)
        # print("-----------------------------------------------")
        print("Fuzzy results::",query, ":", combined_text , similarity_score)
        if similarity_score >= threshold:
            results.append({"product_title": combined_title, "score": similarity_score, "product_id":product_id})
    
    # Sort results by similarity score in descending order
    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def weight_results(prod_type, prod_color, prod_occasion):
    prod_type_weight = 3
    prod_color_weight = 2
    prod_occasion_weight = 1

    # Calculate the weighted sum
    prod = (prod_type_weight * prod_type) + (prod_color_weight * prod_color) + (prod_occasion_weight * prod_occasion)

    # Determine the divisor based on nonzero parameters
    total_weight = (
        (prod_type_weight if prod_type else 0) +
        (prod_color_weight if prod_color else 0) +
        (prod_occasion_weight if prod_occasion else 0)
    )

    # Normalize only if total_weight is nonzero
    return prod / (total_weight * 100) if total_weight else 0


def match_products_partial(list1, list2, list3):
    """
    Matches product_id across three lists and returns a combined list of dictionaries.
    Includes product_ids found in any of the lists, even if not common across all.
    
    Args:
        list1 (list): First list of dictionaries.
        list2 (list): Second list of dictionaries.
        list3 (list): Third list of dictionaries.
    
    Returns:
        list: A list of dictionaries in the specified format.
    """
    # Create dictionaries with product_id as the key for quick lookup
    dict1 = {item["product_id"]: item for item in list1}
    dict2 = {item["product_id"]: item for item in list2}
    dict3 = {item["product_id"]: item for item in list3}

    # Combine all product_ids from the three lists
    all_product_ids = set(dict1.keys()) | set(dict2.keys()) | set(dict3.keys())

    # Create the final result list
    final_list = []
    for product_id in all_product_ids:
        final_list.append({
            "prod_type": dict1.get(product_id, {}),
            "prod_color": dict2.get(product_id, {}),
            "product_occasion": dict3.get(product_id, {}),
            "product_id": product_id
        })
    
    return final_list


def weight_list_of_product_results(product_results):
    final_res = []
    for pro_res in product_results:
        product_id = pro_res.get("product_id")
        res = weight_results(pro_res.get("prod_type").get("score", 0),
                            pro_res.get("prod_color").get("score", 0),
                            pro_res.get("product_occasion").get("score", 0)
                            )
        final_res.append({
            "product_id":product_id,
            "total_weight": res,
            "overall_score":res*100,
            "prod_type": pro_res.get("prod_type") if pro_res.get("prod_type") else False,
            "prod_color":  pro_res.get("prod_color") if pro_res.get("prod_color") else False,
            "product_occasion": pro_res.get("product_occasion") if pro_res.get("product_occasion") else False,
            
        })
    return final_res


def product_search(user_message, product_attr, user_gender):
    print("==================semantic_product_search====================")
    print("--------product_attr---------", product_attr)
    product_type = product_attr.get("product_type") or ""
    product_color = product_attr.get("product_color") or ""
    product_occasion = product_attr.get("product_occasion") or ""
    product_desc = product_attr.get("product_desc") or ""
    kendra_query = product_type+" "+product_color+" "+product_occasion+" "+product_desc
    kendra_res, kendra_params = retrieve_kendra( kendra_query, {}, user_gender, 20)
    grouped_titles = kendra_res
    
#   fuzzy search
    p_t_res, p_c_res, p_o_res = [], [], []
    if product_type:
        p_t_res = fuzzy_search(product_type , grouped_titles, 0) # o(m*n)
    if product_color:
        p_c_res = fuzzy_search(product_color, grouped_titles, 0)
    if product_occasion:
        p_o_res = fuzzy_search(product_occasion, grouped_titles, 0)
    
    
    
#   Matching product partially based on color, prod_type and occaasion
    matched_prod_list = match_products_partial(p_t_res, p_c_res, p_o_res)
    
#   Adding variable weights to important fields
    weighted_results = weight_list_of_product_results(matched_prod_list)

# sorting the results    
    results = sorted(weighted_results, key=lambda x: x["total_weight"], reverse=True)
    product_ids = [res["product_id"] for res in results]
    
    final_results = []
    final_product_ids = []
    for res in results:
        if product_type:
            if res.get("prod_type"):
                final_results.append(res.copy())
                final_product_ids.append(res["product_id"])
        else:
            final_results.append(res.copy())
            final_product_ids.append(res["product_id"])
    print("######################## final_results from search", final_results)
    return final_product_ids, kendra_params, final_results


if __name__ == "__main__":
    results = product_search("red dress", 
                                {"product_type":"dress", 
                                "product_color":"red", 
                                "product_occasion":"party",
                                "product_desc":"backless"
                                }, 
                "male")
    print(results)