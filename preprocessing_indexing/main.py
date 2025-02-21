import boto3
import json
import re
import traceback
import time
import math
from var import SYNONYMS, STOP_WORDS
from collections import defaultdict

kendra = boto3.client( 'kendra', region_name='ap-southeast-2')


def update_index_metadata(index_id):
    """
    Updates the specified Kendra index with the given metadata configurations.

    Args:
        index_id (str): The unique identifier of the Kendra index to update.

    Returns:
        The response from the Kendra service after updating the index metadata.

    Raises:
        Exception: If there is an error updating the index metadata.
    """

    default_fields = [
        {
            "Name": "_last_updated_at",     # get fresh items
            "Type": "DATE_VALUE",
            "Relevance": {
                "Freshness": True,
                "Importance": 10
            }
        },
        {
            'Name': 'gender',
            'Type': 'STRING_VALUE',
            'Relevance': {
                'Importance': 10,
                # 'Duration': 'string',
                # 'RankOrder': 'ASCENDING'|'DESCENDING',
                'ValueImportanceMap': {
                    'male': 10,
                    'female':10,
                    'male female':8
                }
            },
            'Search': {
                'Facetable': True,
                'Searchable': True,
                'Displayable': True,
                'Sortable': True
            }
        },
        
        {
            'Name': 'product_type',    # important
            'Type': 'STRING_LIST_VALUE',
            # 'Relevance': {
            #     'Importance': 9,
            #     # 'Duration': 'string',
            #     # 'RankOrder': 'ASCENDING'|'DESCENDING',
            # },
            'Search': {
                'Facetable': True,
                'Searchable': True,
                'Displayable': True,
                'Sortable': False
            }
        },


        {
            'Name': 'occasion',    # important
            'Type': 'STRING_LIST_VALUE',
            # 'Relevance': {
            #     'Importance': 9,
            #     # 'Duration': 'string',
            #     # 'RankOrder': 'ASCENDING'|'DESCENDING',
            # },
            'Search': {
                'Facetable': True,
                'Searchable': True,
                'Displayable': True,
                'Sortable': False
            }
        },
        {
            'Name': 'brand',    # important
            'Type': 'STRING_VALUE',
            # 'Relevance': {
            #     'Importance': 5,
            #     # 'Duration': 'string',
            #     # 'RankOrder': 'ASCENDING'|'DESCENDING',
            # },
            'Search': {
                'Facetable': True,
                'Searchable': True,
                'Displayable': True,
                'Sortable': True
            }
        },
        {
            'Name': 'product_id',    # important
            'Type': 'STRING_VALUE',
            'Search': {
                'Facetable': False,
                'Searchable': False,
                'Displayable': True,
                'Sortable': True
            }
        },
        {
            'Name': 'category',    # important
            'Type': 'STRING_VALUE',
            #  'Relevance': {
            #     'Importance': 8,
            #     # 'Duration': 'string',
            #     # 'RankOrder': 'ASCENDING'|'DESCENDING',
            # },
            'Search': {
                'Facetable': True,
                'Searchable': True,
                'Displayable': True,
                'Sortable': False
            }
        },
        {
            'Name': 'color',    # good to have
            'Type': 'STRING_LIST_VALUE',
            # 'Relevance': {
            #     'Importance': 8,
            #     # 'Duration': 'string',
            #     # 'RankOrder': 'ASCENDING'|'DESCENDING',
            # },
            'Search': {
                'Facetable': True,
                'Searchable': True,
                'Displayable': True,
                'Sortable': False
            }
        },


        # Long values / Numerics
        {
            'Name': 'rating',
            'Type': 'LONG_VALUE',
            'Search': {
                'Facetable': True,
                'Searchable': False,
                'Displayable': True,
                'Sortable': True
            },
            # 'Relevance': {
            #     # 'Freshness': True|False,
            #     'Importance': 8,
            #     # 'Duration': 'string',
            #     'RankOrder': 'ASCENDING',
            # }
        },
        {
            'Name': 'final_price',
            'Type': 'LONG_VALUE',
            'Search': {
                'Facetable': True,
                'Searchable': False,
                'Displayable': True,
                'Sortable': True
            },
            # 'Relevance': {
            #     # 'Freshness': True|False,
            #     'Importance': 7,
            #     # 'Duration': 'string',
            #     'RankOrder': 'DESCENDING',
            # }
        },
        {
            'Name': 'discount',
            'Type': 'LONG_VALUE',
            'Search': {
                'Facetable': True,
                'Searchable': False,
                'Displayable': True,
                'Sortable': True
            },
            # 'Relevance': {
            #     # 'Freshness': True|False,
            #     'Importance': 6,
            #     # 'Duration': 'string',
            #     'RankOrder': 'ASCENDING',
            # }
        }
    ]

    # Update the Kendra index with the specified metadata configurations
    try:
        response = kendra.update_index(
            Id=index_id,
            DocumentMetadataConfigurationUpdates=default_fields
        )
        #print("Index metadata updated successfully.", response)
    except Exception as e:
        print("Error updating index metadata:", str(e))



def change_gender(txt):
    txt = txt.lower()  # Convert text to lowercase for case-insensitive matching
    # print("Gender text::" , txt)
    male_terms = ['male', 'men', 'boys', 'mens', 'boy',]
    female_terms = ['female', 'women', 'ladies', 'girls', 'womens', 'ladie', 'girl']

    # Create regex patterns for whole-word matching
    male_pattern = r'\b(?:' + '|'.join(male_terms) + r')\b'
    female_pattern = r'\b(?:' + '|'.join(female_terms) + r')\b'

    has_male = bool(re.search(male_pattern, txt))
    has_female = bool(re.search(female_pattern, txt))

    if has_male and has_female:
        return 'male female'
    elif has_male:
        return 'male'
    elif has_female:
        return 'female'
    return 'unknown'  # Return 'unknown' if no match

def remove_redundant_words(text):
    # Tokenize the text into phrases (assuming phrases are separated by commas)
    phrases = [phrase.strip() for phrase in text.split(',')]

    # Extract the main words from each phrase (e.g., "formal wear" -> ["formal", "wear"])
    phrase_components = [phrase.split() for phrase in phrases]

    # Group phrases by their last word (e.g., "wear")
    grouped_phrases = defaultdict(list)
    for components in phrase_components:
        if components:
            grouped_phrases[components[-1]].append(' '.join(components[:-1]))

    # Reconstruct phrases intelligently
    processed_phrases = []
    for ending_word, modifiers in grouped_phrases.items():
        if modifiers:
            # Combine modifiers and append the shared ending word
            combined_modifiers = ', '.join(sorted(modifiers))
            processed_phrases.append(f"{combined_modifiers} {ending_word}")
        else:
            processed_phrases.append(ending_word)

    # Join the processed phrases into a final text
    return ', '.join(processed_phrases)


# def preprocess_text(text):
#     if not isinstance(text, str):
#         # #print("The value is not a string.")
#         return text
#     text = text.lower()
#     for term, synonyms in SYNONYMS.items():
#         for synonym in synonyms:
#             text = re.sub(rf"\b{synonym}\b", term, text, flags=re.IGNORECASE)
#     # text = re.sub(r'[^a-zA-Z0-9\s-]', '', text)
#     text = re.sub(r'[^a-zA-Z0-9\s,-]', '', text)
#     words = text.split()
#     return " ".join(word for word in words if word not in STOP_WORDS) or " "


def preprocess_text(text):
    if not isinstance(text, str):
        return text

    text = text.lower()

    for term, synonyms in SYNONYMS.items():
        for synonym in synonyms:
            pattern = rf"\b{synonym}\b"
            text = re.sub(pattern, rf"{synonym}, {term}", text, flags=re.IGNORECASE)  # Keep both original and mapped word

    # text = re.sub(r'[^a-zA-Z0-9\s-]', '', text)
    text = re.sub(r'[^a-zA-Z0-9\s,-]', '', text)

    words = text.split()
    processed_text = " ".join(word for word in words if word not in STOP_WORDS) or " "

    return processed_text


def get_list(input_string):
    return [word.strip() for word in input_string.split(",") if word.strip()]

def clean_and_convert_to_number(input_string):
    if not isinstance(input_string, str):
        return math.ceil(input_string)
            
    # Define the characters to remove (excluding the decimal point '.')
    unwanted_chars = "$% "
    
    # Remove unwanted characters
    cleaned_string = ''.join(char for char in input_string if char not in unwanted_chars)
    
    # Convert to a float if it contains a decimal, otherwise to an int
    # if '.' in cleaned_string:
    # return float(cleaned_string)
    float_val = float(cleaned_string)
    return int(math.ceil(float_val))


def add_thesaurus_to_kendra(index_id, s3_path, role_arn, thesaurus_name, description=None):
    """
    Adds a thesaurus file from S3 to an Amazon Kendra index.

    :param index_id: str, The Kendra index ID to which the thesaurus will be added.
    :param s3_path: str, The S3 URI of the thesaurus file (e.g., s3://bucket-name/path/to/thesaurus.csv).
    :param role_arn: str, The ARN of the IAM role that gives Kendra permissions to access the S3 bucket.
    :param thesaurus_name: str, Name for the thesaurus in Kendra.
    :param description: str, Optional description for the thesaurus.
    :return: str, The ID of the created thesaurus.
    """

    try:
        # Create the thesaurus
        response = kendra.create_thesaurus(
            IndexId=index_id,
            Name=thesaurus_name,
            RoleArn=role_arn,
            SourceS3Path={
                'Bucket': s3_path.split('/')[2],  # Extract bucket name
                'Key': '/'.join(s3_path.split('/')[3:])  # Extract object key
            },
            Description=description if description else '',
        )
        
        thesaurus_id = response['Id']
        print(f"Thesaurus '{thesaurus_name}' added to index {index_id}. Thesaurus ID: {thesaurus_id}")
        return thesaurus_id
    except Exception as e:
        print(f"Error adding thesaurus: {str(e)}")
        raise


def normalize_item(item):
    def preprocess_optional_field(key, default=" "):
        # Safely preprocess text with a default value if key is missing
        # return preprocess_text(item.get(key, {}).get('S', default))
        if key in ['discount' , 'rating' , 'final_price']:
            if item.get(key, {}).get('S', None):
                return item.get(key, {}).get('S', None)
            elif item.get(key, {}).get('N', None):
                return  item.get(key, {}).get('N', None)
            else:
                return default
        else:
            if item.get(key, {}).get('S', None):
                return preprocess_text(item.get(key, {}).get('S', None))
            elif item.get(key, {}).get('N', None):
                return  preprocess_text(item.get(key, {}).get('N', None))
            else:
                return  default
    
    # Attributes
    # Extract and preprocess key fields
    gender = change_gender(preprocess_optional_field('gender', default="male female"))
    category = preprocess_optional_field('category', default="Unknown")
    brand = preprocess_optional_field('brand', default="Unknown")
    
    product_type = remove_redundant_words(preprocess_optional_field('product_type', default="Unknown"))
    color = remove_redundant_words(preprocess_optional_field('color', default="Unknown"))
    occasion = remove_redundant_words(preprocess_optional_field('occasion', default="Any Occasion"))

    discount = preprocess_optional_field('discount' , default=0)
    rating = preprocess_optional_field('rating' , default=0)
    final_price =  preprocess_optional_field('final_price' , default=0)
    description = preprocess_optional_field('description', default="Unknown")
    
    rating_int = int(float(rating)*10) or 1
    discount_int = clean_and_convert_to_number(discount)  or 0
    final_price_int = clean_and_convert_to_number(final_price) or 100
    
    # return (
    #     color, product_type, occasion, item.get('product_id', {}).get('S', "")
    # )
    
    kendra_product_chunk_list = get_chunks_for_product(get_list(color), get_list(product_type), get_list(occasion))
    
    normalized_items = []
    
    for index, chunk in enumerate(kendra_product_chunk_list):
        normalized_items.append({
        "Id": item.get('product_id', {}).get('S', "")+"__"+str(index),
        "Title": chunk,  # Construct title safely
        "Blob": chunk +" ,"+ description,  # Blob should only store the main content (description)
        "ContentType": "PLAIN_TEXT",
        "Attributes": [
            {
                "Key": "gender",
                "Value": {
                    "StringValue": gender
                }
            },
            {
                "Key": "category",
                "Value": {
                    "StringValue": category
                }
            },
            {
                "Key": "brand",
                "Value": {
                    "StringValue": brand
                }
            },
            {
                "Key": "product_id",
                "Value": {
                    "StringValue": item.get('product_id', {}).get('S', "")
                }
            },
            {
                "Key": "product_type",
                "Value": {
                    "StringListValue": get_list(product_type)[:10]
                }
            },
            {
                "Key": "color",
                "Value": {
                    "StringListValue": get_list(color)[:10]
                }
            },
            {
                "Key": "occasion",
                "Value": {
                    "StringListValue": get_list(occasion)[:10]
                }
            },
            
            
            
#             Long values
            
            
            {
                "Key": "rating",
                "Value": {
                    "LongValue": rating_int
                }
            },
            {
                "Key": "discount",
                "Value": {
                    "LongValue": discount_int
                }
            },
            {
                "Key": "final_price",
                "Value": {
                    "LongValue": final_price_int
                }
            }
        ]
    })
    return normalized_items


def get_chunks_for_product(color, product_type, occasion):
    cross_prod_occasion = doc_chunks(product_type, occasion)
    cross_color = doc_chunks(color, cross_prod_occasion)
    return cross_color
    

def doc_chunks(list1, list2):
    # Determine a dynamic window size based on list lengths
    min_length = min(len(list1), len(list2))
    
    if min_length == 0:  # Handle empty lists
        return []

    # # Use a fraction of the min length (e.g., 1/3) but at least 1
    # print("lengths::", len(list1), len(list2))
    window_size = min_length
    # window_size = max(1, min(4, min_length // 2))
    # print("window_size::", window_size)
    cross_product_list = []
    for i in range(0, max(1, len(list1) - window_size + 1)):
        for j in range(0, max(1, len(list2) - window_size + 1)):
            l1 = list1[i:i + window_size] if i + window_size <= len(list1) else list1[i:]
            l2 = list2[j:j + window_size] if j + window_size <= len(list2) else list2[j:]
            cross_product_list.append(" ".join(l1) + " " + " ".join(l2))
    
    return cross_product_list

s3_uri = ""
role_arn = ""
thesaurus_name = "product_color"
description = "A thesaurus file for custom synonyms of product types and colors."



def main(items, index_id):
    try:
        #print("Initial items (first 2):", items[:2])
        # Updating the attribute (index fields in the Kendra index to have gender and category before ingestion)
        update_index_metadata(index_id)
        time.sleep(10)

        try:
            thesaurus_id = add_thesaurus_to_kendra(index_id, s3_uri, role_arn, thesaurus_name, description)
            print(f"Created Thesaurus ID: {thesaurus_id}")
        except Exception as e:
            print("Failed adding theasaurus file to kendra" , e)
        
        documents = []
        for item in items:
            normalized_item = normalize_item(item)
            # print("normalized_item::len::" , len(normalized_item))
            documents.extend(normalized_item)
        
        print("Normalized documents (first 2):", documents[:2])
        
        
#         # Process documents in batches of 10
        batch_size = 10
        print("document size::" , len(documents))
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i+batch_size]
            # print(batch)
            response = kendra.batch_put_document(IndexId=index_id, Documents=batch)

            # Check response for successful or failed uploads
            successful = response.get('FailedDocuments', [])
            if successful:
                print(f"Batch {i//10 + 1}: Some documents failed to upload. Details: {successful}")
            else:
                print(f"Batch {i//10 + 1}: All documents uploaded successfully.")

        # return {"status": "completed"}
        print("length of documents::", len(documents))
        return documents
    except Exception:
        print("Error:", traceback.format_exc())
        raise
