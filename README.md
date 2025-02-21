# Building a Smarter Product Search: Combining Semantic Search, Fuzzy Matching, and Synonyms

In today’s competitive e-commerce landscape, delivering an exceptional product search experience is critical. Customers expect to find what they’re looking for quickly and effortlessly, even when their queries are imprecise or misspelled.

This article explores the development of a sophisticated hybrid product search engine that leverages **semantic search, fuzzy matching, and a robust synonym system** to deliver highly relevant results. We’ll walk through a practical implementation using **AWS Kendra, RapidFuzz, and a custom data preprocessing and indexing strategy.**

---

## The Challenge: Moving Beyond Keyword Search
Traditional keyword-based search engines often fall short when handling the nuances of language. Synonyms, misspellings, and variations in phrasing can lead to missed or irrelevant results.

For example, a customer searching for a **"crimson party dress"** might not see results for **"red evening gown"** or **"burgundy cocktail dress."** This is where **semantic search and fuzzy matching** become indispensable.

Our product catalog includes attributes such as:
- **Color:** Specifies the product’s color.
- **Product Type:** Defines the category (e.g., Shirt, Jeans).
- **Occasion:** Indicates the suitable occasion (e.g., Casual, Formal, Party).
- **Product Description:** Contains additional details like neckline, material, and other product-specific information.

---

## The Solution: A Hybrid Approach with Synonyms
Our hybrid search engine combines the strengths of **semantic search, fuzzy matching, and a comprehensive synonym system**. We use **AWS Kendra** for semantic understanding, **RapidFuzz** for handling variations in user input, and a **custom preprocessing step** to leverage synonyms during indexing.

### 1. Preprocessing and Synonym Expansion: Laying the Foundation
Before indexing product data in Kendra, we preprocess product descriptions to expand color and other attribute information using a custom **synonym dictionary**.

For instance, a product description containing **"burgundy dress"** is expanded to **"burgundy, red dress."** This ensures that both the specific shade (**"burgundy"**) and the broader color category (**"red"**) are indexed, enabling retrieval when a user searches for either term.

#### Leveraging Amazon Kendra’s Built-in Thesaurus
Amazon Kendra’s built-in **thesaurus feature** allows us to define synonym mappings in a structured format. By uploading a **UTF-8-encoded file** containing synonym lists in the **Solr synonym format**, Kendra can automatically recognize and apply synonyms at query time.

For example, if a user searches for **"burgandi"** (a common misspelling of **"burgundy"**), Kendra retrieves results for both **"burgandi"** and the broader category **"red."**

This hybrid approach ensures users find the most relevant products, **regardless of variations in terminology, spelling, or specificity.**

---

### 2. Metadata Chunking Strategy: Granular Indexing
To optimize Kendra’s retrieval capabilities, we employ a **metadata chunking strategy**. Instead of indexing the entire product description as a single unit, we break it down into **smaller, focused chunks.**

Each chunk combines key product attributes (**Color, Product Type, Occasion, etc.**) with a relevant portion of the description.

#### Benefits of Chunking in Kendra Retrieval:
- **Enhanced Matching:** Multiple indexed chunks enable retrieval even with partial query matches.
- **Improved Recall:** Distinct attribute combinations in metadata chunks surface more relevant products.
- **Semantic Understanding:** Intent-based searches yield relevant products beyond keyword matching.

---

### 3. Kendra Retrieval: Semantic Powerhouse
AWS Kendra performs the heavy lifting of **semantic search**. Thanks to **synonym expansion** during preprocessing and **granular metadata chunks**, Kendra understands the intent behind user queries and retrieves highly relevant results.

For example, a search for **"crimson dress"** will return products indexed with both **"crimson"** and **"red"** in their descriptions.

---

### 4. Fuzzy Matching: Handling Imperfections
**RapidFuzz** complements Kendra’s semantic search by handling **variations in user input**. We use **partial_ratio** to calculate the similarity between the user’s query and the product titles retrieved from Kendra.

This ensures that products are surfaced even if the user **misspells a word** or uses **slightly different phrasing**.

---

### 5. Combining and Ranking: The Best of Both Worlds
Results from **Kendra (semantic search)** and **RapidFuzz (fuzzy matching)** are combined and ranked using a **weighted scoring system**.

#### The process involves:
1. Identifying products that match any attribute (**product type, color, or occasion**) through **fuzzy search**.
2. Assigning a **weighted score** based on the relevance of these attributes, with **product type given the highest priority**.
3. Sorting products in descending order of their **total weighted scores** to ensure the most relevant results appear first.

---

## Example Workflow

### **User Query:**
A user searches for **"blue formal shirt for wedding."**

### **Query Processing:**
The **LLM** parses the query, identifying key attributes:
- **Product Type:** "shirt"
- **Color:** "blue"
- **Occasion:** "wedding"
- **Style Cues:** "formal"

### **Semantic Search (Kendra):**
- A semantically rich query is sent to **Kendra**.
- **Synonym expansion** ensures products indexed with **"blue," "azure," or "cerulean"** are retrieved.
- **Chunking strategy** ensures products are retrieved even if only some attributes match.

### **Result Consolidation:**
- Kendra returns **"chunks"** of product data.
- These chunks are **grouped by product ID** to create a complete product view.

### **Fuzzy Matching:**
- Handles variations in spelling or phrasing, ensuring **"blue shirt"** or **"formal blue shirts"** still match.

### **Weighted Ranking:**
- Products are ranked based on a **weighted score**, with matches on key attributes like **product type** receiving **higher weights**.

### **Filtering:**
- The system applies filters based on extracted attributes, ensuring only **relevant products** (e.g., shirts) are included.

### **Final Results:**
- The user sees a **ranked list** of **blue formal shirts** suitable for a **wedding**, with the **most relevant products** displayed first.

---

## Conclusion
By combining **semantic search, fuzzy matching, and a robust synonym system**, we’ve created a **product search engine** that understands **user intent** and delivers **highly relevant results**. **Preprocessing** and **metadata chunking** significantly enhance the effectiveness of **Kendra and RapidFuzz**.

---

## Links
- **[GitHub](https://github.com/tinkukalluri/Hybrid_product_search)**
- **[LinkedIn](https://www.linkedin.com/in/abhinandan-kalluri/)**
