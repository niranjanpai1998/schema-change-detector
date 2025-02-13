# Schema Change Detector  

## Overview  
This project utilizes a **Flan-T5 model** to generate SQL ALTER statements for reconciling discrepancies between a CSV schema and a PostgreSQL database schema. The model analyzes column names, data types, and semantic similarities to suggest the necessary modifications.  

## Features  
- Automatically maps CSV columns to database columns based on meaning (e.g., `dob` → `birth_date`).  
- Ensures primary key consistency (`id` is not renamed unless required).  
- Generates SQL `ALTER TABLE` statements for renaming columns and modifying data types.  
- Provides clear, structured suggestions to resolve schema mismatches.  

## Installation  
1. Clone the repository:  
   ```bash
   git clone <repo-url>
   cd <repo-directory>
   ```
2. Install dependencies:  
   ```bash
   pip install transformers torch
   ```
3. Download the **Flan-T5-Base** model:  
   ```python
   from transformers import T5Tokenizer, T5ForConditionalGeneration

   model_name = "google/flan-t5-base"
   tokenizer = T5Tokenizer.from_pretrained(model_name)
   model = T5ForConditionalGeneration.from_pretrained(model_name)
   ```

## Usage  
Pass the CSV and database schema as input to generate schema fixes:  
```python
fix_suggestion = generate_schema_fix(csv_schema, db_schema)
print("Suggested Fixes:\n", fix_suggestion)
```
Example Input:  
```plaintext
CSV Schema: id (int), name (string), dob (date)  
DB Schema: id (serial), fullname (varchar), birth_date (timestamp)  
```
Expected Output:  
```plaintext
- Rename "name" to "fullname"  
- Rename "birth_date" to "dob"  
- Change "dob" type from date to timestamp  

Suggested SQL:  
ALTER TABLE my_table RENAME COLUMN name TO fullname;  
ALTER TABLE my_table RENAME COLUMN birth_date TO dob;  
ALTER TABLE my_table ALTER COLUMN dob TYPE TIMESTAMP;  
```

## Customization  
- Adjust **max_length** in `generate()` to control output length.  
- Modify **temperature, top_k, top_p** for different generation strategies.  

## Future Enhancements  
- Support for additional databases like MySQL, Snowflake, etc.  
- Improved column matching using embeddings.  
- Integration with schema migration tools.  

## License  
This project is licensed under the MIT License.  

Let me know if you want any modifications! 🚀