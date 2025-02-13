from transformers import T5Tokenizer, T5ForConditionalGeneration

model_name = "google/flan-t5-base"
tokenizer = T5Tokenizer.from_pretrained(model_name)
model = T5ForConditionalGeneration.from_pretrained(model_name)

def generate_schema_fix(csv_schema, db_schema):
    prompt = f"""
    You are a database schema migration expert.  
    Compare the CSV schema and PostgreSQL schema **column by column**.  

    Rules:
    - **Do NOT rename "id" unless the database has a different primary key.**
    - **Map columns by meaning (e.g., "dob" should match "birth_date").**
    - **Suggest SQL ALTER statements for required changes.**

    Example:
    CSV Schema: id (int), fullname (string), dob (date)  
    DB Schema: id (serial), name (varchar), birth_date (timestamp)  

    Output:
    - Rename "name" to "fullname"
    - Rename "birth_date" to "dob"
    - Change "dob" type from date to timestamp

    Suggested SQL:
    ALTER TABLE my_table RENAME COLUMN name TO fullname;
    ALTER TABLE my_table RENAME COLUMN birth_date TO dob;
    ALTER TABLE my_table ALTER COLUMN dob TYPE TIMESTAMP;
    ALTER TABLE my_table ALTER COLUMN id TYPE SERIAL;

    Now process the following schema:
    
    CSV Schema: {csv_schema}  
    DB Schema: {db_schema}  

    Suggested Fixes:
    """

    inputs = tokenizer(prompt, return_tensors="pt")
    outputs = model.generate(**inputs, max_length=300)
    # outputs = model.generate(**inputs, max_length=300, temperature=0.7, top_k=40, top_p=0.9)
    response = tokenizer.decode(outputs[0], skip_special_tokens=True)

    return response


csv_schema = "id (int), name (string), dob (date)"
db_schema = "id (serial), fullname (varchar), birth_date (timestamp)"

fix_suggestion = generate_schema_fix(csv_schema, db_schema)
print("Suggested Fixes:\n", fix_suggestion)
