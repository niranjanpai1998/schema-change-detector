import difflib
from transformers import T5Tokenizer, T5ForConditionalGeneration

model_name = "google/flan-t5-base"
tokenizer = T5Tokenizer.from_pretrained(model_name)
model = T5ForConditionalGeneration.from_pretrained(model_name)


def match_columns(csv_schema, db_schema):
    """
    Matches CSV schema columns with DB schema columns using similarity scoring.
    """
    csv_columns = [col.split()[0] for col in csv_schema.split(", ")]
    db_columns = [col.split()[0] for col in db_schema.split(", ")]

    matches = []
    for csv_col in csv_columns:
        best_match = difflib.get_close_matches(csv_col, db_columns, n=1, cutoff=0.6)
        if best_match:
            matches.append((csv_col, best_match[0])) 
        else:
            matches.append((csv_col, None)) 
    
    return matches

def generate_schema_fix(csv_schema, db_schema):
    column_mappings = match_columns(csv_schema, db_schema)

    prompt = "Identify schema mismatches and suggest SQL fixes.\n\n"
    for csv_col, db_col in column_mappings:
        if db_col and csv_col != db_col:
            prompt += f'- Rename "{csv_col}" to "{db_col}"\n'
        elif not db_col:
            prompt += f'- Add new column "{csv_col}"\n'

    prompt += "\nSuggest SQL ALTER TABLE statements."

    print(prompt)

    inputs = tokenizer(prompt, return_tensors="pt")
    outputs = model.generate(**inputs, max_length=300)
    response = tokenizer.decode(outputs[0], skip_special_tokens=True)

    return response

csv_schema = "id (int), name (string), dob (date)"
db_schema = "id (serial), fullname (varchar), birth_date (timestamp)"

fix_suggestion = generate_schema_fix(csv_schema, db_schema)
print("Suggested Fixes:\n", fix_suggestion)