import logging
from langchain_community.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

# Set up logging (can be placed in your main app file to avoid duplication)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/langchain_agent.log"),
        logging.StreamHandler()
    ]
)


def build_agent(merged_meta, openai_api_key):
    logging.info("Building SQL agent with provided metadata")

    # Create a compact description of the schema for the system-level context
    schema_description = "\n".join(
        [f"{table}: {', '.join([col for col, _ in cols])}" for table,
         cols in merged_meta.items()]
    )
    logging.debug(f"Schema description:\n{schema_description}")

    # Create prompt
    prompt = PromptTemplate(
        input_variables=["question"],
        template=f"""
You are a Snowflake SQL expert. Your job is to write correct and optimized SQL queries based on the user's request.

Here is the database schema (only include relevant tables and columns):

{schema_description}

Guidelines:
- Only use tables and columns from the schema.
- Ensure that the SQL query is efficient and performs well on large datasets.
- Use proper aliases for tables and avoid unnecessary subqueries.

User Question:
{{question}}

Provide only the SQL query.
"""
    )
    logging.info("Prompt template created successfully")

    # Instantiate LLM and chain
    llm = OpenAI(temperature=0, openai_api_key=openai_api_key)
    chain = LLMChain(llm=llm, prompt=prompt)
    logging.info("LLMChain initialized")

    # Return agent function with logging
    def agent_fn(question):
        logging.info(f"Received question: {question}")
        sql_query = chain.run({"question": question})
        logging.info(f"Generated SQL: {sql_query}")
        return sql_query

    return agent_fn
