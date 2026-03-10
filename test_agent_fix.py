import asyncio
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

class Config:
    BANNER = "PG AI Query Engine v3.2.0"
    MODEL = "openai" # Map to Pollinations supported model
    BASE_URL = "https://text.pollinations.ai/v1"

class SQLResponse(BaseModel):
    sql: str
    explanation: str

llm = OpenAIChatModel(
    model_name=Config.MODEL,
    provider=OpenAIProvider(
        base_url=Config.BASE_URL,
        api_key='keyless'
    )
)

agent = Agent(
    llm,
    output_type=SQLResponse,
    system_prompt="Return a SQL query for the user."
)

async def test():
    print("Running agent...")
    try:
        result = await agent.run("SELECT 1")
        print(f"Result type: {type(result)}")
        print(f"Result output: {result.output}")
        print(f"Result output type: {type(result.output)}")
        if isinstance(result.output, SQLResponse):
            print("SUCCESS: Result.output is SQLResponse")
        else:
            print("FAILURE: Result.output is NOT SQLResponse")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test())
