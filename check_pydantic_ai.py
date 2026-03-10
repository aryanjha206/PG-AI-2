import pydantic_ai
import inspect
from pydantic_ai import Agent

print(f"Pydantic AI Version: {pydantic_ai.__version__}")
print("Agent __init__ signature:")
sig = inspect.signature(Agent.__init__)
print(sig)
for name, param in sig.parameters.items():
    print(f"  {name}: {param.kind}")

import pydantic_ai.agent
print(f"File: {pydantic_ai.agent.__file__}")
