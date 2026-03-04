from openai import OpenAI

client = OpenAI()

response = client.responses.create(
    model="gpt-5",
    input="Say: ChatGPT connected successfully"
)

print(response.output_text)