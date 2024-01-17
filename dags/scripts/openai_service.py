from openai import OpenAI

class OpenAIService:
    
    def __init__(self, key:str, model:str="gpt-3.5-turbo", token:int=150, temp:float=0.7):
        self.client = OpenAI(api_key=key)
        self.model = model
        self.token = token
        self.temp = temp
        self.prompt:str

    def set_prompt(self, text:str):
        self.prompt = text

    def get_response(self):
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "user", "content": self.prompt}
            ],
            temperature=self.temp,
            max_tokens=self.token
        )
        return response.choices[0].message.content

