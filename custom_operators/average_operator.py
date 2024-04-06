from airflow.models.baseoperator import BaseOperator

class AverageOperator(BaseOperator):
    def __init__(self, data: list, **kwargs) -> None:
        super().__init__(**kwargs)
        self.data = data
    
    def execute(self, context):
        result = sum(self.data) / len(self.data)
        print(f"average result: {result}")

        return result