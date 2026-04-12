# # from pydantic import BaseModel
# # import pathlib

# # class User(BaseModel):
# #     id: int
# #     name: str
# #     email: str

# # data_dict = {
# #     "id": 1,
# #     "name": "Alice",
# #     "email": "alice@example.com"
# # }

# # user = User(data_dict)   # unpack dictionary into fields
# # print(user)
# # # id=1 name='Alice' email='alice@example.com'


# import pathlib

# from pydantic import BaseModel, EmailStr, PositiveInt
# import json 

# class Person(BaseModel):
#     name: str
#     age: PositiveInt
#     email: EmailStr


# json_string = pathlib.Path('/home/omarben/workspace/airflow_spark_project_datawarehouse_nybike/src/jobs/pyspark/json_.json').read_text()
# dict_from_json = json.loads(json_string)
# person = Person.model_validate_json(json.dumps(dict_from_json))
# # print(repr(person))
# # print(dict_from_json)
# #> Person(name='John Doe', age=30, email='john@example.com')





# from abc import ABC, abstractmethod
# from typing import Dict, Any

# # Abstract base class
# class Animal(ABC):
#     def __init__(self, name: str, age: int):
#         self.name = name
#         self.age = age
    
#     @abstractmethod
#     def make_sound(self) -> str:
#         pass

# # Concrete implementations
# class Dog(Animal):
#     def __init__(self, name: str, age: int, breed: str = "Mixed"):
#         super().__init__(name, age)
#         self.breed = breed
    
#     def make_sound(self) -> str:
#         return "Woof!"

# class Cat(Animal):
#     def __init__(self, name: str, age: int, color: str = "Tabby"):
#         super().__init__(name, age)
#         self.color = color
    
#     def make_sound(self) -> str:
#         return "Meow!"

# # Simple factory function
# def create_animal(animal_type: str, **kwargs) -> Animal:
#     if animal_type.lower() == "dog":
#         return Dog(**kwargs)
#     elif animal_type.lower() == "cat":
#         return Cat(**kwargs)
#     else:
#         raise ValueError(f"Unknown animal type: {animal_type}")

# # Usage
# dog = create_animal("dog", name="Buddy", age=3, breed="Golden Retriever")
# cat = create_animal("cat", name="Whiskers", age=2, color="Black")

# print(f"{dog.name} says {dog.make_sound()} and is a {dog.breed}")
# print(f"{cat.name} says {cat.make_sound()} and is a {cat.color}")





config={
    'path':'s3://bucket-raw-data/raw_data_nybike',
    'reader':'ReaderCsvFromS3Storage'
}

if 's3://' in config['path']:
    print("It's an S3 path")
else:
    print("It's a local path")