import torch
from transformers import phi3

class TextDataloader:
    def __init__(self, dataset:str):
        self.dataset = dataset

