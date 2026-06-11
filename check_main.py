import re

with open('main.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if '@app.' in line or 'async def' in line or 'def ' in line:
        print(f"{i+1}: {line.strip()}")
