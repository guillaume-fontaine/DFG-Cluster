import sys
import time
import random
import os

def read_int(filepath):
    with open(filepath, 'r') as f:
        content = f.read().strip()
        return int(content) if content else 1

def write_int(filepath, value):
    with open(filepath, 'w') as f:
        f.write(str(value))

def main():
    args = sys.argv[1:]
    
    if '-' not in args:
        print("Error: Separator '-' not found in arguments.")
        sys.exit(1)
        
    separator_index = args.index('-')
    sources = args[:separator_index]
    destinations = args[separator_index+1:]
    
    # Simulate processing time
    time.sleep(random.uniform(0.01, 0.02))
    
    # Logic MULT
    total_product = 1
    for src in sources:
        try:
            total_product *= read_int(src)
        except ValueError:
            print(f"Warning: Could not read integer from {src}")

    exponent = 1
    for dest in destinations:
        write_int(dest, total_product ** exponent)
        exponent += 1

if __name__ == "__main__":
    main()
