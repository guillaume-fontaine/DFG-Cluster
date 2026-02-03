import sys
import time
import random
import hashlib

def read_content(filepath):
    with open(filepath, 'r') as f:
        return f.read()

def write_content(filepath, value):
    # Convertir la valeur hexadécimale en décimale
    decimal_value = str(int(value, 16))
    with open(filepath, 'w') as f:
        f.write(decimal_value)

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
    
    # Logic HASH
    combined_content = ""
    for src in sources:
        combined_content += read_content(src)

    current_hash = hashlib.sha1(combined_content.encode('utf-8')).hexdigest()
    
    for dest in destinations:
        write_content(dest, current_hash)
        current_hash = hashlib.sha1(current_hash.encode('utf-8')).hexdigest()

if __name__ == "__main__":
    main()
