import sys
import random

output_count = int(sys.argv[1])

for i in range(output_count):
    print(f"http://example.com/item/{i} {random.randint(1, 1_000_000_000)}")
