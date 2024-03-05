import sys
import random
import json


try:
    input_string = sys.argv[1]
    min_port = int(sys.argv[2])
    max_port = int(sys.argv[3])

    random.seed(input_string)
    port = random.randint(min_port, max_port)

    output = {"port": str(port)}
    print(json.dumps(output))
except Exception as e:
    print(f"Error: {str(e)}", file=sys.stderr)
    sys.exit(1)
