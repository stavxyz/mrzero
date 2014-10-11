#!/python
import sys

try:
    with open('/dev/out/reducer', 'a') as f:
        f.write(open('/dev/input', 'r').read())
except Exception as err:
    sys.stderr.write(str(err))
