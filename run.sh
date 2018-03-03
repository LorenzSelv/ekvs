
# Replace environment variable in config/vm.args
export RELX_REPLACE_OS_VARS=true

export NODE_NAME="node@`hostname -i`"

echo "Starting $NODE_NAME"

# Run the application
/artifacts/lab4kvs/bin/lab4kvs foreground
