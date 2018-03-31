
# Replace environment variable in config/vm.args
export RELX_REPLACE_OS_VARS=true

export NODE_NAME="node@`hostname -i`"

echo "Starting $NODE_NAME"

# Run the application
/artifacts/ekvs/bin/ekvs foreground
