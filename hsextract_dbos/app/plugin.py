import asyncio
import logging
import redpanda_connect

@redpanda_connect.processor
def transform_message(msg: redpanda_connect.Message) -> redpanda_connect.Message:
    # Your transformation logic here
    msg.payload = msg.payload.upper()
    return msg

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(redpanda_connect.processor_main(transform_message))