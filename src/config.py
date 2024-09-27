import os

# Redis settings
redis_host = os.getenv("REDIS_HOST","localhost")
redis_port = os.getenv("REDIS_PORT", 6379)

# Channels, streams, and key names
pubsub_channel = os.getenv("PUBSUB_CHANNEL","messages:published")
stream_name = os.getenv("STREAM_NAME","messages:processed")
stats_name = os.getenv("STATS_NAME","consumer:stats")
lock_name = os.getenv("LOCK_NAME","consumer:lock")
consumer_ids = os.getenv("CONSUMER_IDS","consumer:ids")

# Consumer settings
consumers_group_size = os.getenv("GROUP_SIZE",100)

# Consumer manager settings
consumer_manager_ttl = float(os.getenv("CONSUMER_MANAGER_TTL", 60))
consumer_manager_interval = os.getenv("CONSUMER_MANAGER_INTERVAL", 10)