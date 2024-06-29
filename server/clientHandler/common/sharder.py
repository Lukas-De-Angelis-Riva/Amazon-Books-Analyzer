import hashlib


def shard(text, n) -> int:
    h = hashlib.sha256(text.encode()).hexdigest()
    return int(h[-2:], 16) % n
