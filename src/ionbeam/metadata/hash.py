import hashlib


def id_hash(x):
    hash_object = hashlib.sha256(x.encode())
    full_hash = hash_object.hexdigest()
    return full_hash[:16]