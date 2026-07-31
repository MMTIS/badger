class RelationKeyCodec:
    def full_key_to_clazz_idx(full_key: bytes) -> tuple[bytes, bytes]:
        # Original implementation
        # full_int = int.from_bytes(full_key, 'little')
        # clazz_idx = (full_int >> 32).to_bytes(2, 'little')
        # key = (full_int & 0xFFFFFFFF).to_bytes(4, 'little')
        # return clazz_idx, key
        return full_key[4:6], full_key[:4]

    def full_key_to_clazz(full_key: bytes) -> bytes:
        return full_key[4:6]

    def full_key_to_idx(full_key: bytes) -> bytes:
        return full_key[:4]

    def get_fullkey_by_clazz_idx(idx: bytes, this_clazz_idx: bytes) -> bytes:
        # Original implementation
        # this_clazz_idx = self.clazz_idx[clazz]
        # full_key = ((int.from_bytes(this_clazz_idx, 'little') << 32) | key).to_bytes(8, 'little')
        return idx + this_clazz_idx.ljust(4, b'\x00')
